package s3store

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/davidbirdsong/roster"
)

// MarkerPoller implements roster.HintSource by polling the marker object and emitting a HintRescan whenever its
// ETag changes. The marker names no owner, so the consumer's answer is always to read a snapshot, which is cheap
// because only objects whose ETag changed are fetched. It needs no queue or fan out: each consumer polls on its
// own, so consumers can come and go freely, and a poll is one HEAD request.
//
// Marks that arrive faster than the interval collapse into one, which is right: a read after the last mark
// sees everything before it. It also puts a ceiling on the fan out of a change, because one consumer cannot
// be made to read more than once per interval. See docs/scaling.md.
type MarkerPoller struct {
	api      API
	cfg      Config
	interval time.Duration
	retryMin time.Duration
	retryMax time.Duration
	onError  func(error)
	log      roster.Logger
}

var _ roster.HintSource = (*MarkerPoller)(nil)

type PollOption func(*MarkerPoller)

// WithPollInterval sets the time between polls. The default is 2 seconds. A change reaches consumers within
// about one interval.
func WithPollInterval(d time.Duration) PollOption { return func(f *MarkerPoller) { f.interval = d } }

// WithPollLogger sets the logger the default error handler uses. A nil logger is ignored.
func WithPollLogger(l roster.Logger) PollOption {
	return func(f *MarkerPoller) {
		if l != nil {
			f.log = l
		}
	}
}

// WithPollErrorHandler receives poll failures, which the poller retries. It replaces the default, which logs them
// through the poller's logger.
func WithPollErrorHandler(fn func(error)) PollOption {
	return func(f *MarkerPoller) { f.onError = fn }
}

// WithPollRetry bounds the exponential backoff after a failed poll.
func WithPollRetry(min, max time.Duration) PollOption {
	return func(f *MarkerPoller) { f.retryMin, f.retryMax = min, max }
}

func NewMarkerPoller(api API, cfg Config, opts ...PollOption) (*MarkerPoller, error) {
	if api == nil {
		return nil, fmt.Errorf("s3store: an API is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("s3store: %w", err)
	}
	f := &MarkerPoller{
		api: api, cfg: cfg,
		interval: 2 * time.Second, retryMin: time.Second, retryMax: time.Minute,
		log: slog.Default(),
	}
	for _, o := range opts {
		o(f)
	}
	if f.onError == nil {
		f.onError = func(err error) { f.log.Error("roster marker poller", "err", err) }
	}
	// An interval or a retry floor of zero makes sleep return at once, which turns the loop into an
	// unpaced flood of HEAD requests against one key. Refuse it here rather than discover it in a bill.
	switch {
	case f.interval <= 0:
		return nil, fmt.Errorf("s3store: poll interval must be positive, got %s", f.interval)
	case f.retryMin <= 0:
		return nil, fmt.Errorf("s3store: retry floor must be positive, got %s", f.retryMin)
	case f.retryMax < f.retryMin:
		return nil, fmt.Errorf("s3store: retry ceiling %s is below the floor %s", f.retryMax, f.retryMin)
	}
	return f, nil
}

// Subscribe starts polling. A subscription begins with two HintRescans: one before anything is polled, so a
// marker that is missing or unreachable cannot hold up a consumer's first read, and one once the baseline ETag
// is known, so the consumer's next listing happens strictly after that HEAD. After those, a HintRescan is
// emitted whenever the marker's ETag differs from the last poll, including the first time a marker appears. A
// failed poll discards the baseline, so a recovery takes it again and emits a rescan the same way, because
// marks may have been missed. The channel is unbuffered, so a slow consumer slows polling and nothing is
// dropped.
func (f *MarkerPoller) Subscribe(ctx context.Context) (<-chan roster.Hint, error) {
	out := make(chan roster.Hint)
	go f.run(ctx, out)
	return out, nil
}

func (f *MarkerPoller) run(ctx context.Context, out chan<- roster.Hint) {
	defer close(out)

	// Tell the consumer to read before polling anything. The marker is advisory and the snapshot read does
	// not depend on it, so a marker that is missing or unreachable must never hold up a consumer's first
	// read.
	if !send(ctx, out, roster.Hint{Kind: roster.HintRescan}) {
		return
	}

	var (
		last         string
		haveBaseline bool
		delay        = f.retryMin
	)
	for ctx.Err() == nil {
		etag, err := f.poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			f.onError(fmt.Errorf("poll %s: %w", f.cfg.MarkerKey(), err))
			// Marks may land while polling is failing, so the baseline is unknown again and the
			// recovery is treated like a fresh start.
			haveBaseline = false
			if !sleep(ctx, jittered(delay)) {
				return
			}
			delay = min(delay*2, f.retryMax)
			continue
		}
		delay = f.retryMin

		switch {
		case !haveBaseline:
			// A rescan is emitted after the baseline is taken, not before, which is what closes the
			// window. The consumer's next listing then happens strictly after this HEAD, so a write
			// that landed before it is certainly seen, and one that lands after it changes the ETag
			// and produces another rescan. Emitting only the first rescan and seeding the baseline
			// afterwards used to lose a write that landed between the consumer's listing and this
			// HEAD, leaving it for the consumer's own periodic read to find.
			//
			// The cost is one extra read per subscribe and per recovery, for a write that landed
			// between the baseline and the consumer's listing. A redundant read is cheap, because the
			// reader fetches only objects whose ETag changed.
			last, haveBaseline = etag, true
			if !send(ctx, out, roster.Hint{Kind: roster.HintRescan}) {
				return
			}
		case etag != last:
			last = etag
			if !send(ctx, out, roster.Hint{Kind: roster.HintRescan}) {
				return
			}
		}

		if !sleep(ctx, jittered(f.interval)) {
			return
		}
	}
}

// jittered spreads a wait over [d, d+d/4). Consumers that start together (a deploy, or a shared outage that
// ends) would otherwise stay in step for as long as they run, turning a steady request rate into a spike every
// interval. The mean barely moves; the peak does.
func jittered(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + time.Duration(rand.Int64N(int64(d)/4+1))
}

// poll returns the marker's ETag, or "" when there is no marker yet.
func (f *MarkerPoller) poll(ctx context.Context) (string, error) {
	out, err := f.api.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(f.cfg.Bucket), Key: aws.String(f.cfg.MarkerKey())})
	if err != nil {
		if isMissing(err) {
			return "", nil
		}
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

func send(ctx context.Context, out chan<- roster.Hint, c roster.Hint) bool {
	select {
	case out <- c:
		return true
	case <-ctx.Done():
		return false
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
