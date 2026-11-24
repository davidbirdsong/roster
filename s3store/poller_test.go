package s3store_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/rostertest"
	"github.com/davidbirdsong/roster/s3store"
	"github.com/davidbirdsong/roster/s3store/s3fake"
)

type errSink struct {
	mu  sync.Mutex
	all []error
}

func (e *errSink) add(err error) { e.mu.Lock(); e.all = append(e.all, err); e.mu.Unlock() }
func (e *errSink) get() []error  { e.mu.Lock(); defer e.mu.Unlock(); return append([]error{}, e.all...) }

func startPoller(t *testing.T, f *s3fake.Fake) (<-chan roster.Hint, context.CancelFunc, *errSink) {
	t.Helper()
	sink := &errSink{}
	poller, err := s3store.NewMarkerPoller(f, cfg, s3store.WithPollInterval(5*time.Millisecond),
		s3store.WithPollRetry(time.Millisecond, 4*time.Millisecond), s3store.WithPollErrorHandler(sink.add))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	ch, err := poller.Subscribe(ctx)
	require.NoError(t, err)
	t.Cleanup(cancel)
	return ch, cancel, sink
}

func next(t *testing.T, ch <-chan roster.Hint) roster.Hint {
	t.Helper()
	select {
	case c, ok := <-ch:
		require.True(t, ok, "the poller closed")
		return c
	case <-time.After(3 * time.Second):
		t.Fatal("no change arrived")
		return roster.Hint{}
	}
}

func quiet(t *testing.T, ch <-chan roster.Hint) {
	t.Helper()
	select {
	case c := <-ch:
		t.Fatalf("an unexpected change: %+v", c)
	case <-time.After(60 * time.Millisecond):
	}
}

// start consumes the two rescans every subscription begins with: one emitted before any polling, so a
// marker that is unreachable cannot hold up a consumer's first read, and one emitted after the baseline HEAD,
// so the consumer's next listing happens strictly after that HEAD. Only after both is the poller diffing.
func started(t *testing.T, ch <-chan roster.Hint) {
	t.Helper()
	require.Equal(t, roster.HintRescan, next(t, ch).Kind, "a rescan before any polling")
	require.Equal(t, roster.HintRescan, next(t, ch).Kind, "a rescan once the baseline is known")
}

func mark(t *testing.T, f *s3fake.Fake, id string) {
	t.Helper()
	b, err := s3store.NewMarker(f, cfg, id)
	require.NoError(t, err)
	require.NoError(t, b.Update(t.Context()))
}

func TestASubscriptionStartsWithARescanAndThenAnUnchangedMarkerSaysNothing(t *testing.T) {
	f := s3fake.New()
	mark(t, f, "c1")
	ch, _, _ := startPoller(t, f)
	started(t, ch)
	quiet(t, ch)
}

// The first rescan must not wait on the marker. A consumer can read the roster whether or not the marker is
// reachable, so a broken or missing marker costs hint latency and never the first read.
func TestTheFirstRescanDoesNotWaitForTheMarker(t *testing.T) {
	f := s3fake.New()
	f.SetFault(func(op, _ string) error {
		if op == "HeadObject" {
			return errors.New("marker unreachable")
		}
		return nil
	})
	ch, _, _ := startPoller(t, f)
	require.Equal(t, roster.HintRescan, next(t, ch).Kind,
		"a consumer is told to read even while the marker cannot be polled")
}

func TestAMarkIsAResync(t *testing.T) {
	f := s3fake.New()
	mark(t, f, "c1")
	ch, _, _ := startPoller(t, f)
	started(t, ch)
	quiet(t, ch)
	mark(t, f, "c2")
	require.Equal(t, roster.HintRescan, next(t, ch).Kind)
	quiet(t, ch)
}

func TestTheFirstMarkerEverIsAChangeToo(t *testing.T) {
	f := s3fake.New() // no marker yet
	ch, _, _ := startPoller(t, f)
	started(t, ch)
	quiet(t, ch)
	mark(t, f, "c1")
	require.Equal(t, roster.HintRescan, next(t, ch).Kind)
}

func TestEveryMarkChangesTheMarkerEvenFromTheSamePublisher(t *testing.T) {
	f := s3fake.New()
	mark(t, f, "c1")
	first, _ := f.Get("marker")
	mark(t, f, "c1")
	second, _ := f.Get("marker")
	require.NotEqual(t, first, second)
}

func TestAFailedPollIsRetriedAndFollowedByAResync(t *testing.T) {
	f := s3fake.New()
	mark(t, f, "c1")
	var mu sync.Mutex
	failing := true
	f.SetFault(func(op, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		if op == "HeadObject" && failing {
			return errors.New("throttled")
		}
		return nil
	})
	ch, _, sink := startPoller(t, f)
	require.Equal(t, roster.HintRescan, next(t, ch).Kind)
	require.Eventually(t, func() bool { return len(sink.get()) >= 2 }, 3*time.Second, 5*time.Millisecond)

	mu.Lock()
	failing = false
	mu.Unlock()
	require.Equal(t, roster.HintRescan, next(t, ch).Kind,
		"changes may have been missed during the failure")
	quiet(t, ch)
}

func TestAConsumerThatReadsLateStillSeesTheLatestChange(t *testing.T) {
	f := s3fake.New()
	mark(t, f, "c1")
	ch, _, _ := startPoller(t, f)
	next(t, ch)
	for i := 0; i < 5; i++ {
		mark(t, f, "c2")
		time.Sleep(10 * time.Millisecond)
	}
	// However many marks landed, the consumer gets a rescan and a read after it sees all of them.
	require.Equal(t, roster.HintRescan, next(t, ch).Kind)
}

func TestCancellingTheContextClosesTheChannel(t *testing.T) {
	f := s3fake.New()
	ch, cancel, _ := startPoller(t, f)
	next(t, ch)
	cancel()
	require.Eventually(t, func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	}, 3*time.Second, 5*time.Millisecond)
}

func TestAMarkFailureIsReportedToTheCaller(t *testing.T) {
	f := s3fake.New()
	f.SetFault(func(op, _ string) error {
		if op == "PutObject" {
			return errors.New("denied")
		}
		return nil
	})
	b, err := s3store.NewMarker(f, cfg, "c1")
	require.NoError(t, err)
	require.ErrorContains(t, b.Update(t.Context()), "denied")
}

func TestAWriterWithTheMarkerAttachedMarks(t *testing.T) {
	f := s3fake.New()
	b, err := s3store.NewMarker(f, cfg, "c1")
	require.NoError(t, err)
	reg, err := roster.NewRegistration(newStore(t, f), "c1", "web", roster.WithRegistrationHintNotifier(b))
	require.NoError(t, err)
	ch, _, _ := startPoller(t, f)
	next(t, ch)
	require.NoError(t, reg.Register(t.Context(), []roster.Endpoint{be("", "", "a", roster.StateReady)}))
	require.Equal(t, roster.HintRescan, next(t, ch).Kind)

	r := newReader(t, f)
	snap, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.Endpoints, 1, "a read after the change sees the state the mark announced")
}

func TestTheDefaultPollErrorHandlerLogsThroughTheFeedLogger(t *testing.T) {
	f := s3fake.New()
	f.SetFault(func(op, _ string) error {
		if op == "HeadObject" {
			return errors.New("throttled")
		}
		return nil
	})
	log := &rostertest.Logger{}
	poller, err := s3store.NewMarkerPoller(f, cfg, s3store.WithPollInterval(5*time.Millisecond),
		s3store.WithPollRetry(time.Millisecond, 4*time.Millisecond), s3store.WithPollLogger(log))
	require.NoError(t, err)
	ch, err := poller.Subscribe(t.Context())
	require.NoError(t, err)
	require.Equal(t, roster.HintRescan, next(t, ch).Kind,
		"the poller polls only once its first value is read")

	require.Eventually(t, func() bool { return len(log.Entries()) > 0 }, 3*time.Second, 5*time.Millisecond)
	e := log.Entries()[0]
	require.Equal(t, "error", e.Level)
	require.Equal(t, "roster marker poller", e.Msg)
	require.ErrorContains(t, e.Fields["err"].(error), "throttled")
}

// A zero interval or retry floor makes the sleep return at once, which turns the poll loop into an unpaced
// flood of HEAD requests against a single key.
func TestNewMarkerPollerRefusesAPaceItCannotHold(t *testing.T) {
	f := s3fake.New()
	for name, opts := range map[string][]s3store.PollOption{
		"a zero interval":           {s3store.WithPollInterval(0)},
		"a negative interval":       {s3store.WithPollInterval(-time.Second)},
		"a zero retry floor":        {s3store.WithPollRetry(0, time.Minute)},
		"a ceiling below the floor": {s3store.WithPollRetry(time.Minute, time.Second)},
	} {
		_, err := s3store.NewMarkerPoller(f, cfg, opts...)
		require.Error(t, err, name)
	}
}

// The baseline HEAD has to happen before the rescan that covers it. A consumer answers a rescan by listing,
// so with the old order (rescan first, baseline after) a write that landed between the consumer's listing and
// the baseline HEAD was folded into the baseline and never reported, leaving it to the consumer's own periodic
// read. Observing the ordering is enough: anything written before this HEAD is certainly visible to a listing
// that starts after the rescan it precedes, and anything after it changes the ETag.
func TestTheBaselineIsTakenBeforeTheRescanThatCoversIt(t *testing.T) {
	f := s3fake.New()
	mark(t, f, "c1")
	var heads atomic.Int64
	f.SetFault(func(op, _ string) error {
		if op == "HeadObject" {
			heads.Add(1)
		}
		return nil
	})
	ch, _, _ := startPoller(t, f)

	require.Equal(t, roster.HintRescan, next(t, ch).Kind)
	require.Equal(t, roster.HintRescan, next(t, ch).Kind)
	require.NotZero(t, heads.Load(), "the second rescan follows a poll, it is not emitted before one")
	quiet(t, ch)
}

// A recovery is treated like a fresh start, because marks may have landed while polling was failing. The
// baseline is unknown again, so it is taken first and the rescan that covers it follows.
func TestARecoveryRebaselinesBeforeItsRescan(t *testing.T) {
	f := s3fake.New()
	mark(t, f, "c1")
	var mu sync.Mutex
	failing := false
	f.SetFault(func(op, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		if op == "HeadObject" && failing {
			return errors.New("throttled")
		}
		return nil
	})
	ch, _, sink := startPoller(t, f)
	started(t, ch)
	quiet(t, ch)

	mu.Lock()
	failing = true
	mu.Unlock()
	require.Eventually(t, func() bool { return len(sink.get()) >= 2 }, 3*time.Second, time.Millisecond)
	// A mark that lands while polling is down must not be lost when it recovers.
	mark(t, f, "c2")
	mu.Lock()
	failing = false
	mu.Unlock()

	require.Equal(t, roster.HintRescan, next(t, ch).Kind, "the recovery rescan covers what was missed")
	quiet(t, ch)
}
