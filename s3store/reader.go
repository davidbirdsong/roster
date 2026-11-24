package s3store

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/wirev1"
)

// DefaultGetConcurrency bounds how many objects a Reader fetches at once.
const DefaultGetConcurrency = 16

// Reader implements roster.Reader over the bucket. Each Snapshot lists the objects, fetches only those whose
// ETag changed since the last read, decodes them with the decoder of their wire major version, and builds one
// snapshot. A Reader is safe for concurrent use, and reads are serialized.
type Reader struct {
	api         API
	cfg         Config
	now         func() time.Time
	decoders    map[string]decodeFunc
	concurrency int

	mu       sync.Mutex
	cache    map[string]*decoded // by key
	counter  uint64
	lastHash string
}

var _ roster.Reader = (*Reader)(nil)

// decoded is the result of reading one object, kept while its ETag stays the same. An object that cannot be
// decoded is cached too, so a persistently bad object is not fetched again on every read.
type decoded struct {
	etag      string
	endpoints []roster.Endpoint
	rejected  []wirev1.Rejection
	err       error
}

type ReaderOption func(*Reader)

// WithClock sets the clock that stamps a snapshot's ReadAt.
func WithClock(now func() time.Time) ReaderOption { return func(r *Reader) { r.now = now } }

// WithGetConcurrency bounds parallel fetches. The default is DefaultGetConcurrency.
func WithGetConcurrency(n int) ReaderOption {
	return func(r *Reader) { r.concurrency = max(1, n) }
}

// WithDecoder adds or replaces the decoder for a wire major version, for example a newer version before it is
// the default, or a test double.
func WithDecoder(major string, d func(ownerID, service string, data []byte) ([]roster.Endpoint, []wirev1.Rejection, error)) ReaderOption {
	return func(r *Reader) { r.decoders[major] = d }
}

func NewReader(api API, cfg Config, opts ...ReaderOption) (*Reader, error) {
	if api == nil {
		return nil, errors.New("s3store: an API is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("s3store: %w", err)
	}
	r := &Reader{
		api: api, cfg: cfg, now: time.Now, decoders: defaultDecoders(),
		concurrency: DefaultGetConcurrency, cache: map[string]*decoded{},
	}
	for _, o := range opts {
		o(r)
	}
	// Both of these used to be a panic much later and far from the call that caused them: a nil clock at the
	// first snapshot, and a nil decoder only once an object of that major was read, after the major had
	// already counted as supported and so stopped the object being reported as skipped.
	if r.now == nil {
		return nil, errors.New("s3store: WithClock needs a clock, got nil")
	}
	for major, d := range r.decoders {
		if !majorRe.MatchString(major) {
			return nil, fmt.Errorf("s3store: WithDecoder: %q is not a wire major version", major)
		}
		if d == nil {
			return nil, fmt.Errorf("s3store: WithDecoder(%q, nil) needs a decoder", major)
		}
	}
	return r, nil
}

// cell is one (service, owner): what a publisher owner stores for one service.
type cell struct{ service, owner string }

// object is a listed state object together with where it sits.
type object struct {
	listed
	major string
	cell
}

// Snapshot reads the whole roster. A failed LIST or GET returns an error, never a partial snapshot. An object
// that cannot be decoded, one that was listed and then could not be read at all, or one under a wire major
// this reader does not support when no supported object covers the same service and owner, is reported in
// Skipped and makes the snapshot incomplete, so a consumer keeps its last good state. A key that is not a valid state key, and a record that is not acceptable, are left
// out and reported in Rejected without making the snapshot incomplete.
//
// Generation and Revision are a counter that belongs to this Reader. It increases exactly when the content of
// the snapshot changes and stays the same when it does not, which is what the controller's snapshot acceptor
// needs: an equal revision is a duplicate, and a lower one is stale. It starts again after a restart.
func (r *Reader) Snapshot(ctx context.Context) (roster.Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	supported, unsupported, err := r.listMajors(ctx)
	if err != nil {
		return roster.Snapshot{}, err
	}

	chosen := map[cell]object{}
	var rejected []roster.RejectedEndpoint
	var skipped []roster.SkippedItem
	for _, major := range supported {
		objs, bad, err := r.listMajor(ctx, major)
		if err != nil {
			return roster.Snapshot{}, err
		}
		rejected = append(rejected, bad...)
		for _, o := range objs {
			if _, taken := chosen[o.cell]; !taken {
				chosen[o.cell] = o
			}
		}
	}
	for _, major := range unsupported {
		objs, bad, err := r.listMajor(ctx, major)
		if err != nil {
			return roster.Snapshot{}, err
		}
		rejected = append(rejected, bad...)
		for _, o := range objs {
			if _, covered := chosen[o.cell]; !covered {
				skipped = append(skipped, roster.SkippedItem{
					Service: o.service, OwnerID: o.owner, Key: o.Key,
					Err: fmt.Errorf("wire version %s is not supported by this reader", major),
				})
			}
		}
	}

	results, err := r.fetch(ctx, chosen)
	if err != nil {
		return roster.Snapshot{}, err
	}

	var endpoints []roster.Endpoint
	lastRefresh := map[string]time.Time{}
	for _, c := range slices.SortedFunc(maps.Keys(chosen), func(a, b cell) int {
		return cmp.Or(cmp.Compare(a.owner, b.owner), cmp.Compare(a.service, b.service))
	}) {
		o := chosen[c]
		d, ok := results[o.Key]
		if !ok {
			// The key was listed and then could not be fetched. Usually that is a real deletion, and
			// almost always one this reader's own publisher just made. It is not always: a transient 404
			// from a store that is not S3, or a lifecycle rule expiring an object, looks identical.
			//
			// Reporting it keeps the two apart. Leaving it out silently is indistinguishable from an
			// owner that never published this service, which is a state a consumer acts on, and an
			// incomplete snapshot is exactly the signal that says do not act on an absence yet. The
			// endpoints that were read are still returned, so a consumer keeping its last good state for
			// this one document loses nothing else.
			skipped = append(skipped, roster.SkippedItem{
				Service: o.service, OwnerID: o.owner, Key: o.Key,
				Err: errors.New("listed but gone before it could be read, so its absence is not proof of a deletion"),
			})
			continue
		}
		if t, seen := lastRefresh[o.owner]; !seen || o.Modified.Before(t) {
			lastRefresh[o.owner] = o.Modified
		}
		if d.err != nil {
			skipped = append(skipped, roster.SkippedItem{
				Service: o.service, OwnerID: o.owner, Key: o.Key, Err: d.err,
			})
			continue
		}
		endpoints = append(endpoints, d.endpoints...)
		for _, rj := range d.rejected {
			rejected = append(rejected, roster.RejectedEndpoint{
				OwnerID:    o.owner,
				EndpointID: roster.EndpointID(rj.InstanceID), Service: o.service, Key: o.Key, Err: rj.Err,
			})
		}
	}
	var owners []roster.OwnerStatus
	for _, id := range slices.Sorted(maps.Keys(lastRefresh)) {
		owners = append(owners, roster.OwnerStatus{OwnerID: id, LastRefreshAt: lastRefresh[id]})
	}

	// Forget what is no longer there.
	for key := range r.cache {
		if _, still := results[key]; !still {
			delete(r.cache, key)
		}
	}

	snap := roster.NewSnapshot(r.now(), endpoints, owners, skipped, rejected)
	if r.counter == 0 || snap.ContentHash != r.lastHash {
		r.counter++
		r.lastHash = snap.ContentHash
	}
	snap.SetRevision(r.counter)
	return snap, nil
}

// listMajors finds the wire majors present in the bucket, split into those this reader decodes (newest first)
// and those it does not.
func (r *Reader) listMajors(ctx context.Context) (supported, unsupported []string, err error) {
	_, prefixes, err := listAll(ctx, r.api, r.cfg, r.cfg.KeyPrefix, "/")
	if err != nil {
		return nil, nil, err
	}
	for _, p := range prefixes {
		major := p[len(r.cfg.KeyPrefix) : len(p)-1]
		if !majorRe.MatchString(major) {
			continue
		}
		if _, ok := r.decoders[major]; ok {
			supported = append(supported, major)
		} else {
			unsupported = append(unsupported, major)
		}
	}
	newestFirst := func(a, b string) int {
		return cmp.Compare(majorNumber(b), majorNumber(a))
	}
	slices.SortFunc(supported, newestFirst)
	slices.SortFunc(unsupported, newestFirst)
	return supported, unsupported, nil
}

// listMajor lists the state objects of one major. A key that is not a valid state key is rejected.
func (r *Reader) listMajor(ctx context.Context, major string) ([]object, []roster.RejectedEndpoint, error) {
	listing, _, err := listAll(ctx, r.api, r.cfg, r.cfg.majorPrefix(major), "")
	if err != nil {
		return nil, nil, err
	}
	var objs []object
	var rejected []roster.RejectedEndpoint
	for _, l := range listing {
		_, service, owner, err := r.cfg.parseKey(l.Key)
		if err != nil {
			rejected = append(rejected, roster.RejectedEndpoint{Key: l.Key, Err: err})
			continue
		}
		objs = append(objs, object{
			listed: l, major: major,
			cell: cell{service: service, owner: owner},
		})
	}
	return objs, rejected, nil
}

// fetch returns the decoded content of every chosen object, from the cache when its ETag is unchanged and from
// the bucket otherwise. An object that disappeared before it could be fetched is left out of the result, and
// Snapshot reports that rather than reading it as an absence.
func (r *Reader) fetch(ctx context.Context, chosen map[cell]object) (map[string]*decoded, error) {
	results := map[string]*decoded{}
	var todo []object
	for _, o := range chosen {
		if d, ok := r.cache[o.Key]; ok && d.etag == o.ETag {
			results[o.Key] = d
			continue
		}
		todo = append(todo, o)
	}
	if len(todo) == 0 {
		return results, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		sem      = make(chan struct{}, r.concurrency)
	)
	for _, o := range todo {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d, err := r.get(ctx, o)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				if firstErr == nil {
					firstErr = err
					cancel()
				}
			case d != nil:
				results[o.Key] = d
				r.cache[o.Key] = d
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

// get fetches and decodes one object. It returns nil, nil when the object is gone, which Snapshot reports
// rather than treating as an absence.
func (r *Reader) get(ctx context.Context, o object) (*decoded, error) {
	out, err := r.api.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(r.cfg.Bucket), Key: aws.String(o.Key)})
	if err != nil && isMissing(err) {
		// One retry, because a key that was just listed and is reported missing may be a transient 404
		// rather than a deletion. It costs one extra GET per vanished key per snapshot, which is nothing
		// against reporting a document as gone when it is not.
		out, err = r.api.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(r.cfg.Bucket), Key: aws.String(o.Key)})
		if err != nil && isMissing(err) {
			return nil, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", o.Key, err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", o.Key, err)
	}
	// The ETag the fetch returned is the one to cache by: the object may have changed since the list.
	etag := aws.ToString(out.ETag)
	if etag == "" {
		etag = o.ETag
	}
	bs, rejected, err := r.decoders[o.major](o.owner, o.service, data)
	if err != nil {
		return &decoded{etag: etag, err: err}, nil
	}
	return &decoded{etag: etag, endpoints: bs, rejected: rejected}, nil
}
