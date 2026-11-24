package s3store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/rostertest"
	"github.com/davidbirdsong/roster/s3store"
	"github.com/davidbirdsong/roster/s3store/s3fake"
	"github.com/davidbirdsong/roster/wirev1"
)

var cfg = s3store.Config{Bucket: "reg", Writer: "test/1"}

func be(owner, service, id string, state roster.State) roster.Endpoint {
	return roster.Endpoint{
		OwnerID: owner, Service: service, LogicalName: id, InstanceID: id, Address: "10.0.0.1", Port: 8080,
		Protocol: roster.ProtocolHTTP, State: state, Serving: state == roster.StateReady,
	}
}

// markCounter is a real Marker over the fake, attached to writers as a hint notifier, that counts the marks it
// makes. With err set it fails every notification instead.
type markCounter struct {
	m   *s3store.Marker
	mu  sync.Mutex
	n   int
	err error
}

func newMarkCounter(t *testing.T, f *s3fake.Fake) *markCounter {
	t.Helper()
	m, err := s3store.NewMarker(f, cfg, "test")
	require.NoError(t, err)
	return &markCounter{m: m}
}

func (r *markCounter) Notify(ctx context.Context, n roster.Notification, want roster.Ack) roster.Receipt {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return roster.Receipt{Err: r.err}
	}
	got := r.m.Notify(ctx, n, want)
	if got.Reached == roster.AckAccepted {
		r.n++
	}
	return got
}

func (r *markCounter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)} }
func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newStore(t *testing.T, f *s3fake.Fake, opts ...s3store.StoreOption) *s3store.Store {
	t.Helper()
	s, err := s3store.NewStore(f, cfg, opts...)
	require.NoError(t, err)
	return s
}

func TestPutServiceWritesOneObjectThatDecodesBack(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	require.NoError(t, s.PutService(t.Context(), "i-1", "web", []roster.Endpoint{
		be("i-1", "web", "i-1", roster.StateReady),
	}))
	require.Equal(t, []string{"v1/web/i-1"}, f.Keys())

	data, ok := f.Get("v1/web/i-1")
	require.True(t, ok)
	got, rejected, err := wirev1.Decode("i-1", "web", data)
	require.NoError(t, err)
	require.Empty(t, rejected)
	require.Len(t, got, 1)
	require.Contains(t, string(data), `"writer": "test/1"`)
}

func TestPutServiceRefusesEndpointsOfAnotherOwnerOrService(t *testing.T) {
	s := newStore(t, s3fake.New())
	err := s.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c2", "web", "a", roster.StateReady),
	})
	require.ErrorContains(t, err, "not owner")
	err = s.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "api", "a", roster.StateReady),
	})
	require.ErrorContains(t, err, "not owner")
	require.Error(t, s.PutService(t.Context(), "c1", "bad/service", nil))
}

// Liveness is the object's LastModified, so an unchanged service must still be written.
func TestRewritingIdenticalBytesMovesLastModified(t *testing.T) {
	f := s3fake.New()
	clk := newClock()
	f.SetClock(clk.now)
	s := newStore(t, f)
	b := []roster.Endpoint{be("c1", "web", "a", roster.StateReady)}

	require.NoError(t, s.PutService(t.Context(), "c1", "web", b))
	first, _ := f.Modified("v1/web/c1")
	clk.advance(30 * time.Second)
	require.NoError(t, s.PutService(t.Context(), "c1", "web", b))
	second, _ := f.Modified("v1/web/c1")
	require.Equal(t, first.Add(30*time.Second), second, "an identical write is still a write")
}

func TestTheStoreNeverMarks(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	require.NoError(t, s.PutService(t.Context(), "c1", "web", []roster.Endpoint{be("c1", "web", "a", roster.StateReady)}))
	require.NoError(t, s.DeleteService(t.Context(), "c1", "web"))
	_, ok := f.Get(cfg.MarkerKey())
	require.False(t, ok, "marking is the writer's, through a hint notifier")
}

func TestAFailedWriteIsAnError(t *testing.T) {
	f := s3fake.New()
	f.SetFault(func(op, _ string) error {
		if op == "PutObject" {
			return errors.New("denied")
		}
		return nil
	})
	s := newStore(t, f)
	require.ErrorContains(t, s.PutService(t.Context(), "c1", "web", nil), "denied")
}

func TestDeleteServiceRemovesTheObject(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	require.NoError(t, s.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "web", "a", roster.StateReady),
	}))
	require.NoError(t, s.DeleteService(t.Context(), "c1", "web"))
	require.Empty(t, f.Keys())
	require.NoError(t, s.DeleteService(t.Context(), "c1", "web"),
		"deleting what is not there is not an error")
}

func TestListOwnedReturnsOnlyTheOwnersObjectsAcrossServices(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	require.NoError(t, s.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "web", "a", roster.StateReady),
	}))
	require.NoError(t, s.PutService(t.Context(), "c1", "api", []roster.Endpoint{
		be("c1", "api", "b", roster.StateReady), be("c1", "api", "c", roster.StateNotReady),
	}))
	require.NoError(t, s.PutService(t.Context(), "c2", "web", []roster.Endpoint{
		be("c2", "web", "z", roster.StateReady),
	}))

	got, err := s.ListOwned(t.Context(), "c1")
	require.NoError(t, err)
	require.Len(t, got, 2, "one document per service of this owner, and none of owner c2's")
	byService := map[string]roster.OwnedService{}
	for _, doc := range got {
		require.False(t, doc.Damaged())
		byService[doc.Service] = doc
		for _, b := range doc.Endpoints {
			require.Equal(t, "c1", b.OwnerID)
			require.Equal(t, doc.Service, b.Service)
		}
	}
	require.Len(t, byService["web"].Endpoints, 1)
	require.Len(t, byService["api"].Endpoints, 2)
}

// A publisher reconciles against ListOwned, so a read that failed must never look like an empty service.
func TestListOwnedFailsRatherThanReturningAPartialResult(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	require.NoError(t, s.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "web", "a", roster.StateReady),
	}))

	f.SetFault(func(op, _ string) error {
		if op == "GetObject" {
			return errors.New("throttled")
		}
		return nil
	})
	_, err := s.ListOwned(t.Context(), "c1")
	require.ErrorContains(t, err, "throttled")

	f.SetFault(func(op, _ string) error {
		if op == "ListObjectsV2" {
			return errors.New("denied")
		}
		return nil
	})
	_, err = s.ListOwned(t.Context(), "c1")
	require.ErrorContains(t, err, "denied")
}

// An object that is fetched but cannot be decoded is not a failed read. If it made ListOwned fail, one damaged
// object would make every later reconcile of its owner fail, and the publisher could never repair it. It is
// reported rather than left out, because a publisher only acts on the services it is told about: an undecodable
// object that is left out cannot be deleted when its service is no longer desired, and it would sit in the
// bucket making every reader's snapshot incomplete forever.
func TestListOwnedReportsAnObjectItCannotDecodeRatherThanHidingIt(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	require.NoError(t, s.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "web", "a", roster.StateReady),
	}))
	f.Put("v1/api/c1", []byte("not json"))
	f.Put("v1/api/c2", []byte("not json either, and another owner's"))

	got, err := s.ListOwned(t.Context(), "c1")
	require.NoError(t, err)
	require.Len(t, got, 2, "the damaged object is reported, not hidden")
	byService := map[string]roster.OwnedService{}
	for _, doc := range got {
		byService[doc.Service] = doc
	}
	require.False(t, byService["web"].Damaged())
	damaged := byService["api"]
	require.True(t, damaged.Damaged(), "the undecodable object of this owner is reported")
	require.Error(t, damaged.DecodeErr)
	require.Equal(t, "v1/api/c1", damaged.Key, "with the key, so an operator can find it")
	require.Empty(t, damaged.Endpoints)

	// The publisher repairs it by writing the service.
	require.NoError(t, s.PutService(t.Context(), "c1", "api", []roster.Endpoint{
		be("c1", "api", "b", roster.StateReady),
	}))
	got, err = s.ListOwned(t.Context(), "c1")
	require.NoError(t, err)
	require.Len(t, got, 2)
	for _, doc := range got {
		require.False(t, doc.Damaged(), "service %q is repaired", doc.Service)
	}
}

func TestListOwnedFollowsEveryPage(t *testing.T) {
	f := s3fake.New()
	f.SetPageSize(2)
	s := newStore(t, f)
	for _, g := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, s.PutService(t.Context(), "c1", g, []roster.Endpoint{
			be("c1", g, g, roster.StateReady),
		}))
	}
	got, err := s.ListOwned(t.Context(), "c1")
	require.NoError(t, err)
	require.Len(t, got, 5)
}

func TestNewStoreRefusesWhatItCannotUse(t *testing.T) {
	_, err := s3store.NewStore(nil, cfg)
	require.Error(t, err)
	_, err = s3store.NewStore(s3fake.New(), s3store.Config{})
	require.Error(t, err)
}

func TestVerify(t *testing.T) {
	f := s3fake.New()
	require.NoError(t, s3store.Verify(t.Context(), f, cfg))
	f.SetFault(func(string, string) error { return errors.New("no such bucket") })
	require.ErrorContains(t, s3store.Verify(t.Context(), f, cfg), "no such bucket")
	require.Error(t, s3store.Verify(t.Context(), f, s3store.Config{}))
}

func TestTheStoreLogsThroughTheLoggerItIsGiven(t *testing.T) {
	log := &rostertest.Logger{}
	f := s3fake.New()
	s := newStore(t, f, s3store.WithLogger(log))
	f.Put("v1/web/c1", []byte("not json"))
	_, err := s.ListOwned(t.Context(), "c1")
	require.NoError(t, err)

	got := log.Entries()
	require.Len(t, got, 1)
	require.Equal(t, "warn", got[0].Level)
	require.Contains(t, got[0].Msg, "cannot be decoded")
}

func TestANilStoreLoggerIsIgnored(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f, s3store.WithLogger(nil))
	f.Put("v1/web/c1", []byte("not json"))
	_, err := s.ListOwned(t.Context(), "c1")
	require.NoError(t, err)
}

// badLister wraps an API and rewrites its listings, to stand in for a store that does not follow the
// pagination protocol. A real S3 does not behave this way; another implementation behind the same API might.
type badLister struct {
	s3store.API
	rewrite func(*s3.ListObjectsV2Output)
}

func (b badLister) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	out, err := b.API.ListObjectsV2(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	b.rewrite(out)
	return out, nil
}

// A truncated page says there is more to come. Without a usable token there is no way to ask for it, so the
// listing is short. Returning it as a success is how a publisher comes to believe a service is gone and
// deletes it, and how a reader reports a complete snapshot that is missing endpoints.
func TestAListingThatIsTruncatedWithoutATokenIsAnErrorNotAShortListing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		token *string
	}{
		{"no token", nil},
		{"empty token", aws.String("")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := s3fake.New()
			put(t, newStore(t, f), "c1", "web", be("c1", "web", "a", roster.StateReady))

			api := badLister{API: f, rewrite: func(out *s3.ListObjectsV2Output) {
				out.IsTruncated = aws.Bool(true)
				out.NextContinuationToken = tc.token
			}}
			s, err := s3store.NewStore(api, cfg)
			require.NoError(t, err)
			_, err = s.ListOwned(t.Context(), "c1")
			require.ErrorContains(t, err, "truncated")

			r, err := s3store.NewReader(api, cfg)
			require.NoError(t, err)
			_, err = r.Snapshot(t.Context())
			require.ErrorContains(t, err, "truncated", "a reader must not build a snapshot from a short listing")
		})
	}
}

// A store that keeps handing back the same token would either loop forever or, if the caller stopped, return a
// short listing. Neither may look like success.
func TestAListingThatRepeatsItsContinuationTokenIsAnError(t *testing.T) {
	f := s3fake.New()
	put(t, newStore(t, f), "c1", "web", be("c1", "web", "a", roster.StateReady))

	api := badLister{API: f, rewrite: func(out *s3.ListObjectsV2Output) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String("stuck")
	}}
	s, err := s3store.NewStore(api, cfg)
	require.NoError(t, err)
	_, err = s.ListOwned(t.Context(), "c1")
	require.ErrorContains(t, err, "repeated")
}

// The top level listing uses a delimiter to find the wire majors. S3 counts common prefixes against the page
// size, so that listing can be truncated too, and every page of it has to be followed. Until the fake counted
// prefixes the same way, this could not be tested at all.
func TestTheMajorListingFollowsEveryPageOfCommonPrefixes(t *testing.T) {
	f := s3fake.New()
	put(t, newStore(t, f), "c1", "web", be("c1", "web", "a", roster.StateReady))
	// Majors the reader does not know still have to be seen, or an object under them is silently ignored
	// rather than reported as skipped.
	f.Put("v2/api/c1", []byte(`{"endpoints":[]}`))
	f.Put("v3/db/c1", []byte(`{"endpoints":[]}`))
	f.SetPageSize(1) // one item per page, so the delimited listing truncates
	before := f.Calls("ListObjectsV2")

	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.Greater(t, f.Calls("ListObjectsV2")-before, 4,
		"the delimited listing of three majors had to be paged, not returned whole")
	require.False(t, snap.Complete, "v2 and v3 are unsupported majors, so the snapshot is incomplete")
	var keys []string
	for _, sk := range snap.Skipped {
		keys = append(keys, sk.Key)
	}
	require.ElementsMatch(t, []string{"v2/api/c1", "v3/db/c1"}, keys,
		"every page of the delimited listing was followed")
	require.Len(t, snap.Endpoints, 1)
}

// The fake is a test contract of its own: a page size that cannot fit a single item would hand back a
// truncated page with no token, which is exactly what the library now refuses.
func TestTheFakeRefusesAPageSizeThatCannotProgress(t *testing.T) {
	f := s3fake.New()
	put(t, newStore(t, f), "c1", "web", be("c1", "web", "a", roster.StateReady))
	f.SetPageSize(0)
	_, err := newStore(t, f).ListOwned(t.Context(), "c1")
	require.ErrorContains(t, err, "cannot make progress")
}
