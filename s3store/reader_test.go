package s3store_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/s3store"
	"github.com/davidbirdsong/roster/s3store/s3fake"
	"github.com/davidbirdsong/roster/wirev1"
)

func newReader(t *testing.T, f *s3fake.Fake, opts ...s3store.ReaderOption) *s3store.Reader {
	t.Helper()
	r, err := s3store.NewReader(f, cfg, opts...)
	require.NoError(t, err)
	return r
}

func put(t *testing.T, s *s3store.Store, owner, service string, bs ...roster.Endpoint) {
	t.Helper()
	require.NoError(t, s.PutService(t.Context(), owner, service, bs))
}

func TestSnapshotReadsEveryServiceOfEveryOwner(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	put(t, s, "c1", "web", be("c1", "web", "a", roster.StateReady))
	put(t, s, "c1", "api", be("c1", "api", "b", roster.StateReady))
	put(t, s, "c2", "web", be("c2", "web", "c", roster.StateTerminating))

	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Complete)
	require.Empty(t, snap.Skipped)
	require.Empty(t, snap.Rejected)
	require.Len(t, snap.Endpoints, 3)
	require.Len(t, snap.Owners, 2)
	// The owner and the service come from the key.
	for _, b := range snap.Endpoints {
		require.Contains(t, []string{"c1", "c2"}, b.OwnerID)
		require.Contains(t, []string{"web", "api"}, b.Service)
	}
	require.Len(t, snap.ByService()["web"], 2)
}

func TestAOwnersLastReconcileIsTheOldestLastModifiedOfItsObjects(t *testing.T) {
	f := s3fake.New()
	clk := newClock()
	f.SetClock(clk.now)
	s := newStore(t, f)
	start := clk.now()
	put(t, s, "c1", "web", be("c1", "web", "a", roster.StateReady))
	clk.advance(time.Minute)
	put(t, s, "c1", "api", be("c1", "api", "b", roster.StateReady))
	clk.advance(time.Minute)
	put(t, s, "c2", "web", be("c2", "web", "c", roster.StateReady))

	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	byID := map[string]time.Time{}
	for _, c := range snap.Owners {
		byID[c.OwnerID] = c.LastRefreshAt
	}
	require.Equal(t, start, byID["c1"], "if any object of an owner is stale, the owner is")
	require.Equal(t, start.Add(2*time.Minute), byID["c2"])

	// A refresh of only one of c1's objects does not make c1 fresh.
	clk.advance(time.Minute)
	put(t, s, "c1", "api", be("c1", "api", "b", roster.StateReady))
	snap, err = newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	for _, c := range snap.Owners {
		if c.OwnerID == "c1" {
			require.Equal(t, start, c.LastRefreshAt)
		}
	}
}

func TestOnlyObjectsWhoseETagChangedAreFetchedAgain(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	for i := 0; i < 5; i++ {
		g := fmt.Sprintf("g%d", i)
		put(t, s, "c1", g, be("c1", g, "a", roster.StateReady))
	}
	r := newReader(t, f)
	_, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, 5, f.Calls("GetObject"))

	_, err = r.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, 5, f.Calls("GetObject"), "nothing changed, so nothing is fetched")

	put(t, s, "c1", "g2", be("c1", "g2", "a", roster.StateTerminating))
	snap, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, 6, f.Calls("GetObject"), "one object changed, one fetch")
	require.Len(t, snap.Endpoints, 5)
}

// The controller's acceptor treats an equal revision as a duplicate whose new content it ignores, and a lower
// one as stale, so the counter has to move exactly when the content does.
func TestTheRevisionMovesExactlyWhenTheServedContentChanges(t *testing.T) {
	f := s3fake.New()
	clk := newClock()
	f.SetClock(clk.now)
	s := newStore(t, f)
	r := newReader(t, f)
	a := be("c1", "web", "a", roster.StateReady)
	put(t, s, "c1", "web", a)

	read := func() roster.Snapshot {
		t.Helper()
		snap, err := r.Snapshot(t.Context())
		require.NoError(t, err)
		return snap
	}
	first := read()
	require.Equal(t, uint64(1), first.Generation)
	require.Equal(t, "1", first.Revision)

	clk.advance(time.Minute)
	put(t, s, "c1", "web", a) // a refresh: the bytes and the content are the same
	again := read()
	require.Equal(t, first.Revision, again.Revision, "a refresh is not a change")
	require.NotEqual(t, first.Owners[0].LastRefreshAt, again.Owners[0].LastRefreshAt, "but it is proof of life")

	a.State, a.Serving = roster.StateTerminating, true
	put(t, s, "c1", "web", a)
	changed := read()
	require.Equal(t, "2", changed.Revision)

	a.State, a.Serving = roster.StateReady, true
	put(t, s, "c1", "web", a)
	require.Equal(t, "3", read().Revision, "going back is another change, and never a lower revision")

	// A restarted reader starts its own count.
	require.Equal(t, "1", read2(t, f).Revision)
}

func read2(t *testing.T, f *s3fake.Fake) roster.Snapshot {
	t.Helper()
	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	return snap
}

func TestSnapshotFollowsEveryPageOfTheListing(t *testing.T) {
	f := s3fake.New()
	f.SetPageSize(3)
	s := newStore(t, f)
	for i := 0; i < 10; i++ {
		g := fmt.Sprintf("g%02d", i)
		put(t, s, "c1", g, be("c1", g, "a", roster.StateReady))
	}
	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.Endpoints, 10)
}

func TestAnObjectThatCannotBeDecodedMakesTheSnapshotIncompleteButNotTheRest(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	put(t, s, "c1", "web", be("c1", "web", "a", roster.StateReady))
	f.Put("v1/api/c2", []byte("{not json"))

	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.False(t, snap.Complete, "a consumer must keep its last good state")
	require.Len(t, snap.Skipped, 1)
	require.Equal(t, "v1/api/c2", snap.Skipped[0].Key)
	require.Equal(t, "api", snap.Skipped[0].Service, "named from the key, so a consumer can hold back only this service")
	require.Equal(t, "c2", snap.Skipped[0].OwnerID)
	require.Len(t, snap.Endpoints, 1, "the readable objects are still returned")
}

func TestABadKeyIsRejectedAndDoesNotMakeTheSnapshotIncomplete(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	put(t, s, "c1", "web", be("c1", "web", "a", roster.StateReady))
	f.Put("v1/only-two", []byte(`{"endpoints":[]}`))
	f.Put("v1/g/c/extra", []byte(`{"endpoints":[]}`))
	f.Put("v1/g*/c", []byte(`{"endpoints":[]}`))

	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Complete, "one bad writer must not freeze every reader")
	require.Len(t, snap.Endpoints, 1)
	var keys []string
	for _, rj := range snap.Rejected {
		keys = append(keys, rj.Key)
		require.Error(t, rj.Err)
	}
	require.ElementsMatch(t, []string{"v1/only-two", "v1/g/c/extra", "v1/g*/c"}, keys)
}

func TestARecordTheDecoderRejectsIsReportedWithItsKeyAndTheRestIsServed(t *testing.T) {
	f := s3fake.New()
	f.Put("v1/web/c1", []byte(`{"endpoints":[
		{"instance_id":"ok","logical_name":"ok","address":"10.0.0.1","port":80,"protocol":"http","state":"READY","serving":true},
		{"instance_id":"odd","logical_name":"odd","address":"10.0.0.2","port":80,"protocol":"http","state":"DRAINING"}]}`))
	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Complete)
	require.Len(t, snap.Endpoints, 1)
	require.Len(t, snap.Rejected, 1)
	require.Equal(t, "v1/web/c1", snap.Rejected[0].Key)
	require.Equal(t, "c1", snap.Rejected[0].OwnerID)
	require.Equal(t, "web", snap.Rejected[0].Service)
	require.Equal(t, roster.EndpointID("odd"), snap.Rejected[0].EndpointID)
}

func TestAWireVersionTheReaderDoesNotKnowIsIncompleteUnlessAKnownOneCoversIt(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	put(t, s, "c1", "web", be("c1", "web", "a", roster.StateReady))
	f.Put("v2/web/c1", []byte(`{"anything":"a newer publisher writes"}`)) // covered by v1: ignored
	f.Put("v2/api/c3", []byte(`{"anything":true}`))                       // only v2 has it

	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.False(t, snap.Complete)
	require.Len(t, snap.Skipped, 1)
	require.Equal(t, "v2/api/c3", snap.Skipped[0].Key)
	require.Equal(t, "api", snap.Skipped[0].Service, "named from the key, so a consumer can hold back only this service")
	require.Equal(t, "c3", snap.Skipped[0].OwnerID)
	require.ErrorContains(t, snap.Skipped[0].Err, "v2")
	require.Len(t, snap.Endpoints, 1)

	f2 := s3fake.New()
	require.NoError(t, newStore(t, f2).PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "web", "a", roster.StateReady),
	}))
	f2.Put("v2/web/c1", []byte(`{"anything":true}`))
	snap, err = newReader(t, f2).Snapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Complete, "a v1 object covers the cell, so the v2 one is not needed")
}

func TestTheNewestSupportedWireVersionWinsPerServiceAndOwner(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	put(t, s, "c1", "web", be("c1", "web", "old", roster.StateReady))
	f.Put("v2/web/c1", []byte("v2 body"))
	v2 := func(ownerID, service string, data []byte) ([]roster.Endpoint, []wirev1.Rejection, error) {
		return []roster.Endpoint{be(ownerID, service, "new", roster.StateReady)}, nil, nil
	}
	snap, err := newReader(t, f, s3store.WithDecoder("v2", v2)).Snapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Complete)
	require.Len(t, snap.Endpoints, 1)
	require.Equal(t, "new", snap.Endpoints[0].InstanceID)
}

func TestAFailedListOrFetchIsAnErrorNotAPartialSnapshot(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	put(t, s, "c1", "web", be("c1", "web", "a", roster.StateReady))
	put(t, s, "c2", "web", be("c2", "web", "b", roster.StateReady))
	r := newReader(t, f)

	f.SetFault(func(op, _ string) error {
		if op == "ListObjectsV2" {
			return errors.New("list denied")
		}
		return nil
	})
	_, err := r.Snapshot(t.Context())
	require.ErrorContains(t, err, "list denied")

	f.SetFault(func(op, key string) error {
		if op == "GetObject" && key == "v1/web/c2" {
			return errors.New("throttled")
		}
		return nil
	})
	_, err = r.Snapshot(t.Context())
	require.ErrorContains(t, err, "throttled")

	f.SetFault(nil)
	snap, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.Endpoints, 2, "and it recovers")
}

// A key that was listed and then could not be read is reported, not left out. Silent absence is
// indistinguishable from an owner that never published the service, and that is a state a consumer acts on.
// Reporting it makes the snapshot incomplete, which is the signal that says an absence is not yet proof.
func TestAnObjectListedAndThenGoneIsReportedNotSilentlyAbsent(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	put(t, s, "c1", "web", be("c1", "web", "a", roster.StateReady))
	put(t, s, "c2", "web", be("c2", "web", "b", roster.StateReady))
	var gets atomic.Int64
	f.SetFault(func(op, key string) error {
		if op == "GetObject" && key == "v1/web/c2" {
			gets.Add(1)
			return &types.NoSuchKey{}
		}
		return nil
	})
	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.False(t, snap.Complete, "one document could not be read")
	require.Len(t, snap.Skipped, 1)
	require.Equal(t, "v1/web/c2", snap.Skipped[0].Key)
	require.Equal(t, "web", snap.Skipped[0].Service, "named from the key, so a consumer can hold back only this service")
	require.Equal(t, "c2", snap.Skipped[0].OwnerID)
	require.EqualValues(t, 2, gets.Load(), "a missing key is retried once, in case the 404 was transient")

	// What was readable is still returned, so a consumer holding its last good state for one document keeps
	// serving everything else.
	require.Len(t, snap.Endpoints, 1)
	require.Len(t, snap.Owners, 1, "an owner whose only document could not be read has no status")
}

// The retry is what keeps a transient 404 from being reported as a gone document.
func TestAMissingKeyThatComesBackOnTheRetryIsReadNormally(t *testing.T) {
	f := s3fake.New()
	put(t, newStore(t, f), "c1", "web", be("c1", "web", "a", roster.StateReady))
	var gets atomic.Int64
	f.SetFault(func(op, key string) error {
		if op == "GetObject" && key == "v1/web/c1" && gets.Add(1) == 1 {
			return &types.NoSuchKey{}
		}
		return nil
	})
	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Complete)
	require.Len(t, snap.Endpoints, 1)
}

func TestFetchesAreBoundedByTheConcurrencyLimit(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	for i := 0; i < 12; i++ {
		g := fmt.Sprintf("g%02d", i)
		put(t, s, "c1", g, be("c1", g, "a", roster.StateReady))
	}
	var running, peak atomic.Int32
	var mu sync.Mutex
	slow := func(ownerID, service string, data []byte) ([]roster.Endpoint, []wirev1.Rejection, error) {
		n := running.Add(1)
		mu.Lock()
		peak.Store(max(peak.Load(), n))
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		return wirev1.Decode(ownerID, service, data)
	}
	snap, err := newReader(t, f, s3store.WithGetConcurrency(3), s3store.WithDecoder("v1", slow)).Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.Endpoints, 12)
	require.LessOrEqual(t, int(peak.Load()), 3)
	require.GreaterOrEqual(t, int(peak.Load()), 2, "and the fetches do overlap")
}

func TestReadAtComesFromTheReadersClockAndAnEmptyBucketIsAnEmptyCompleteSnapshot(t *testing.T) {
	clk := newClock()
	snap, err := newReader(t, s3fake.New(), s3store.WithClock(clk.now)).Snapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Complete)
	require.Empty(t, snap.Endpoints)
	require.Equal(t, clk.now(), snap.ReadAt)
}

func TestTheMarkerAndOtherPrefixesAreNotStateObjects(t *testing.T) {
	f := s3fake.New()
	s := newStore(t, f)
	put(t, s, "c1", "web", be("c1", "web", "a", roster.StateReady))
	f.Put("marker", []byte("mark"))
	f.Put("notes/readme.txt", []byte("hello"))
	snap, err := newReader(t, f).Snapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Complete)
	require.Empty(t, snap.Rejected)
	require.Len(t, snap.Endpoints, 1)
}

func TestKeyPrefixKeepsTwoRegistriesApart(t *testing.T) {
	f := s3fake.New()
	a, err := s3store.NewStore(f, s3store.Config{Bucket: "reg", KeyPrefix: "a/"})
	require.NoError(t, err)
	require.NoError(t, a.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "web", "x", roster.StateReady),
	}))
	b, err := s3store.NewStore(f, s3store.Config{Bucket: "reg", KeyPrefix: "b/"})
	require.NoError(t, err)
	require.NoError(t, b.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "web", "y", roster.StateReady), be("c1", "web", "z", roster.StateReady),
	}))

	ra, err := s3store.NewReader(f, s3store.Config{Bucket: "reg", KeyPrefix: "a/"})
	require.NoError(t, err)
	snap, err := ra.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.Endpoints, 1)
}

// Both of these were a panic at a distance: the clock at the first snapshot, the decoder only once an object
// of that major was read, by which point the major had counted as supported and the object was not reported as
// skipped either.
func TestNewReaderRefusesOptionsThatWouldPanicLater(t *testing.T) {
	f := s3fake.New()
	for name, opt := range map[string]s3store.ReaderOption{
		"a nil clock":   s3store.WithClock(nil),
		"a nil decoder": s3store.WithDecoder("v9", nil),
		"a bogus major": s3store.WithDecoder("nine", func(string, string, []byte) ([]roster.Endpoint, []wirev1.Rejection, error) { return nil, nil, nil }),
	} {
		_, err := s3store.NewReader(f, cfg, opt)
		require.Error(t, err, name)
	}
}
