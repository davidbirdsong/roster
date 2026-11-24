package rostertest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
)

func endpoint(uid string) roster.Endpoint {
	return roster.Endpoint{
		OwnerID: "c1", Service: "g1", LogicalName: uid, InstanceID: uid,
		Address: "10.0.0.1", Port: 443, Protocol: roster.ProtocolHTTPS, SNI: "ingress.example.com",
		State: roster.StateReady,
	}
}

func TestStore_SnapshotReflectsWritesAndHeartbeats(t *testing.T) {
	s := NewStore(endpoint("u2"))
	s.Now = time.Unix(9, 0)
	require.NoError(t, s.Upsert(t.Context(), endpoint("u1")))
	require.NoError(t, s.Heartbeat(t.Context(), "c1", time.Unix(5, 0)))

	snap, err := s.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, []roster.Endpoint{endpoint("u1"), endpoint("u2")}, snap.Endpoints)
	require.Equal(t, []roster.OwnerStatus{{OwnerID: "c1", LastRefreshAt: time.Unix(5, 0)}}, snap.Owners,
		"a heartbeat records the reconcile time")
	require.Equal(t, "1", snap.Revision, "the first snapshot, as a real reader's first is")
	require.Equal(t, time.Unix(9, 0), snap.ReadAt)
	require.True(t, snap.Complete)

	require.NoError(t, s.Delete(t.Context(), "c1", "u1:443"))
	after, _ := s.Snapshot(t.Context())
	require.Equal(t, "2", after.Revision, "a delete changes the content")
}

// The revision contract is the one thing a consumer of this double orders on, so it has to
// behave the way s3store.Reader's does: a refresh is not a change. A double that counted
// writes instead would report a change on every periodic Reconcile, and a consumer built
// against it would push unchanged state forever.
func TestStore_TheRevisionMovesExactlyWhenTheContentChanges(t *testing.T) {
	s := NewStore()
	want := []roster.Endpoint{endpoint("u1")}
	require.NoError(t, s.PutService(t.Context(), "c1", "g1", want))

	first, err := s.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, "1", first.Revision)

	// A reconcile heartbeat: every object rewritten, nothing changed.
	s.Now = time.Unix(30, 0)
	require.NoError(t, s.PutService(t.Context(), "c1", "g1", want))
	again, err := s.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, first.Revision, again.Revision, "a refresh is not a change")
	require.Equal(t, time.Unix(30, 0), again.Owners[0].LastRefreshAt,
		"but the owner's last refresh still advanced, which is the point of the heartbeat")

	require.NoError(t, s.PutService(t.Context(), "c1", "g1", []roster.Endpoint{endpoint("u2")}))
	changed, err := s.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, "2", changed.Revision)
}

func TestStore_FailedWritesAreNotAContentChange(t *testing.T) {
	s := NewStore()
	snap, err := s.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, "1", snap.Revision)
	require.Empty(t, snap.Owners)

	s.UpsertFails = func(roster.Endpoint) bool { return true }
	require.Error(t, s.Upsert(t.Context(), endpoint("u1")))
	s.DeleteFails = true
	require.Error(t, s.Delete(t.Context(), "c1", "x"))

	after, err := s.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, snap.Revision, after.Revision, "nothing was stored, so nothing changed")
	require.Empty(t, after.Owners)
}

func TestStore_SnapshotFailureInjection(t *testing.T) {
	s := NewStore()
	s.SnapshotErr = errors.New("boom")
	_, err := s.Snapshot(t.Context())
	require.Error(t, err)

	s.SnapshotErr = nil
	s.SkipOnSnapshot = []roster.SkippedItem{{Service: "x", OwnerID: "c1", Key: "c1/x"}}
	snap, err := s.Snapshot(t.Context())
	require.NoError(t, err)
	require.False(t, snap.Complete)
}

func TestHints_RescanThenSentHints(t *testing.T) {
	f := NewHints()
	f.Send(roster.Hint{Kind: roster.HintRescan})

	ch, err := f.Subscribe(t.Context())
	require.NoError(t, err)
	require.Equal(t, roster.HintRescan, (<-ch).Kind, "a subscriber always gets a rescan first")
	require.Equal(t, roster.Hint{Kind: roster.HintRescan}, <-ch, "then the queued hint")
}

func TestHints_ClosesWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ch, err := NewHints().Subscribe(ctx)
	require.NoError(t, err)
	require.Equal(t, roster.HintRescan, (<-ch).Kind)

	cancel()
	require.Eventually(t, func() bool { _, ok := <-ch; return !ok }, 2*time.Second, time.Millisecond)
}

func TestHints_SubscribeError(t *testing.T) {
	f := NewHints()
	f.SubscribeErr = errors.New("boom")
	_, err := f.Subscribe(t.Context())
	require.Error(t, err)
	_, err = (&Hints{}).Subscribe(t.Context())
	require.Error(t, err, "a zero Hints is a mistake, use NewHints")
}

func TestStore_PutServiceReplacesTheServiceAndNothingElse(t *testing.T) {
	s := NewStore()
	other := endpoint("keep")
	other.Service = "g2"
	require.NoError(t, s.PutService(t.Context(), "c1", "g2", []roster.Endpoint{other}))
	require.NoError(t, s.PutService(t.Context(), "c1", "g1", []roster.Endpoint{
		endpoint("u1"), endpoint("u2"),
	}))
	require.NoError(t, s.PutService(t.Context(), "c1", "g1", []roster.Endpoint{endpoint("u3")}))

	snap, err := s.Snapshot(t.Context())
	require.NoError(t, err)
	var got []string
	for _, b := range snap.Endpoints {
		got = append(got, b.InstanceID)
	}
	require.Equal(t, []string{"keep", "u3"}, got)
	require.Len(t, s.Puts, 3)

	require.NoError(t, s.DeleteService(t.Context(), "c1", "g1"))
	require.Equal(t, []ServiceRef{{OwnerID: "c1", Service: "g1"}}, s.ServiceDeletes)
	snap, _ = s.Snapshot(t.Context())
	require.Len(t, snap.Endpoints, 1)
}

func TestStore_ALastReconcileIsTheOldestServiceWriteUnlessAHeartbeatSaysOtherwise(t *testing.T) {
	s := NewStore()
	s.Now = time.Unix(10, 0)
	g2 := endpoint("b")
	g2.Service = "g2"
	require.NoError(t, s.PutService(t.Context(), "c1", "g1", []roster.Endpoint{endpoint("a")}))
	s.Now = time.Unix(20, 0)
	require.NoError(t, s.PutService(t.Context(), "c1", "g2", []roster.Endpoint{g2}))

	snap, _ := s.Snapshot(t.Context())
	require.Equal(t, time.Unix(10, 0), snap.Owners[0].LastRefreshAt)

	require.NoError(t, s.Heartbeat(t.Context(), "c1", time.Unix(99, 0)))
	snap, _ = s.Snapshot(t.Context())
	require.Equal(t, time.Unix(99, 0), snap.Owners[0].LastRefreshAt)
}

func TestStore_ServiceWriteFailureInjection(t *testing.T) {
	s := NewStore()
	s.PutFails = func(_, service string) bool { return service == "g1" }
	require.Error(t, s.PutService(t.Context(), "c1", "g1", []roster.Endpoint{endpoint("a")}))
	require.NoError(t, s.PutService(t.Context(), "c1", "g2", nil))
	s.DeleteFails = true
	require.Error(t, s.DeleteService(t.Context(), "c1", "g2"))
	require.Equal(t, 1, s.Writes(), "failed writes are not recorded")
}

func TestNotifier_RecordsReturnsAndHolds(t *testing.T) {
	n := NewNotifier()
	n.Receipt = roster.Receipt{Reached: roster.AckReceived}
	note := roster.Notification{Owner: "c1", Writes: []roster.ServiceWrite{{Service: "g1", Kind: roster.WritePut}}}
	require.Equal(t, n.Receipt, n.Notify(t.Context(), note, roster.AckReceived))

	n.Hold()
	done := make(chan roster.Receipt)
	go func() { done <- n.Notify(t.Context(), note, roster.AckNone) }()
	require.Eventually(t, func() bool { return len(n.Calls()) == 2 }, time.Second, time.Millisecond,
		"a held call is recorded on arrival")
	n.Release()
	require.Equal(t, n.Receipt, <-done)

	n.Hold()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got := n.Notify(ctx, note, roster.AckReceived)
	require.ErrorIs(t, got.Err, context.Canceled, "a held call ends with its context")
	require.Equal(t, roster.AckNone, got.Reached)
	n.Release()

	calls := n.Calls()
	require.Len(t, calls, 3)
	require.Equal(t, note, calls[0].Notification)
	require.Equal(t, roster.AckReceived, calls[0].Want)
	require.ErrorIs(t, calls[2].CtxErr, context.Canceled)
}
