package roster_test

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/policy"
	"github.com/davidbirdsong/roster/rostertest"
)

func endpoint(uid, service string) roster.Endpoint {
	return roster.Endpoint{
		OwnerID: "c1", Service: service, LogicalName: uid, InstanceID: uid,
		Address: "10.0.0.1", Port: 8443, Protocol: "https", SNI: "ingress.example.com",
		State: roster.StateReady, Serving: true,
	}
}

func newPublisher(t *testing.T, store roster.DocumentStore) *roster.Publisher {
	t.Helper()
	p, err := roster.NewPublisher(store, "c1",
		roster.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	return p
}

// ids lists the unique IDs of a write, in order.
func ids(bs []roster.Endpoint) []string {
	var out []string
	for _, b := range bs {
		out = append(out, b.InstanceID)
	}
	return out
}

func TestNewPublisher_Validation(t *testing.T) {
	_, err := roster.NewPublisher(nil, "c1")
	require.Error(t, err)
	_, err = roster.NewPublisher(rostertest.NewStore(), "")
	require.Error(t, err)
	_, err = roster.NewPublisher(rostertest.NewStore(), "web/i-1")
	require.Error(t, err, "an owner ID is one segment of a storage key")
}

func TestReconcile_WritesAServiceThatIsMissing(t *testing.T) {
	store := rostertest.NewStore()
	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{
		endpoint("u1", "a"),
	}))
	require.Len(t, store.Puts, 1)
	require.Equal(t, "a", store.Puts[0].Service)
	require.Equal(t, []string{"u1"}, ids(store.Puts[0].Endpoints))
	require.Empty(t, store.ServiceDeletes)
}

func TestReconcile_ADeletesAServiceThatIsNoLongerDesired(t *testing.T) {
	store := rostertest.NewStore(endpoint("gone", "a"))
	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), nil))
	require.Equal(t, []rostertest.ServiceRef{{OwnerID: "c1", Service: "a"}}, store.ServiceDeletes)
	require.Empty(t, store.Puts)
}

func TestReconcile_AWholeServiceIsOneWrite(t *testing.T) {
	store := rostertest.NewStore()
	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{
		endpoint("u2", "a"), endpoint("u1", "a"), endpoint("u3", "b"),
	}))
	require.Len(t, store.Puts, 2)
	require.Equal(t, []string{"u1", "u2"}, ids(store.Puts[0].Endpoints),
		"one write for the service, sorted by ID")
	require.Equal(t, []string{"u3"}, ids(store.Puts[1].Endpoints))
}

// Liveness is the write time of an owner's objects, so a reconcile rewrites a service even when nothing changed.
func TestReconcile_AnUnchangedServiceIsRewrittenWithIdenticalContent(t *testing.T) {
	store := rostertest.NewStore(endpoint("u1", "a"))
	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{
		endpoint("u1", "a"),
	}))
	require.Len(t, store.Puts, 1, "the refresh is the proof of life")
	require.Equal(t, []string{"u1"}, ids(store.Puts[0].Endpoints))
	require.Empty(t, store.ServiceDeletes)
	got, _ := store.Item("u1:8443")
	require.True(t, got.Equal(endpoint("u1", "a")))
}

func TestReconcile_ListErrorDoesNothing(t *testing.T) {
	store := rostertest.NewStore(endpoint("u1", "a"))
	store.ListErr = io.ErrUnexpectedEOF
	require.Error(t, newPublisher(t, store).Reconcile(t.Context(), nil))
	require.Zero(t, store.Writes(), "a failed roster read must never lead to deletes")
}

func TestReconcile_RefusesForeignOwnerRecords(t *testing.T) {
	store := rostertest.NewStore()
	other := endpoint("x", "a")
	other.OwnerID = "other"
	store.Foreign = []roster.Endpoint{other}
	require.Error(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{
		endpoint("u1", "a"),
	}))
	require.Zero(t, store.Writes())
}

func TestReconcile_DriftedRegistryRecordIsRewritten(t *testing.T) {
	drifted := endpoint("u1", "a")
	drifted.State = roster.StateNotReady
	store := rostertest.NewStore(drifted)
	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{
		endpoint("u1", "a"),
	}))
	got, _ := store.Item("u1:8443")
	require.Equal(t, roster.StateReady, got.State)
}

func TestReconcile_ProtectedServiceIsKeptAliveAndOtherwiseUntouched(t *testing.T) {
	// Service "a" is protected, so its published endpoint survives even though it is absent from the desired
	// set, and the reconcile rewrites it unchanged so the owner does not look stale. Service "b" is still cleaned up.
	store := rostertest.NewStore(endpoint("keep", "a"), endpoint("stale", "b"))
	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), nil, roster.PreserveServices("a")))
	require.Len(t, store.Puts, 1)
	require.Equal(t, "a", store.Puts[0].Service)
	require.Equal(t, []string{"keep"}, ids(store.Puts[0].Endpoints))
	require.Equal(t, []rostertest.ServiceRef{{OwnerID: "c1", Service: "b"}}, store.ServiceDeletes)
}

// A stored document that cannot be decoded is reported by ListOwned rather than hidden, so a Reconcile can
// act on it. If it were hidden, the publisher would never learn that the object exists, could not delete it
// once its service was no longer desired, and the object would make every reader's snapshot incomplete for as
// long as the bucket kept it.
func TestReconcile_DeletesAnUndecodableDocumentThatIsNoLongerDesired(t *testing.T) {
	store := rostertest.NewStore(endpoint("keep", "web"))
	store.Damaged = map[string]error{"api": errors.New("invalid character 'o'")}

	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{endpoint("keep", "web")}))

	require.Equal(t, []rostertest.ServiceRef{{OwnerID: "c1", Service: "api"}}, store.ServiceDeletes,
		"the damaged document of an undesired service is deleted, not stranded")
}

func TestReconcile_RepairsAnUndecodableDocumentThatIsStillDesired(t *testing.T) {
	store := rostertest.NewStore()
	store.Damaged = map[string]error{"api": errors.New("unexpected end of JSON input")}

	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{endpoint("u1", "api")}))

	require.Len(t, store.Puts, 1, "the damaged document is overwritten with the desired set")
	require.Equal(t, "api", store.Puts[0].Service)
	require.Equal(t, []string{"u1"}, ids(store.Puts[0].Endpoints))
	require.Empty(t, store.ServiceDeletes, "a desired service is repaired, never deleted")
}

// A protected service's document can be neither repaired (there is no desired set for it) nor refreshed
// (there is no readable content to rewrite). Silently leaving it would hide a stuck object, so it is reported.
func TestReconcile_ReportsAnUndecodableDocumentOfAProtectedService(t *testing.T) {
	store := rostertest.NewStore(endpoint("keep", "web"))
	store.Damaged = map[string]error{"api": errors.New("invalid character 'o'")}

	err := newPublisher(t, store).Reconcile(t.Context(), nil, roster.PreserveServices("api"))

	require.ErrorContains(t, err, "api")
	require.ErrorContains(t, err, "cannot be decoded")
	for _, d := range store.ServiceDeletes {
		require.NotEqual(t, "api", d.Service, "a protected service is never deleted, damaged or not")
	}
	for _, p := range store.Puts {
		require.NotEqual(t, "api", p.Service, "there is no content to rewrite it with")
	}
}

// A failed read is not a damaged document. It still aborts the whole reconcile before any delete, so a
// publisher cannot mistake an outage for an empty roster.
func TestReconcile_AFailedReadStillAbortsBeforeAnyDelete(t *testing.T) {
	store := rostertest.NewStore(endpoint("gone", "web"))
	store.ListErr = errors.New("throttled")

	require.ErrorContains(t, newPublisher(t, store).Reconcile(t.Context(), nil), "throttled")
	require.Zero(t, store.Writes(), "nothing is written or deleted on a failed read")
}

func TestReconcile_ProtectedServiceIsNotWritten(t *testing.T) {
	store := rostertest.NewStore()
	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{
		endpoint("u1", "a"),
	}, roster.PreserveServices("a")))
	require.Zero(t, store.Writes())
}

func TestReconcile_PartialWriteFailureIsReportedAndOnlyTheFailedServiceIsRetried(t *testing.T) {
	store := rostertest.NewStore()
	store.PutFails = func(_, service string) bool { return service == "b" }
	p := newPublisher(t, store)
	want := []roster.Endpoint{endpoint("u1", "a"), endpoint("u2", "b")}

	require.Error(t, p.Reconcile(t.Context(), want))
	require.Len(t, store.Puts, 1)
	require.Equal(t, "a", store.Puts[0].Service)

	store.PutFails = nil
	require.NoError(t, p.Publish(t.Context(), want))
	require.Len(t, store.Puts, 2, "only the service that failed is retried")
	require.Equal(t, "b", store.Puts[1].Service)
}

// A reader takes the oldest write among an owner's services as its last reconcile, so a reconcile that cannot refresh
// one service leaves the owner looking stale, as a missing heartbeat did.
func TestReconcile_AFailedRefreshOfOneServiceLeavesTheOwnerLookingStale(t *testing.T) {
	store := rostertest.NewStore()
	p := newPublisher(t, store)
	want := []roster.Endpoint{endpoint("u1", "a"), endpoint("u2", "b")}

	t0 := time.Unix(1000, 0)
	store.Now = t0
	require.NoError(t, p.Reconcile(t.Context(), want))

	store.Now = t0.Add(time.Minute)
	store.PutFails = func(_, service string) bool { return service == "b" }
	require.Error(t, p.Reconcile(t.Context(), want))
	snap, err := store.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, t0, snap.Owners[0].LastRefreshAt, "service b was not refreshed, so the owner's last reconcile is still the first one")

	store.PutFails = nil
	store.Now = t0.Add(2 * time.Minute)
	require.NoError(t, p.Reconcile(t.Context(), want))
	snap, err = store.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, t0.Add(2*time.Minute), snap.Owners[0].LastRefreshAt)
}

func TestReconcile_ARefreshMovesTheOwnersLastReconcileToTheNewestWrite(t *testing.T) {
	store := rostertest.NewStore()
	p := newPublisher(t, store)
	want := []roster.Endpoint{endpoint("u1", "a"), endpoint("u2", "b")}

	store.Now = time.Unix(1000, 0)
	require.NoError(t, p.Reconcile(t.Context(), want))
	store.Now = time.Unix(2000, 0)
	require.NoError(t, p.Reconcile(t.Context(), want))

	snap, err := store.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, time.Unix(2000, 0), snap.Owners[0].LastRefreshAt,
		"every service was rewritten, so the oldest write is the newest reconcile")
}

func TestPublish_RequiresReconcile(t *testing.T) {
	store := rostertest.NewStore()
	require.ErrorIs(t, newPublisher(t, store).Publish(t.Context(), nil), roster.ErrNotInitialized)
	require.Zero(t, store.Writes())
}

func TestPublish_PublishesChangesWithoutReadingRegistry(t *testing.T) {
	store := rostertest.NewStore()
	p := newPublisher(t, store)
	require.NoError(t, p.Reconcile(t.Context(), []roster.Endpoint{endpoint("u1", "a")}))

	// Pod u1 is replaced by u2: the whole service is replaced in one write.
	store.ListErr = io.ErrUnexpectedEOF // Publish must not read the roster
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("u2", "a")}))
	require.Len(t, store.Puts, 2)
	require.Equal(t, []string{"u2"}, ids(store.Puts[1].Endpoints))
	require.Empty(t, store.ServiceDeletes)
	_, hasOld := store.Item("u1:8443")
	require.False(t, hasOld)
}

func TestPublish_WritesOnlyTheServicesThatChanged(t *testing.T) {
	store := rostertest.NewStore()
	p := newPublisher(t, store)
	require.NoError(t, p.Reconcile(t.Context(), []roster.Endpoint{endpoint("u1", "a"), endpoint("u2", "b")}))
	before := len(store.Puts)

	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("u1", "a"), endpoint("u3", "b")}))
	require.Len(t, store.Puts, before+1)
	require.Equal(t, "b", store.Puts[before].Service)

	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("u3", "b")}))
	require.Equal(t, []rostertest.ServiceRef{{OwnerID: "c1", Service: "a"}}, store.ServiceDeletes,
		"a service that has no endpoints left is deleted")
}

func TestPublish_RepeatedCallIsIdempotent(t *testing.T) {
	store := rostertest.NewStore()
	p := newPublisher(t, store)
	want := []roster.Endpoint{endpoint("u1", "a")}
	require.NoError(t, p.Reconcile(t.Context(), want))
	before := store.Writes()
	require.NoError(t, p.Publish(t.Context(), want))
	require.NoError(t, p.Publish(t.Context(), want))
	require.Equal(t, before, store.Writes(), "a sync that finds nothing changed writes nothing")
}

func TestPublish_DeleteFailureIsRetried(t *testing.T) {
	store := rostertest.NewStore(endpoint("gone", "a"))
	p := newPublisher(t, store)

	store.DeleteFails = true
	require.Error(t, p.Reconcile(t.Context(), nil))
	store.DeleteFails = false
	require.NoError(t, p.Publish(t.Context(), nil))
	require.Equal(t, []rostertest.ServiceRef{{OwnerID: "c1", Service: "a"}}, store.ServiceDeletes)
}

func TestPrepare_StampsEmptyOwnerID(t *testing.T) {
	store := rostertest.NewStore()
	b := endpoint("u1", "a")
	b.OwnerID = ""
	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{b}))
	got, ok := store.Item("u1:8443")
	require.True(t, ok)
	require.Equal(t, "c1", got.OwnerID)
}

func TestPrepare_RejectsBadInputBeforeAnyRegistryAccess(t *testing.T) {
	foreign := endpoint("u1", "a")
	foreign.OwnerID = "other"
	invalid := endpoint("u2", "a")
	invalid.Address = "host.example.com"

	for name, input := range map[string][]roster.Endpoint{
		"foreign owner":             {foreign},
		"invalid endpoint":          {invalid},
		"duplicate id":              {endpoint("u3", "a"), endpoint("u3", "a")},
		"duplicate id across":       {endpoint("u3", "a"), endpoint("u3", "b")},
		"service that is not a key": {endpoint("u4", "a/b")},
	} {
		t.Run(name, func(t *testing.T) {
			store := rostertest.NewStore(endpoint("existing", "a"))
			require.Error(t, newPublisher(t, store).Reconcile(t.Context(), input))
			require.Zero(t, store.Writes(), "one bad endpoint must not cause deletes or partial writes")
		})
	}
}

func TestServicesOf(t *testing.T) {
	require.Equal(t, []string{"a", "b"}, roster.ServicesOf([]roster.Endpoint{
		endpoint("1", "b"), endpoint("2", "a"), endpoint("3", "b"),
	}))
	require.Empty(t, roster.ServicesOf(nil))
}

func TestPublisher_LabelChangesAreWrittenAndNilEqualsEmpty(t *testing.T) {
	store := rostertest.NewStore()
	p := newPublisher(t, store)
	b := endpoint("u1", "a")
	require.NoError(t, p.Reconcile(t.Context(), []roster.Endpoint{b}))
	require.Len(t, store.Puts, 1)

	b.Labels = map[string]string{}
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{b}))
	require.Len(t, store.Puts, 1, "an empty map is not a change from none")

	b.Labels = map[string]string{"tier": "web"}
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{b}))
	require.Len(t, store.Puts, 2)
	got, _ := store.Item(b.ID())
	require.Equal(t, map[string]string{"tier": "web"}, got.Labels)

	b.Labels = nil
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{b}))
	require.Len(t, store.Puts, 3, "removing the labels is a change")
}

func TestPublisher_OwnsTheLabelsItWasGiven(t *testing.T) {
	store := rostertest.NewStore()
	p := newPublisher(t, store)
	labels := map[string]string{"tier": "web"}
	b := endpoint("u1", "a")
	b.Labels = labels
	require.NoError(t, p.Reconcile(t.Context(), []roster.Endpoint{b}))

	// The caller reuses its map. The publisher must not see the change as already published.
	labels["tier"] = "api"
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{b}))
	got, _ := store.Item(b.ID())
	require.Equal(t, "api", got.Labels["tier"], "the changed label is written, so it was not aliased")
	require.Len(t, store.Puts, 2)
}

func TestPublisher_RejectsInvalidLabelsBeforeAnyWrite(t *testing.T) {
	store := rostertest.NewStore(endpoint("existing", "a"))
	bad := endpoint("u1", "a")
	bad.Labels = map[string]string{"tier": "we b"}
	require.Error(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{bad}))
	require.Zero(t, store.Writes())
}

func TestReconcile_SeveralRecordsMayShareALogicalName(t *testing.T) {
	// A draining instance and its successor hold the same hash identity, and both
	// must be stored so a consumer can see the up and down states.
	old, successor := endpoint("u1", "a"), endpoint("u2", "a")
	old.LogicalName, successor.LogicalName = "web-0", "web-0"
	old.State = roster.StateTerminating

	store := rostertest.NewStore()
	require.NoError(t, newPublisher(t, store).Reconcile(t.Context(), []roster.Endpoint{old, successor}))

	_, hasOld := store.Item(old.ID())
	_, hasNew := store.Item(successor.ID())
	require.True(t, hasOld && hasNew, "neither record overwrites the other")
	require.Len(t, policy.PreferEndpoint([]roster.Endpoint{old, successor}), 1, "a consumer resolves them to one")
}

var _ roster.Logger = (*slog.Logger)(nil)

func TestThePublisherLogsThroughTheLoggerItIsGiven(t *testing.T) {
	log := &rostertest.Logger{}
	p, err := roster.NewPublisher(rostertest.NewStore(endpoint("gone", "b")), "c1", roster.WithLogger(log))
	require.NoError(t, err)
	require.NoError(t, p.Reconcile(t.Context(), []roster.Endpoint{endpoint("u1", "a")}))

	got := log.Entries()
	require.Len(t, got, 2, "a write and a delete are logged, a refresh is not")
	for _, e := range got {
		require.Equal(t, "info", e.Level)
		require.Equal(t, "published", e.Msg)
	}
	require.Equal(t, map[string]any{"op": "write", "service": "a", "endpoints": 1}, got[0].Fields)
	require.Equal(t, map[string]any{"op": "delete", "service": "b"}, got[1].Fields)
}

func TestANilLoggerIsIgnored(t *testing.T) {
	p, err := roster.NewPublisher(rostertest.NewStore(), "c1", roster.WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, p.Reconcile(t.Context(), []roster.Endpoint{endpoint("u1", "a")}))
}
