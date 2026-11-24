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

// member is the endpoint of one pool member, with owner and service left for the Registration to fill in.
func member(uid string) roster.Endpoint {
	e := endpoint(uid, "")
	e.OwnerID = ""
	return e
}

func newRegistration(t *testing.T, w roster.EndpointWriter, owner string) *roster.Registration {
	t.Helper()
	r, err := roster.NewRegistration(w, owner, "web",
		roster.WithRegistrationLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	return r
}

func TestNewRegistration_Validation(t *testing.T) {
	_, err := roster.NewRegistration(nil, "i-1", "web")
	require.Error(t, err)
	_, err = roster.NewRegistration(rostertest.NewStore(), "", "web")
	require.Error(t, err)
	_, err = roster.NewRegistration(rostertest.NewStore(), "i-1", "")
	require.Error(t, err)
	_, err = roster.NewRegistration(rostertest.NewStore(), "web/i-1", "web")
	require.Error(t, err, "an owner ID is one segment of a storage key")
}

func TestRegistration_RegisterWritesOnlyItsOwnDocument(t *testing.T) {
	store := rostertest.NewStore()
	r := newRegistration(t, store, "i-1")
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))

	require.Len(t, store.Puts, 1)
	require.Equal(t, "i-1", store.Puts[0].OwnerID)
	require.Equal(t, "web", store.Puts[0].Service)
	got := store.Puts[0].Endpoints
	require.Equal(t, []string{"u1"}, ids(got))
	require.Equal(t, "i-1", got[0].OwnerID, "the owner is stamped")
	require.Equal(t, "web", got[0].Service, "the service is stamped")
	require.Equal(t, got, r.Endpoints())
}

func TestRegistration_RegisterRejectsBeforeAnyWrite(t *testing.T) {
	for name, e := range map[string]roster.Endpoint{
		"another owner":   func() roster.Endpoint { e := member("u1"); e.OwnerID = "i-2"; return e }(),
		"another service": func() roster.Endpoint { e := member("u1"); e.Service = "api"; return e }(),
		"invalid":         func() roster.Endpoint { e := member("u1"); e.Address = "not-an-ip"; return e }(),
	} {
		t.Run(name, func(t *testing.T) {
			store := rostertest.NewStore()
			require.Error(t, newRegistration(t, store, "i-1").Register(t.Context(), []roster.Endpoint{e}))
			require.Zero(t, store.Writes())
		})
	}
	t.Run("a duplicate", func(t *testing.T) {
		store := rostertest.NewStore()
		require.Error(t, newRegistration(t, store, "i-1").Register(t.Context(), []roster.Endpoint{member("u1"), member("u1")}))
		require.Zero(t, store.Writes())
	})
	t.Run("nothing", func(t *testing.T) {
		store := rostertest.NewStore()
		require.Error(t, newRegistration(t, store, "i-1").Register(t.Context(), nil))
		require.Zero(t, store.Writes())
	})
}

func TestRegistration_SeveralPortsAreOneDocument(t *testing.T) {
	store := rostertest.NewStore()
	a, b := member("u1"), member("u1")
	b.Port = 9443
	require.NoError(t, newRegistration(t, store, "i-1").Register(t.Context(), []roster.Endpoint{b, a}))
	require.Len(t, store.Puts, 1)
	require.Len(t, store.Puts[0].Endpoints, 2)
}

func TestRegistration_CallerCannotChangeWhatWasPublished(t *testing.T) {
	store := rostertest.NewStore()
	r := newRegistration(t, store, "i-1")
	e := member("u1")
	e.Labels = map[string]string{"tier": "a"}
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{e}))
	e.Labels["tier"] = "b"
	r.Endpoints()[0].Labels["tier"] = "c"
	require.NoError(t, r.Refresh(t.Context()))
	require.Equal(t, "a", store.Puts[1].Endpoints[0].Labels["tier"])
}

func TestRegistration_RefreshRewritesTheSameEndpoints(t *testing.T) {
	store := rostertest.NewStore()
	r := newRegistration(t, store, "i-1")
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))
	require.NoError(t, r.Refresh(t.Context()))
	require.Len(t, store.Puts, 2)
	require.Equal(t, store.Puts[0], store.Puts[1])
}

func TestRegistration_SetState(t *testing.T) {
	store := rostertest.NewStore()
	r := newRegistration(t, store, "i-1")
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))

	require.NoError(t, r.SetState(t.Context(), roster.StateReady), "unchanged")
	require.Len(t, store.Puts, 1, "an unchanged state writes nothing")

	require.NoError(t, r.SetState(t.Context(), roster.StateTerminating))
	require.Len(t, store.Puts, 2)
	want := store.Puts[0].Endpoints[0]
	want.State = roster.StateTerminating
	require.Equal(t, []roster.Endpoint{want}, store.Puts[1].Endpoints, "only the state changes")

	require.Error(t, r.SetState(t.Context(), roster.State("GONE")))
	require.Len(t, store.Puts, 2)
}

func TestRegistration_NothingToWriteBeforeRegister(t *testing.T) {
	store := rostertest.NewStore()
	r := newRegistration(t, store, "i-1")
	require.ErrorIs(t, r.Refresh(t.Context()), roster.ErrNotRegistered)
	require.ErrorIs(t, r.SetState(t.Context(), roster.StateTerminating), roster.ErrNotRegistered)
	require.Zero(t, store.Writes())
}

func TestRegistration_AFailedWriteLeavesWhatIsRemembered(t *testing.T) {
	store := rostertest.NewStore()
	r := newRegistration(t, store, "i-1")
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))
	before := r.Endpoints()

	store.PutFails = func(string, string) bool { return true }
	require.Error(t, r.SetState(t.Context(), roster.StateTerminating))
	require.Error(t, r.Register(t.Context(), []roster.Endpoint{member("u2")}))
	require.Equal(t, before, r.Endpoints())

	store.PutFails = nil
	require.NoError(t, r.SetState(t.Context(), roster.StateTerminating), "a retry converges")
	require.Equal(t, roster.StateTerminating, store.Puts[len(store.Puts)-1].Endpoints[0].State)
}

func TestRegistration_Deregister(t *testing.T) {
	store := rostertest.NewStore()
	r := newRegistration(t, store, "i-1")
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))

	require.NoError(t, r.Deregister(t.Context()))
	require.Equal(t, []rostertest.ServiceRef{{OwnerID: "i-1", Service: "web"}}, store.ServiceDeletes)
	require.Nil(t, r.Endpoints())

	t.Run("a heartbeat after it cannot write the document back", func(t *testing.T) {
		require.ErrorIs(t, r.Refresh(t.Context()), roster.ErrDeregistered)
		require.ErrorIs(t, r.SetState(t.Context(), roster.StateReady), roster.ErrDeregistered)
		require.Len(t, store.Puts, 1)
	})
	t.Run("it can be repeated", func(t *testing.T) {
		require.NoError(t, r.Deregister(t.Context()))
		require.Len(t, store.ServiceDeletes, 2)
	})
	t.Run("register rejoins", func(t *testing.T) {
		require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))
		require.NoError(t, r.Refresh(t.Context()))
		require.Len(t, store.Puts, 3)
	})
}

func TestRegistration_DeregisterWithoutARegisterStillDeletes(t *testing.T) {
	store := rostertest.NewStore()
	r := newRegistration(t, store, "i-1")
	store.PutFails = func(string, string) bool { return true }
	require.Error(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}), "the write may have landed even though it reported failure")
	require.NoError(t, r.Deregister(t.Context()))
	require.Len(t, store.ServiceDeletes, 1)
}

func TestRegistration_AFailedDeregisterCanBeRetried(t *testing.T) {
	store := rostertest.NewStore()
	r := newRegistration(t, store, "i-1")
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))
	store.DeleteFails = true
	err := r.Deregister(t.Context())
	require.Error(t, err)
	require.False(t, errors.Is(err, roster.ErrDeregistered))
	require.NoError(t, r.Refresh(t.Context()), "still registered, so the heartbeat keeps going")

	store.DeleteFails = false
	require.NoError(t, r.Deregister(t.Context()))
}

// TestRegistration_APool is the intended use: members join and leave on their own, and a reader sees each one
// come and go without any member writing another's document. A member that crashes without deregistering is
// dropped once it is stale.
func TestRegistration_APool(t *testing.T) {
	store := rostertest.NewStore()
	store.Now = time.Unix(1_000, 0)
	members := map[string]*roster.Registration{}
	for _, id := range []string{"i-1", "i-2", "i-3"} {
		members[id] = newRegistration(t, store, id)
		require.NoError(t, members[id].Register(t.Context(), []roster.Endpoint{member("u-" + id)}))
	}
	owners := func() []string {
		snap, err := store.Snapshot(t.Context())
		require.NoError(t, err)
		var out []string
		for _, e := range snap.Endpoints {
			out = append(out, e.OwnerID)
		}
		return out
	}
	require.Equal(t, []string{"i-1", "i-2", "i-3"}, owners())

	// i-2 shuts down cleanly.
	require.NoError(t, members["i-2"].SetState(t.Context(), roster.StateTerminating))
	require.NoError(t, members["i-2"].Deregister(t.Context()))
	require.Equal(t, []string{"i-1", "i-3"}, owners())

	// Time passes. i-1 keeps refreshing, i-3 has crashed.
	store.Now = time.Unix(2_000, 0)
	require.NoError(t, members["i-1"].Refresh(t.Context()))
	snap, err := store.Snapshot(t.Context())
	require.NoError(t, err)
	kept, stale := policy.FilterStaleOwners(snap, 10*time.Minute)
	require.Len(t, kept, 1)
	require.Equal(t, "i-1", kept[0].OwnerID)
	require.Len(t, stale, 1)
	require.Equal(t, "i-3", stale[0].OwnerID, "i-2 left cleanly, so it has nothing to go stale")

	for _, p := range store.Puts {
		require.Equal(t, p.OwnerID, p.Endpoints[0].OwnerID, "no member wrote another's document")
	}
}
