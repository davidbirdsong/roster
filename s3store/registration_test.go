package s3store_test

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/policy"
	"github.com/davidbirdsong/roster/s3store"
	"github.com/davidbirdsong/roster/s3store/s3fake"
)

// TestAPoolOfSelfRegisteringMembers runs the intended use against the bucket: each member of pool web is its own
// process with its own Store and its own owner ID. Members join, refresh, drain and leave without reading the
// roster and without coordinating, and a reader follows them.
func TestAPoolOfSelfRegisteringMembers(t *testing.T) {
	f := s3fake.New()
	clk := newClock()
	f.SetClock(clk.now)
	marks := newMarkCounter(t, f)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	members := map[string]*roster.Registration{}
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("i-%d", i)
		store := newStore(t, f)
		r, err := roster.NewRegistration(store, id, "web", roster.WithRegistrationLogger(quiet),
			roster.WithRegistrationHintNotifier(marks))
		require.NoError(t, err)
		require.NoError(t, r.Register(t.Context(), []roster.Endpoint{be("", "", id, roster.StateReady)}))
		members[id] = r
	}
	require.Equal(t, 3, marks.count(), "each join marks")
	require.Equal(t, []string{cfg.MarkerKey(), "v1/web/i-1", "v1/web/i-2", "v1/web/i-3"}, f.Keys())

	reader := newReader(t, f, s3store.WithClock(clk.now))
	owners := func() []string {
		t.Helper()
		snap, err := reader.Snapshot(t.Context())
		require.NoError(t, err)
		require.True(t, snap.Complete)
		var out []string
		for _, e := range snap.ByService()["web"] {
			out = append(out, e.OwnerID)
		}
		return out
	}
	require.Equal(t, []string{"i-1", "i-2", "i-3"}, owners())
	gets := f.Calls("GetObject")

	// A refresh rewrites identical bytes: no mark, and the reader fetches nothing.
	clk.advance(time.Minute)
	for _, r := range members {
		require.NoError(t, r.Refresh(t.Context()))
	}
	require.Equal(t, 3, marks.count())
	require.Equal(t, []string{"i-1", "i-2", "i-3"}, owners())
	require.Equal(t, gets, f.Calls("GetObject"))

	// i-2 drains and leaves. Both steps mark, and the reader sees each.
	require.NoError(t, members["i-2"].SetState(t.Context(), roster.StateTerminating))
	require.Equal(t, 4, marks.count())
	snap, err := reader.Snapshot(t.Context())
	require.NoError(t, err)
	for _, e := range snap.Endpoints {
		require.Equal(t, e.OwnerID == "i-2", e.State == roster.StateTerminating)
	}
	require.NoError(t, members["i-2"].Deregister(t.Context()))
	require.Equal(t, 5, marks.count())
	require.Equal(t, []string{"i-1", "i-3"}, owners())

	// i-3 crashes: it stops refreshing and never deregisters. i-1 carries on. Once i-3 is older than the
	// readers' stale timeout, policy drops it.
	clk.advance(10 * time.Minute)
	require.NoError(t, members["i-1"].Refresh(t.Context()))
	snap, err = reader.Snapshot(t.Context())
	require.NoError(t, err)
	kept, stale := policy.FilterStaleOwners(snap, 5*time.Minute)
	require.Len(t, kept, 1)
	require.Equal(t, "i-1", kept[0].OwnerID)
	require.Len(t, stale, 1)
	require.Equal(t, "i-3", stale[0].OwnerID)
	require.True(t, stale[0].Dropped)

	// Registration never reads the roster: every LIST and GET above was the reader's.
	readerOnly := s3fake.New()
	store := newStore(t, readerOnly)
	r, err := roster.NewRegistration(store, "i-9", "web", roster.WithRegistrationLogger(quiet))
	require.NoError(t, err)
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{be("", "", "i-9", roster.StateReady)}))
	require.NoError(t, r.Refresh(t.Context()))
	require.NoError(t, r.SetState(t.Context(), roster.StateTerminating))
	require.NoError(t, r.Deregister(t.Context()))
	require.Zero(t, readerOnly.Calls("ListObjectsV2"))
	require.Zero(t, readerOnly.Calls("GetObject"))
	require.Zero(t, readerOnly.Calls("HeadObject"))
	require.Equal(t, 3, readerOnly.Calls("PutObject"))
	require.Equal(t, 1, readerOnly.Calls("DeleteObject"))
}
