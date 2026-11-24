package policy

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
)

func TestFilterStaleOwners(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	ago := func(h int) time.Time { return now.Add(-time.Duration(h) * time.Hour) }
	rec := func(owner string) roster.Endpoint {
		x := b("u-"+owner, 443, roster.StateReady)
		x.OwnerID = owner
		return x
	}
	snap := func(readAt time.Time, reconciles map[string]time.Time, owners ...string) roster.Snapshot {
		s := roster.Snapshot{ReadAt: readAt}
		for _, c := range owners {
			s.Endpoints = append(s.Endpoints, rec(c))
			s.Owners = append(s.Owners, roster.OwnerStatus{OwnerID: c, LastRefreshAt: reconciles[c]})
		}
		return s
	}
	ids := func(bs []roster.Endpoint) []string {
		var out []string
		for _, x := range bs {
			out = append(out, x.OwnerID)
		}
		return out
	}

	t.Run("an owner that stopped reconciling is dropped and reported", func(t *testing.T) {
		s := snap(now, map[string]time.Time{"c1": ago(0), "c2": ago(30)}, "c1", "c2")
		kept, stale := FilterStaleOwners(s, 24*time.Hour)
		require.Equal(t, []string{"c1"}, ids(kept))
		require.Equal(t, []StaleOwner{{OwnerID: "c2", LastRefreshAt: ago(30), Age: 30 * time.Hour, Dropped: true}}, stale)
		require.Len(t, s.Endpoints, 2, "the snapshot is not changed")
	})

	t.Run("a terminating record goes with its stale owner", func(t *testing.T) {
		s := snap(now, map[string]time.Time{"c1": ago(0), "c2": ago(30)}, "c1", "c2")
		s.Endpoints[1].State = roster.StateTerminating
		kept, _ := FilterStaleOwners(s, 24*time.Hour)
		require.Equal(t, []string{"c1"}, ids(kept))
	})

	t.Run("an owner exactly at the limit is not stale", func(t *testing.T) {
		s := snap(now, map[string]time.Time{"c1": ago(0), "c2": ago(24)}, "c1", "c2")
		kept, stale := FilterStaleOwners(s, 24*time.Hour)
		require.Equal(t, []string{"c1", "c2"}, ids(kept))
		require.Empty(t, stale)
	})

	t.Run("an owner with no reconcile time is never stale", func(t *testing.T) {
		s := snap(now, map[string]time.Time{"c1": ago(0)}, "c1", "c2")
		kept, stale := FilterStaleOwners(s, time.Hour)
		require.Equal(t, []string{"c1", "c2"}, ids(kept))
		require.Empty(t, stale)
	})

	t.Run("a reconcile time in the future is not stale", func(t *testing.T) {
		s := snap(now, map[string]time.Time{"c1": ago(0), "c2": now.Add(time.Hour)}, "c1", "c2")
		kept, stale := FilterStaleOwners(s, time.Hour)
		require.Equal(t, []string{"c1", "c2"}, ids(kept))
		require.Empty(t, stale)
	})

	t.Run("when every owner is stale none is dropped", func(t *testing.T) {
		s := snap(now, map[string]time.Time{"c1": ago(40), "c2": ago(30)}, "c1", "c2")
		kept, stale := FilterStaleOwners(s, 24*time.Hour)
		require.Equal(t, []string{"c1", "c2"}, ids(kept))
		require.Len(t, stale, 2)
		for _, st := range stale {
			require.False(t, st.Dropped)
		}
	})

	t.Run("a stale owner with no records does not count against the all stale rule", func(t *testing.T) {
		s := snap(now, map[string]time.Time{"c1": ago(0), "c2": ago(30)}, "c1")
		s.Owners = append(s.Owners, roster.OwnerStatus{OwnerID: "c2", LastRefreshAt: ago(30)})
		kept, stale := FilterStaleOwners(s, 24*time.Hour)
		require.Equal(t, []string{"c1"}, ids(kept))
		require.Equal(t, []StaleOwner{{OwnerID: "c2", LastRefreshAt: ago(30), Age: 30 * time.Hour, Dropped: true}}, stale)
	})

	t.Run("the all stale rule is per service", func(t *testing.T) {
		// One owner per pool member, as self registering publishers are. Pool g1 lost every member at once, pool g2
		// lost one of two. A live member of g2 says nothing about g1, so g1 is kept, and g2 drops its stale member.
		s := snap(now, map[string]time.Time{"a1": ago(30), "a2": ago(40), "b1": ago(0), "b2": ago(30)},
			"a1", "a2", "b1", "b2")
		for i := range s.Endpoints {
			if s.Endpoints[i].OwnerID[0] == 'b' {
				s.Endpoints[i].Service = "g2"
			}
		}
		kept, stale := FilterStaleOwners(s, 24*time.Hour)
		require.Equal(t, []string{"a1", "a2", "b1"}, ids(kept))
		require.Equal(t, []StaleOwner{
			{OwnerID: "a1", LastRefreshAt: ago(30), Age: 30 * time.Hour, Dropped: false},
			{OwnerID: "a2", LastRefreshAt: ago(40), Age: 40 * time.Hour, Dropped: false},
			{OwnerID: "b2", LastRefreshAt: ago(30), Age: 30 * time.Hour, Dropped: true},
		}, stale)
	})

	t.Run("an owner in several services is dropped only where a live owner remains", func(t *testing.T) {
		s := snap(now, map[string]time.Time{"agg": ago(30), "live": ago(0)}, "agg", "live")
		other := rec("agg")
		other.InstanceID, other.Service = "u-agg-2", "g2"
		s.Endpoints = append(s.Endpoints, other)
		kept, stale := FilterStaleOwners(s, 24*time.Hour)
		require.Len(t, kept, 2)
		for _, x := range kept {
			require.True(t, x.OwnerID == "live" || x.Service == "g2", "agg is dropped from g1 and kept in g2: %+v", x)
		}
		require.Equal(t, []StaleOwner{{OwnerID: "agg", LastRefreshAt: ago(30), Age: 30 * time.Hour, Dropped: false}}, stale)
	})

	t.Run("zero or negative staleAfter keeps everything", func(t *testing.T) {
		s := snap(now, map[string]time.Time{"c1": ago(0), "c2": ago(1000)}, "c1", "c2")
		for _, d := range []time.Duration{0, -time.Hour} {
			kept, stale := FilterStaleOwners(s, d)
			require.Equal(t, []string{"c1", "c2"}, ids(kept))
			require.Empty(t, stale)
		}
	})

	t.Run("a snapshot with no read time keeps everything", func(t *testing.T) {
		s := snap(time.Time{}, map[string]time.Time{"c1": ago(0), "c2": ago(1000)}, "c1", "c2")
		kept, stale := FilterStaleOwners(s, time.Hour)
		require.Equal(t, []string{"c1", "c2"}, ids(kept))
		require.Empty(t, stale)
	})
}

func TestStaleFilter(t *testing.T) {
	const staleAfter, confirmFor = 180, 60 // seconds
	at := func(sec int) time.Time { return time.Unix(1_000_000+int64(sec), 0) }
	// snap reads at readAt, with each owner's last refresh in seconds.
	snap := func(readAt int, refreshes map[string]int) roster.Snapshot {
		s := roster.Snapshot{ReadAt: at(readAt)}
		for _, owner := range slices.Sorted(maps.Keys(refreshes)) {
			x := b("u-"+owner, 443, roster.StateReady)
			x.OwnerID = owner
			s.Endpoints = append(s.Endpoints, x)
			s.Owners = append(s.Owners, roster.OwnerStatus{OwnerID: owner, LastRefreshAt: at(refreshes[owner])})
		}
		return s
	}
	owners := func(f *StaleFilter, s roster.Snapshot) []string {
		kept, _ := f.Filter(s)
		var out []string
		for _, x := range kept {
			out = append(out, x.OwnerID)
		}
		return out
	}
	newFilter := func() *StaleFilter {
		return NewStaleFilter(staleAfter*time.Second, confirmFor*time.Second)
	}
	all := []string{"a", "b", "c"}

	t.Run("an outage does not drop members one at a time as each crosses the limit", func(t *testing.T) {
		for readAt := 0; readAt <= 1000; readAt += 5 {
			s := snap(readAt, map[string]int{"a": 0, "b": 10, "c": 25})
			require.Equal(t, all, owners(newFilter(), s), "read at %d", readAt)
			kept, _ := FilterStaleOwners(s, staleAfter*time.Second)
			require.Len(t, kept, 3, "read at %d", readAt)
		}
	})

	t.Run("recovery drops nobody who refreshes within confirmFor", func(t *testing.T) {
		// Both for a filter that watched the outage and for one that starts in the middle of the recovery.
		for _, watched := range []bool{true, false} {
			f := newFilter()
			if watched {
				require.Equal(t, all, owners(f, snap(600, map[string]int{"a": 0, "b": 10, "c": 25})))
			}
			require.Equal(t, all, owners(f, snap(1000, map[string]int{"a": 1000, "b": 10, "c": 25})))
			require.Equal(t, all, owners(f, snap(1010, map[string]int{"a": 1000, "b": 1010, "c": 25})))
			require.Equal(t, all, owners(f, snap(1030, map[string]int{"a": 1030, "b": 1010, "c": 1025})))
			require.Equal(t, all, owners(f, snap(1070, map[string]int{"a": 1060, "b": 1070, "c": 1055})))
		}
	})

	t.Run("a crashed owner is dropped once the service has moved on by confirmFor", func(t *testing.T) {
		f := newFilter()
		require.Equal(t, all, owners(f, snap(200, map[string]int{"a": 200, "b": 200, "c": 0})))
		_, stale := f.Filter(snap(259, map[string]int{"a": 259, "b": 230, "c": 0}))
		require.Len(t, stale, 1)
		require.False(t, stale[0].Dropped)
		s := snap(260, map[string]int{"a": 260, "b": 260, "c": 0})
		kept, stale := f.Filter(s)
		require.Len(t, kept, 2)
		require.True(t, stale[0].Dropped)
		require.Len(t, s.Endpoints, 3, "the snapshot is not changed")
	})

	t.Run("a second outage does not run out the wait", func(t *testing.T) {
		f := newFilter()
		for readAt := 1000; readAt <= 2000; readAt += 50 {
			require.Equal(t, all, owners(f, snap(readAt, map[string]int{"a": 1000, "b": 10, "c": 25})))
		}
	})

	t.Run("a refresh starts the wait over", func(t *testing.T) {
		f := newFilter()
		require.Equal(t, all, owners(f, snap(1000, map[string]int{"a": 1000, "b": 1000, "c": 0})))
		// c refreshed at 900 unseen until now and is behind again.
		require.Equal(t, all, owners(f, snap(1100, map[string]int{"a": 1100, "b": 1100, "c": 900})))
		require.Equal(t, []string{"a", "b"}, owners(f, snap(1160, map[string]int{"a": 1160, "b": 1160, "c": 900})))
	})

	t.Run("long dead owners stay dropped when the rest go quiet", func(t *testing.T) {
		f := newFilter()
		owners(f, snap(1000, map[string]int{"a": 1000, "b": 1000, "c": 0}))
		owners(f, snap(1060, map[string]int{"a": 1060, "b": 1060, "c": 0}))
		require.Equal(t, []string{"a", "b"}, owners(f, snap(5000, map[string]int{"a": 1060, "b": 1060, "c": 0})))
	})
}
