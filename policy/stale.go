package policy

import (
	"slices"
	"time"

	"github.com/davidbirdsong/roster"
)

// StaleOwner describes an owner whose last reconcile is older than the stale limit.
type StaleOwner struct {
	OwnerID       string
	LastRefreshAt time.Time
	Age           time.Duration
	// Dropped is false when some of the owner's records were kept, because its service has no newer writes to
	// judge it against or because a StaleFilter has not confirmed it yet.
	Dropped bool
}

// FilterStaleOwners is one use of a StaleFilter with no confirmation, for a caller that keeps no state between
// reads. It is safe while a service cannot be written, but not just after: the first owner to refresh makes
// the ones that have not refreshed yet look lost. A consumer that routes traffic should keep a StaleFilter.
func FilterStaleOwners(snap roster.Snapshot, staleAfter time.Duration) ([]roster.Endpoint, []StaleOwner) {
	return NewStaleFilter(staleAfter, 0).Filter(snap)
}

// StaleFilter drops the endpoints of owners that stopped reconciling. An owner that is decommissioned or lost
// leaves its records behind, and nothing else removes them promptly. A record that is TERMINATING is dropped like
// any other: nothing rewrites it once its publisher is gone, so it ages with the owner, which makes the stale
// limit a deadline for a drain that no publisher is refreshing.
//
// An owner is stale when its last reconcile is more than staleAfter before the snapshot's ReadAt. Its records in
// a service are dropped only if it is also more than staleAfter behind the newest reconcile among the owners in
// that service. When nobody in a service can write (S3 refuses writes, a grant is broken, the reader's clock is
// wrong), its owners stop together, none falls behind another, and the service keeps its members. Dropping by age
// alone would remove them one at a time as each crossed the limit. The rule is per service because an owner is
// usually one member of one pool, so a live member of some other pool says nothing about this one.
//
// When writes resume, the first owner to refresh is far ahead of those that have not had their turn, and one
// snapshot cannot tell them from owners that crashed. So an owner is dropped only once its service's newest
// reconcile has moved on by confirmFor since the filter first saw that owner behind. A live owner refreshes
// within its interval and is forgotten. Set confirmFor longer than the slowest refresh interval in the roster,
// jitter and retries included. It is measured in the write times of the service and not by a clock, so a second
// outage during the wait does not run it out. A lost owner is dropped about staleAfter plus confirmFor after its
// last reconcile, and a new filter waits confirmFor before its first drop.
//
// A StaleFilter reads no clock, belongs to one Reader, and is not safe for concurrent use.
type StaleFilter struct {
	staleAfter, confirmFor time.Duration
	behind                 map[membership]sighting
}

// membership is one owner in one service.
type membership struct{ service, owner string }

// sighting is what a StaleFilter saw when it first found an owner behind in a service.
type sighting struct {
	lastRefresh time.Time // the owner's, so that a refresh starts over
	newest      time.Time // the service's
}

// NewStaleFilter returns a StaleFilter. A zero or negative staleAfter keeps every owner, and a zero or negative
// confirmFor drops without waiting.
func NewStaleFilter(staleAfter, confirmFor time.Duration) *StaleFilter {
	return &StaleFilter{staleAfter: staleAfter, confirmFor: confirmFor}
}

// Filter returns the endpoints of snap less those it drops, and one StaleOwner for each owner it found stale, in
// the order of snap.Owners. A snapshot with no read time keeps every owner, and an owner with no reconcile time
// has not completed one yet (its first may be running), so it is never stale. Call it with every snapshot, in order.
//
// When nothing is dropped the result is snap.Endpoints itself. Otherwise it is a new slice.
func (f *StaleFilter) Filter(snap roster.Snapshot) ([]roster.Endpoint, []StaleOwner) {
	if f.staleAfter <= 0 || snap.ReadAt.IsZero() {
		return snap.Endpoints, nil
	}
	var stale []StaleOwner
	refreshed := map[string]time.Time{}
	staleSet := map[string]bool{}
	for _, c := range snap.Owners {
		if c.LastRefreshAt.IsZero() {
			continue
		}
		refreshed[c.OwnerID] = c.LastRefreshAt
		if age := snap.ReadAt.Sub(c.LastRefreshAt); age > f.staleAfter {
			stale = append(stale, StaleOwner{OwnerID: c.OwnerID, LastRefreshAt: c.LastRefreshAt, Age: age})
			staleSet[c.OwnerID] = true
		}
	}

	// The newest reconcile among the owners with records in each service.
	newest := map[string]time.Time{}
	for _, b := range snap.Endpoints {
		if t := refreshed[b.OwnerID]; t.After(newest[b.Service]) {
			newest[b.Service] = t
		}
	}
	// Rebuilt each time, so an owner that caught up or left is forgotten.
	behind := map[membership]sighting{}
	drop := map[membership]bool{}
	kept := map[string]bool{}
	for _, b := range snap.Endpoints {
		if !staleSet[b.OwnerID] {
			continue
		}
		last, latest := refreshed[b.OwnerID], newest[b.Service]
		if latest.Sub(last) <= f.staleAfter {
			kept[b.OwnerID] = true
			continue
		}
		m := membership{b.Service, b.OwnerID}
		seen, ok := f.behind[m]
		if !ok || !seen.lastRefresh.Equal(last) {
			seen = sighting{lastRefresh: last, newest: latest}
		}
		behind[m] = seen
		if latest.Sub(seen.newest) >= f.confirmFor {
			drop[m] = true
		} else {
			kept[b.OwnerID] = true
		}
	}
	f.behind = behind
	for i := range stale {
		stale[i].Dropped = !kept[stale[i].OwnerID]
	}
	if len(drop) == 0 {
		return snap.Endpoints, stale
	}
	return slices.DeleteFunc(slices.Clone(snap.Endpoints), func(b roster.Endpoint) bool {
		return drop[membership{b.Service, b.OwnerID}]
	}), stale
}
