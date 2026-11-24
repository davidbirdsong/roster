package policy

import (
	"cmp"
	"slices"

	"github.com/davidbirdsong/roster"
)

// PreferEndpoint returns one endpoint for each (owner, service, LogicalName, port) in
// endpoints, so a consumer that hashes on LogicalName never sees a name twice.
//
// Several records can share a LogicalName: a draining instance and its successor,
// or a publisher that reuses a name. HAProxy cannot hold two servers under one
// id, so a consumer has to pick, and this is the one rule every consumer should
// share. The winner is the most useful state: READY, then TERMINATING, then
// NOT_READY. A draining instance outranks one that is still starting because it
// still carries connections and the starting one carries none. Ties go to the
// lowest ID, so the choice never depends on input order. The result is sorted by owner, service, LogicalName and port.
//
// A consumer that wants to keep serving the draining instance can read the
// unresolved endpoints instead. Consumers that need a name unique across
// owners should combine OwnerID with LogicalName.
func PreferEndpoint(endpoints []roster.Endpoint) []roster.Endpoint {
	type key struct {
		owner, service, stable string
		port                   uint16
	}
	best := map[key]roster.Endpoint{}
	for _, b := range endpoints {
		k := key{b.OwnerID, b.Service, b.LogicalName, b.Port}
		if have, ok := best[k]; ok && !better(b, have) {
			continue
		}
		best[k] = b
	}
	out := make([]roster.Endpoint, 0, len(best))
	for _, b := range best {
		out = append(out, b)
	}
	slices.SortFunc(out, func(a, b roster.Endpoint) int {
		return cmp.Or(
			cmp.Compare(a.OwnerID, b.OwnerID),
			cmp.Compare(a.Service, b.Service),
			cmp.Compare(a.LogicalName, b.LogicalName),
			cmp.Compare(a.Port, b.Port),
		)
	})
	return out
}

// better reports whether a should replace b.
func better(a, b roster.Endpoint) bool {
	if ra, rb := useRank(a.State), useRank(b.State); ra != rb {
		return ra < rb
	}
	return a.ID() < b.ID()
}

// useRank orders states by how useful they are to a consumer, best first. It is not Severity, which
// orders how bad a state is.
func useRank(s roster.State) int {
	switch s {
	case roster.StateReady:
		return 0
	case roster.StateTerminating:
		return 1
	}
	return 2
}
