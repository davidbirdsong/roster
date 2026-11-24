package policy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
)

func b(pod string, port uint16, state roster.State) roster.Endpoint {
	return roster.Endpoint{
		OwnerID: "c1", Service: "g1", LogicalName: pod, InstanceID: pod,
		Address: "10.0.0.1", Port: port, Protocol: "https", SNI: "ingress.example.com", State: state,
	}
}

func named(unique, stable string, state roster.State) roster.Endpoint {
	x := b(unique, 443, state)
	x.LogicalName = stable
	return x
}

func TestPreferEndpoint(t *testing.T) {
	t.Run("a unique stable name passes through", func(t *testing.T) {
		a, c := named("u1", "web-0", roster.StateReady), named("u2", "web-1", roster.StateNotReady)
		require.Equal(t, []roster.Endpoint{a, c}, PreferEndpoint([]roster.Endpoint{c, a}))
	})

	t.Run("a draining instance loses to its successor", func(t *testing.T) {
		old, successor := named("u1", "web-0", roster.StateTerminating), named("u2", "web-0", roster.StateReady)
		require.Equal(t, []roster.Endpoint{successor}, PreferEndpoint([]roster.Endpoint{old, successor}))
		require.Equal(t, []roster.Endpoint{successor}, PreferEndpoint([]roster.Endpoint{successor, old}))
	})

	t.Run("state order is ready, terminating, not ready", func(t *testing.T) {
		ready, notReady, term := named("u1", "n", roster.StateReady), named("u2", "n", roster.StateNotReady), named("u3", "n", roster.StateTerminating)
		require.Equal(t, []roster.Endpoint{ready}, PreferEndpoint([]roster.Endpoint{term, notReady, ready}))
		require.Equal(t, []roster.Endpoint{term}, PreferEndpoint([]roster.Endpoint{term, notReady}),
			"a starting successor must not cut off an instance that is still draining")
	})

	t.Run("a tie goes to the lowest id whatever the input order", func(t *testing.T) {
		a, c := named("u1", "n", roster.StateReady), named("u2", "n", roster.StateReady)
		require.Equal(t, []roster.Endpoint{a}, PreferEndpoint([]roster.Endpoint{a, c}))
		require.Equal(t, []roster.Endpoint{a}, PreferEndpoint([]roster.Endpoint{c, a}))
	})

	t.Run("ports, services and owners are separate names", func(t *testing.T) {
		base := named("u1", "n", roster.StateReady)
		otherPort, otherGroup, otherCluster := base, base, base
		otherPort.Port = 8443
		otherGroup.Service = "g2"
		otherCluster.OwnerID = "c2"
		require.Len(t, PreferEndpoint([]roster.Endpoint{base, otherPort, otherGroup, otherCluster}), 4)
	})

	t.Run("nothing in, nothing out", func(t *testing.T) {
		require.Empty(t, PreferEndpoint(nil))
	})
}
