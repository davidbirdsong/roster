package roster

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestContentHash(t *testing.T) {
	a, c := b("p1", 443, StateReady), b("p2", 443, StateReady)

	t.Run("independent of input order", func(t *testing.T) {
		require.Equal(t, ContentHash([]Endpoint{a, c}), ContentHash([]Endpoint{c, a}))
	})
	t.Run("equal content is a duplicate", func(t *testing.T) {
		require.Equal(t, ContentHash([]Endpoint{a}), ContentHash([]Endpoint{a}))
	})
	t.Run("every field matters", func(t *testing.T) {
		base := ContentHash([]Endpoint{a})
		mutations := map[string]func(*Endpoint){
			"owner":       func(x *Endpoint) { x.OwnerID = "other" },
			"service":     func(x *Endpoint) { x.Service = "other" },
			"logicalName": func(x *Endpoint) { x.LogicalName = "other" },
			"region":      func(x *Endpoint) { x.Region = "us-east-1" },
			"address":     func(x *Endpoint) { x.Address = "10.9.9.9" },
			"port":        func(x *Endpoint) { x.Port = 9 },
			"protocol":    func(x *Endpoint) { x.Protocol = ProtocolH2C },
			"sni":         func(x *Endpoint) { x.SNI = "other" },
			"zone":        func(x *Endpoint) { x.Zone = "us-east-1b" },
			"state":       func(x *Endpoint) { x.State = StateTerminating },
			"serving":     func(x *Endpoint) { x.Serving = !x.Serving },
			"instanceId":  func(x *Endpoint) { x.InstanceID = "other" },
		}
		for name, m := range mutations {
			x := a
			m(&x)
			require.NotEqual(t, base, ContentHash([]Endpoint{x}), name)
		}
	})
	t.Run("adding or removing an endpoint changes it", func(t *testing.T) {
		require.NotEqual(t, ContentHash([]Endpoint{a}), ContentHash([]Endpoint{a, c}))
		require.NotEqual(t, ContentHash(nil), ContentHash([]Endpoint{a}))
	})
	t.Run("field boundaries are not ambiguous", func(t *testing.T) {
		x, y := a, a
		x.LogicalName, x.InstanceID = "ab", "c"
		y.LogicalName, y.InstanceID = "a", "bc"
		require.NotEqual(t, ContentHash([]Endpoint{x}), ContentHash([]Endpoint{y}))
	})
	// The hash must not rely on any byte being impossible in a value. These pairs collided while the fields
	// were joined with a NUL, so a real identity change moved no Revision and a consumer saw a duplicate.
	t.Run("a separator byte inside a value cannot forge another record", func(t *testing.T) {
		for name, pair := range map[string][2]func(*Endpoint){
			"logicalName and instanceID": {
				func(x *Endpoint) { x.LogicalName, x.InstanceID = "a\x00b", "c" },
				func(x *Endpoint) { x.LogicalName, x.InstanceID = "a", "b\x00c" },
			},
			"region and zone": {
				func(x *Endpoint) { x.Region, x.Zone = "x\x00y", "" },
				func(x *Endpoint) { x.Region, x.Zone = "x", "y" },
			},
			"label key and value": {
				func(x *Endpoint) { x.Labels = map[string]string{"k\x02v": ""} },
				func(x *Endpoint) { x.Labels = map[string]string{"k": "v"} },
			},
		} {
			x, y := a, a
			pair[0](&x)
			pair[1](&y)
			require.NotEqual(t, ContentHash([]Endpoint{x}), ContentHash([]Endpoint{y}), name)
		}
	})
	t.Run("a label count change is not hidden by the next record", func(t *testing.T) {
		x, y := a, a
		x.Labels = map[string]string{"a": "1", "b": "2"}
		y.Labels = map[string]string{"a": "1b2"}
		require.NotEqual(t, ContentHash([]Endpoint{x}), ContentHash([]Endpoint{y}))
	})
}

func TestNewSnapshot(t *testing.T) {
	at := time.Unix(100, 0)
	b1 := b("p1", 443, StateReady)
	b1.OwnerID = "c2"
	b2 := b("p2", 443, StateReady)
	b2.OwnerID = "c1"
	b3 := b("p3", 443, StateReady)
	b3.OwnerID = "c1"

	s := NewSnapshot(at, []Endpoint{b1, b3, b2}, []OwnerStatus{{OwnerID: "c2"}, {OwnerID: "c1"}}, nil, nil)

	require.Equal(t, []Endpoint{b2, b3, b1}, s.Endpoints, "sorted by owner then id")
	require.Equal(t, "c1", s.Owners[0].OwnerID)
	require.True(t, s.Complete)
	require.Equal(t, at, s.ReadAt)
	require.Equal(t, ContentHash(s.Endpoints), s.ContentHash)
}

func TestSetRevisionAssignsTheNumberAndItsDecimalString(t *testing.T) {
	s := NewSnapshot(time.Time{}, nil, nil, nil, nil)
	require.Zero(t, s.Generation, "a snapshot has no revision until its reader assigns one")
	require.Empty(t, s.Revision)
	s.SetRevision(12)
	require.Equal(t, uint64(12), s.Generation)
	require.Equal(t, "12", s.Revision)
}

func TestNewSnapshotKeepsWhatTheReaderRejected(t *testing.T) {
	bad := errors.New("not a state key")
	s := NewSnapshot(time.Time{}, nil, nil, nil, []RejectedEndpoint{{Key: "v1/x", Err: bad}})
	require.True(t, s.Complete, "a rejected record does not make the snapshot incomplete")
	require.Len(t, s.Rejected, 1)
	require.Equal(t, "v1/x", s.Rejected[0].Key)
}

func TestNewSnapshot_Completeness(t *testing.T) {
	bad := errors.New("bad")
	require.True(t, NewSnapshot(time.Time{}, nil, nil, nil, nil).Complete)
	require.False(t, NewSnapshot(time.Time{}, nil, nil,
		[]SkippedItem{{Service: "g", OwnerID: "c1", Key: "v1/g/c1", Err: bad}}, nil).Complete,
		"a skipped document may hold live endpoints, so absence must not mean deletion")
}

func TestSnapshot_ByService(t *testing.T) {
	x, y, z := b("p1", 443, StateReady), b("p2", 443, StateReady), b("p3", 443, StateReady)
	y.Service = "g2"
	g := NewSnapshot(time.Time{}, []Endpoint{z, y, x}, nil, nil, nil).ByService()
	require.Equal(t, []Endpoint{x, z}, g["g1"])
	require.Equal(t, []Endpoint{y}, g["g2"])
}

func TestHintKindString(t *testing.T) {
	require.Equal(t, "rescan", HintRescan.String())
	require.Equal(t, "unknown", HintKind(99).String())
}
