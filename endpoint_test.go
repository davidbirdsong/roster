package roster

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func b(pod string, port uint16, state State) Endpoint {
	return Endpoint{
		OwnerID: "c1", Service: "g1", LogicalName: pod, InstanceID: pod,
		Address: "10.0.0.1", Port: port, Protocol: "https", SNI: "ingress.example.com", State: state,
	}
}

func TestStateSeverity(t *testing.T) {
	require.Greater(t, StateTerminating.Severity(), StateNotReady.Severity())
	require.Greater(t, StateNotReady.Severity(), StateReady.Severity())
}

func TestEndpointValidate(t *testing.T) {
	good := b("p1", 443, StateReady)
	require.NoError(t, good.Validate())

	mutate := map[string]func(*Endpoint){
		"missing owner":       func(x *Endpoint) { x.OwnerID = "" },
		"missing service":     func(x *Endpoint) { x.Service = "" },
		"missing stable name": func(x *Endpoint) { x.LogicalName = "" },
		"missing unique id":   func(x *Endpoint) { x.InstanceID = "" },
		"unique id too long":  func(x *Endpoint) { x.InstanceID = strings.Repeat("u", MaxInstanceIDLen+1) },
		"missing protocol":    func(x *Endpoint) { x.Protocol = "" },
		"unknown protocol":    func(x *Endpoint) { x.Protocol = "grpc" },
		"missing server name": func(x *Endpoint) { x.SNI = "" },
		"hostname address":    func(x *Endpoint) { x.Address = "ingress.example.com" },
		"empty address":       func(x *Endpoint) { x.Address = "" },
		"zero port":           func(x *Endpoint) { x.Port = 0 },
		"wildcard server":     func(x *Endpoint) { x.SNI = "*.example.com" },
		"ip server name":      func(x *Endpoint) { x.SNI = "10.0.0.1" },
		"unsafe server name":  func(x *Endpoint) { x.SNI = "a.example.com\nb" },
		"plaintext wildcard":  func(x *Endpoint) { x.Protocol, x.SNI = ProtocolH2C, "*.example.com" },
		"unknown state":       func(x *Endpoint) { x.State = "ABSENT" },
	}
	for name, m := range mutate {
		t.Run(name, func(t *testing.T) {
			x := good
			m(&x)
			require.Error(t, x.Validate())
		})
	}

	t.Run("region and zone are optional", func(t *testing.T) {
		x := good
		x.Region, x.Zone = "", ""
		require.NoError(t, x.Validate())
	})
	t.Run("the key is derived from the unique id and port", func(t *testing.T) {
		require.Equal(t, EndpointID("p1:443"), good.ID())
		other := good
		other.Port = 8443
		require.NotEqual(t, good.ID(), other.ID(), "each port of an instance is its own record")
		other = good
		other.LogicalName = "renamed"
		require.Equal(t, good.ID(), other.ID(), "the hash identity is not part of the key")
	})
	t.Run("ipv6 is accepted", func(t *testing.T) {
		x := good
		x.Address = "fd00::1"
		require.NoError(t, x.Validate())
	})
}

func TestProtocol(t *testing.T) {
	for p, tls := range map[Protocol]bool{ProtocolHTTP: false, ProtocolHTTPS: true, ProtocolH2C: false} {
		require.True(t, p.Valid(), p)
		require.Equal(t, tls, p.TLS(), p)
	}
	require.False(t, Protocol("").Valid())
	require.False(t, Protocol("grpc").Valid())
	require.False(t, Protocol("h2").Valid(), "h2 was retired: https negotiates it")
	require.False(t, Protocol("HTTPS").Valid(), "values are lowercase and exact")
}

func TestEndpointValidate_ServerNameDependsOnProtocol(t *testing.T) {
	for _, tc := range []struct {
		proto Protocol
		sni   string
		ok    bool
	}{
		{ProtocolHTTPS, "", false},
		{ProtocolHTTPS, "ingress.example.com", true},
		{ProtocolHTTP, "", true},
		{ProtocolH2C, "", true},
		{ProtocolH2C, "ingress.example.com", true}, // optional, so allowed
	} {
		x := b("p1", 443, StateReady)
		x.Protocol, x.SNI = tc.proto, tc.sni
		if tc.ok {
			require.NoError(t, x.Validate(), "%s %q", tc.proto, tc.sni)
		} else {
			require.Error(t, x.Validate(), "%s %q", tc.proto, tc.sni)
		}
	}
}

func TestEndpointEqual(t *testing.T) {
	a := b("p1", 443, StateReady)
	require.True(t, a.Equal(a))

	nilLabels, emptyLabels := a, a
	emptyLabels.Labels = map[string]string{}
	require.True(t, nilLabels.Equal(emptyLabels), "nil and empty labels are the same")

	x, y := a, a
	x.Labels = map[string]string{"tier": "web"}
	y.Labels = map[string]string{"tier": "web"}
	require.True(t, x.Equal(y), "equal content in different maps is equal")

	y.Labels = map[string]string{"tier": "api"}
	require.False(t, x.Equal(y), "a different value")
	y.Labels = map[string]string{"tier": "web", "zone": "a"}
	require.False(t, x.Equal(y), "an extra label")
	require.False(t, x.Equal(a), "labels against none")

	other := a
	other.Port = 8080
	require.False(t, a.Equal(other), "every other field still counts")
}

func TestValidateLabels(t *testing.T) {
	require.NoError(t, ValidateLabels(nil))
	require.NoError(t, ValidateLabels(map[string]string{
		"tier": "web", "ingress.example.com/zone": "a-1", "x_y.z": "v.1_2-3", "empty": "",
	}))

	many := map[string]string{}
	for i := range MaxLabels + 1 {
		many[strings.Repeat("k", 1)+strconv.Itoa(i)] = "v"
	}
	require.Error(t, ValidateLabels(many), "too many labels")

	for name, labels := range map[string]map[string]string{
		"empty key":           {"": "v"},
		"key starts with -":   {"-a": "v"},
		"key ends with /":     {"a/": "v"},
		"key with a space":    {"a b": "v"},
		"key with a newline":  {"a\nb": "v"},
		"key with a quote":    {`a"b`: "v"},
		"key too long":        {strings.Repeat("a", MaxLabelKeyLen+1): "v"},
		"value with a space":  {"k": "a b"},
		"value with a colon":  {"k": "a:b"},
		"value with a quote":  {"k": `a"b`},
		"value with newline":  {"k": "a\nb"},
		"value starts with -": {"k": "-a"},
		"value too long":      {"k": strings.Repeat("a", MaxLabelValueLen+1)},
		"unicode":             {"k": "café"},
	} {
		require.Error(t, ValidateLabels(labels), name)
	}
}

func TestEndpointValidate_RejectsBadLabels(t *testing.T) {
	x := b("p1", 443, StateReady)
	x.Labels = map[string]string{"tier": "web"}
	require.NoError(t, x.Validate())
	x.Labels = map[string]string{"tier": "we b"}
	require.Error(t, x.Validate())
}

func TestContentHashIncludesLabelsInAnyOrder(t *testing.T) {
	a := b("p1", 443, StateReady)
	withLabels := a
	withLabels.Labels = map[string]string{"a": "1", "b": "2"}
	reordered := a
	reordered.Labels = map[string]string{"b": "2", "a": "1"}

	require.NotEqual(t, ContentHash([]Endpoint{a}), ContentHash([]Endpoint{withLabels}))
	require.Equal(t, ContentHash([]Endpoint{withLabels}), ContentHash([]Endpoint{reordered}))

	changed := a
	changed.Labels = map[string]string{"a": "1", "b": "3"}
	require.NotEqual(t, ContentHash([]Endpoint{withLabels}), ContentHash([]Endpoint{changed}))

	// Key and value boundaries must not blur: {"a": "b=c"} is not {"a=b": "c"}.
	x, y := a, a
	x.Labels = map[string]string{"a": "b=c"}
	y.Labels = map[string]string{"a=b": "c"}
	require.NotEqual(t, ContentHash([]Endpoint{x}), ContentHash([]Endpoint{y}))
}

func TestEndpointValidate_ServiceMustBeUsableAsAKeySegment(t *testing.T) {
	for _, bad := range []string{"a/b", "a*", "a?", "a$b", "a b", "..", "."} {
		x := b("p1", 443, StateReady)
		x.Service = bad
		require.Error(t, x.Validate(), "service %q", bad)
	}
	x := b("p1", 443, StateReady)
	x.Service = "shard-a.1_x"
	require.NoError(t, x.Validate())
}

func TestValidSegment(t *testing.T) {
	require.NoError(t, ValidSegment("service", "web"))
	require.NoError(t, ValidSegment("owner ID", "i-0abc123:eu"))
	for _, bad := range []string{"", ".", "..", "a/b", "a*", "a?", "a$", "a b", "a\tb", "a\x7f", strings.Repeat("x", MaxSegmentLen+1)} {
		require.Error(t, ValidSegment("service", bad), "%q", bad)
	}
	require.NoError(t, ValidSegment("service", strings.Repeat("x", MaxSegmentLen)))
	require.ErrorContains(t, ValidSegment("owner ID", ""), "owner ID")
}

// Every string field needs a bound and a charset, or one publisher can store a value no reader can log,
// compare or write into a configuration. A NUL was the sharp case: it used to be accepted here and it used to
// make two different records hash the same.
func TestEndpointValidate_TextFieldsAreBoundedAndFreeOfControlCharacters(t *testing.T) {
	for name, mutate := range map[string]func(*Endpoint){
		"NUL in LogicalName":      func(x *Endpoint) { x.LogicalName = "a\x00b" },
		"NUL in InstanceID":       func(x *Endpoint) { x.InstanceID = "a\x00b" },
		"newline in LogicalName":  func(x *Endpoint) { x.LogicalName = "a\nb" },
		"tab in Region":           func(x *Endpoint) { x.Region = "us\teast" },
		"NUL in Zone":             func(x *Endpoint) { x.Zone = "a\x00b" },
		"LogicalName is too long": func(x *Endpoint) { x.LogicalName = strings.Repeat("a", MaxLogicalNameLen+1) },
		"Region is too long":      func(x *Endpoint) { x.Region = strings.Repeat("a", MaxRegionLen+1) },
		"Zone is too long":        func(x *Endpoint) { x.Zone = strings.Repeat("a", MaxZoneLen+1) },
	} {
		x := b("p1", 443, StateReady)
		mutate(&x)
		require.Error(t, x.Validate(), name)
	}
}

// These fields are not key segments, so the characters a key cannot carry are fine in them. An ARN style
// instance ID is the case that matters.
func TestEndpointValidate_TextFieldsAllowWhatAKeySegmentCannot(t *testing.T) {
	x := b("p1", 443, StateReady)
	x.InstanceID = "arn:aws:ecs:us-east-1:123456789012:task/cluster/0abc"
	x.LogicalName = "shard-a/0"
	require.NoError(t, x.Validate())
}
