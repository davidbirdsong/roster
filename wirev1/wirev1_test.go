package wirev1_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/wirev1"
)

type expected struct {
	Owner     string            `json:"owner"`
	Service   string            `json:"service"`
	Endpoints []roster.Endpoint `json:"endpoints"`
	Rejected  []struct {
		Index      int    `json:"index"`
		InstanceID string `json:"instance_id"`
	} `json:"rejected"`
}

// corpusInputs returns the frozen objects, without their expected results.
func corpusInputs(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("testdata/*.json")
	require.NoError(t, err)
	var out []string
	for _, f := range files {
		if !strings.HasSuffix(f, ".expected.json") {
			out = append(out, f)
		}
	}
	require.NotEmpty(t, out, "the corpus is empty")
	return out
}

// Every frozen object must decode, with this version of the code, to the result stored beside it. This is what
// keeps readers compatible with everything a publisher ever wrote.
func TestCorpusDecodesToItsExpectedResult(t *testing.T) {
	for _, in := range corpusInputs(t) {
		t.Run(filepath.Base(in), func(t *testing.T) {
			data, err := os.ReadFile(in)
			require.NoError(t, err)
			raw, err := os.ReadFile(strings.TrimSuffix(in, ".json") + ".expected.json")
			require.NoError(t, err, "every frozen object needs an expected result")
			var want expected
			require.NoError(t, json.Unmarshal(raw, &want))

			got, rejected, err := wirev1.Decode(want.Owner, want.Service, data)
			require.NoError(t, err)
			require.Len(t, got, len(want.Endpoints))
			for i := range got {
				require.True(t, got[i].Equal(want.Endpoints[i]),
					"endpoint %d:\n got %+v\nwant %+v", i, got[i], want.Endpoints[i])
			}
			require.Len(t, rejected, len(want.Rejected), "rejections: %v", rejected)
			for i, r := range rejected {
				require.Equal(t, want.Rejected[i].Index, r.Index)
				require.Equal(t, want.Rejected[i].InstanceID, r.InstanceID)
				require.Error(t, r.Err)
			}
		})
	}
}

// A frozen file must never change, and a new one must be added to corpus.sum, which only ever grows. A reader
// that decodes the old bytes today must decode them tomorrow.
func TestTheCorpusIsFrozen(t *testing.T) {
	sum, err := os.ReadFile("testdata/corpus.sum")
	require.NoError(t, err)
	listed := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(sum)), "\n") {
		hash, name, ok := strings.Cut(line, "  ")
		require.True(t, ok, "bad corpus.sum line %q", line)
		listed[name] = hash
	}
	files, err := filepath.Glob("testdata/*.json")
	require.NoError(t, err)
	for _, f := range files {
		name := filepath.Base(f)
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		h := sha256.Sum256(data)
		got := hex.EncodeToString(h[:])
		want, ok := listed[name]
		require.True(t, ok, "%s is not in corpus.sum: append the line\n%s  %s", name, got, name)
		require.Equal(t, want, got, "%s is frozen and was edited. Add a new file instead of changing it", name)
		delete(listed, name)
	}
	require.Empty(t, listed, "files are listed in corpus.sum but missing: a frozen file must never be removed")
}

func TestFieldsIgnoredByAnOlderReaderDoNotChangeTheResult(t *testing.T) {
	data := []byte(`{"endpoints":[{"instance_id":"a","logical_name":"a","address":"10.0.0.1","port":80,"protocol":"http","state":"READY","serving":true,"future":{"x":[1,2]}}],"future_top":true}`)
	got, rejected, err := wirev1.Decode("c", "g", data)
	require.NoError(t, err)
	require.Empty(t, rejected)
	require.Len(t, got, 1)
}

func TestAnObjectThatIsNotJSONIsAnErrorNotAnEmptyService(t *testing.T) {
	for _, bad := range []string{``, `not json`, `{"endpoints": [`, `{"endpoints":[]} trailing`, `[]`} {
		_, _, err := wirev1.Decode("c", "g", []byte(bad))
		require.Error(t, err, "%q", bad)
	}
}

func TestAnEmptyServiceIsValid(t *testing.T) {
	got, rejected, err := wirev1.Decode("c", "g", []byte(`{"endpoints":[]}`))
	require.NoError(t, err)
	require.Empty(t, got)
	require.Empty(t, rejected)
}

func sample() []roster.Endpoint {
	return []roster.Endpoint{
		{
			OwnerID: "c1", Service: "g", LogicalName: "b", InstanceID: "u2", Address: "10.0.0.2",
			Port: 8080, Protocol: roster.ProtocolHTTP, State: roster.StateReady, Serving: true,
			Labels: map[string]string{"z": "1", "a": "2", "m": "3"},
		},
		{
			OwnerID: "c1", Service: "g", LogicalName: "a", InstanceID: "u1", Address: "10.0.0.1",
			Port: 443, Protocol: roster.ProtocolHTTPS, SNI: "x.example.com",
			State: roster.StateTerminating, Serving: false,
			Region: "us-east-1", Zone: "us-east-1a",
		},
	}
}

func TestEncodeThenDecodeGivesTheSameEndpoints(t *testing.T) {
	data, err := wirev1.Encode("w/1", sample())
	require.NoError(t, err)
	got, rejected, err := wirev1.Decode("c1", "g", data)
	require.NoError(t, err)
	require.Empty(t, rejected)
	require.Len(t, got, 2)
	want := sample()
	// Decode sorts by ID, so u1 comes first.
	require.True(t, got[0].Equal(want[1]), "%+v", got[0])
	require.True(t, got[1].Equal(want[0]), "%+v", got[1])
}

// An unchanged service must produce identical bytes, whatever order the endpoints arrive in and however Go orders
// a map, or every refresh would look like a change.
func TestEncodingIsByteStable(t *testing.T) {
	first, err := wirev1.Encode("w/1", sample())
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		b := sample()
		if i%2 == 0 {
			b[0], b[1] = b[1], b[0]
		}
		again, err := wirev1.Encode("w/1", b)
		require.NoError(t, err)
		require.True(t, bytes.Equal(first, again), "run %d differs:\n%s\n%s", i, first, again)
	}
}

func TestServingIsAlwaysWrittenSoFalseIsNotAbsent(t *testing.T) {
	data, err := wirev1.Encode("", sample())
	require.NoError(t, err)
	require.Contains(t, string(data), `"serving": false`)
	require.Contains(t, string(data), `"serving": true`)
}

func TestEncodeRefusesWhatADecoderWouldReject(t *testing.T) {
	dup := append(sample(), sample()[0])
	_, err := wirev1.Encode("", dup)
	require.ErrorContains(t, err, "duplicate")

	bad := sample()
	bad[0].State = "DRAINING"
	_, err = wirev1.Encode("", bad)
	require.ErrorContains(t, err, "state")

	bad = sample()
	bad[0].Protocol = "grpc"
	_, err = wirev1.Encode("", bad)
	require.ErrorContains(t, err, "protocol")
}

func TestNothingTimeVaryingIsInTheObject(t *testing.T) {
	data, err := wirev1.Encode("w/1", sample())
	require.NoError(t, err)
	var generic map[string]any
	require.NoError(t, json.Unmarshal(data, &generic))
	keys := make([]string, 0, len(generic))
	for k := range generic {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	require.Equal(t, []string{"endpoints", "writer"}, keys)
}

// Go matches JSON names case insensitively, so two names that differ only by case would decode into one field.
func TestFieldNamesDifferByMoreThanCase(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf(wirev1.ServiceDocument{}), reflect.TypeOf(wirev1.Endpoint{})} {
		seen := map[string]string{}
		for i := 0; i < typ.NumField(); i++ {
			name, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
			require.NotEmpty(t, name, "%s.%s needs a json name", typ.Name(), typ.Field(i).Name)
			lower := strings.ToLower(name)
			prev, dup := seen[lower]
			require.False(t, dup, "%s: %q and %q differ only by case", typ.Name(), prev, name)
			seen[lower] = name
		}
	}
}
