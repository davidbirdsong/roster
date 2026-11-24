package s3store

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateKeyAndParseKeyAreInverses(t *testing.T) {
	for _, prefix := range []string{"", "roster/", "a/b/"} {
		cfg := Config{Bucket: "b", KeyPrefix: prefix}
		key, err := cfg.DocumentKey("v1", "web", "i-0abc")
		require.NoError(t, err)
		require.Equal(t, prefix+"v1/web/i-0abc", key)
		major, service, owner, err := cfg.parseKey(key)
		require.NoError(t, err)
		require.Equal(t, []string{"v1", "web", "i-0abc"}, []string{major, service, owner})
	}
}

func TestStateKeyRefusesWhatCannotBeASegment(t *testing.T) {
	cfg := Config{Bucket: "b"}
	for _, bad := range []string{"", "a/b", "a*", "a?", "a$b", "a b", "a\tb", ".", "..", strings.Repeat("x", 256)} {
		_, err := cfg.DocumentKey("v1", bad, "c")
		require.Error(t, err, "service %q", bad)
		_, err = cfg.DocumentKey("v1", "g", bad)
		require.Error(t, err, "owner %q", bad)
	}
	for _, bad := range []string{"", "1", "v", "vx", "v1/", "V1"} {
		_, err := cfg.DocumentKey(bad, "g", "c")
		require.Error(t, err, "major %q", bad)
	}
}

func TestParseKeyRefusesAnythingButThreeValidSegments(t *testing.T) {
	cfg := Config{Bucket: "b", KeyPrefix: "r/"}
	for _, bad := range []string{
		"v1/g/c",         // outside the prefix
		"r/v1/g",         // two segments
		"r/v1/g/c/extra", // four segments
		"r/v1//c",        // empty service
		"r/v1/g/",        // empty owner
		"r/x1/g/c",       // not a major
		"r/v1/g*/c",      // wildcard in a segment
		"r/v1/g/c d",     // whitespace
		"r/marker",       // not a state key
		"r/v1/g/../c",    // four segments
		"r/v1/./c",       // dot segment
		"r/v1/g/" + "..", // dot dot segment
	} {
		_, _, _, err := cfg.parseKey(bad)
		require.Error(t, err, "key %q", bad)
	}
}

func TestConfigValidation(t *testing.T) {
	require.Error(t, Config{}.Validate())
	require.Error(t, Config{Bucket: "b", KeyPrefix: "no-slash"}.Validate())
	require.Error(t, Config{Bucket: "b", KeyPrefix: "/leading/"}.Validate())
	require.NoError(t, Config{Bucket: "b"}.Validate())
	require.NoError(t, Config{Bucket: "b", KeyPrefix: "roster/"}.Validate())
	require.Equal(t, "roster/marker", Config{Bucket: "b", KeyPrefix: "roster/"}.MarkerKey())
}

func TestMajorsOrderNumerically(t *testing.T) {
	require.Greater(t, majorNumber("v10"), majorNumber("v9"))
	require.Greater(t, majorNumber("v2"), majorNumber("v1"))
}
