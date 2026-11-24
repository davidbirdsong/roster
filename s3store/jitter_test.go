package s3store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Without jitter, consumers that start together (a deploy, or a shared outage that ends) stay in step for as
// long as they run, so a steady fleet wide rate arrives as a spike once per interval. Measuring the spread
// through the poll loop is too noisy to assert on, so the spreading itself is tested here.
func TestJitteredSpreadsTheWaitWithoutShorteningIt(t *testing.T) {
	const d = 100 * time.Millisecond
	seen := map[time.Duration]bool{}
	for range 500 {
		got := jittered(d)
		require.GreaterOrEqual(t, got, d, "jitter adds to the wait, it never shortens it")
		require.LessOrEqual(t, got, d+d/4, "jitter stays a small fraction, so the mean rate barely moves")
		seen[got] = true
	}
	require.Greater(t, len(seen), 50, "the wait varies rather than being a fixed interval")
}

// A zero or negative wait is left alone: NewMarkerPoller refuses those for the interval and the retry floor,
// and jitter must not turn a deliberate zero into a wait.
func TestJitteredLeavesANonPositiveWaitAlone(t *testing.T) {
	require.Zero(t, jittered(0))
	require.Equal(t, -time.Second, jittered(-time.Second))
}
