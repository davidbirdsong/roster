package s3store_test

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/rostertest"
	"github.com/davidbirdsong/roster/s3store"
	"github.com/davidbirdsong/roster/s3store/s3fake"
)

func TestMarkerNotifyMarksForAChangeAndNotForARefresh(t *testing.T) {
	f := s3fake.New()
	m, err := s3store.NewMarker(f, cfg, "c1")
	require.NoError(t, err)
	note := func(kinds ...roster.WriteKind) roster.Notification {
		n := roster.Notification{Owner: "c1"}
		for _, k := range kinds {
			n.Writes = append(n.Writes, roster.ServiceWrite{Service: "web", Kind: k})
		}
		return n
	}

	require.Zero(t, m.Notify(t.Context(), note(roster.WriteUnchanged, roster.WriteUnchanged), roster.AckNone))
	require.Zero(t, f.Calls("PutObject"), "a refresh leaves readers alone")

	for i, kind := range []roster.WriteKind{roster.WritePut, roster.WriteDeleted} {
		got := m.Notify(t.Context(), note(roster.WriteUnchanged, kind), roster.AckNone)
		require.Equal(t, roster.Receipt{Reached: roster.AckAccepted}, got)
		require.Equal(t, i+1, f.Calls("PutObject"), "one mark per notification with a change")
	}

	f.SetFault(func(op, _ string) error { return errors.New("denied") })
	got := m.Notify(t.Context(), note(roster.WritePut), roster.AckReceived)
	require.Equal(t, roster.AckNone, got.Reached)
	require.ErrorContains(t, got.Err, "denied")
}

// docs/internals/TODO.md item 3: a restarted publisher learns what is stored from ListOwned, so a Reconcile of identical content
// is no change and does not mark.
func TestARestartedPublisherDoesNotMarkIdenticalContent(t *testing.T) {
	f := s3fake.New()
	desired := []roster.Endpoint{be("c1", "web", "a", roster.StateReady), be("c1", "api", "b", roster.StateReady)}
	start := func() (*roster.Publisher, *markCounter) {
		marks := newMarkCounter(t, f)
		p, err := roster.NewPublisher(newStore(t, f), "c1", roster.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), roster.WithHintNotifier(marks))
		require.NoError(t, err)
		return p, marks
	}

	first, marks := start()
	require.NoError(t, first.Reconcile(t.Context(), desired))
	require.Equal(t, 1, marks.count(), "one mark per call, however many services it wrote")

	restarted, marks := start()
	require.NoError(t, restarted.Reconcile(t.Context(), desired))
	require.Zero(t, marks.count())
}

func TestAMarkFailureIsLoggedByTheWriterAndDoesNotFailTheWrite(t *testing.T) {
	f := s3fake.New()
	log := &rostertest.Logger{}
	marks := newMarkCounter(t, f)
	marks.err = errors.New("marker is down")
	r, err := roster.NewRegistration(newStore(t, f), "c1", "web", roster.WithRegistrationLogger(log),
		roster.WithRegistrationHintNotifier(marks))
	require.NoError(t, err)

	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{be("", "", "a", roster.StateReady)}))
	require.Equal(t, []string{"v1/web/c1"}, f.Keys())
	var warned bool
	for _, e := range log.Entries() {
		if e.Level == "warn" {
			warned = true
			require.ErrorContains(t, e.Fields["err"].(error), "marker is down")
		}
	}
	require.True(t, warned)
}
