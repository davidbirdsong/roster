package roster_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/rostertest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func notifiedRegistration(t *testing.T, store *rostertest.Store, opts ...roster.RegistrationOption) *roster.Registration {
	t.Helper()
	r, err := roster.NewRegistration(store, "i-1", "web", append([]roster.RegistrationOption{roster.WithRegistrationLogger(quiet)}, opts...)...)
	require.NoError(t, err)
	return r
}

func notifiedPublisher(t *testing.T, store *rostertest.Store, opts ...roster.PublisherOption) *roster.Publisher {
	t.Helper()
	p, err := roster.NewPublisher(store, "c1", append([]roster.PublisherOption{roster.WithLogger(quiet)}, opts...)...)
	require.NoError(t, err)
	require.NoError(t, p.Reconcile(t.Context(), nil))
	return p
}

// kinds summarises a notification as service to kind.
func kinds(n roster.Notification) map[string]roster.WriteKind {
	out := map[string]roster.WriteKind{}
	for _, w := range n.Writes {
		out[w.Service] = w.Kind
	}
	return out
}

func TestNotify_RefusedBeforeAnythingIsWritten(t *testing.T) {
	var rc roster.Receipt
	for name, call := range map[string]func(*roster.Registration) error{
		"want without a primary": func(r *roster.Registration) error {
			return r.Register(t.Context(), []roster.Endpoint{member("u1")}, roster.Want(roster.AckReceived, &rc))
		},
		"nil receipt": func(r *roster.Registration) error {
			return r.Register(t.Context(), []roster.Endpoint{member("u1")}, roster.Want(roster.AckReceived, nil))
		},
		"unknown level": func(r *roster.Registration) error {
			return r.Register(t.Context(), []roster.Endpoint{member("u1")}, roster.Want(roster.Ack(9), &rc))
		},
		"preserve services": func(r *roster.Registration) error {
			return r.Register(t.Context(), []roster.Endpoint{member("u1")}, roster.PreserveServices("web"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := rostertest.NewStore()
			require.Error(t, call(notifiedRegistration(t, store)))
			require.Empty(t, store.Puts)
		})
	}

	store := rostertest.NewStore()
	p := notifiedPublisher(t, store)
	require.Error(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web")}, roster.Want(roster.AckAccepted, &rc)))
	require.Empty(t, store.Puts)
}

func TestNotify_ConstructorRefusesBadNotifiers(t *testing.T) {
	store := rostertest.NewStore()
	n := rostertest.NewNotifier()
	_, err := roster.NewRegistration(store, "i-1", "web", roster.WithRegistrationNotifier(n), roster.WithRegistrationNotifier(n))
	require.ErrorContains(t, err, "at most one primary")
	_, err = roster.NewRegistration(store, "i-1", "web", roster.WithRegistrationHintNotifier(nil))
	require.Error(t, err)
	_, err = roster.NewRegistration(store, "i-1", "web", roster.WithRegistrationHintTimeout(0))
	require.Error(t, err)
	_, err = roster.NewPublisher(store, "c1", roster.WithNotifier(n), roster.WithNotifier(n))
	require.ErrorContains(t, err, "at most one primary")
	_, err = roster.NewPublisher(store, "c1", roster.WithNotifier(nil))
	require.Error(t, err)
}

// Rule 1: a primary that waits for evidence must not hold up the next call on the same writer.
func TestNotify_ABlockedPrimaryHoldsUpNoOtherCall(t *testing.T) {
	store := rostertest.NewStore()
	primary := rostertest.NewNotifier()
	r := notifiedRegistration(t, store, roster.WithRegistrationNotifier(primary))
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))

	primary.Hold()
	defer primary.Release()
	var rc roster.Receipt
	done := make(chan error, 1)
	go func() { done <- r.Refresh(t.Context(), roster.Want(roster.AckReceived, &rc)) }()
	require.Eventually(t, func() bool { return len(primary.Calls()) == 2 }, time.Second, time.Millisecond)
	require.NoError(t, r.Refresh(t.Context()), "the lock is free while the primary waits")
	require.Len(t, r.Endpoints(), 1)
	primary.Release()
	require.NoError(t, <-done)

	pstore := rostertest.NewStore()
	pprimary := rostertest.NewNotifier()
	p := notifiedPublisher(t, pstore, roster.WithNotifier(pprimary))
	pprimary.Hold()
	defer pprimary.Release()
	go func() {
		done <- p.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web")}, roster.Want(roster.AckReceived, &rc))
	}()
	require.Eventually(t, func() bool { return len(pprimary.Calls()) == 1 }, time.Second, time.Millisecond)
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web")}),
		"a Publish with nothing to write runs while the primary waits")
	pprimary.Release()
	require.NoError(t, <-done)
}

// Rule 2: a notifier reports evidence, and never changes the call's error or what the writer remembers.
func TestNotify_APrimaryFailureIsOnlyEvidence(t *testing.T) {
	store := rostertest.NewStore()
	primary := rostertest.NewNotifier()
	primary.Receipt = roster.Receipt{Reached: roster.AckAccepted, Refused: true, Err: errors.New("NACK")}
	r := notifiedRegistration(t, store, roster.WithRegistrationNotifier(primary))

	var rc roster.Receipt
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}, roster.Want(roster.AckReceived, &rc)))
	require.Equal(t, primary.Receipt, rc)
	require.Len(t, r.Endpoints(), 1)
	require.Equal(t, roster.AckReceived, primary.Calls()[0].Want)
}

func TestNotify_WithoutWantThePrimaryIsAskedForNothing(t *testing.T) {
	primary := rostertest.NewNotifier()
	r := notifiedRegistration(t, rostertest.NewStore(), roster.WithRegistrationNotifier(primary))
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))
	calls := primary.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, roster.AckNone, calls[0].Want)
	require.Equal(t, map[string]roster.WriteKind{"web": roster.WritePut}, kinds(calls[0].Notification))
	require.Equal(t, "i-1", calls[0].Notification.Owner)
}

func TestNotify_OnlyWhatWasStoredIsListed(t *testing.T) {
	store := rostertest.NewStore()
	hint := rostertest.NewNotifier()
	p := notifiedPublisher(t, store, roster.WithHintNotifier(hint))

	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web"), endpoint("b", "api")}))
	changed := endpoint("a", "web")
	changed.State = roster.StateTerminating
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{changed, endpoint("b", "api")}))
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{changed, endpoint("b", "api")}),
		"nothing to write, nothing to notify")

	calls := hint.Calls()
	require.Len(t, calls, 2)
	require.Equal(t, map[string]roster.WriteKind{"api": roster.WritePut, "web": roster.WritePut}, kinds(calls[0].Notification))
	require.Equal(t, "api", calls[0].Notification.Writes[0].Service, "writes are sorted by service")
	require.Equal(t, map[string]roster.WriteKind{"web": roster.WritePut}, kinds(calls[1].Notification))
	require.Equal(t, roster.StateTerminating, calls[1].Notification.Writes[0].Endpoints[0].State)
}

func TestNotify_PartialFailureListsOnlyTheWritesThatSucceeded(t *testing.T) {
	store := rostertest.NewStore()
	store.PutFails = func(_, service string) bool { return service == "api" }
	hint := rostertest.NewNotifier()
	p := notifiedPublisher(t, store, roster.WithHintNotifier(hint))

	require.Error(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web"), endpoint("b", "api")}))
	calls := hint.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, map[string]roster.WriteKind{"web": roster.WritePut}, kinds(calls[0].Notification))
}

func TestNotify_NoWriteNoNotification(t *testing.T) {
	primary := rostertest.NewNotifier()
	stale := roster.Receipt{Reached: roster.AckReceived}

	store := rostertest.NewStore()
	p, err := roster.NewPublisher(store, "c1", roster.WithLogger(quiet), roster.WithNotifier(primary))
	require.NoError(t, err)
	rc := stale
	require.ErrorIs(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web")}, roster.Want(roster.AckReceived, &rc)),
		roster.ErrNotInitialized)
	require.Zero(t, rc, "a receipt from an earlier call does not survive")

	store.ListErr = errors.New("list failed")
	rc = stale
	require.Error(t, p.Reconcile(t.Context(), nil, roster.Want(roster.AckReceived, &rc)))
	require.Zero(t, rc)

	store.ListErr = nil
	require.NoError(t, p.Reconcile(t.Context(), nil))
	bad := endpoint("a", "web")
	bad.Port = 0
	require.Error(t, p.Publish(t.Context(), []roster.Endpoint{bad}))
	require.Empty(t, primary.Calls())
}

func TestNotify_DeletesAreNamed(t *testing.T) {
	store := rostertest.NewStore()
	hint := rostertest.NewNotifier()
	p := notifiedPublisher(t, store, roster.WithHintNotifier(hint))
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web"), endpoint("b", "api")}))
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web")}))
	last := hint.Calls()[1].Notification
	require.Equal(t, map[string]roster.WriteKind{"api": roster.WriteDeleted}, kinds(last))
	require.Nil(t, last.Writes[0].Endpoints)

	rhint := rostertest.NewNotifier()
	r := notifiedRegistration(t, rostertest.NewStore(), roster.WithRegistrationHintNotifier(rhint))
	require.NoError(t, r.Deregister(t.Context()), "the delete is issued even with no Register")
	require.Equal(t, map[string]roster.WriteKind{"web": roster.WriteDeleted}, kinds(rhint.Calls()[0].Notification))
}

func TestNotify_UnchangedReachesNotifiersOnlyWithWant(t *testing.T) {
	store := rostertest.NewStore()
	primary, hint := rostertest.NewNotifier(), rostertest.NewNotifier()
	r := notifiedRegistration(t, store, roster.WithRegistrationNotifier(primary), roster.WithRegistrationHintNotifier(hint))
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))
	require.Len(t, primary.Calls(), 1)

	require.NoError(t, r.Refresh(t.Context()))
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))
	require.NoError(t, r.SetState(t.Context(), roster.StateReady))
	require.Len(t, primary.Calls(), 1, "a heartbeat and a repeated Register notify nobody")
	require.Len(t, hint.Calls(), 1)
	require.Len(t, store.Puts, 3, "but both were written")

	var rc roster.Receipt
	require.NoError(t, r.SetState(t.Context(), roster.StateReady, roster.Want(roster.AckReceived, &rc)))
	require.Len(t, primary.Calls(), 1, "a SetState that writes nothing notifies nothing, even with Want")

	require.NoError(t, r.Refresh(t.Context(), roster.Want(roster.AckReceived, &rc)))
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}, roster.Want(roster.AckReceived, &rc)))
	calls := primary.Calls()
	require.Len(t, calls, 3)
	for _, c := range calls[1:] {
		require.Equal(t, map[string]roster.WriteKind{"web": roster.WriteUnchanged}, kinds(c.Notification))
		require.Equal(t, roster.AckReceived, c.Want)
		require.Len(t, c.Notification.Writes[0].Endpoints, 1)
	}
	require.Len(t, hint.Calls(), 3, "hints are told whenever the primary is")
	require.Equal(t, roster.AckNone, hint.Calls()[2].Want)
}

func TestNotify_ReconcileOfIdenticalContentIsUnchanged(t *testing.T) {
	store := rostertest.NewStore()
	first := notifiedPublisher(t, store)
	require.NoError(t, first.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web")}))

	// A restarted publisher learns what is stored from ListOwned, so identical content is not a change.
	primary := rostertest.NewNotifier()
	restarted, err := roster.NewPublisher(store, "c1", roster.WithLogger(quiet), roster.WithNotifier(primary))
	require.NoError(t, err)
	require.NoError(t, restarted.Reconcile(t.Context(), []roster.Endpoint{endpoint("a", "web")}))
	require.Empty(t, primary.Calls())

	var rc roster.Receipt
	require.NoError(t, restarted.Reconcile(t.Context(), []roster.Endpoint{endpoint("a", "web")}, roster.Want(roster.AckAccepted, &rc)))
	require.Equal(t, map[string]roster.WriteKind{"web": roster.WriteUnchanged}, kinds(primary.Calls()[0].Notification))
}

func TestNotify_ADamagedDocumentIsRepairedOrDeleted(t *testing.T) {
	store := rostertest.NewStore()
	store.Damaged = map[string]error{"web": errors.New("truncated"), "api": errors.New("hand edited")}
	hint := rostertest.NewNotifier()
	p, err := roster.NewPublisher(store, "c1", roster.WithLogger(quiet), roster.WithHintNotifier(hint))
	require.NoError(t, err)
	require.NoError(t, p.Reconcile(t.Context(), []roster.Endpoint{endpoint("a", "web")}))
	require.Equal(t, map[string]roster.WriteKind{"api": roster.WriteDeleted, "web": roster.WritePut}, kinds(hint.Calls()[0].Notification))
}

func TestNotify_ProtectedServicesAreNeverListed(t *testing.T) {
	store := rostertest.NewStore()
	primary := rostertest.NewNotifier()
	p := notifiedPublisher(t, store, roster.WithNotifier(primary))
	require.NoError(t, p.Publish(t.Context(), []roster.Endpoint{endpoint("a", "web"), endpoint("b", "api")}))

	var rc roster.Receipt
	require.NoError(t, p.Reconcile(t.Context(), []roster.Endpoint{endpoint("b", "api")},
		roster.PreserveServices("web"), roster.Want(roster.AckReceived, &rc)))
	require.Equal(t, map[string]roster.WriteKind{"api": roster.WriteUnchanged}, kinds(primary.Calls()[1].Notification),
		"web is refreshed but the call claims nothing about it")
}

// Rule 3: a call cancelled after it stored something still hints what it stored.
func TestNotify_ACancelledCallStillHintsWhatItStored(t *testing.T) {
	store := rostertest.NewStore()
	ctx, cancel := context.WithCancel(t.Context())
	store.PutFails = func(_, service string) bool {
		if service == "api" {
			cancel() // the first service is stored, and the loop stops before the next
		}
		return false
	}
	primary, hint := rostertest.NewNotifier(), rostertest.NewNotifier()
	p := notifiedPublisher(t, store, roster.WithNotifier(primary), roster.WithHintNotifier(hint))

	require.ErrorIs(t, p.Publish(ctx, []roster.Endpoint{endpoint("a", "web"), endpoint("b", "api")}), context.Canceled)
	// The call does not wait for hints once its context is done, so the hint may still be on its way.
	require.Eventually(t, func() bool { return len(hint.Calls()) == 1 }, time.Second, time.Millisecond)
	require.Equal(t, map[string]roster.WriteKind{"api": roster.WritePut}, kinds(hint.Calls()[0].Notification))
	require.NoError(t, hint.Calls()[0].CtxErr, "the hint ignores the caller's cancellation")
	require.ErrorIs(t, primary.Calls()[0].CtxErr, context.Canceled, "the primary runs on the caller's context")
}

// Rule 3: the call does not wait for a hint past its own deadline, and the hint carries on in the background.
func TestNotify_ACallReturnsAtItsDeadlineWhileAHintBlocks(t *testing.T) {
	hint := rostertest.NewNotifier()
	hint.Hold()
	r := notifiedRegistration(t, rostertest.NewStore(), roster.WithRegistrationHintNotifier(hint),
		roster.WithRegistrationHintTimeout(time.Minute))

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.NoError(t, r.Deregister(ctx))
	require.Less(t, time.Since(start), 10*time.Second, "returned at the caller's deadline, not the hint timeout")
	require.Len(t, hint.Calls(), 1)
	hint.Release()
}

// Rule 7: a hint that fails, or runs out of time, is logged with the writer's logger and goes nowhere else.
func TestNotify_AHintFailureIsLoggedAtWarn(t *testing.T) {
	log := &rostertest.Logger{}
	hint := rostertest.NewNotifier()
	hint.Hold()
	defer hint.Release()
	r, err := roster.NewRegistration(rostertest.NewStore(), "i-1", "web", roster.WithRegistrationLogger(log),
		roster.WithRegistrationHintNotifier(hint), roster.WithRegistrationHintTimeout(10*time.Millisecond))
	require.NoError(t, err)

	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{member("u1")}))
	var warns []rostertest.Entry
	for _, e := range log.Entries() {
		if e.Level == "warn" {
			warns = append(warns, e)
		}
	}
	require.Len(t, warns, 1)
	require.Contains(t, warns[0].Msg, "hint notifier failed")
	require.ErrorIs(t, warns[0].Fields["err"].(error), context.DeadlineExceeded)
}

// Rule 5: a notifier may keep and change what it is given.
func TestNotify_ANotificationIsACopy(t *testing.T) {
	primary := rostertest.NewNotifier()
	r := notifiedRegistration(t, rostertest.NewStore(), roster.WithRegistrationNotifier(primary))
	e := member("u1")
	e.Labels = map[string]string{"zone": "a"}
	require.NoError(t, r.Register(t.Context(), []roster.Endpoint{e}))

	got := primary.Calls()[0].Notification.Writes[0].Endpoints
	got[0].Labels["zone"] = "changed"
	got[0].Port = 1
	require.Equal(t, "a", r.Endpoints()[0].Labels["zone"])
	require.Equal(t, uint16(8443), r.Endpoints()[0].Port)
}
