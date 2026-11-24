package roster

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// Ack is how far a notification got. Levels are ordered, and each implies the ones below it.
type Ack int

const (
	AckNone      Ack = iota // nothing beyond the write to S3
	AckAccepted             // the notifier's own transport took it (an MQTT PUBACK, a pharos RPC that returned)
	AckDelivered            // handed to every downstream consumer the notifier knows of
	AckReceived             // every one of those consumers confirmed receipt
)

// Valid reports whether a is one of the defined levels.
func (a Ack) Valid() bool { return a >= AckNone && a <= AckReceived }

// WriteKind is what one call did to one service.
type WriteKind int

const (
	WritePut       WriteKind = iota // the content changed, or is new to this writer
	WriteUnchanged                  // rewritten with the content this writer stored before (a refresh)
	WriteDeleted                    // the service's document was deleted
)

// ServiceWrite is what one call did to one service of the owner.
type ServiceWrite struct {
	Service   string
	Kind      WriteKind
	Endpoints []Endpoint // what is now stored, sorted by ID; nil only for WriteDeleted
}

// Notification is what one call stored, for one owner.
type Notification struct {
	Owner  string
	Writes []ServiceWrite // sorted by service; only services this call stored (put or deleted) and that returned ok
}

// Receipt is a notifier's report on one notification.
type Receipt struct {
	Reached Ack   // the highest level with evidence
	Refused bool  // a consumer rejected the content; waiting longer will not help
	Err     error // why Reached is below what was wanted; nil when it was reached
	Detail  any   // implementation specific; each implementation documents its concrete type
}

// Notifier is told about every write a Registration or Publisher stores.
//
// It is called after the write, outside the writer's lock, so calls for one owner may arrive in any
// order and may overlap. want is the level the caller asked for: an implementation returns as soon
// as it has reached want, been refused, or ctx is done, and reports the highest level it has
// evidence for. A primary called with AckNone hands the notification off and returns. A hint is
// always called with AckNone, in its own goroutine and with its own timeout, so it may block on
// its transport.
//
// A call where every write is WriteUnchanged reaches a notifier only when the caller asked for a
// level above AckNone, and must not cause a push, a new version or a mark.
type Notifier interface {
	Notify(ctx context.Context, n Notification, want Ack) Receipt
}

// DefaultHintTimeout bounds each hint notifier call.
const DefaultHintTimeout = 5 * time.Second

// callConfig is what the options of one write call set.
type callConfig struct {
	protected map[string]struct{}
	want      Ack
	receipt   *Receipt
	err       error // a bad option, reported before anything is written
}

// CallOption adjusts one write call of a Registration or a Publisher.
type CallOption func(*callConfig)

// PreserveServices leaves the published records of the named services as they are:
// nothing in them is changed or deleted. Use it when a service's desired state
// could not be determined (bad configuration, for example), so a transient
// problem cannot empty the service. An Reconcile still rewrites a protected service
// with its published content, so the service does not look stale. A protected
// service is never listed in a notification. A Registration refuses it, because it
// owns one service and has nothing to preserve.
func PreserveServices(services ...string) CallOption {
	return func(c *callConfig) {
		for _, g := range services {
			c.protected[g] = struct{}{}
		}
	}
}

// Want asks the primary notifier for level and stores its receipt in r once the call returns. The
// receipt is zero when nothing was notified. A call with Want and no primary notifier attached is
// refused before anything is written.
func Want(level Ack, r *Receipt) CallOption {
	return func(c *callConfig) {
		switch {
		case r == nil:
			c.err = errors.Join(c.err, errors.New("roster: Want needs a receipt to store into"))
		case !level.Valid():
			c.err = errors.Join(c.err, fmt.Errorf("roster: Want with unknown level %d", level))
		default:
			c.want, c.receipt = level, r
		}
	}
}

func newCallConfig(opts []CallOption) callConfig {
	c := callConfig{protected: map[string]struct{}{}}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// notifiers is the notification machinery both writers share.
type notifiers struct {
	primary     Notifier
	hints       []Notifier
	hintTimeout time.Duration
	optionErr   error // a bad option, reported by the constructor
}

func newNotifiers() notifiers { return notifiers{hintTimeout: DefaultHintTimeout} }

func (ns *notifiers) setPrimary(n Notifier) {
	switch {
	case n == nil:
		ns.optionErr = errors.Join(ns.optionErr, errors.New("roster: notifier is nil"))
	case ns.primary != nil:
		ns.optionErr = errors.Join(ns.optionErr, errors.New("roster: at most one primary notifier, attach the others as hints"))
	default:
		ns.primary = n
	}
}

func (ns *notifiers) addHint(n Notifier) {
	if n == nil {
		ns.optionErr = errors.Join(ns.optionErr, errors.New("roster: hint notifier is nil"))
		return
	}
	ns.hints = append(ns.hints, n)
}

func (ns *notifiers) setHintTimeout(d time.Duration) {
	if d <= 0 {
		ns.optionErr = errors.Join(ns.optionErr, fmt.Errorf("roster: hint timeout must be positive, got %s", d))
		return
	}
	ns.hintTimeout = d
}

// begin checks a call's options before anything is written, and clears the caller's receipt so a
// value from an earlier call never survives.
func (ns *notifiers) begin(cfg callConfig) error {
	if cfg.err != nil {
		return cfg.err
	}
	if cfg.receipt != nil {
		if ns.primary == nil {
			return errors.New("roster: Want needs a primary notifier")
		}
		*cfg.receipt = Receipt{}
	}
	return nil
}

// notify tells every notifier what one call stored. It is called after the writer's lock is
// released. n may share slices with what the writer remembers, which the writer replaces and never
// changes in place, and each notifier is given its own copy.
//
// The primary runs on ctx with the caller's level, and its receipt goes to the caller. Each hint
// runs in its own goroutine on a context that ignores the caller's cancellation, bounded by the hint
// timeout, so a call cancelled after it stored something still hints what it stored. The call waits
// for the hints until they return or ctx is done, whichever is first. A hint still running then
// finishes in the background, within its timeout.
func (ns *notifiers) notify(ctx context.Context, log Logger, n Notification, cfg callConfig) {
	if len(n.Writes) == 0 {
		return
	}
	if cfg.want == AckNone && !slices.ContainsFunc(n.Writes, func(w ServiceWrite) bool { return w.Kind != WriteUnchanged }) {
		return
	}

	var wg sync.WaitGroup
	hctx := context.WithoutCancel(ctx)
	for _, h := range ns.hints {
		wg.Go(func() {
			c, cancel := context.WithTimeout(hctx, ns.hintTimeout)
			defer cancel()
			if r := h.Notify(c, cloneNotification(n), AckNone); r.Err != nil {
				log.Warn("roster: hint notifier failed, readers will notice at their next poll", "owner", n.Owner, "err", r.Err)
			}
		})
	}

	if ns.primary != nil {
		r := ns.primary.Notify(ctx, cloneNotification(n), cfg.want)
		if cfg.receipt != nil {
			*cfg.receipt = r
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func cloneNotification(n Notification) Notification {
	out := Notification{Owner: n.Owner, Writes: slices.Clone(n.Writes)}
	for i := range out.Writes {
		out.Writes[i].Endpoints = cloneEndpoints(out.Writes[i].Endpoints)
	}
	return out
}

// cloneEndpoints copies endpoints and their label maps, so the copy can be kept and changed freely.
func cloneEndpoints(es []Endpoint) []Endpoint {
	if es == nil {
		return nil
	}
	out := slices.Clone(es)
	for i := range out {
		out[i].Labels = maps.Clone(out[i].Labels)
	}
	return out
}
