package roster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

// ErrDeregistered is returned by Refresh and SetState after Deregister, so a
// heartbeat that races shutdown cannot write the document back.
var ErrDeregistered = errors.New("roster: registration is deregistered")

// ErrNotRegistered is returned by Refresh and SetState before the first
// successful Register, because there is nothing known to write.
var ErrNotRegistered = errors.New("roster: nothing is registered yet")

// Registration is one process's own entry in one pool (a service). Each member
// of a pool has its own Registration and its own owner ID, so it owns exactly
// one document, writes it without coordinating with any other member, and
// never reads the roster.
//
// The owner ID must be unique to the running process and stable for its life:
// an instance ID, a task ID or a pod UID. Two live processes that share an
// owner ID in one pool overwrite each other's document, and nothing here can
// tell, because nothing here reads.
//
// The library runs no timer. A member's life is driven by the caller:
//
//	start:     Register, with READY or NOT_READY endpoints
//	running:   Refresh every interval, well under the readers' stale timeout;
//	           SetState when health changes
//	shutdown:  SetState(StateTerminating), wait out the drain grace, then
//	           Deregister with a context that is not the cancelled one
//	crash:     nothing; the document stops being refreshed and readers
//	           drop it once it is stale
//
// What a Registration remembers changes only after a write succeeds, so a
// failed call can be retried and converges. Calls are serialized.
type Registration struct {
	w       EndpointWriter
	ownerID string
	service string
	log     Logger
	notifiers

	mu           sync.Mutex
	endpoints    []Endpoint // sorted by ID, nil before the first Register
	registered   bool
	deregistered bool
}

// RegistrationOption configures a Registration.
type RegistrationOption func(*Registration)

// WithRegistrationLogger sets the logger for what the Registration writes and deletes. A nil logger is ignored.
func WithRegistrationLogger(l Logger) RegistrationOption {
	return func(r *Registration) {
		if l != nil {
			r.log = l
		}
	}
}

// NewRegistration returns the Registration of ownerID in service. It writes nothing.
func NewRegistration(w EndpointWriter, ownerID, service string, opts ...RegistrationOption) (*Registration, error) {
	if w == nil {
		return nil, errors.New("roster: writer is required")
	}
	if err := ValidSegment("owner ID", ownerID); err != nil {
		return nil, fmt.Errorf("roster: %w", err)
	}
	if err := ValidSegment("service", service); err != nil {
		return nil, fmt.Errorf("roster: %w", err)
	}
	r := &Registration{w: w, ownerID: ownerID, service: service, log: slog.Default(), notifiers: newNotifiers()}
	for _, o := range opts {
		o(r)
	}
	if r.optionErr != nil {
		return nil, r.optionErr
	}
	return r, nil
}

// WithRegistrationNotifier attaches the primary notifier, whose receipt a call returns through Want.
// At most one may be attached.
func WithRegistrationNotifier(n Notifier) RegistrationOption {
	return func(r *Registration) { r.setPrimary(n) }
}

// WithRegistrationHintNotifier attaches a notifier that is told of every change and whose receipt is
// only logged on failure, such as an s3store.Marker. Any number may be attached.
func WithRegistrationHintNotifier(n Notifier) RegistrationOption {
	return func(r *Registration) { r.addHint(n) }
}

// WithRegistrationHintTimeout bounds each hint notifier call. The default is DefaultHintTimeout.
func WithRegistrationHintTimeout(d time.Duration) RegistrationOption {
	return func(r *Registration) { r.setHintTimeout(d) }
}

// Register replaces this member's endpoints with endpoints, which must be
// every endpoint the member serves in the pool (usually one, one per port when
// it has several). OwnerID and Service may be left empty and are filled in;
// one naming another owner or service is an error. Every endpoint is validated
// before anything is written.
//
// Register after Deregister rejoins the pool.
func (r *Registration) Register(ctx context.Context, endpoints []Endpoint, opts ...CallOption) error {
	if len(endpoints) == 0 {
		return errors.New("roster: register needs at least one endpoint, use Deregister to leave the pool")
	}
	desired, err := prepareEndpoints(r.ownerID, r.service, endpoints)
	if err != nil {
		return err
	}
	want := desired[r.service]
	return r.call(ctx, opts, func() (WriteKind, error) {
		kind := WritePut
		if r.registered && sameEndpoints(r.endpoints, want) {
			kind = WriteUnchanged
		}
		if err := r.put(ctx, want, "register"); err != nil {
			return 0, err
		}
		r.endpoints = want
		r.registered = true
		r.deregistered = false
		return kind, nil
	})
}

// SetState sets the state of every registered endpoint, for example to
// StateTerminating when shutdown begins, and writes the document if that
// changed it. A SetState that changes nothing writes and notifies nothing; use
// Refresh with Want to ask for evidence of the current state.
func (r *Registration) SetState(ctx context.Context, s State, opts ...CallOption) error {
	if !s.Valid() {
		return fmt.Errorf("roster: invalid state %q", s)
	}
	return r.call(ctx, opts, func() (WriteKind, error) {
		if err := r.usable(); err != nil {
			return 0, err
		}
		want := slices.Clone(r.endpoints)
		changed := false
		for i := range want {
			if want[i].State != s {
				want[i].State = s
				changed = true
			}
		}
		if !changed {
			return 0, errNothingWritten
		}
		if err := r.put(ctx, want, "state"); err != nil {
			return 0, err
		}
		r.endpoints = want
		return WritePut, nil
	})
}

// Refresh rewrites the document with the same content. It is the member's
// proof of life: a reader takes the document's write time as the owner's last
// refresh. It is notified as WriteUnchanged, which reaches the notifiers only
// with Want, so a heartbeat marks nothing and readers fetch nothing.
func (r *Registration) Refresh(ctx context.Context, opts ...CallOption) error {
	return r.call(ctx, opts, func() (WriteKind, error) {
		if err := r.usable(); err != nil {
			return 0, err
		}
		if err := r.put(ctx, r.endpoints, "refresh"); err != nil {
			return 0, err
		}
		return WriteUnchanged, nil
	})
}

// Deregister removes this member's document, so readers see its endpoints
// gone. It is safe to call more than once, and it issues the delete even when
// no Register succeeded, so a process that failed part way through its start
// leaves nothing behind. On failure the document stays until it is stale, and
// calling again retries.
func (r *Registration) Deregister(ctx context.Context, opts ...CallOption) error {
	return r.call(ctx, opts, func() (WriteKind, error) {
		if err := r.w.DeleteService(ctx, r.ownerID, r.service); err != nil {
			return 0, fmt.Errorf("deregister owner %q from service %q: %w", r.ownerID, r.service, err)
		}
		r.endpoints = nil
		r.registered = false
		r.deregistered = true
		r.log.Info("published", "op", "deregister", "service", r.service, "owner", r.ownerID)
		return WriteDeleted, nil
	})
}

// errNothingWritten is returned by a call's body when it succeeded without writing, so there is nothing to notify.
var errNothingWritten = errors.New("nothing written")

// call checks the options, runs write under the lock, and notifies what it stored once the lock is
// released, so a notifier that waits for evidence never holds up the next call.
func (r *Registration) call(ctx context.Context, opts []CallOption, write func() (WriteKind, error)) error {
	cfg := newCallConfig(opts)
	if len(cfg.protected) > 0 {
		return errors.New("roster: PreserveServices does not apply to a Registration, which owns one service")
	}
	if err := r.begin(cfg); err != nil {
		return err
	}
	r.mu.Lock()
	kind, err := write()
	stored := r.endpoints
	r.mu.Unlock()
	switch {
	case errors.Is(err, errNothingWritten):
		return nil
	case err != nil:
		return err
	}
	r.notify(ctx, r.log, Notification{Owner: r.ownerID, Writes: []ServiceWrite{{Service: r.service, Kind: kind, Endpoints: stored}}}, cfg)
	return nil
}

// Endpoints returns what the Registration last wrote successfully, or nil when nothing is registered.
func (r *Registration) Endpoints() []Endpoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneEndpoints(r.endpoints)
}

func (r *Registration) usable() error {
	switch {
	case r.deregistered:
		return ErrDeregistered
	case !r.registered:
		return ErrNotRegistered
	}
	return nil
}

// put writes the document. A refresh is not logged, because it happens on every interval.
func (r *Registration) put(ctx context.Context, endpoints []Endpoint, op string) error {
	if err := r.w.PutService(ctx, r.ownerID, r.service, endpoints); err != nil {
		return fmt.Errorf("%s owner %q in service %q: %w", op, r.ownerID, r.service, err)
	}
	if op != "refresh" {
		r.log.Info("published", "op", op, "service", r.service, "owner", r.ownerID, "endpoints", len(endpoints))
	}
	return nil
}
