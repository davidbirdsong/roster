package roster

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"
)

// ErrNotInitialized is returned by Publish before the first successful Reconcile,
// because the Publisher does not yet know what is published.
var ErrNotInitialized = errors.New("roster: published state unknown, reconcile has not completed")

// Publisher makes one owner's roster records match a desired set of
// endpoints. It owns the safety rules, so every publisher built on it gets
// them: nothing is read or written outside its owner, invalid endpoints are
// rejected before any write, and a failed roster read never causes deletes.
//
// The roster stores one object per service, holding the full set of that service
// in the owner, so the Publisher works in services: a service whose endpoints
// changed is rewritten, a service that is no longer desired is deleted, and an
// Reconcile also rewrites every unchanged service with identical content. That
// rewrite is the publisher's proof of life: a reader takes the oldest write
// time among an owner's services as the owner's last reconcile, so a reconcile that
// fails part way leaves the owner looking stale, as a failed heartbeat did.
//
// Calls are serialized. A Publisher must be the only writer for its owner
// (for example, by holding a Kubernetes Lease).
//
// A Publisher is for an aggregator: one process that publishes many instances
// it observes. A process that publishes only its own endpoints, as a member of
// a pool, uses a Registration instead, which never reads the roster.
type Publisher struct {
	store   DocumentStore
	ownerID string
	log     Logger
	notifiers

	mu        sync.Mutex
	published map[string][]Endpoint // by service, each sorted by ID
	// damaged holds the services whose stored document the last Reconcile
	// could not decode. They are not in published, because there is no known
	// content for them, but they do exist in storage and a Reconcile must
	// still repair or remove them.
	damaged     map[string]OwnedService
	initialized bool
}

type PublisherOption func(*Publisher)

// WithLogger sets the logger for what the Publisher writes and deletes. A nil logger is ignored.
func WithLogger(l Logger) PublisherOption {
	return func(p *Publisher) {
		if l != nil {
			p.log = l
		}
	}
}

func NewPublisher(store DocumentStore, ownerID string, opts ...PublisherOption) (*Publisher, error) {
	if store == nil {
		return nil, errors.New("roster: store is required")
	}
	if ownerID == "" {
		return nil, errors.New("roster: ownerID is required")
	}
	if err := ValidSegment("owner ID", ownerID); err != nil {
		return nil, fmt.Errorf("roster: %w", err)
	}
	p := &Publisher{
		store: store, ownerID: ownerID, log: slog.Default(), notifiers: newNotifiers(),
		published: map[string][]Endpoint{}, damaged: map[string]OwnedService{},
	}
	for _, o := range opts {
		o(p)
	}
	if p.optionErr != nil {
		return nil, p.optionErr
	}
	return p, nil
}

// WithNotifier attaches the primary notifier, whose receipt a call returns through Want. At most one
// may be attached.
func WithNotifier(n Notifier) PublisherOption { return func(p *Publisher) { p.setPrimary(n) } }

// WithHintNotifier attaches a notifier that is told of every change and whose receipt is only logged
// on failure, such as an s3store.Marker. Any number may be attached.
func WithHintNotifier(n Notifier) PublisherOption { return func(p *Publisher) { p.addHint(n) } }

// WithHintTimeout bounds each hint notifier call. The default is DefaultHintTimeout.
func WithHintTimeout(d time.Duration) PublisherOption {
	return func(p *Publisher) { p.setHintTimeout(d) }
}

// Publish makes the roster match endpoints, which must be the complete desired
// set for the owner. It compares against the Publisher's in-memory view of
// what it has published and does not read the roster, so it is cheap enough
// to call on every change. It writes only services that changed. Partial
// failures are returned, only the writes that succeeded are recorded, and
// calling again converges.
func (p *Publisher) Publish(ctx context.Context, endpoints []Endpoint, opts ...CallOption) error {
	cfg := newCallConfig(opts)
	if err := p.begin(cfg); err != nil {
		return err
	}
	p.mu.Lock()
	writes, err := p.publish(ctx, endpoints, cfg)
	p.mu.Unlock()
	p.notify(ctx, p.log, Notification{Owner: p.ownerID, Writes: writes}, cfg)
	return err
}

func (p *Publisher) publish(ctx context.Context, endpoints []Endpoint, cfg callConfig) ([]ServiceWrite, error) {
	if !p.initialized {
		return nil, ErrNotInitialized
	}
	desired, err := p.prepare(endpoints)
	if err != nil {
		return nil, err
	}
	return p.apply(ctx, desired, cfg, false)
}

// Reconcile rebuilds the Publisher's view from the roster, repairs any drift,
// and rewrites every service so that a reader sees the owner is alive. Call it
// at startup and periodically. A failed roster read returns an error before
// any write or delete.
func (p *Publisher) Reconcile(ctx context.Context, endpoints []Endpoint, opts ...CallOption) error {
	cfg := newCallConfig(opts)
	if err := p.begin(cfg); err != nil {
		return err
	}
	p.mu.Lock()
	writes, err := p.reconcile(ctx, endpoints, cfg)
	p.mu.Unlock()
	p.notify(ctx, p.log, Notification{Owner: p.ownerID, Writes: writes}, cfg)
	return err
}

func (p *Publisher) reconcile(ctx context.Context, endpoints []Endpoint, cfg callConfig) ([]ServiceWrite, error) {
	desired, err := p.prepare(endpoints)
	if err != nil {
		return nil, err
	}
	owned, err := p.store.ListOwned(ctx, p.ownerID)
	if err != nil {
		return nil, fmt.Errorf("list owned: %w", err)
	}
	published := map[string][]Endpoint{}
	damaged := map[string]OwnedService{}
	for _, doc := range owned {
		if doc.Damaged() {
			damaged[doc.Service] = doc
			continue
		}
		for _, b := range doc.Endpoints {
			if b.OwnerID != p.ownerID {
				return nil, fmt.Errorf("store returned endpoint %q owned by owner %q, refusing to reconcile", b.ID(), b.OwnerID)
			}
			published[b.Service] = append(published[b.Service], b)
		}
	}
	for _, bs := range published {
		sortByID(bs)
	}
	p.published = published
	p.damaged = damaged
	p.initialized = true

	return p.apply(ctx, desired, cfg, true)
}

// prepare stamps the owner ID, validates every endpoint and rejects
// duplicate IDs (one instance and port listed twice). Several endpoints may share
// a LogicalName.
func (p *Publisher) prepare(endpoints []Endpoint) (map[string][]Endpoint, error) {
	return prepareEndpoints(p.ownerID, "", endpoints)
}

// prepareEndpoints is the validation every write path shares. It stamps
// ownerID onto every endpoint and rejects one that names another owner. With a
// service, it does the same for the service. It then validates each endpoint,
// rejects duplicate IDs, and takes its own copy of each label map. Any problem
// fails the whole call before the roster is touched. It returns the endpoints
// by service, each sorted by ID.
func prepareEndpoints(ownerID, service string, endpoints []Endpoint) (map[string][]Endpoint, error) {
	seen := make(map[EndpointID]bool, len(endpoints))
	desired := map[string][]Endpoint{}
	var errs []error
	for _, b := range endpoints {
		switch b.OwnerID {
		case "":
			b.OwnerID = ownerID
		case ownerID:
		default:
			errs = append(errs, fmt.Errorf("endpoint %q belongs to owner %q, publisher is for %q", b.ID(), b.OwnerID, ownerID))
			continue
		}
		if service != "" {
			switch b.Service {
			case "":
				b.Service = service
			case service:
			default:
				errs = append(errs, fmt.Errorf("endpoint %q is in service %q, registration is for %q", b.ID(), b.Service, service))
				continue
			}
		}
		if err := b.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("endpoint %q: %w", b.ID(), err))
			continue
		}
		if seen[b.ID()] {
			errs = append(errs, fmt.Errorf("duplicate endpoint %q: InstanceID and Port must be unique", b.ID()))
			continue
		}
		seen[b.ID()] = true
		// Own the labels, so a caller that reuses its map cannot change what the
		// publisher believes it has published.
		b.Labels = maps.Clone(b.Labels)
		if len(b.Labels) == 0 {
			b.Labels = nil
		}
		desired[b.Service] = append(desired[b.Service], b)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	for _, bs := range desired {
		sortByID(bs)
	}
	return desired, nil
}

// apply brings each service to its desired state. With refresh, a service that is
// already right is rewritten anyway. It returns what it stored, in service order, for
// the notification: protected services, services that needed no write and failed
// writes are left out.
//
// Damaged documents are only considered on a refresh (that is, from Reconcile),
// because that is the only path that read storage and so the only path that
// knows they are there.
func (p *Publisher) apply(ctx context.Context, desired map[string][]Endpoint, cfg callConfig, refresh bool) ([]ServiceWrite, error) {
	services := slices.Sorted(maps.Keys(desired))
	for g := range p.published {
		if _, ok := desired[g]; !ok {
			services = append(services, g)
		}
	}
	if refresh {
		for g := range p.damaged {
			services = append(services, g)
		}
	}
	slices.Sort(services)
	services = slices.Compact(services)

	var (
		errs   []error
		writes []ServiceWrite
	)
	stored := func(g string, kind WriteKind, endpoints []Endpoint) {
		writes = append(writes, ServiceWrite{Service: g, Kind: kind, Endpoints: endpoints})
	}
	for _, g := range services {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		want, wanted := desired[g]
		have, hasHave := p.published[g]
		_, protected := cfg.protected[g]
		broken, isDamaged := p.damaged[g]
		isDamaged = isDamaged && refresh

		switch {
		// A damaged document holds no readable content, so there is nothing to
		// refresh it with and nothing to compare against. It is either
		// overwritten with the desired set or deleted, and a protected one can
		// be neither, so it is reported instead of being silently left behind.
		case isDamaged && protected:
			errs = append(errs, fmt.Errorf(
				"service %q is protected and its stored document cannot be decoded, so it can be neither repaired nor refreshed (key %q): %w",
				g, broken.Key, broken.DecodeErr))
		case isDamaged && !wanted:
			if err := p.store.DeleteService(ctx, p.ownerID, g); err != nil {
				errs = append(errs, fmt.Errorf("delete damaged service %q: %w", g, err))
				continue
			}
			delete(p.damaged, g)
			stored(g, WriteDeleted, nil)
			p.log.Info("published", "op", "delete", "service", g, "reason", "undecodable and no longer desired")
		case isDamaged:
			if err := p.put(ctx, g, want, "repair"); err != nil {
				errs = append(errs, err)
				continue
			}
			delete(p.damaged, g)
			p.published[g] = want
			stored(g, WritePut, want)
		case protected:
			// A protected service is rewritten so it does not look stale, but the
			// call claims nothing about it, so it is not notified.
			if refresh && hasHave {
				errs = append(errs, p.put(ctx, g, have, "refresh"))
			}
		case !wanted:
			if err := p.store.DeleteService(ctx, p.ownerID, g); err != nil {
				errs = append(errs, fmt.Errorf("delete service %q: %w", g, err))
				continue
			}
			delete(p.published, g)
			stored(g, WriteDeleted, nil)
			p.log.Info("published", "op", "delete", "service", g)
		case !hasHave || !sameEndpoints(have, want):
			if err := p.put(ctx, g, want, "write"); err != nil {
				errs = append(errs, err)
				continue
			}
			p.published[g] = want
			stored(g, WritePut, want)
		case refresh:
			if err := p.put(ctx, g, want, "refresh"); err != nil {
				errs = append(errs, err)
				continue
			}
			stored(g, WriteUnchanged, want)
		}
	}
	return writes, errors.Join(errs...)
}

// put writes one service. A refresh is not logged, because it happens for every
// service on every reconcile.
func (p *Publisher) put(ctx context.Context, service string, endpoints []Endpoint, op string) error {
	if err := p.store.PutService(ctx, p.ownerID, service, endpoints); err != nil {
		return fmt.Errorf("%s service %q: %w", op, service, err)
	}
	if op != "refresh" {
		p.log.Info("published", "op", op, "service", service, "endpoints", len(endpoints))
	}
	return nil
}

func sortByID(bs []Endpoint) {
	slices.SortFunc(bs, func(a, b Endpoint) int { return cmp.Compare(a.ID(), b.ID()) })
}

// sameEndpoints reports whether two sets, both sorted by ID, are the same.
func sameEndpoints(a, b []Endpoint) bool {
	return slices.EqualFunc(a, b, func(x, y Endpoint) bool { return x.Equal(y) })
}

// ServicesOf returns the distinct services in endpoints, sorted. It is a small
// helper for callers building PreserveServices from a partial desired set.
func ServicesOf(endpoints []Endpoint) []string {
	var out []string
	for _, b := range endpoints {
		out = append(out, b.Service)
	}
	slices.Sort(out)
	return slices.Compact(out)
}
