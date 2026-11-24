// Package rostertest provides in-memory test doubles for the roster
// library, so code built on it can be unit tested without AWS: a Store (which
// also implements roster.Reader) with failure injection, a controllable
// roster.HintSource, and a recording roster.Notifier. Their fields are not
// safe to change while the code under test is running.
//
// Store implements roster.DocumentStore, the interface a Publisher writes through,
// and so roster.EndpointWriter, the one a Registration writes through. It also
// has record level helpers (Upsert, Delete, Heartbeat) that tests use to change
// the roster the way an outside writer would.
package rostertest

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/davidbirdsong/roster"
)

var errInjected = errors.New("rostertest: injected failure")

type Store struct {
	items map[roster.EndpointID]roster.Endpoint

	// Failure injection. Nil or false means the call succeeds.
	ListErr error
	// PutFails makes PutService fail for the services it returns true for.
	PutFails func(ownerID, service string) bool
	// UpsertFails, DeleteFails and HeartbeatErr affect the record level helpers. DeleteFails also makes
	// DeleteService fail.
	UpsertFails  func(roster.Endpoint) bool
	DeleteFails  bool
	HeartbeatErr error
	// Foreign is returned by ListOwned in addition to the stored items, to
	// simulate records owned by another owner.
	Foreign []roster.Endpoint
	// Damaged makes ListOwned report these services as stored documents that
	// could not be decoded, keyed by service name, as a hand edited or
	// truncated object in the real roster would be. A service here is reported
	// with its DecodeErr and no endpoints, whether or not it also has items.
	Damaged map[string]error
	// SnapshotErr makes Snapshot fail, as a failed roster read would.
	SnapshotErr error
	// SkipOnSnapshot, if set, is reported as skipped items by Snapshot, as
	// undecodable stored items would be.
	SkipOnSnapshot []roster.SkippedItem
	// RejectOnSnapshot, if set, is reported as rejected records by Snapshot, as a reader would report records
	// it left out. They do not make the snapshot incomplete.
	RejectOnSnapshot []roster.RejectedEndpoint
	// Now is used for Snapshot.ReadAt. Zero means the zero time.
	Now time.Time

	// Recorded successful calls, in order.
	Puts           []PutRecord
	ServiceDeletes []ServiceRef
	Upserts        []roster.EndpointID
	Deletes        []roster.EndpointID
	Heartbeats     []time.Time

	// lastRefresh is set by Heartbeat. When an owner has none, its last refresh is the oldest time any of its
	// services was written by PutService, the way a reader of the real roster sees it.
	lastRefresh map[string]time.Time
	written     map[string]map[string]time.Time // owner, service
	// owners holds every owner a write has been seen for, so an owner whose
	// endpoints have all been removed still has a status in the snapshot, the
	// way a reader of the real roster still lists it.
	owners map[string]bool
	// counter and lastHash mint the snapshot's revision the way s3store.Reader
	// does: the counter moves only when the content hash moves, so rewriting
	// the same endpoints (a Publisher's periodic Reconcile) advances an owner's
	// last refresh without producing a new revision. A test double that counted
	// writes instead would report a change on every heartbeat, which no reader
	// of the real roster does.
	counter  uint64
	lastHash string
}

// PutRecord is one successful PutService.
type PutRecord struct {
	OwnerID   string
	Service   string
	Endpoints []roster.Endpoint
}

// ServiceRef names one service of one owner.
type ServiceRef struct{ OwnerID, Service string }

var (
	_ roster.DocumentStore  = (*Store)(nil)
	_ roster.EndpointWriter = (*Store)(nil)
	_ roster.Reader         = (*Store)(nil)
)

func NewStore(initial ...roster.Endpoint) *Store {
	s := &Store{
		items:       map[roster.EndpointID]roster.Endpoint{},
		lastRefresh: map[string]time.Time{}, written: map[string]map[string]time.Time{}, owners: map[string]bool{},
	}
	for _, b := range initial {
		s.items[b.ID()] = b
		s.owners[b.OwnerID] = true // as if each owner had been written once
	}
	return s
}

// Item returns the stored endpoint with the given ID.
func (s *Store) Item(id roster.EndpointID) (roster.Endpoint, bool) {
	b, ok := s.items[id]
	return b, ok
}

// Writes is the number of successful writes of any kind.
func (s *Store) Writes() int {
	return len(s.Puts) + len(s.ServiceDeletes) + len(s.Upserts) + len(s.Deletes)
}

// ListOwned returns the stored items as one OwnedService per service, plus any Foreign records and any
// Damaged services, so a test can drive every branch a Publisher has for stored state.
func (s *Store) ListOwned(context.Context, string) ([]roster.OwnedService, error) {
	if s.ListErr != nil {
		return nil, s.ListErr
	}
	byService := map[string][]roster.Endpoint{}
	for _, b := range s.items {
		byService[b.Service] = append(byService[b.Service], b)
	}
	for _, b := range s.Foreign {
		byService[b.Service] = append(byService[b.Service], b)
	}
	var out []roster.OwnedService
	for _, service := range slices.Sorted(maps.Keys(byService)) {
		out = append(out, roster.OwnedService{
			Service: service, Endpoints: byService[service], Key: "test/" + service,
		})
	}
	for _, service := range slices.Sorted(maps.Keys(s.Damaged)) {
		out = append(out, roster.OwnedService{
			Service: service, DecodeErr: s.Damaged[service], Key: "test/" + service,
		})
	}
	return out, nil
}

// PutService replaces the stored endpoints of one service in an owner, as the real roster does. Writing the same
// endpoints again still counts as a write and records the time, which is what keeps the owner looking alive. It
// changes no content, so it moves no revision, which is what makes a periodic Reconcile cheap for readers.
func (s *Store) PutService(_ context.Context, ownerID, service string, endpoints []roster.Endpoint) error {
	if s.PutFails != nil && s.PutFails(ownerID, service) {
		return errInjected
	}
	s.removeService(ownerID, service)
	for _, b := range endpoints {
		s.items[b.ID()] = b
	}
	if s.written[ownerID] == nil {
		s.written[ownerID] = map[string]time.Time{}
	}
	s.written[ownerID][service] = s.Now
	s.owners[ownerID] = true
	s.Puts = append(s.Puts, PutRecord{
		OwnerID: ownerID, Service: service,
		Endpoints: append([]roster.Endpoint(nil), endpoints...),
	})
	return nil
}

// DeleteService removes the stored endpoints of one service in an owner.
func (s *Store) DeleteService(_ context.Context, ownerID, service string) error {
	if s.DeleteFails {
		return errInjected
	}
	s.removeService(ownerID, service)
	delete(s.written[ownerID], service)
	s.owners[ownerID] = true
	s.ServiceDeletes = append(s.ServiceDeletes, ServiceRef{OwnerID: ownerID, Service: service})
	return nil
}

func (s *Store) removeService(ownerID, service string) {
	for id, b := range s.items {
		if b.OwnerID == ownerID && b.Service == service {
			delete(s.items, id)
		}
	}
}

// Upsert writes one endpoint, as a writer outside the code under test would. It is not part of roster.DocumentStore.
func (s *Store) Upsert(_ context.Context, b roster.Endpoint) error {
	if s.UpsertFails != nil && s.UpsertFails(b) {
		return errInjected
	}
	s.items[b.ID()] = b
	s.owners[b.OwnerID] = true
	s.Upserts = append(s.Upserts, b.ID())
	return nil
}

// Delete removes one endpoint. It is not part of roster.DocumentStore.
func (s *Store) Delete(_ context.Context, ownerID string, id roster.EndpointID) error {
	if s.DeleteFails {
		return errInjected
	}
	delete(s.items, id)
	s.owners[ownerID] = true
	s.Deletes = append(s.Deletes, id)
	return nil
}

// Heartbeat records the time of an owner's last refresh, overriding what its service writes imply. It is not part
// of roster.DocumentStore.
func (s *Store) Heartbeat(_ context.Context, ownerID string, at time.Time) error {
	if s.HeartbeatErr != nil {
		return s.HeartbeatErr
	}
	s.Heartbeats = append(s.Heartbeats, at)
	s.lastRefresh[ownerID] = at
	return nil
}

// Snapshot implements roster.Reader over everything in the store, with a
// heartbeat row for each owner that has recorded one.
//
// Its revision moves exactly when the content of the snapshot changes and stays
// the same when it does not, as a real Reader's does, so a refresh is not a
// change. The first snapshot always has revision 1.
func (s *Store) Snapshot(context.Context) (roster.Snapshot, error) {
	if s.SnapshotErr != nil {
		return roster.Snapshot{}, s.SnapshotErr
	}
	var endpoints []roster.Endpoint
	for _, b := range s.items {
		endpoints = append(endpoints, b)
	}
	ids := map[string]bool{}
	for _, b := range s.items {
		ids[b.OwnerID] = true
	}
	for id := range s.owners {
		ids[id] = true
	}
	for id := range s.lastRefresh {
		ids[id] = true
	}
	var owners []roster.OwnerStatus
	for id := range ids {
		owners = append(owners, roster.OwnerStatus{OwnerID: id, LastRefreshAt: s.lastRefreshOf(id)})
	}
	snap := roster.NewSnapshot(s.Now, endpoints, owners, s.SkipOnSnapshot, s.RejectOnSnapshot)
	if s.counter == 0 || snap.ContentHash != s.lastHash {
		s.counter++
		s.lastHash = snap.ContentHash
	}
	snap.SetRevision(s.counter)
	return snap, nil
}

// lastRefreshOf is the Heartbeat time when there is one, and otherwise the oldest time any service of the owner
// was written.
func (s *Store) lastRefreshOf(ownerID string) time.Time {
	if at, ok := s.lastRefresh[ownerID]; ok {
		return at
	}
	var oldest time.Time
	first := true
	for _, at := range s.written[ownerID] {
		if first || at.Before(oldest) {
			oldest, first = at, false
		}
	}
	return oldest
}
