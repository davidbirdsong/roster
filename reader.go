package roster

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"time"
)

// The read side. A consumer implements its own source interfaces on top of
// two small ones, Reader for complete snapshots and HintSource for advisory
// change notifications, and never sees the storage schema or transport.

// OwnerStatus is what a snapshot says about one publisher owner.
//
// LastRefreshAt is a freshness signal: the oldest time any of the owner's
// stored services was written, which is when the owner last completed an
// reconcile. It is zero when the reader cannot tell. How old is too old is the
// consumer's policy.
type OwnerStatus struct {
	OwnerID       string
	LastRefreshAt time.Time
}

// SkippedItem is a stored document that could not be read (hand edited,
// truncated, written in a format version this reader does not know, or listed
// and then gone before it could be fetched). It hides that service's endpoints,
// so a snapshot holding one is incomplete and an absence in it is not a
// deletion.
//
// A record that was read but is not acceptable is a RejectedEndpoint instead,
// which does not make a snapshot incomplete. That is the whole distinction: a
// document nobody can read, against a record known to be wrong.
type SkippedItem struct {
	// Service and OwnerID name the stored document. They are known from the
	// storage key, so they are available even when the document's contents are
	// not, and even when it was never read at all, which is what lets a consumer
	// hold back only the affected service instead of its whole view. Either is
	// empty when the reader cannot tell. A consumer reads these rather than
	// parsing Key, which is the store's own schema.
	Service string
	OwnerID string
	Key     string // best effort identity, for example the storage key of the object
	Err     error
}

// RejectedEndpoint is something a reader left out of a snapshot because it is
// not acceptable: a storage key that is not a valid state key, or a record
// with an unknown state or protocol or one that fails validation. It is not a
// decoding problem, so it does not make the snapshot incomplete: the record is
// known to be wrong, and holding the whole roster back for it would let one
// bad writer freeze every reader.
type RejectedEndpoint struct {
	OwnerID    string
	EndpointID EndpointID
	Service    string
	// Key is the storage key of the object the record came from, when the reader has one. A reader that cannot
	// tell which record was meant (a key it cannot parse) sets only Key and Err.
	Key string
	Err error
}

// Snapshot is one read of every owner's endpoints. It is not a point in
// time: writes may land while it is being read, so a consumer that acts on
// change hints should treat a snapshot as at least as new as Revision.
type Snapshot struct {
	// Endpoints of every owner, sorted by owner ID then endpoint ID.
	Endpoints []Endpoint
	// Owners holds each owner's status, sorted by owner ID.
	Owners []OwnerStatus
	// Generation and Revision are assigned by the reader (see SetRevision): a
	// counter that belongs to that reader, increasing exactly when the content
	// of the snapshot changes and equal when it does not. Revision is the same
	// number as a decimal string. A consumer that orders revisions can use it
	// directly, and must not compare revisions of different readers.
	Generation uint64
	Revision   string
	// ContentHash is a hash of Endpoints, independent of the revision. It is for
	// diagnostics and deduplication only: hashes have no order.
	ContentHash string
	// Complete is false when any item was skipped. Absence from an incomplete
	// snapshot must not be read as deletion: a skipped item may hold live
	// endpoints. Keep your last good state.
	Complete bool
	Skipped  []SkippedItem
	// Rejected lists what the reader left out because it is not acceptable. It
	// does not make the snapshot incomplete. Surface it: each entry is a
	// publisher writing something it should not.
	Rejected []RejectedEndpoint
	ReadAt   time.Time
}

// SetRevision assigns the snapshot's revision, as both the number and its decimal string.
func (s *Snapshot) SetRevision(n uint64) {
	s.Generation = n
	s.Revision = strconv.FormatUint(n, 10)
}

// NewSnapshot assembles a Snapshot: it sorts, hashes the content, and sets Complete. The caller sets the
// revision with SetRevision.
func NewSnapshot(readAt time.Time, endpoints []Endpoint, owners []OwnerStatus, skipped []SkippedItem, rejected []RejectedEndpoint) Snapshot {
	endpoints = sortedEndpoints(endpoints)
	owners = slices.Clone(owners)
	slices.SortFunc(owners, func(a, b OwnerStatus) int { return cmp.Compare(a.OwnerID, b.OwnerID) })
	return Snapshot{
		Endpoints:   endpoints,
		Owners:      owners,
		ContentHash: ContentHash(endpoints),
		Complete:    len(skipped) == 0,
		Skipped:     skipped,
		Rejected:    rejected,
		ReadAt:      readAt,
	}
}

// ByService services endpoints by service, each in snapshot order.
func (s Snapshot) ByService() map[string][]Endpoint {
	out := map[string][]Endpoint{}
	for _, b := range s.Endpoints {
		out[b.Service] = append(out[b.Service], b)
	}
	return out
}

// ContentHash returns a hash of the endpoints, independent of input order.
//
// Every value is written length prefixed rather than joined with a separator
// byte. A separator is only unambiguous while no value can contain it, and
// Revision moves exactly when this hash moves, so an ambiguity here is a real
// change that looks like no change. Length prefixing does not depend on
// Validate refusing any particular byte.
func ContentHash(endpoints []Endpoint) string {
	h := sha256.New()
	for _, b := range sortedEndpoints(endpoints) {
		hashText(h, b.OwnerID, b.Service, b.LogicalName, b.InstanceID, b.Address,
			string(b.Protocol), b.SNI, b.Region, b.Zone, string(b.State))
		// Writes to a hash never fail.
		_, _ = fmt.Fprintf(h, "%d;%t;%d;", b.Port, b.Serving, len(b.Labels))
		for _, k := range slices.Sorted(maps.Keys(b.Labels)) {
			hashText(h, k, b.Labels[k])
		}
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// hashText writes each value as its byte length, a colon, and the bytes, which
// no other sequence of values can reproduce.
func hashText(h io.Writer, values ...string) {
	for _, v := range values {
		_, _ = fmt.Fprintf(h, "%d:%s", len(v), v)
	}
}

func sortedEndpoints(endpoints []Endpoint) []Endpoint {
	out := slices.Clone(endpoints)
	slices.SortFunc(out, func(a, b Endpoint) int {
		return cmp.Or(cmp.Compare(a.OwnerID, b.OwnerID), cmp.Compare(a.ID(), b.ID()))
	})
	return out
}

// Reader returns a read of the roster. It returns an error, never a partial
// snapshot, if the read itself fails. Undecodable items are the one partial
// case, and they are reported in Snapshot.Skipped with Complete false.
type Reader interface {
	Snapshot(ctx context.Context) (Snapshot, error)
}

type HintKind int

const (
	// HintRescan means something may have changed, or changes may have been
	// missed, so read a fresh snapshot. It is the only kind, and it is the
	// first value after every (re)connect and after every recovery from an
	// error. A per owner kind existed once and was removed: no hint source
	// can name the owner that changed, because the S3 marker names none.
	HintRescan HintKind = iota
)

func (k HintKind) String() string {
	if k == HintRescan {
		return "rescan"
	}
	return "unknown"
}

// Hint is one change hint. It carries no identity: a hint says that something
// moved, never what.
type Hint struct {
	Kind HintKind
}

// HintSource delivers change hints. A hint says only that something moved, so
// the consumer's response is always to read a snapshot, which is cheap because
// readers re-read only what changed. Hints can be coalesced or
// delayed, so a consumer should also read a snapshot periodically.
//
// A hint is advisory in both directions: a source may deliver more hints than
// there were changes, and a consumer must treat a redundant read as normal
// rather than as a signal. Sources use that latitude to avoid missing a change,
// which is the only failure that matters here.
type HintSource interface {
	// Subscribe starts delivery. The channel closes when ctx is done. The first
	// value after every (re)connect is a HintRescan. A source may send further
	// hints at once, and MarkerPoller does: one before it has polled anything,
	// so an unreachable marker cannot hold up a consumer's first read, and one
	// after its baseline.
	Subscribe(ctx context.Context) (<-chan Hint, error)
}
