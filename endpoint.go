// Package roster is the public library for publishing ingress endpoints to
// the shared roster. A publisher builds a list of Endpoint values and hands
// it to a Publisher; it never sees how they are stored, which lives behind the
// DocumentStore interface (see the s3store subpackage).
//
//	store, _ := s3store.NewStore(s3Client, s3store.Config{Bucket: "myteam-roster-us-east-1"})
//	pub, _ := roster.NewPublisher(store, "eks-us-east-1-a")
//	err := pub.Reconcile(ctx, endpoints) // full repair, call at startup and periodically
//	err = pub.Publish(ctx, endpoints)   // cheap incremental pass after a change
//
// Publish and Reconcile return their errors and start no goroutines. What the library logs goes to a Logger, which
// WithLogger (here, and in s3store for the DocumentStore and the marker poller) sets. *slog.Logger satisfies it. Another
// logger needs a small adapter: Logger methods take alternating keys and values, for example for zerolog
//
//	func (z zlog) Info(msg string, args ...any) { z.l.Info().Fields(args).Msg(msg) }
//
// The package has no Kubernetes or AWS imports.
package roster

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"reflect"
	"regexp"
	"slices"
)

type State string

const (
	StateNotReady    State = "NOT_READY"
	StateReady       State = "READY"
	StateTerminating State = "TERMINATING"
)

// Valid reports whether s is one of the defined states.
func (s State) Valid() bool {
	return s == StateNotReady || s == StateReady || s == StateTerminating
}

// Protocol is how a reader should talk to an endpoint. It combines two things:
// whether the connection uses TLS, and which HTTP version it speaks. A value
// states a fact about the endpoint, and a consumer applies its own policy: only
// where a consumer cannot discover the HTTP version (plaintext HTTP/2) does the
// value pin one.
type Protocol string

const (
	// ProtocolHTTP is plaintext HTTP/1.1.
	ProtocolHTTP Protocol = "http"
	// ProtocolHTTPS is TLS, with the HTTP version negotiated by ALPN. A consumer
	// may use any version the endpoint offers.
	ProtocolHTTPS Protocol = "https"
	// ProtocolH2C is plaintext HTTP/2 with prior knowledge. With no TLS there
	// is no ALPN, so the reader cannot discover this and must be told.
	ProtocolH2C Protocol = "h2c"
)

func (p Protocol) Valid() bool {
	switch p {
	case ProtocolHTTP, ProtocolHTTPS, ProtocolH2C:
		return true
	}
	return false
}

// TLS reports whether connections use TLS, and so need an SNI.
func (p Protocol) TLS() bool { return p == ProtocolHTTPS }

// Severity orders states when two observations collide on one endpoint.
// Higher is worse.
func (s State) Severity() int {
	switch s {
	case StateTerminating:
		return 2
	case StateNotReady:
		return 1
	default:
		return 0
	}
}

// EndpointID is an endpoint's storage key within one owner, derived from its
// InstanceID and Port. Publishers never set it. It is exposed only so DocumentStore
// implementations can address a record, and it is never reused for a different
// endpoint, because InstanceID is unique to one instance.
type EndpointID string

// MaxInstanceIDLen caps InstanceID, which is part of the storage key.
const MaxInstanceIDLen = 512

// Endpoint is one individually addressable endpoint. Compare endpoints with
// Equal, not ==: Labels is a map. An endpoint must not carry fields that change
// without a real change to the endpoint (timestamps, versions), or unchanged
// reconciles would write and move the owner's generation.
//
// Three fields identify an endpoint, and they are not interchangeable:
//
//   - InstanceID names one running instance. It is never reused.
//   - LogicalName is the value a consumer hashes on (for example as an HAProxy
//     server id). The publisher chooses it. Affinity is available on this value
//     but not required: a publisher that keeps the name across instance
//     replacements gives consumers affinity, and one that cannot just uses
//     whatever identifies the instance and gives none. Several records may share
//     one LogicalName, for example a draining instance and its successor. See
//     PreferEndpoint.
//   - Service says which pool the endpoint belongs to.
type Endpoint struct {
	OwnerID string // filled in by the Publisher when empty
	// Service is the publisher's name for the pool this endpoint belongs to (a
	// shard, a service). It is the publisher's own identifier and never a
	// consumer's local name: consumers map it to theirs, optionally using Labels.
	Service string
	// LogicalName is the identity a consumer hashes on. Affinity is available on
	// it, not required. See Endpoint.
	LogicalName string
	// InstanceID is any string unique to the instance behind this endpoint and never
	// reused for another one: a Kubernetes pod UID, an ECS task ARN, a hostname
	// that is unique per instance. All ports of one instance share it.
	InstanceID string
	Address    string // IP address only, never a hostname
	Port       uint16
	Protocol   Protocol
	// SNI is the DNS name sent as the TLS server name and checked against the
	// server's certificate when connecting to Address. Required when Protocol
	// uses TLS, optional otherwise. It must be a concrete name that the server's
	// certificate covers, never a wildcard pattern, because SNI cannot carry one.
	// It is not the instance's own host name, which is usually different.
	SNI     string
	Region  string // optional, for example "us-east-1"
	Zone    string // optional, for example "us-east-1a"
	State   State
	Serving bool
	// Labels are small publisher supplied attributes a consumer can use to
	// decide what an endpoint means to it, for example which of its own pools it
	// belongs to. They are data, never commands: a consumer must treat keys and
	// values as untrusted text. See ValidateLabels for the limits. Nil and empty
	// are the same.
	Labels map[string]string
}

// ID returns the endpoint's storage key, <InstanceID>:<Port>. One instance can
// expose several ports, and each is its own record.
func (b Endpoint) ID() EndpointID {
	return EndpointID(fmt.Sprintf("%s:%d", b.InstanceID, b.Port))
}

// Equal reports whether two endpoints are the same, treating nil and empty
// Labels as equal.
func (b Endpoint) Equal(o Endpoint) bool {
	return reflect.DeepEqual(b.normalized(), o.normalized())
}

func (b Endpoint) normalized() Endpoint {
	if len(b.Labels) == 0 {
		b.Labels = nil
	}
	return b
}

// Limits on labels. They keep an item small and the charset boring, so a
// consumer that turns labels into names or config never meets surprises. Keys
// and values follow Kubernetes label syntax, so Service labels always fit.
const (
	MaxLabels        = 20
	MaxLabelKeyLen   = 128
	MaxLabelValueLen = 63
)

var (
	labelKeyRe   = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._/-]*[A-Za-z0-9])?$`)
	labelValueRe = regexp.MustCompile(`^([A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)?$`)
)

// ValidateLabels checks a label set: at most MaxLabels entries, keys of
// letters, digits and . _ / - (starting and ending with a letter or digit, at
// most MaxLabelKeyLen long), values of letters, digits and . _ - (the same
// shape, at most MaxLabelValueLen long, and may be empty).
func ValidateLabels(labels map[string]string) error {
	if len(labels) > MaxLabels {
		return fmt.Errorf("%d labels, at most %d are allowed", len(labels), MaxLabels)
	}
	var errs []error
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		if len(k) > MaxLabelKeyLen || !labelKeyRe.MatchString(k) {
			errs = append(errs, fmt.Errorf("label key %q is not valid", k))
		}
		if v := labels[k]; len(v) > MaxLabelValueLen || !labelValueRe.MatchString(v) {
			errs = append(errs, fmt.Errorf("label %q has an invalid value %q", k, v))
		}
	}
	return errors.Join(errs...)
}

// Validate reports whether the endpoint can be published. The roster
// rejects invalid endpoints up front, because a stored item that cannot be
// decoded later would fail every subsequent reconcile of its owner.
func (b Endpoint) Validate() error {
	var errs []error
	require := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	require("OwnerID", b.OwnerID)
	if b.OwnerID != "" {
		if err := ValidSegment("OwnerID", b.OwnerID); err != nil {
			errs = append(errs, err)
		}
	}
	require("Service", b.Service)
	if b.Service != "" {
		if err := ValidSegment("Service", b.Service); err != nil {
			errs = append(errs, err)
		}
	}
	require("LogicalName", b.LogicalName)
	if err := ValidText("LogicalName", b.LogicalName, MaxLogicalNameLen); err != nil {
		errs = append(errs, err)
	}
	require("InstanceID", b.InstanceID)
	if err := ValidText("InstanceID", b.InstanceID, MaxInstanceIDLen); err != nil {
		errs = append(errs, err)
	}
	if err := ValidText("Region", b.Region, MaxRegionLen); err != nil {
		errs = append(errs, err)
	}
	if err := ValidText("Zone", b.Zone, MaxZoneLen); err != nil {
		errs = append(errs, err)
	}
	if !b.Protocol.Valid() {
		errs = append(errs, fmt.Errorf("Protocol %q is not one of http, https, h2c", b.Protocol))
	} else if b.Protocol.TLS() {
		require("SNI", b.SNI)
	}
	if b.SNI != "" {
		if err := validSNI(b.SNI); err != nil {
			errs = append(errs, err)
		}
	}
	if err := ValidateLabels(b.Labels); err != nil {
		errs = append(errs, err)
	}
	if _, err := netip.ParseAddr(b.Address); err != nil {
		errs = append(errs, fmt.Errorf("address %q is not an IP address", b.Address))
	}
	if b.Port == 0 {
		errs = append(errs, errors.New("port is required"))
	}
	if !b.State.Valid() {
		errs = append(errs, fmt.Errorf("State %q is not one of NOT_READY, READY, TERMINATING", b.State))
	}
	return errors.Join(errs...)
}

var sniRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$`)

// validSNI accepts a concrete DNS name. Consumers reject anything else,
// and a consumer that validates a whole snapshot at once would reject all of it
// for one bad name, so it is stopped here.
func validSNI(s string) error {
	if len(s) > 253 || !sniRe.MatchString(s) {
		return fmt.Errorf("SNI %q must be a concrete DNS name (letters, digits, hyphens and dots; no wildcard)", s)
	}
	if _, err := netip.ParseAddr(s); err == nil {
		return fmt.Errorf("SNI %q is an IP address; SNI needs a DNS name", s)
	}
	return nil
}

// OwnedService is one stored service document of one owner, as a DocumentStore
// found it. It names the service even when the document's contents are
// unusable, which is what lets a Publisher repair or remove a damaged document
// instead of leaving it in storage forever.
//
// Exactly one of Endpoints and DecodeErr is meaningful. When DecodeErr is set
// the stored bytes could not be decoded (hand edited, truncated, or written by
// something that does not speak the wire format) and Endpoints is nil. A
// failed read is not a damaged document: that is an error from ListOwned.
type OwnedService struct {
	Service   string
	Endpoints []Endpoint
	DecodeErr error
	// Key is the storage identity of the document, for diagnostics, when the
	// store has one. It is never parsed by the Publisher.
	Key string
}

// Damaged reports whether the stored document could not be decoded.
func (o OwnedService) Damaged() bool { return o.DecodeErr != nil }

// EndpointWriter writes and removes one owner's document in one service, and
// never reads. It is all a Registration needs, because a process that publishes
// only itself knows the one document it owns without looking.
//
// The unit of storage is one service in one owner: the full set of that
// service's endpoints, written and removed as a whole.
type EndpointWriter interface {
	// PutService replaces the stored set of one service in the owner. Writing the
	// same set again changes nothing a reader can see except that the write is
	// recent, which is how a publisher shows it is alive.
	PutService(ctx context.Context, ownerID, service string, endpoints []Endpoint) error
	// DeleteService removes the stored set of one service. Removing one that is not
	// there is not an error.
	DeleteService(ctx context.Context, ownerID, service string) error
}

// DocumentStore is the persistence boundary of a Publisher, which also reads
// back what an owner has stored. Every call is scoped to one owner, so an
// implementation can never be asked to touch another owner's records.
// Implementations own the storage schema.
type DocumentStore interface {
	EndpointWriter
	// ListOwned returns one OwnedService for every service document the owner
	// has stored, following all pagination. It must return an error, never a
	// partial list, so a failed read cannot look like an empty service.
	//
	// A document that was read but could not be decoded is returned with its
	// DecodeErr set, never left out. Leaving it out would hide a document that
	// the owner still owns and that nothing else can remove, because a
	// publisher only ever acts on the services it knows about.
	ListOwned(ctx context.Context, ownerID string) ([]OwnedService, error)
}
