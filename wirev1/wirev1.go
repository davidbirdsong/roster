// Package wirev1 is version 1 of the roster wire format: the JSON object a publisher stores for one service in
// one owner, and the code that reads and writes it. It imports only the standard library and the roster
// package, so a publisher outside this repository can use it without any cloud SDK, and a publisher in another
// language can write the same JSON by hand.
//
// An object holds the full desired set of one service in one owner. The service and the owner ID are not in the
// object, they come from where it is stored, so there is nothing in it to forge. Nothing in it changes unless
// the endpoints change (no timestamps), so an unchanged service encodes to identical bytes.
//
// # Rules that keep the format compatible
//
// The names in the JSON are the identity of a field, so:
//
//   - A name is never renamed, never reused for a different meaning, and never changes type. Retired names are
//     listed below and are never reused.
//   - Inside a major version a change only adds optional fields. A reader ignores fields it does not know, so
//     never decode with DisallowUnknownFields.
//   - A field whose zero value is meaningful is a pointer (Serving). A pointer with omitempty omits only nil,
//     so a written false is kept. Every field has a permanent meaning for "missing", written next to it.
//   - Enum values are open strings. A reader that meets an unknown one rejects that record and reports it.
//   - Go matches JSON names case insensitively, so two names must differ by more than case (a test checks it).
//   - A breaking change is a new major (wirev2, stored under "v2/"), never an edit of this one.
//
// The frozen files in testdata are the compatibility contract: every version of this package must decode every
// one of them to the result stored beside it, and a test fails if a frozen file changes. A new shape is a new
// file, appended to corpus.sum.
//
// Retired names: none.
// Retired enum values: protocol "h2" (TLS with HTTP/2 only). A reader rejects a record that has it, like any
// unknown value. It was removed before any publisher used it, so it is not a new major.
package wirev1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/davidbirdsong/roster"
)

// Major is the first segment of the key an object of this version is stored under.
const Major = "v1"

// ServiceDocument is the object stored for one service in one owner.
type ServiceDocument struct {
	// Writer names the publisher and its version, for diagnostics. It must be constant for a running publisher,
	// or an unchanged service would encode to different bytes.
	Writer    string     `json:"writer,omitempty"`
	Endpoints []Endpoint `json:"endpoints"`
}

// Endpoint is one endpoint. Its fields mean what roster.Endpoint's do.
type Endpoint struct {
	InstanceID  string `json:"instance_id"`
	LogicalName string `json:"logical_name"`
	Address     string `json:"address"`
	Port        int    `json:"port"`
	// Protocol is "http", "https" or "h2c".
	Protocol string `json:"protocol"`
	SNI      string `json:"sni,omitempty"`
	Region   string `json:"region,omitempty"`
	Zone     string `json:"zone,omitempty"`
	// State is "NOT_READY", "READY" or "TERMINATING". It is required: a record without one is rejected.
	State string `json:"state"`
	// Serving says whether the endpoint can take connections. Missing means it is derived from State: READY
	// serves, anything else does not. That mapping is permanent.
	Serving *bool             `json:"serving,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}

// Rejection is a record of an object that was left out because it is not acceptable. The other records of the
// object are still returned: one bad record must not hide the rest.
type Rejection struct {
	// Index is the position of the record in the object.
	Index      int
	InstanceID string
	Err        error
}

func (r Rejection) Error() string {
	return fmt.Sprintf("record %d (instance_id %q): %v", r.Index, r.InstanceID, r.Err)
}

// Encode returns the object for one service: the endpoints sorted by ID, written the same way every time, so an
// unchanged service gives identical bytes. All endpoints must belong to the service the object is stored for, which
// the caller knows and the object does not carry. writer is stored as is.
func Encode(writer string, endpoints []roster.Endpoint) ([]byte, error) {
	out := ServiceDocument{Writer: writer, Endpoints: make([]Endpoint, 0, len(endpoints))}
	sorted := slices.Clone(endpoints)
	slices.SortFunc(sorted, func(a, b roster.Endpoint) int { return compareIDs(a.ID(), b.ID()) })
	for i, b := range sorted {
		if i > 0 && sorted[i-1].ID() == b.ID() {
			return nil, fmt.Errorf("wirev1: duplicate endpoint %q", b.ID())
		}
		if !b.State.Valid() {
			return nil, fmt.Errorf("wirev1: endpoint %q has state %q", b.ID(), b.State)
		}
		if !b.Protocol.Valid() {
			return nil, fmt.Errorf("wirev1: endpoint %q has protocol %q", b.ID(), b.Protocol)
		}
		serving := b.Serving
		w := Endpoint{
			InstanceID: b.InstanceID, LogicalName: b.LogicalName, Address: b.Address, Port: int(b.Port),
			Protocol: string(b.Protocol), SNI: b.SNI, Region: b.Region, Zone: b.Zone,
			State: string(b.State), Serving: &serving,
		}
		if len(b.Labels) > 0 {
			w.Labels = b.Labels
		}
		out.Endpoints = append(out.Endpoints, w)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return nil, fmt.Errorf("wirev1: encode: %w", err)
	}
	return buf.Bytes(), nil
}

func compareIDs(a, b roster.EndpointID) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// Decode reads the document stored for service under owner ownerID, which the caller takes from the key. It returns
// the acceptable endpoints, sorted by ID, and a Rejection for each record it left out. An error means the object
// as a whole cannot be read (it is not valid JSON), and the caller decides what that means for its snapshot.
func Decode(ownerID, service string, data []byte) ([]roster.Endpoint, []Rejection, error) {
	var gs ServiceDocument
	if err := json.Unmarshal(data, &gs); err != nil {
		return nil, nil, fmt.Errorf("wirev1: %w", err)
	}
	var (
		out      []roster.Endpoint
		rejected []Rejection
		seen     = map[roster.EndpointID]bool{}
	)
	for i, w := range gs.Endpoints {
		b, err := toEndpoint(ownerID, service, w)
		if err == nil && seen[b.ID()] {
			err = fmt.Errorf("duplicate of an earlier record with the same instance_id and port")
		}
		if err != nil {
			rejected = append(rejected, Rejection{Index: i, InstanceID: w.InstanceID, Err: err})
			continue
		}
		seen[b.ID()] = true
		out = append(out, b)
	}
	slices.SortFunc(out, func(a, b roster.Endpoint) int { return compareIDs(a.ID(), b.ID()) })
	return out, rejected, nil
}

func toEndpoint(ownerID, service string, w Endpoint) (roster.Endpoint, error) {
	state := roster.State(w.State)
	if w.State == "" {
		return roster.Endpoint{}, errors.New("state is required")
	}
	if !state.Valid() {
		return roster.Endpoint{}, fmt.Errorf("unknown state %q", w.State)
	}
	proto := roster.Protocol(w.Protocol)
	if !proto.Valid() {
		return roster.Endpoint{}, fmt.Errorf("unknown protocol %q", w.Protocol)
	}
	if w.Port < 1 || w.Port > 65535 {
		return roster.Endpoint{}, fmt.Errorf("port %d is out of range", w.Port)
	}
	serving := state == roster.StateReady
	if w.Serving != nil {
		serving = *w.Serving
	}
	b := roster.Endpoint{
		OwnerID: ownerID, Service: service, LogicalName: w.LogicalName, InstanceID: w.InstanceID,
		Address: w.Address, Port: uint16(w.Port), Protocol: proto, SNI: w.SNI, Region: w.Region, Zone: w.Zone,
		State: state, Serving: serving,
	}
	if len(w.Labels) > 0 {
		b.Labels = w.Labels
	}
	if err := b.Validate(); err != nil {
		return roster.Endpoint{}, err
	}
	return b, nil
}
