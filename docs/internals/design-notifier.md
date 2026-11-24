# Write notifiers

Status: built (`notify.go`, `rostertest/notifier.go`, `s3store.Marker.Notify`).

An optional hook that a `Registration` or a `Publisher` calls after a write has been stored, so
something downstream can learn of the write at once. The caller chooses, per call, how much
acknowledgement it wants back. The write itself still depends on S3 alone.

## Why

The library's write path ends at S3. Anything downstream learns of a write by polling the marker,
which costs up to a poll interval (2 seconds by default) and tells the writer nothing about whether
anyone saw it. Two kinds of program want more:

- **A push transport** (MQTT, for example) that wants to hint consumers sooner than the poll.
  It wants nothing back.
- **A delivery service with evidence**, such as pharos, which serves the roster to an external
  edge over xDS and can report whether the edge was sent a write and acknowledged it. A rotator
  gates teardown of an old proxy on that evidence.

The way to get either today is to put the program in front of the write: the publisher sends its
write to pharos, and pharos calls the `Publisher` or `Registration`. That makes every write depend
on the program being up, which is strictly less available than S3. While it is down, heartbeats stop
too, so every other consumer applying `policy.FilterStaleOwners` sees these owners go stale. When it
comes back, S3 holds whatever was written before the outage, not the current state. It also
contradicts this library's own model: "nothing funnels writes through a central publisher"
(README).

A notifier inverts that. The writer stores to S3 itself, exactly as it does now, and then tells the
notifier what it stored. If the notifier is down, the write has still happened, the marker still
fires, and pollers still see it. The caller loses only the evidence and the speed.

This is the write side counterpart of something already anticipated. `design-s3-hints.md` (the
write path, step 2) has the mark publish to MQTT, and `analysis-s3-mqtt-complexity.md` puts MQTT
behind `HintSource` on the read side. A notifier is where the publishing half of that goes.

## What it is not

- **Not a delivery guarantee or a queue.** A notification that fails is not retried by the library.
  The periodic snapshot read stays the correctness backstop for every consumer.
- **Not part of the write.** A notifier never makes a stored write report an error, and never
  causes one to be undone.
- **Not stateful.** The library keeps nothing per notification beyond the call.
- **Not a judgment.** As everywhere in roster, health, freshness, trust and routing stay with the
  caller. A notifier reports evidence. What the caller does with it is policy.

## The interface

New file `notify.go` in the root package. Standard library only.

```go
// Ack is how far a notification got. Levels are ordered, and each implies the ones below it.
type Ack int

const (
	AckNone      Ack = iota // nothing beyond the write to S3
	AckAccepted             // the notifier's own transport took it (an MQTT PUBACK, a pharos RPC that returned)
	AckDelivered            // handed to every downstream consumer the notifier knows of
	AckReceived             // every one of those consumers confirmed receipt
)

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
// evidence for. AckNone means the caller wants nothing back, so return as soon as the
// notification is handed off.
type Notifier interface {
	Notify(ctx context.Context, n Notification, want Ack) Receipt
}
```

### Levels

| Level | Means | marker | MQTT QoS 0 | MQTT QoS 1 | pharos |
|---|---|---|---|---|---|
| `AckNone` | stored, nothing more claimed | always | always | always | always |
| `AckAccepted` | the notifier's transport took it | the marker PUT returned | not reachable (no ack exists) | PUBACK | the RPC returned (pharos triggered its read) |
| `AckDelivered` | handed to every known consumer | not reachable | not reachable | not reachable | PUSHED |
| `AckReceived` | every known consumer confirmed receipt | not reachable | not reachable | not reachable | ACKED |

The levels are deliberately few and generic, so caller code can branch on them without knowing which
notifier is attached. What an implementation means precisely at each level is its own documentation.
For pharos, `AckReceived` is xDS ACK, which is receipt and not application (pharos design D2). The
rest of the implementation's richness (subscriber counts, NACK text, versions, supersession) travels
in `Detail`.

`Refused` is generic because its meaning is: a consumer said no, so waiting longer will not help.
Without it, a caller cannot tell "slow" from "rejected" without reading `Detail`.

## Attaching notifiers

There are two roles, set by separate options.

| Option | How many | Called with | Its receipt |
|---|---|---|---|
| `WithNotifier(n)` / `WithRegistrationNotifier(n)` | at most one, the primary | the caller's `want` | returned to the caller |
| `WithHintNotifier(n)` / `WithRegistrationHintNotifier(n)` | any number | always `AckNone` | logged on failure, never returned |

`WithHintTimeout(d)` / `WithRegistrationHintTimeout(d)` bound each hint call (default 5 seconds). The option types of the two writers differ, so each option comes in two names, as `WithLogger` and `WithRegistrationLogger` do.

The marker is a hint notifier. pharos is a primary. An MQTT publisher is a hint.

**Why two roles, and not a list with an aggregated receipt.** A list needs a rule for combining
receipts, and every rule is wrong somewhere. The minimum across notifiers would make the marker
(which can reach only `AckAccepted`) cap pharos's `AckReceived`. The maximum would let any one
notifier answer for all of them. Naming one notifier whose evidence is returned, and treating the
rest as fire and forget, keeps the receipt's meaning plain.

Setting a second primary, passing a nil notifier, or a hint timeout that is not positive is an error from the constructor.

## The API change

This is the breaking part. roster has no tags yet, so this is the cheapest moment to make it.

`ApplyOption` is renamed to `CallOption`, and every write method on both writers takes it.

| Before | After |
|---|---|
| `Registration.Register(ctx, endpoints ...Endpoint) error` | `Registration.Register(ctx, endpoints []Endpoint, opts ...CallOption) error` |
| `Registration.SetState(ctx, s) error` | `Registration.SetState(ctx, s, opts ...CallOption) error` |
| `Registration.Refresh(ctx) error` | `Registration.Refresh(ctx, opts ...CallOption) error` |
| `Registration.Deregister(ctx) error` | `Registration.Deregister(ctx, opts ...CallOption) error` |
| `Publisher.Publish(ctx, endpoints, opts ...ApplyOption) error` | `Publisher.Publish(ctx, endpoints, opts ...CallOption) error` |
| `Publisher.Reconcile(ctx, endpoints, opts ...ApplyOption) error` | `Publisher.Reconcile(ctx, endpoints, opts ...CallOption) error` |
| `PreserveServices(...) ApplyOption` | `PreserveServices(...) CallOption` |

New option:

```go
// Want asks the primary notifier for level and stores its receipt in r once the call returns.
func Want(level Ack, r *Receipt) CallOption
```

Usage:

```go
var r roster.Receipt
err := reg.Register(ctx, []roster.Endpoint{ep}, roster.Want(roster.AckReceived, &r))
switch {
case err != nil:
	// not stored, or only in part; the receipt covers only what was stored
case r.Reached >= roster.AckReceived:
	// the edge has it
case r.Refused:
	// the edge rejected it; r.Detail says why
default:
	// stored, evidence incomplete; r.Err says how far it got
}
```

These are refused before anything is written, with an error from the call:
- `Want` with no primary notifier attached.
- `Want` with a nil receipt pointer.
- `Want` with a level outside the constants.
- `PreserveServices` on a `Registration`, which owns one service and has nothing to preserve.

Without `Want`, the primary is called with `AckNone`, so a pharos primary still triggers its read
and propagation stays fast.

Register takes a slice, as `Publish` does, so that it can take options.

## What each call notifies

A notification lists only what the call stored. Two rules decide whether one is made at all:

1. At least one write or delete returned ok.
2. A call where every write is `WriteUnchanged` notifies nobody, primary or hint, unless `Want`
   asked for a level above `AckNone`.

Rule 2 exists because `Refresh` is the most frequent call (`../scaling.md`). Without it, every
heartbeat would call every notifier, and a pharos primary would do a snapshot read per heartbeat
for nothing. A caller that does want to ask "does the edge have my current state" calls
`Refresh(ctx, roster.Want(...))`. That really writes, so it is listed as `WriteUnchanged`, and
pharos answers it from existing evidence (pharos design D6).

| Call | Notification |
|---|---|
| `Register` | one `ServiceWrite` for the member's service. `WriteUnchanged` when this Registration is registered and its endpoints equal the new ones, otherwise `WritePut`. The first `Register` in a process is always `WritePut`. |
| `SetState`, state changed | one `ServiceWrite`, `WritePut` |
| `SetState`, nothing changed | none, with or without `Want`, because nothing is written. Use `Refresh` with `Want` to ask for evidence. |
| `Refresh` | one `ServiceWrite`, `WriteUnchanged`. Made only with `Want` (rule 2). |
| `Deregister` | one `ServiceWrite`, `WriteDeleted`. This happens even if no `Register` succeeded, since the delete is issued anyway. |
| `Publish` | one per service put or deleted. A desired service that needed no write is not listed, and a `Publish` with nothing to write notifies nothing. |
| `Reconcile` | one per service put, refreshed or deleted. A refreshed service is `WriteUnchanged`, a repaired damaged document is `WritePut`, and a deleted damaged document is `WriteDeleted`. A `Reconcile` that only refreshes notifies only with `Want` (rule 2). |
| a protected service (`PreserveServices`) | never listed, even when refreshed. The call claims nothing about it. |
| partial failure | only the services whose write or delete returned ok. The call's error is unchanged. |
| failure before any write (validation, `ErrNotInitialized`, `ListOwned`) | none. The receipt stays zero. |
| context cancelled part way | the services stored before the cancel (see contexts below) |

**Deletes must be explicit.** A consumer cannot infer a delete from what a notification leaves out,
because the notification may race the consumer's own read of S3. If the consumer already read the
delete, the service is in neither its before state nor its after state, and a delete that is not
named would never be waited on. For pharos that would be an `AckReceived` reported before the edge
acknowledged the empty assignment, which is the exact false evidence pharos is built to refuse.

## Rules the library keeps

1. **Notify after the write, outside the lock.** Each writer's mutex is held for the write and
   the update of what it remembers, then released, and only then is any notifier called. A
   notifier that blocks for a whole deadline waiting for `AckReceived` must never hold up `Refresh`
   or `SetState` on the same Registration. The notification itself is built while the lock is
   held, from the values just stored, so a later call cannot change what it reports. Those values
   are never changed in place (a writer replaces its slices), and each notifier is then given its
   own clone (rule 5).
2. **A notifier never changes the call's error**, and never changes what the writer remembers.
3. **Contexts.** The primary runs on the caller's `ctx`, because the caller controls how long it
   waits for evidence. Hints run on `context.WithoutCancel(ctx)` with their own timeout
   (`WithHintTimeout`, default 5 seconds), so a call whose context was cancelled after it stored
   something still hints what it stored. The call waits for hints only until they finish or the
   caller's ctx is done, whichever is first. A hint still running then continues in the
   background until it returns or its timeout ends. A `Deregister` on a shutdown deadline must not
   block past that deadline, and a hint lost to the process exiting is caught by pollers.
4. **Concurrency and goroutines.** The primary and every hint are called concurrently, and the
   call returns when the primary has returned and the hints have returned or the caller's ctx is
   done (rule 3). No goroutine started by the library outlives the hint timeout. An implementation
   that wants to send in the background owns that goroutine and its lifetime.
5. **The notification is a copy.** `Endpoints` slices and their `Labels` maps are cloned for each
   notifier, so a notifier may keep and change them, and neither the writer nor another notifier
   sees it.
6. **No ordering across calls.** Within one notification, `Writes` is sorted by service. Across
   notifications for one owner, any order is possible (rule 1), and so are duplicates: a caller
   retrying a partial failure notifies the services it rewrites again.
7. **Hint failures are logged** with the writer's logger, at warn, and go nowhere else.

## Removing MarkerWriter

Today `s3store.Store` takes `WithMarkerWriter(m)`, keeps a hash of what it last wrote to each key,
and calls `m.Update` after a PUT whose bytes changed (or whose key is new to the process) and after
every DELETE, logging and swallowing a failure. That is a hint notifier, built into the store
because there was nowhere else to put it. With notifiers, it moves up a layer.

**Change.**
- `s3store.Marker` implements `roster.Notifier`. `Notify` marks once when any `ServiceWrite` is
  not `WriteUnchanged`, does nothing for a notification that is all `WriteUnchanged`, and reports
  `AckAccepted` once the PUT returns. `Update` stays, for marking by hand.
- `s3store.MarkerWriter`, `WithMarkerWriter`, `Store.hashes` and `Store.mark` are removed. The store
  only stores, and the docs of `PutService` and `DeleteService` say plainly that they do not mark.
- Wiring changes from

  ```go
  store, _ := s3store.NewStore(client, loc, s3store.WithMarkerWriter(marker))
  reg, _ := roster.NewRegistration(store, self, "web")
  ```

  to

  ```go
  store, _ := s3store.NewStore(client, loc)
  reg, _ := roster.NewRegistration(store, self, "web", roster.WithRegistrationHintNotifier(marker))
  ```

**Behaviour differences, each acceptable.**

| Today | After | Why it is fine |
|---|---|---|
| one mark per changed service, inside each PUT | one mark per call, after its last write | Marks collapse anyway (`../scaling.md`), and a reader that reads after the last mark sees every write before it. A multi service `Publish` hints slightly later for its first service and causes fewer reads. |
| a restarted publisher marks once per key on its first write, even with identical bytes (`TODO.md` item 3) | `Reconcile` compares with what `ListOwned` read back, so an unchanged service is `WriteUnchanged` and does not mark | Settles TODO item 3. A member's first `Register` is still `WritePut` and still marks, which that item says it should. |
| a changed `Writer` string with identical endpoints marks (the bytes differ) | `WriteUnchanged`, no mark | Readers refetch on the ETag and find the same endpoints, so no revision moves. The mark was pure cost. |
| any direct caller of `Store.PutService` marks | only writers with the marker attached mark | Emergency writes go through a `Registration` or `Publisher` with the marker attached, not through raw store calls, so nothing depends on remembering to mark by hand. |
| a mark failure logged by the store's logger | logged by the writer's logger (rule 7) | The same information, one layer up. |

IAM does not change: the marker PUT is already granted to every publisher (`../s3-layout.md`).

**Why not keep both.** Two mark paths would double mark every write, or need a rule for which one
wins. The store keeping a hash of every key is state that exists only to decide whether to mark, and
the writer layer can decide that exactly, because it knows what it meant to store.

## Changes by file

| File | Change |
|---|---|
| `notify.go` (new) | `Ack`, `WriteKind`, `ServiceWrite`, `Notification`, `Receipt`, `Notifier`, `Want`, `CallOption`, `PreserveServices`, `DefaultHintTimeout`, and an unexported `notifiers` struct that both writers embed: the option checks, and `notify` (the all `WriteUnchanged` skip, concurrent calls, the hint context and early return, cloning per notifier, logging). |
| `publisher.go` | `ApplyOption` and `applyConfig` are replaced by `CallOption` and `callConfig` in `notify.go`, and the notifier options are added. `apply` collects a `[]ServiceWrite` as it walks services: put, deleted, refreshed (`WriteUnchanged`) and repaired, skipping protected services, services that needed no write, and failed writes. `Publish` and `Reconcile` release `mu` before calling `notify`. Locking moves from `defer` to an explicit unlock ahead of the notify. |
| `registration.go` | New signatures, notifier options, the `WriteUnchanged` decision in `Register`, and notify after unlock in every method. `PreserveServices` is refused. |
| `endpoint.go` | No change: nothing in it mentions `ApplyOption`. |
| `s3store/store.go` | Remove `MarkerWriter`, `WithMarkerWriter`, `hashes` and `mark`. The `PutService` and `DeleteService` docs say they do not mark. |
| `s3store/marker.go` | `Marker.Notify`, and `var _ roster.Notifier = (*Marker)(nil)`. |
| `rostertest/notifier.go` (new) | `Notifier` double: records every notification and `want`, returns a configurable `Receipt`, and can block until released (to test rule 1). |
| `.golangci.yml` | No change. The root package stays standard library only, and `s3store` already imports the root. |

## Tests

In `registration_test.go` and `publisher_test.go`, against `rostertest.Notifier`:

- **Rule 1.** While the primary blocks in `Notify`, a `Refresh` on the same Registration, and a
  `Publish` on the same Publisher, complete.
- **Rule 2.** A primary returning `Err` and `Refused` leaves the call's error nil and its memory
  updated.
- **Partial failure.** With `PutFails` on one service, only the others are notified, and the
  error is as today.
- **No write, no notification.** Validation errors, `ErrNotInitialized` and `ListErr` produce no
  notification and a zero receipt.
- **Deletes.**
  - A `Publish` that drops a service lists it as `WriteDeleted`.
  - `Deregister` without a prior `Register` lists its service as `WriteDeleted`.
- **Only what was stored.** A `Publish` that changes one service lists only that service, and a
  `Publish` with nothing to write notifies nothing.
- **Unchanged.**
  - `Refresh`, and a `Reconcile` of identical content, notify nothing without `Want`.
  - `Refresh` with `Want(AckReceived)` notifies once, `WriteUnchanged`.
  - A repeated `Register` of identical endpoints is `WriteUnchanged`, and the first one is
    `WritePut`.
  - `SetState` with no change notifies nothing, even with `Want`.
- **Protected services** are never listed.
- **Contexts.**
  - A context cancelled after the first service of a `Publish` still delivers a hint for that
    service, and the primary sees the cancelled context.
  - A call whose context ends while a hint blocks returns at the deadline, and the hint still
    completes within the hint timeout.
- **Refusals before writing.** `Want` without a primary, a nil pointer, a bad level, and
  `PreserveServices` on a Registration are refused, and nothing is written.
- **Copies.** Changing a notified endpoint's `Labels` does not change the writer's memory.

In `s3store`:
- `Marker.Notify` marks once per notification with a `WritePut` or `WriteDeleted`, and not for an
  all `WriteUnchanged` one.
- The existing store tests that count marks move to Registration and Publisher tests with the
  marker attached as a hint. The restart test (`store_test.go:133`) inverts: a restarted
  `Reconcile` of identical content no longer marks.

Run `go test -race ./...` and the integration suite against s3mock, and lint per `CLAUDE.md`,
saying where it was run.

## Docs to update in the same change

- `README.md`: the quick start wiring, the call tables in "A member's life" and "Aggregating
  publishers" (new signatures), a new section "Notifying something downstream", and "Testing your
  publisher" (the notifier double).
- `../scaling.md`: the marker PUT row becomes one per call with a change, and the "G per
  publisher restart" term goes for a Publisher.
- `docs/design-s3-hints.md`: the write path step 2, the marker bullets, and the line saying the store
  keeps a hash to decide marking.
- `docs/TODO.md`: item 3 is settled.
- `CLAUDE.md`: the root package line adds `Notifier`, and the rule "Add or change a
  `DocumentStore`, `Reader` or `HintSource` behavior, then update `rostertest`" gains `Notifier`.

## Guide for implementers

The contract, the MQTT example and how to test an implementation are in `../notifiers.md`.

### Worked example: pharos (a primary with evidence)

pharos serves the roster as xDS EDS to an external edge, reading S3 through a `roster.Reader`. Today
its write RPC performs the roster call itself. As a notifier, the roster call moves to the
publisher, and pharos keeps everything after the persist.

- **Client side.** `pharos/client.Notifier` implements `roster.Notifier` over a new unary RPC,
  `Await(owner, writes[{service, kind, endpoints}], level)`, which returns the same
  `WriteResult` the write RPC returns today. The level mapping is:
  - `AckNone`: the RPC is sent at PERSISTED level and not waited on, which still triggers pharos's
    read. A call that only refreshed never reaches the notifier without `Want`, so a read is
    triggered only by a real change.
  - `AckAccepted`: PERSISTED.
  - `AckDelivered`: PUSHED.
  - `AckReceived`: ACKED.

  A NACK sets `Refused`. `Detail` is `*pharosv1.WriteResult`.
- **Server side.**
  - `Await` triggers a snapshot read. The read begins after the RPC arrived, which is after the PUT
    returned, so causality orders them and no clocks are compared. pharos then runs its existing
    ladder walk (`internal/write.climb`).
  - The affected clusters come from the services named in the notification, translated to cluster
    names. They no longer come from a diff between the served state before and after the write.
    That diff assumed pharos captured "before" ahead of the PUT, which is no longer true: the marker
    poll may already have read the write. For a named delete, the removed cluster is counted, and
    the empty assignment is waited on.
  - Supersession (pharos D7) is checked per named service: a put compares its endpoints, and a
    delete expects absence.
  - `WriteUnchanged` writes go through pharos's unchanged rule (D6): `AckReceived` only on
    existing evidence for the served version.
- **What pharos sheds.** The Publisher per owner it keeps, the restart retry of PUBLISH as
  RECONCILE (D9), the per document lock, and writer contention tracking (pharos no longer sees
  writers). Write authorization shrinks to "may this caller ask for evidence about owner X". It
  stays, because the ladder reveals consumer state, but it grants nothing that writes. Publishers
  write S3 with their own IAM, scoped by key prefix as `../s3-layout.md` describes.
- **What pharos keeps.** The write RPC, deprecated, for any publisher that cannot hold S3
  credentials.
- **Wiring in a rotator.**

  ```go
  reg, _ := roster.NewRegistration(store, self, "fe-edge",
  	roster.WithRegistrationHintNotifier(marker), // pollers
  	roster.WithRegistrationNotifier(pharosNotifier))

  var r roster.Receipt
  err := reg.Register(ctx, []roster.Endpoint{next}, roster.Want(roster.AckReceived, &r))
  if err == nil && r.Reached >= roster.AckReceived {
  	_ = reg.SetState(ctx, roster.StateTerminating) // on the old member's own Registration, in practice
  }
  ```

## Open questions

1. **Whether `AckAccepted` earns its place.** It is kept for pharos as a primary: PERSISTED tells a
   caller "pharos is down" apart from "the edge is slow". The marker and MQTT can reach it too, but
   they are hints, so their receipts are discarded. If no primary ever reports it, three levels are
   simpler.
2. **The default hint timeout.** 5 seconds is a guess, roughly an S3 PUT under trouble.
3. **Whether `Receipt` should report hint outcomes.** Today they are only logged. A caller could
   want to know the marker failed, though nothing it could do differs.
4. **A writer level default for `want`**, so a rotator does not pass `Want` on every call. It is
   left out until a caller shows it is wanted, because per call is the explicit form.
5. **Whether background hints need a bound.** A burst of calls cancelled while their hints block
   leaves one goroutine per hint running for up to the hint timeout. Whether the library caps
   that, or leaves it to the caller's call rate, is undecided.

