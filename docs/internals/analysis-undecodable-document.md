# The undecodable document fix

A walkthrough of the one deliberate interface change in `DocumentStore`: why a single corrupt object used to be immortal, why the worst of the damage was on the read side rather than the write side, and why the fix is a wider return type instead of a flag or an error.

This was the highest priority item in the review of this library, ahead of everything the review itself ranked above it.

## The shape of the system

One object holds the full endpoint set of one service for one owner, at `v1/<service>/<owner>`. Publishers write their own objects. Readers list everything and build a snapshot. There are no tombstones and no generation counters: every read is of current state, and absence means absence.

Two consequences matter here. A publisher is the **only** thing that can remove one of its own objects, because every `DocumentStore` call is scoped to one owner. And a reader that cannot read an object has no way to tell anyone to go fix it, because readers never write.

So if a publisher cannot see an object, nothing in the system can remove it.

## The bug

`ListOwned` reads back everything the owner has stored, and the publisher reconciles against that. The old version dropped anything it could not decode:

```go
bs, _, err := wirev1.Decode(owner, service, data)
if err != nil {
	s.log.Warn("s3store: skipping an object of this owner that cannot be decoded", "key", o.Key, "err", err)
	continue                      // <- the object is now invisible to the publisher
}
out = append(out, bs...)
```

The old signature returned a flat `[]Endpoint`, which is the root of it. A flat endpoint list has no way to say "there is a document here, for this service, and its contents are unusable." Dropping it was the only option the type allowed.

A document becomes undecodable from hand editing, a truncated write, a lifecycle or replication tool rewriting it, or anything that does not speak the wire format writing to the same key. It is not frequent. It needs to happen once.

## Why the write side consequence is bad

`(*Publisher).apply` builds its work list from the services it knows about: the desired set, plus whatever `published` says is already stored.

A dropped document is in neither. So:

- Its service is no longer desired. Nothing deletes it, because nothing knows it is there. It stays in the bucket forever.
- Its service is still desired. The publisher sees no stored content for it, so it writes the desired set, which happens to repair the object. This case was already fine by accident.

The first case is the leak. An operator can delete the object by hand, if they know to look, which they do only if they read the warning log.

## Why the read side consequence is worse, and what the review missed

Readers also encounter that object, and they handle it correctly:

```go
if d.err != nil {
	skipped = append(skipped, roster.SkippedItem{
		Service: o.service, OwnerID: o.owner, Key: o.Key, Err: d.err,
	})
	continue
}
```

`s3store/reader.go:188`. A document that will not decode is reported in `Snapshot.Skipped`. And completeness is defined as the absence of skips, in `reader.go:109`:

```go
Complete: len(skipped) == 0,
```

`Complete` is the signal that says an absence in this snapshot can be acted on. A consumer that sees `Complete == false` is supposed to keep its last good state rather than treat a missing endpoint as a removal. That rule is the reason the library exists in the form it does.

So one corrupt object pins **every reader in the fleet** at `Complete == false`, permanently. Not during an incident. Forever, until someone notices and deletes the object by hand.

The effect is not that readers break. It is that the completeness signal stops carrying information. Every snapshot is incomplete, so consumers either ignore the flag, which discards the protection it exists to provide, or honour it and stop acting on real removals. A fleet wide, silent, indefinite degradation of the one safety property this design is built around, caused by one bad object that nothing is able to clean up.

That is the argument for fixing this first, and it is the half of the finding the review did not make. The review found the publisher leak and called it a stranded object. The stranded object is the symptom. The reader consequence is the damage.

## Why not a flag, an error, or a sentinel

Three cheaper shapes were available and all of them are wrong.

**Return an error from `ListOwned`.** This conflates a damaged document with a failed read, and those must behave in opposite ways. A failed read must abort the whole reconcile before any delete, because an empty or short list is indistinguishable from "this owner has no services" and the publisher would delete everything. A damaged document, on the other hand, is a successful read of a real document and the reconcile should proceed and fix it. Making one an error would stop the reconcile that is the only thing able to repair it.

**Keep the flat list and add a parallel `[]string` of damaged services.** Two return values that must be read together, with nothing in the type system saying so, and a caller that reads only the first one gets exactly the old bug back silently.

**Put a sentinel `Endpoint` in the list.** A damaged document has no valid endpoints by definition, and every consumer of the list would need to filter it.

The actual answer is that the return type was lossy. One stored document is the unit the publisher acts on, so the return type should be a list of stored documents, not a list of endpoints flattened out of them.

## The fix

`OwnedService` in `endpoint.go:283`:

```go
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
```

`Service` is outside the union on purpose. It is known from the key, so it is available even when nothing else about the document is, and it is the only thing the publisher needs in order to act. `Key` is for the operator reading the error, and the doc comment says the publisher never parses it, so the storage schema stays the store's business.

The interface, `endpoint.go:310`:

```go
// ListOwned returns one OwnedService for every service document the owner
// has stored, following all pagination. It must return an error, never a
// partial list, so a failed read cannot look like an empty service.
//
// A document that was read but could not be decoded is returned with its
// DecodeErr set, never left out. Leaving it out would hide a document that
// the owner still owns and that nothing else can remove, because a
// publisher only ever acts on the services it knows about.
ListOwned(ctx context.Context, ownerID string) ([]OwnedService, error)
```

Both halves of the contract are now written down: never a partial list, and never a silent omission. They are the same rule applied to two different failures.

The S3 implementation, `s3store/store.go:148`:

```go
bs, _, err := wirev1.Decode(owner, service, data)
if err != nil {
	s.log.Warn("s3store: an object of this owner cannot be decoded, reporting it for repair or removal",
		"key", o.Key, "err", err)
	out = append(out, roster.OwnedService{Service: service, DecodeErr: err, Key: o.Key})
	continue
}
out = append(out, roster.OwnedService{Service: service, Endpoints: bs, Key: o.Key})
```

Note that the GET failing a few lines above still returns an error and aborts. Only a successful read that will not decode becomes an `OwnedService`.

## The publisher side

`Reconcile` now sorts what it read into two maps, `publisher.go:133`:

```go
published := map[string][]Endpoint{}
damaged := map[string]OwnedService{}
for _, doc := range owned {
	if doc.Damaged() {
		damaged[doc.Service] = doc
		continue
	}
	for _, b := range doc.Endpoints {
		if b.OwnerID != p.ownerID {
			return fmt.Errorf("store returned endpoint %q owned by owner %q, refusing to reconcile", b.ID(), b.OwnerID)
		}
		published[b.Service] = append(published[b.Service], b)
	}
}
```

`damaged` is state on the `Publisher`, `publisher.go:44`, because `apply` runs after `Reconcile` has released its view of storage:

```go
// damaged holds the services whose stored document the last Reconcile
// could not decode. They are not in published, because there is no known
// content for them, but they do exist in storage and a Reconcile must
// still repair or remove them.
damaged     map[string]OwnedService
```

Those services join the work list, `publisher.go:221`:

```go
if refresh {
	for g := range p.damaged {
		services = append(services, g)
	}
}
```

`refresh` is true only from `Reconcile`, which is the only path that read storage and therefore the only path that knows anything is damaged. `Publish` does not read, so it must not act on a stale `damaged` map.

Then three new branches in the switch, `publisher.go:245`:

```go
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
	p.log.Info("published", "op", "delete", "service", g, "reason", "undecodable and no longer desired")
case isDamaged:
	if err := p.put(ctx, g, want, "repair"); err != nil {
		errs = append(errs, err)
		continue
	}
	delete(p.damaged, g)
	p.published[g] = want
```

Three cases because there are exactly three things a publisher can do about a document whose contents it cannot read:

| Case | Action | Why |
|---|---|---|
| no longer desired | delete | nothing else can remove it, and it is making every reader incomplete |
| still desired | overwrite with the desired set | the content is known from the desired state, so the damage is repairable |
| protected | report an error | `PreserveServices` says do not touch it, and there is no readable content to refresh it with either |

The protected case is the interesting one. `PreserveServices` exists so that a publisher managing part of a roster does not delete another writer's services during its reconcile. A protected service whose document is damaged can be neither repaired (there is no desired set for it, by definition) nor refreshed (there is nothing readable to rewrite). Silently leaving it is how the original bug behaved. So it returns an error naming the service, the key and the decode failure, which goes to the operator who is the only one who can resolve it. The reconcile still completes for every other service, because `apply` accumulates into `errs` and joins at the end.

Deleting the damaged entry from the map on success matters: without it a later `Publish` with a stale `damaged` map would act on a document that is no longer damaged.

## The test double

`CLAUDE.md` requires `rostertest` to change in the same commit as any `DocumentStore` behaviour, so `rostertest.Store` grew a `Damaged` field:

```go
// Damaged makes ListOwned report these services as stored documents that
// could not be decoded, keyed by service, with the error to report.
Damaged map[string]error
```

and `ListOwned` groups its items by service into `OwnedService` values and appends the damaged ones after. Without this, none of the three new publisher branches would be reachable from a test at all, which is most of the reason the original bug lived as long as it did: there was no way to express "a document exists and is unreadable" in the test vocabulary.

## Tests

Four in `publisher_test.go`, one per branch plus the case it must not be confused with:

| Test | Asserts |
|---|---|
| `TestReconcile_DeletesAnUndecodableDocumentThatIsNoLongerDesired` | the delete happens, so the object is not stranded |
| `TestReconcile_RepairsAnUndecodableDocumentThatIsStillDesired` | overwritten with the desired set, never deleted |
| `TestReconcile_ReportsAnUndecodableDocumentOfAProtectedService` | an error naming the service, and no put or delete for it |
| `TestReconcile_AFailedReadStillAbortsBeforeAnyDelete` | `store.Writes()` is zero, so an outage is still not an empty roster |

That last one is there because the whole fix moves in the direction of "act on what you read," and the opposite rule for a failed read has to stay nailed down beside it.

On the store side, `TestListOwnedReportsAnObjectItCannotDecodeRatherThanHidingIt` writes a corrupt object for this owner and another for a different owner, then asserts the first is reported with its key and the second is still filtered out. Owner scoping is not weakened by reporting damage.

One of these tests was wrong when first written. `TestReconcile_ReportsAnUndecodableDocumentOfAProtectedService` asserted that nothing at all was deleted, but the fixture also had an undesired, unprotected service `web`, which the publisher deletes correctly. The assertion now checks only that `api` is absent from the deletes. The test was wrong, not the code.

Negative check, with the damaged work list removed from `apply`:

```text
--- FAIL: TestReconcile_DeletesAnUndecodableDocumentThatIsNoLongerDesired
--- FAIL: TestReconcile_ReportsAnUndecodableDocumentOfAProtectedService
```

The repair test passes without the fix, which is correct and worth understanding. A desired service whose stored document is damaged has no entry in `published`, so it falls through to the ordinary `!hasHave` branch and gets written anyway. That is the case that was already fine by accident, as above. The two failures are the two cases that were genuinely broken.

## What is still open

A damaged document of a protected service returns an error on every reconcile, forever, until an operator intervenes. That is intentional, because the alternatives are to delete another writer's document or to hide the problem, and both are worse. It does mean a reconcile loop that treats any error as a failure will report an owner as unhealthy indefinitely for a condition only a human can clear. Worth knowing when wiring up the reconcile loop's alerting.
