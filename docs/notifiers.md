# Notifiers

A writer stores to S3 and then tells its notifiers what it stored. A notifier is something downstream that wants to learn of a write at once (a push transport such as MQTT), or a delivery service that can report whether its consumers have it. The write never depends on a notifier: if one is down, the write has still happened, the marker still fires, and pollers still see it.

There are two roles.

| Option | How many | Called with | Its receipt |
|---|---|---|---|
| `WithNotifier` / `WithRegistrationNotifier` | at most one, the primary | the level the caller asks for with `Want` | returned to the caller |
| `WithHintNotifier` / `WithRegistrationHintNotifier` | any number | always `AckNone` | logged at warn on failure |

`s3store.Marker` is a hint, as the quick start shows. A primary is a service that can report evidence:

```go
var r roster.Receipt
err := reg.Register(ctx, []roster.Endpoint{ep}, roster.Want(roster.AckReceived, &r))
switch {
case err != nil:
	// not stored, or only in part
case r.Reached >= roster.AckReceived:
	// every known consumer confirmed receipt
case r.Refused:
	// a consumer rejected it; r.Detail says why
default:
	// stored, evidence incomplete; r.Err says how far it got
}
```

The levels are `AckNone`, `AckAccepted` (the notifier's transport took it), `AckDelivered` (handed to every known consumer) and `AckReceived` (every known consumer confirmed). Each implementation documents what it can reach.

A notification lists only what the call stored, one `ServiceWrite` per service, as `WritePut`, `WriteUnchanged` (a refresh) or `WriteDeleted`. A call where everything is `WriteUnchanged`, such as every `Refresh`, notifies nobody unless it asked with `Want`, so heartbeats cost a notifier nothing. To ask whether downstream has a member's current state, call `Refresh(ctx, roster.Want(...))`.

Notifiers are called after the writer's lock is released, so a primary waiting for evidence holds up no other call. The primary runs on the caller's context. Hints run on a context that ignores the caller's cancellation, bounded by `WithHintTimeout` (5 seconds by default), and the call waits for them only until its own context is done. To write a notifier, see the guide below. [internals/design-notifier.md](internals/design-notifier.md) lists what each call notifies.

## Guide for implementers

A notifier lives in its own module or repository, never in roster, which imports no
client of the library. The one exception is `s3store.Marker`, because the marker is part of the
storage contract.

### The contract an implementation keeps

1. **Never claim a level without evidence.** `Reached` is the highest level something
   demonstrably happened at. A disconnect, zero consumers, a timeout or a restart is not evidence.
   Report the level reached, and say why in `Err`.
2. **Return at `want`, not later.** Once `want` is reached, return, even if more evidence is coming.
   A primary called with `AckNone` hands the notification off and returns, without waiting on the
   network for a reply, because the caller waits for it. A hint is always called with `AckNone`, but
   the library runs it in its own goroutine and bounds it with the hint timeout, so a hint may block
   on its transport, as `s3store.Marker` blocks on its PUT.
3. **Honour `ctx`.** When it is done, return at once with what was reached. A caller's deadline is
   its budget for evidence, and the library has already stored the write.
4. **Be safe for concurrent use, and expect any order.** Two notifications for one owner can
   overlap, arrive reversed, or repeat. Work from content (what is stored now), never from deltas
   between notifications. A later notification may make an earlier one's evidence moot. Decide what
   that means for you, and document it.
5. **Treat `WriteUnchanged` as no change.** It arrives only when the caller asked for evidence, so
   answer it from evidence you already hold. It must not cause a push, a new version or a mark.
6. **Treat `WriteDeleted` as a delete**, and absence from a notification as nothing at all. A
   notification lists only what one call stored, so absence is never a delete.
7. **Do not dial in `Notify`.** Connection lifecycle belongs to the implementation's constructor and
   its own `Close`. When disconnected, return at once with the level reached and `Err` set. The
   write has happened, and pollers will see it.
8. **Hold nothing authoritative.** S3 is the only source of truth. A notifier that has to know
   current state reads it through a `roster.Reader`, as any consumer does, and treats an incomplete
   snapshot the way the library says (never as a deletion).
9. **Document `Detail`.** Export its concrete type, and say in the package doc which levels you can
   reach and what each means for your consumers.
10. **Test with `rostertest`.** Drive your notifier from a real `Registration` and `Publisher` over
    `rostertest.Store`. The library's notifications are the contract, so do not hand build
    `Notification` values except to test malformed input.

### Worked example: MQTT (a hint, fire and forget)

An MQTT notifier publishes a hint so that MQTT subscribers read a snapshot sooner than their marker
poll. It is the publishing half of an MQTT `HintSource`, which the library does not include.

- **Payload: content free**, matching `HintRescan`, the only hint kind. A consumer answers by reading
  a snapshot. If owner or service names are ever carried, add the hint kind that uses them at the
  same time. Do not carry endpoints: S3 is the truth, and a message
  carrying state invites a consumer to act on it without reading.
- **Levels.** QoS 0 reaches `AckNone` only, since no acknowledgement exists to be evidence. QoS 1
  reaches `AckAccepted` on PUBACK, which is the broker's receipt and not any subscriber's. MQTT has
  no per subscriber acknowledgement, so `AckDelivered` and `AckReceived` are not reachable. Do not
  fake them.
- **Ignore notifications that are all `WriteUnchanged`.** They reach a hint only when a primary was
  asked for evidence, and a message for them would wake every subscriber for nothing.
- **Attach it as a hint**, beside the marker, never instead of it. The marker poll is the fallback
  when the broker is down, and the snapshot timer is the fallback when both are.
- **Connection.** Connect in the constructor and reconnect in the background, never in `Notify`
  (contract rule 7). A disconnected notifier returns at once with `Err` set, and the writer logs it.

### Testing an implementation

`rostertest.Notifier` is for testing writers. To test a notifier, drive it from real writers over
`rostertest.Store`, and check at least that:

- it returns at once when `want` is `AckNone`, with the downstream stalled
- it returns when `ctx` ends, with the level reached and `Err` set
- concurrent and reversed notifications for one owner do not corrupt its evidence
- a `WriteUnchanged` notification causes no push, mark or message
- a `WriteDeleted` service is treated as a delete, including when your own read already saw it
- `Reached` never exceeds what your downstream proved, with zero consumers and after a disconnect
