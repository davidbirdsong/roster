# Roster TODO

Open questions and follow ups on the roster. Each item says where the code is and what would settle it. None of them is a known outage. Revisit them when touching `s3store` or the proxy's roster source.

## 1. The per owner hint kind was removed (settled)

Where: `s3store/poller.go`, `reader.go` (`HintKind`), the proxy's roster source in the consuming repository (`backendregistry/changes.go:39`).

A second hint kind named the owner that changed. No hint source could produce it, because the marker names no owner, so against S3 only `rostertest.Hints` ever emitted it and the per owner path in the proxy adapter was exercised by tests and never by production traffic.

Settled during the rename to `roster` by taking the second option, removal, rather than keeping it as the shape a future push transport (MQTT, see `analysis-s3-mqtt-complexity.md`) would fill. `HintRescan` is now the only kind and `Hint` carries no identity. A transport that can name an owner adds the kind back with the code that produces it, which is the point at which the shape can be designed against something real.

## 2. The baseline race after a rescan was closed (settled)

Where: `MarkerPoller.run` in `s3store/poller.go`.

The poller sent the initial `HintRescan` and then took its baseline ETag with a HEAD, while the consumer started its LIST in parallel. A publisher that wrote and marked after the consumer's LIST but before the poller's baseline HEAD had its mark folded into the baseline, and no rescan followed. The same window existed after a failed poll recovered. The damage was bounded by the consumer's own periodic full read, so a missed TERMINATING signal could be delayed by about 2.4 minutes against a goal of a second or two.

Settled by taking the baseline first and emitting the rescan that covers it afterwards, which is option 2 of the two listed here. The ordering is the whole fix: a consumer that starts listing after that rescan cannot miss a write that landed before the HEAD, and a write that lands after it changes the ETag and produces another rescan.

Option 2 on its own would have made an unreachable marker hold up a consumer's first read, because the first rescan would wait on a successful HEAD. The marker is advisory and a snapshot read does not depend on it, so a subscription now begins with two rescans: one before any polling, and one once the baseline is known. The cost is one extra read per subscribe and per recovery, for a write that landed between the baseline and the consumer's listing. A redundant read is cheap, because the reader fetches only objects whose ETag changed, and a redundant read is always safer than a missed one.

Question 1 here is answered too: `poller_test.go` did not cover the window, and now does, in `TestTheBaselineIsTakenBeforeTheRescanThatCoversIt` and `TestARecoveryRebaselinesBeforeItsRescan`.

`analysis-marker-baseline-ordering.md` walks through the whole thing: the timeline of the lost mark, why the obvious fix was the wrong one, what the ordering argument rests on, and the contract change it forced.

## 3. First write after a restart always marks (settled)

Where: `Store.PutService` in `s3store/store.go` (`s.hashes` is process memory).

Settled by the write notifiers (`design-notifier.md`). The store no longer marks or keeps hashes. A `Publisher`'s `Reconcile` compares with what `ListOwned` read back, so after a restart identical content is `WriteUnchanged` and does not mark. A member's first `Register` is still a change and still marks, as the last paragraph below says it should. What follows is the item as it was.

After a publisher restart, the first `PutService` for each service updates the marker even when the bytes match what is already in S3. An owner with many services updates the marker once per service at startup. Readers collapse these into one or two snapshots, so it costs a few extra LISTs and no correctness.

Probably the right trade, since the alternative is persisted state or a read before every first write. Revisit only if startup mark volume shows up in S3 request metrics.

For a pool member this is not a cost at all: its first write after a start is its registration, which is a change readers should see, so it should mark.

## 4. ETag semantics are load bearing

Where: `Reader.fetch` and `Reader.get` in `s3store/reader.go`, `../s3-layout.md` (default encryption row), `s3fake/fake.go`.

Quiet refreshes depend on an identical rewrite keeping its ETag. That holds with SSE-S3, where the ETag is the MD5 of the content. With SSE-KMS the ETag is not the MD5, so every reconcile rewrite changes it and every reader refetches every object after every reconcile. The cache still returns correct results, but its savings vanish. `../s3-layout.md` already warns about this and tells operators to confirm it before choosing KMS.

Follow ups:

1. The in memory `s3fake` uses the MD5, so it models SSE-S3 only. State that in its doc comment, and consider a mode with unstable ETags so the cost of the KMS case is a test and not a footnote.
2. Check that the S3 mock used by `S3_ENDPOINT=http://localhost:19000` returns real ETags. If it returns empty ones, `get` falls back to the listed ETag and the cache path is not really exercised.
3. Consider a startup check, next to `s3store.Verify`, that writes an object twice and compares ETags, so a bucket with KMS default encryption is reported at start.

## 5. Latency claim versus the poll interval

Where: `design-s3-hints.md` (requirement for fast draining), `WithPollInterval` default of 2 seconds.

The drain signal path is 0 to 2 seconds of polling, then a LIST and any GETs, then the proxy's `debounce` (250 ms by default in `core/reconcile`), then the HAProxy reload debounce. "Within about one interval" in the code comment is accurate. "A second or two" in the design doc holds only at the favorable end.

To settle it, either measure the end to end time from a publisher's `PutService` to a server state change in HAProxy and write the real number in the design, or lower the default poll interval and accept the extra HEAD requests (about 40 per second at 80 consumers with a 2 second interval).
