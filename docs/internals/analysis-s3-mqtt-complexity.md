# Analysis: does complexity go down with S3 paths and MQTT?

This was written before the change and weighs the design in [design-s3-hints.md](design-s3-hints.md) against the current DynamoDB roster. Numbers marked verified come from the code as it stands (line counts of non-test files, call graph and import queries). Numbers marked estimate are judgments and have not been measured.

The question has two halves that deserve separate answers: S3 with JSON objects as the state and contract, and MQTT as the hint transport.

## Short answer

- **S3 plus JSON reduces complexity where it matters most for the stated goal.** The publisher write path gets simpler, the storage schema stops being a contract, and the burden of version skew moves to controllers, which are few and owned.
- **Controllers get modestly more complex.** They gain a version split, a LIST and GET reader, and a synthesized revision.
- **MQTT adds complexity and buys latency only.** Correctness does not depend on it, because the periodic list remains. Defer it behind the existing `HintSource` interface and implement the doorbell first.
- **Net, with MQTT deferred, complexity falls in concepts, and code size is about the same.** See "Outcome" below for what was measured after building it. Complexity is moved more than it is removed, and the move is to the right place.

## What stays the same

These are the things that look like they might change and do not:

- `Endpoint`, `Equal`, `policy.PreferEndpoint`, `NewSnapshot`, `Snapshot.Rejected`, and the stale owner logic (verified: the proxy adapter depends only on `roster.Reader` and `roster.HintSource`).
- The planner, the drain path, the reload debounce, and everything in the proxy's core.
- Single writer per owner as a convention, `StaleAfter` for liveness (the source of the timestamp changes, not the rule), and the cached last version as the outage gate.
- The need to scope writers by service. It changes from a payload attribute, a scoped owner ID and a library check to a key prefix plus IAM, so the scoped owner ID code goes away (see the ledger).

## Expected scale

From the user: about 20 controllers in steady state, growing to 60 to 80. Writers are the same order of magnitude, each with its own owner ID and many sharing a service. Total objects are in the hundreds, perhaps 1000.

- **Doorbell polling is cheap.** 80 controllers polling once a second is 80 HEAD requests a second, a few million requests a day. The code issues `HeadObject` and compares ETags, not a conditional GET (`s3store/poller.go`). Check current pricing, but this is small. See `../scaling.md` for the full request model.
- **Cold start is bounded.** Up to 1000 GETs per controller. If the whole fleet restarts together, about 80,000 GETs in a burst, which bounded concurrency spreads out.
- **Refresh traffic is small.** At 1000 objects and a 30 second interval, publishers make about 33 PUTs a second.
- **MQTT is not needed for scale.** 80 connections and a few dozen hints a minute are trivial for IoT Core. It would be adopted for latency only.
- **Kinesis is out regardless.** 80 consumers is beyond the default enhanced fan-out consumer limit (about 20, from recall, to confirm).

## Ledger

| Area | Today (DynamoDB) | S3 plus JSON | Direction |
| --- | --- | --- | --- |
| Storage layer code | `pkg/roster/dynamo`: 979 non-test lines in five files (verified: dynamo.go 314, reader.go 231, spec.go 165, feed.go 139, verify.go 130) | Deleted. Replaced by an S3 store and reader (estimate: a few hundred lines) | Down, then partly back up |
| Table spec and IAM tooling | `cmd/roster-spec` (51 lines, verified), `schema/roster-spec.json` and its freshness test, `docs/roster/dynamodb-schema.md` (178 lines, verified) | None. Bucket and IAM prefix grants are documented in the design | Down |
| Table health checks | `Verify`, point in time recovery check, `SkipVerify`, `SkipRecoveryCheck` settings | None | Down |
| Publisher write path | `Store` has 4 methods and per record writes. `Publisher` keeps an in-memory view, `Diff`, a Publish and Reconcile split, `ErrNotInitialized`, transactional generation bumps | Per service atomic replace. Diff at service granularity. Fewer partial failure states | Down |
| Change detection | Generation counter, clusters table, `MarkerPoller` (139 lines, verified) | Doorbell poll or MQTT hint, ETags | Roughly neutral for the doorbell, up for MQTT |
| Reader | Scan of cluster rows, Query per changed cluster, one generation sum for the revision | LIST and GET fan-out, ETag cache, decoder per major, synthetic revision | Up |
| Version skew | No mechanism. Schema change is a fleet migration | Defined: additive inside a major, decoders per major in readers, census, reader first order | Down for publishers, up for controllers |
| Service scoping | `LeadingKeys` on the owner ID prefix, scoped owner IDs (`<service>/<member>`), a service attribute checked in the library (`scope.go`, 72 lines verified, plus its test, the `Validate` hook and the EC2 publisher's use of it) | IAM on `v1/<service>/<id prefix>*`, service taken from the key, plain owner IDs. `scope.go` is deleted, and `Snapshot.Rejected` stays as a generic channel | Down |
| Liveness | A clusters table row per owner, a `Heartbeat` store method, a generation counter | `LastModified` of the state objects, refreshed with identical bytes. No second object type | Down, with the cost of about 33 PUTs a second at 1000 objects |
| Orphaned cluster rows (design.md open decision 7) | An open TODO | An S3 lifecycle rule on `v1/` (day granularity) | Down |
| Toolchain | None beyond the Go modules | None new. A frozen compatibility corpus and its manifest test (protobuf would have added `buf`, committed generated code, a CI drift step and a dependency) | Roughly neutral |
| Hint transport | Not present (polling only) | MQTT client, reconnect, resubscribe, unique client IDs, SigV4 WebSocket signing, IoT policies, per message billing | Up |
| Operational surface | Two tables, point in time recovery, IAM with `LeadingKeys` | One bucket, lifecycle rules, bucket and IAM prefix grants, optionally an IoT endpoint | Roughly neutral without MQTT, up with it |

## Where the complexity goes

The schema problem has a cause that storage does not fix: there was no contract, and publishers carried semantics. Three things fix it:

1. **The wire format is the contract**, not a library and not a table. Any language can publish it.
2. **Additive evolution is enforced** by a frozen, append only compatibility corpus that every reader version must decode (a test, not a structural checker), and by the presence and default rules written down in the design.
3. **Readers own meaning.** Ranking, state mapping, and the meaning of `logical_name` change in controllers without touching what publishers send.

This is a trade. Publishers, many and uncontrolled, stop needing coordinated changes. Controllers, few and owned, accept a permanent obligation: keep a decoder for every major any publisher still writes, and keep a census so a decoder can be retired. That obligation is real work, and it is the work the user asked controllers to carry.

A critical note: S3 and JSON do not create the fix by themselves. DynamoDB attributes with an additive rule, a version attribute, and reader owned semantics would also have decoupled publishers, at the cost of per attribute plumbing. What the change buys is that the contract becomes one explicit artifact (the `wirev1` package and its corpus) and that the storage layer shrinks to a key and a blob. If the additive discipline lapses, a JSON contract fails in the same way a table schema did. The frozen corpus test is what makes the difference, so it belongs in the first change set and not in a later cleanup.

### JSON instead of protobuf

Protobuf was the first choice and was dropped. JSON follows the same additive rules, has real presence (a key is absent or present), and its Go output is byte stable across releases, where protobuf's deterministic marshal is not guaranteed across releases. It needs no toolchain, objects are readable with `jq`, and a non-Go publisher can be a shell script. What JSON gives up is field numbers (a rename is a silent break because the name is the identity) and a mature structural breaking change checker. The corpus test replaces the checker with a behavioral one, and the rule that names are never renamed or reused replaces the numbers. At hundreds of small objects and one team, that trade favors JSON. It would not if many independent teams wrote the format.

## MQTT on its own

MQTT answers one question: can drain hints arrive in well under a second.

Costs:

- A long lived connection per controller, with reconnect, resubscribe, backoff, and session handling. There is no official Go device SDK for IAM auth over WebSocket that I know of, so signing the handshake is code to write and maintain (estimate: a few hundred lines including tests, larger than `MarkerPoller`).
- IAM and IoT policy surface (`iot:Connect`, `iot:Subscribe`, `iot:Receive`, `iot:Publish`), a second AWS service in the data path, and per message delivery billing that scales with controller count.
- More failure modes. A connection can look healthy and deliver nothing, and the only protection is the periodic list.

Benefits:

- Hint latency in the low hundreds of milliseconds instead of the poll interval.
- Presence needs no cleanup, topic scoped publish permissions, and optional presence events.

The drain path is already immediate on the controller (the planner drains a `TERMINATING` member on the same pass, before the reload debounce), so the transport only has to shorten the time until the controller notices. With a default 10 second `--drain-delay` and a one to two second doorbell poll, the poll interval is a small fraction of the drain window. MQTT is worth its cost only if measured drain latency or controller poll cost says so. Until then it is complexity bought for a gain that has not been shown to matter.

## Risks and conditions that would change the verdict

- **Object count per controller.** At the expected hundreds to about 1000 objects this is comfortable (one or two LIST pages, up to 1000 GETs at a cold start). If counts grow by an order of magnitude, LIST and GET cost and cold start time rise, and a manifest object or a per service refresh would be needed. This is the main way S3 could lose to DynamoDB's single Scan, and it is not close at this scale.
- **Controller fleet size.** Doorbell polling cost scales with controllers times poll rate. At 80 it is small. A fleet in the hundreds or thousands moves the balance toward push (MQTT) or a relay tier.
- **Refresh traffic.** Liveness by rewriting every object scales with objects divided by the interval. At 1000 objects and 30 seconds it is small, and a longer interval with a longer `StaleAfter` trades liveness detection time for fewer PUTs.
- **Revision semantics (checked).** The core store keeps revisions in memory only, so a reader local counter resets safely on restart. The constraint is that the counter must increase exactly when served content changes: an equal revision is treated as a duplicate and the new content is ignored, and a lower one is rejected as stale. This must be covered by a test.
- **Pricing and quotas.** S3 request pricing, IoT Core publish and connection limits, and S3 conditional write support are from recall and need confirming against current AWS documentation.
- **Writer discipline.** A new writer that skips the hint is only slower, but a new writer that violates the additive rules harms every reader. The corpus test on `wirev1` is the control.

## Recommendation

1. Adopt S3 with JSON objects and the `v1/` layout. This delivers the stated goal.
2. Implement the doorbell `HintSource` first and delete `dynamo` in the same change set. There is no migration to carry.
3. Put the frozen compatibility corpus (append only, with a manifest test) and the reader version split (decoder roster, census, incomplete on undecodable) in the first change set, not later.
4. Build the MQTT `HintSource` as a second implementation only after measuring drain latency and poll cost with the doorbell.
5. Write the reader's revision counter with a test that pins the equal, lower and higher cases of `Acceptor.Evaluate`, since a wrong counter silently drops updates.
6. Settle the remaining open questions in the design (the owner ID prefix convention per writer kind, and the refresh interval against `StaleAfter`) before writing the store. Pricing and quotas are deliberately deferred.

## Outcome

The S3 design was built and DynamoDB deleted. Measured by line counts of Go files before and after (non-test code, then test code), with gograph and `wc` as the tools:

| | Non-test Go | Test Go | Other |
|---|---|---|---|
| Deleted | 1102 (`pkg/roster/dynamo` 979, `cmd/roster-spec` 51, `scope.go` 72) | 1631 | 335 (`schema/roster-spec.json` 157, `dynamodb-schema.md` 178) |
| Added | 1279 (`s3store` 854 plus the in-memory S3 `s3fake` 233, `wirev1` 192) | 1261 (`s3store` 1051, `wirev1` 210) | `../s3-layout.md` and the frozen corpus |
| `publisher.go` | 221 lines before, 261 after | | |

What the numbers say:

- **Code size did not fall.** Non-test code is about 1100 lines deleted against about 1280 added, or about 1050 without the test double. The prediction that this change would shrink the code was wrong. It moved: the table spec, verification, point in time recovery checks, transactions and the generation counter went away, and a LIST and GET reader, a version aware decoder and a wire package came in.
- **Test code fell** by about 370 lines, mainly because the spec and recording client tests went away.
- **The publisher did not get smaller.** Its logic is the same size, because it now works in services and refreshes unchanged ones, which offsets the loss of the per record diff and the heartbeat.
- **What did fall is concepts and artifacts:** no table spec or committed JSON to keep fresh, no `Verify` and recovery check, no two table schema, no generation counter and cluster row, no scoped owner IDs with a payload service check, no Publish and Reconcile heartbeat split, and no orphaned row TODO. A publisher depends on a wire format and a key layout and not on a table schema.
- **What rose** is the controller side, as predicted: the version aware decoder, the LIST and GET reader with its ETag cache, and a reader owned revision whose behavior is pinned by a test against the controller's acceptor. The permanent obligation to keep a decoder for every wire major that any publisher still writes is also new, and it is the work the design intended controllers to carry.
- **MQTT was not built.** At 20 to 80 controllers the marker polling is cheap, so MQTT remains a latency option, not a need.

The verdict stands in its qualified form: the change decouples publishers from the storage schema, which was the goal, and it did so at roughly constant code size, trading a table schema and its tooling for a wire format and its compatibility tests.
