# Roster on S3 with change hints

Status: implemented, with the push transport (MQTT) still a later option. It replaced the DynamoDB roster. Nothing was live, so DynamoDB was removed outright, with no migration path: no dual write, no fallback reader, no overlap period. `v1` is the first wire version. The bucket layout and IAM are in [s3-layout.md](../s3-layout.md), and the roster's model and the Kubernetes publisher are in [design.md](design.md).

The cost and benefit of the change are weighed in [analysis-s3-mqtt-complexity.md](analysis-s3-mqtt-complexity.md).

## Problem

Publishers write attributes into DynamoDB tables, so every publisher is coupled to the table schema, and there is no defined way to change it. As writers spread out (Kubernetes, EC2, and others to come), a schema change becomes a fleet-wide migration. Most publishers should not care about a schema. They should describe what they serve and hand it over.

The design succeeds if publishers are decoupled from consumers. Publishers write backward compatible messages (writing an S3 object is publishing). Controllers carry the burden of managing a version split, because controllers are few and owned, while publishers are many and not.

Two more requirements shape it:

- Readers need current state, including liveness, and a cold reader must be able to rebuild it.
- Draining should start quickly. A signal that arrives within a second or two is much better than waiting for a poll interval, even if it is only best effort.

## Expected scale

Steady state is about 20 controllers, growing to 60 to 80. Writers are the same order of magnitude. Each writer has its own owner ID, and many writers share a service (a fleet). Total objects are in the hundreds, perhaps 1000.

This is small. Every cost in the design is trivial at this size (see Read path), and it rules out Kinesis independently of the other reasons, because 80 consumers is beyond the default enhanced fan-out consumer limit (about 20, from recall, to confirm).

## Decision summary

1. **State is a JSON object per (service, owner) in S3.** The key carries the service and the owner ID.
2. **Notifications are hints, never state.** A hint names a key. The reader re-reads the object. Late, duplicate, or lost hints cannot corrupt anything, so no ordering protocol is needed.
3. **Hints travel by a doorbell object that readers poll with a HEAD request.** It needs no infrastructure per controller. MQTT (AWS IoT Core) is a possible later addition for lower latency, and would not need any either.
4. **Readers rebuild by listing the tree periodically.** The list is also the repair path for any lost hint.
5. **There is no separate heartbeat.** Each publisher rewrites its state objects every interval, and an object's `LastModified` is its liveness. A refresh that changes no bytes marks no hint.
6. **Controllers own the version split.** Readers decode every wire major they support into one canonical model. Publishers only ever add.
7. **Scoped owner IDs are retired.** The service is in the path, and the owner ID is one plain path segment.

## Layout

```text
v1/<service>/<owner>     State object for one service of one owner (JSON).
marker                       Doorbell: one tiny object for the whole roster, whose content changes on every mark.
```

- **One object per (service, owner).** A service is a shard, and a Kubernetes cluster publishes many services, so one object per owner would be the wrong unit. An object holds the full desired set of one service for one owner, the equivalent of `Reconcile` for that service. A replace is atomic, so no service is ever half updated. A service that cannot be derived is protected by not changing its object. This is the natural form of `PreserveServices`. An EC2 instance is its own owner in one service, so it owns exactly one object.
- **A key is exactly three segments.** `v1/<service>/<owner>`. Neither the service nor the owner ID may contain `/`, `*`, `?`, `$`, whitespace or control characters. Anything else under `v1/` is ignored and reported. This replaces the `<service>/<member>` scoped owner ID and the `ScopedClusterID`, `ClusterGroup` and `checkScope` code (`scope.go`).
- **The service comes from the key and is absent from the payload.** There is nothing to forge, so the cross-check between a record's service and its owner's service has nothing left to check.
- **A major version is the first path segment.** A breaking change publishes under `v2/`. Everything else is additive JSON evolution inside a major.
- **Liveness is the object's `LastModified`.** At 1000 objects and a 30 second interval, refreshing every object is about 33 PUTs a second, a few million a day. A separate heartbeat object would cost the same again and add a second thing to pair and to scope. A reader reads `LastModified` from a LIST, with no GET.
- **Dead publishers clean up after themselves slowly.** A live publisher refreshes every object every interval, so an object that is days old belongs to a publisher that is gone. A lifecycle rule that expires `v1/` objects after a few days removes them, at day granularity. Staleness still hides them from controllers within `StaleAfter`. This replaces the orphaned owner row TODO (`design.md`, open decision 7).
- **An owner's last reconcile is the oldest `LastModified` among its objects.** If a publisher fails to refresh any one of its objects, the owner goes stale as a whole. That matches today's rule that the heartbeat follows only a fully successful reconcile, so `StaleAfter` and the adapter's stale handling stay as they are.
- **Refreshes must not change bytes.** Encode deterministically (endpoints sorted by ID, `encoding/json` with struct fields in declaration order and map keys sorted, which Go keeps stable across releases), and put nothing time varying in the payload. An unchanged service then produces an identical object and no hint. Keep bucket versioning off, or expire noncurrent versions, or every refresh stores a new version.

## Message contract

The payload is JSON. Compactness is not a goal (objects are small and few). What matters is an explicit contract that is easy to read, easy to write from any language, and checkable. An object, for one service for one owner:

```json
{
  "writer": "roster-ec2-publisher/1.4.0",
  "endpoints": [
    {
      "instance_id": "i-0abc",
      "logical_name": "i-0abc",
      "address": "10.0.1.5",
      "port": 8080,
      "protocol": "http",
      "region": "us-east-1",
      "zone": "us-east-1a",
      "state": "READY",
      "serving": true,
      "labels": { "tier": "web" }
    }
  ]
}
```

Enum values are the strings the library already uses (`NOT_READY`, `READY`, `TERMINATING`; `http`, `https`, `h2c`). The service and the owner ID are not in the payload, and neither is a timestamp. A timestamp would change on every refresh and defeat the identical bytes rule, and `LastModified` already says when the object was written. `writer` is constant for a process and is for diagnostics only.

The Go side is a stdlib only package, `wirev1`: the wire structs, `Encode`, `Decode`, and the frozen corpus. A Go publisher can import it without the AWS SDK. A publisher in another language writes the JSON directly, so a shell script with `jq` and `aws s3 cp` can be a publisher.

Rules that make it evolvable:

- **Names are the identity.** A field name is never renamed, never reused for a different meaning, and never changes type. Retired names are listed in the `wirev1` package comment and are never reused. This replaces protobuf's field numbers, and it is the main thing JSON gives up: a rename is a silent break that the compiler does not see.
- **Additive only inside a major.** New fields are optional. Readers ignore fields they do not know (the default for Go's `encoding/json`, so never use `DisallowUnknownFields`).
- **Presence is real.** A JSON key is absent or present, so a field where zero is meaningful uses a pointer in the Go struct (`serving` is `*bool`). A pointer with `omitempty` omits only nil, so a written `false` is kept.
- **Defaults are permanent.** Each field needs a permanent meaning for "missing". Example: an absent `serving` means derived from state (READY serves, TERMINATING does not). That mapping is written down and never changed.
- **Enum strings are open.** A reader that meets an unknown `state` or `protocol` rejects that record and reports it. It does not guess.
- **Names must differ by more than case.** Go matches JSON names case insensitively, so two fields that differ only by case would collide.
- **Publishers state facts, readers decide meaning.** `policy.PreferEndpoint` ranking, the state mapping, and the meaning of `logical_name` live in readers. A semantic change is a reader change and does not touch the publishers' message.

### The compatibility corpus (replaces `buf breaking`)

`wirev1/testdata/` holds frozen example objects, one per shape ever published, each with the canonical endpoints it must decode to. Rules:

- Every reader version must decode every corpus file to exactly the expected result. This tests behavior on real bytes, which catches renames, removals, and type changes that a structural checker can miss.
- The corpus is append only. A manifest of file hashes (`corpus.sum`) is checked by a test, so editing a frozen file fails CI. A new field adds a new file.
- Adding a field means adding a corpus file that uses it and a reader test that an older decoder ignores it.

A JSON Schema file for non-Go writers is optional. Write it when the first non-Go writer is planned, and validate the corpus against it in a test.

### Why not protobuf

Protobuf was the first choice and was dropped. Its advantages (field numbers that survive renames, a mature breaking change checker, generated types) are real, but at this scale they cost more than they return: hundreds of small objects, one team, a possible shell based publisher, and a toolchain (`buf`, generated code committed, a CI drift step, a protobuf dependency). JSON also gives real presence and byte stable output across releases, where protobuf's deterministic marshal is not guaranteed across releases. Revisit protobuf if many independent teams start writing the format, because then an enforced checker earns its keep.

## Consumer side version split

This is the part of the design that is new work, and it belongs to controllers.

- **One canonical in-memory model.** `roster.Endpoint` stays the model the rest of the controller sees. It is internal and versioned independently of the wire. Each supported major has a decoder from its JSON to the canonical model. A reader supports a window of majors.
- **Per owner resolution.** If the same (service, owner) is present under several majors, the highest supported major wins and lower ones are ignored. Liveness is version independent, because it comes from `LastModified`.
- **Failure policy reuses what exists today:**

| Situation | Behavior |
| --- | --- |
| An object cannot be decoded, or is under an unsupported major | The snapshot is marked incomplete and the controller keeps its last good state (as `SkippedItem` does today). The object is reported with its service, owner and key. |
| A key that is not exactly `v1/<service>/<owner>`, or whose service or owner ID has unsafe characters | The object is left out and reported in `Snapshot.Rejected`. The snapshot stays complete, so one bad writer cannot freeze readers. |
| An enum value the reader does not know | The record is not served, counted, and reported in `Snapshot.Rejected`. |

- **Transition order for a new major:** readers first (they gain the `v2` decoder), then publishers write `v2` (and `v1` as well while any reader may still be old), then publishers stop writing `v1`, then the `v1` decoder is removed. A reader reports a census of objects per major, so deprecation is based on data and not on memory. This is the generic procedure for any later major. It is not a migration from DynamoDB.
- **Revision (checked in the code).** The core store's `Acceptor` keeps the last revision per scope in memory only (`NewAcceptor`, a map that is empty at start). The first snapshot after a start is always accepted, so a reader local counter may reset when the process restarts and needs no seeding. Within a process the rules are strict (`Acceptor.Evaluate`): a lower revision is rejected as stale, an equal revision is a duplicate (the new content is ignored, and only the accepted time is refreshed), and a higher one is accepted. So the reader's counter must increase exactly when the content it serves changes, and must stay equal when nothing changed. It increments when the content hash of the served endpoints changes. The adapter's existing `epoch`, which bumps when the set of dropped stale owners changes, stays as it is.
- **Staleness.** `OwnerStatus.LastRefreshAt` becomes the oldest `LastModified` among the owner's objects, and `StaleAfter` applies unchanged. Use `LastModified` for liveness only, never for ordering.

## Write path

For a state change, per writer call:

1. PUT `v1/<service>/<owner>` for each changed service. This is the durable source of truth.
2. Notify: the writer tells its notifiers what it stored (`design-notifier.md`). `s3store.Marker`, attached as a hint notifier, PUTs `marker` once with unique content (so its ETag changes) when the call put or deleted something. An MQTT hint notifier, when there is one, publishes beside it.
3. A failure at step 2 is logged and ignored, because the periodic list repairs it.

The sequence lives in the publisher library. A writer that skips step 2 is not broken, only as slow as the poll interval.

`Reconcile` (startup and periodic): list the objects this owner owns (a LIST of `v1/` filtered to keys ending in `/<owner>`, acceptable at the stated scale) and GET each one, uncached, to read back what is stored, then write changed services, delete services that are no longer desired, and refresh every other object by rewriting identical bytes, including protected services, whose bytes are the ones read back at the start of the reconcile. Only objects whose content changed update the marker. Because liveness is the oldest `LastModified`, a partial failure leaves the owner looking stale, which matches the rule today that the heartbeat follows only a fully successful reconcile. `Publish` writes changed services without the refresh.

A pool member (an EC2 instance, a task, a pod that publishes itself) owns one object and uses `roster.Registration`, which never reads: registering and each refresh are a single PUT, with no LIST and no GET.

For a clean shutdown of a pool member: `SetState(TERMINATING)` (PUT the object, mark), wait out the drain grace, then `Deregister` (delete the object, mark). A crash leaves no signal, and the object's `LastModified` going stale covers it.

Single writer per owner stays a convention. S3 conditional writes (`If-Match` on the ETag) can enforce it. Check that the SDK in use supports them.

## Read path

The controller is level triggered:

- **Cold start:** LIST `v1/`, GET every object (with bounded concurrency), build the snapshot, and run the existing `NewSnapshot`, `policy.PreferEndpoint`, and staleness logic. A cached last version in `state_dir` can serve while this runs. It is outside the contract and is the gate between this system being down and serving the last known state.
- **Steady state:** poll the doorbell (a HEAD request, comparing its ETag with the last one), or receive an MQTT hint. The same ETag means nothing happened. A new ETag or a hint means re-list `v1/`, compare ETags, and GET what changed.
- **Periodic full list** at the full refresh interval, as the repair path for a missed mark. A controller that was offline needs no catch up beyond this.
- **A deleted object is a removal.** There are no tombstones, because every read is of current state.
- **Sizing (expected scale: up to 80 controllers, up to about 1000 objects):** see `../scaling.md`, which owns the request model, the formulas and the worked examples. In short, at that scale the whole system is about 110 requests a second, a fleet restart is 80,000 GETs, and the term that breaks first at ten times the scale is the LIST fan out, not the doorbell.

### Doorbell

- Publishers PUT the single `marker` object with unique content after writing state. There is one marker for the whole roster, not one per service: controllers poll it, so a marker per service would multiply every controller's polling by the number of services.
- Concurrent publishers overwrite each other's marker. That is harmless, because any PUT changes the ETag and the controller then reads every key's actual state, so no mark is lost.
- The cost is one HEAD request per controller per interval, well under S3's per prefix request rate. Check current pricing.
- A mark makes controllers re-list, and the reader lists the whole major flatly rather than one service prefix, because a mark names nothing (`Reader.listMajor`). Coalescing marks in the controller is therefore worth more than it sounds: at most one list per second caps the fan out of a change burst. See `../scaling.md`.
- Latency is the poll interval. With the default 10 second `--drain-delay`, one to two seconds is adequate.

### MQTT (IoT Core)

- Controllers connect outbound and subscribe to `roster/v1/#` or a set of services. A clean session removes the subscription on disconnect, so nothing is created or swept per controller.
- Auth is SigV4 over WebSocket with the instance role or IRSA. Per device X.509 certificates would bring back per controller provisioning.
- Client IDs must be unique per controller, because a second connection with the same ID takes over the first.
- Publishers use the one shot `iotdataplane.Publish` HTTPS call, so a short lived publisher holds no MQTT session. Topic: `roster/v1/<service>/<owner>`, payload `{key, etag}`.
- Use clean sessions, no retained messages, and send hints for state changes only, never for refreshes. Billing is per message delivered, so one hint to 200 controllers is 200 messages.
- SNS has no IoT Core endpoint, so publishers call IoT directly. An IoT rule on the topic can fan out to SQS, Firehose, Lambda, or SNS for reconcile and alerts.
- Verify the quotas for publish rate, connection duration, and subscriptions per connection before relying on it.

### Draining

A `TERMINATING` flip (still serving), or a record that stops being served, is drained on the stats socket on the same plan pass, before the reload debounce. The reload is the part that waits. `TestADrainingOrLeavingMemberIsDrainedNowWhileTheReloadWaits` pins this in the planner. Hint latency is therefore the only delay the transport adds. End to end latency is not measured.

## Authorization

Identity is enforced by IAM on the key prefix and the topic.

The members of pool `web`, whose owner IDs are instance IDs:

```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:DeleteObject"],
  "Resource": [
    "arn:aws:s3:::REGISTRY_BUCKET/v1/web/i-*",
    "arn:aws:s3:::REGISTRY_BUCKET/marker"
  ]
}
```

A Kubernetes publisher for cluster `eks-a` (many services, one owner ID) uses `v1/*/eks-a` and `marker`, plus `s3:ListBucket` with an `s3:prefix` condition of `v1/` for its reconcile.

With scoped owner IDs retired, the service is enforced by the path and the writer kind by an owner ID prefix convention. Instance IDs start with `i-`, so the fleet grant `v1/web/i-*` cannot write a Kubernetes cluster's object in the same service, and the Kubernetes grant cannot write instance objects. Pick an ID prefix per writer kind and put it in the grant. One limit remains, as it did before: any instance in a fleet can overwrite another instance's object in the same service, because IAM cannot tie an object to a particular instance without per instance policies or session tags. The blast radius is one service.

A controller:

- `s3:ListBucket` (prefix condition `v1/`) and `s3:GetObject` on `v1/*` and `marker`.
- For MQTT: `iot:Connect` on `client/roster-controller-*`, `iot:Subscribe` on `topicfilter/roster/v1/*`, `iot:Receive` on `topic/roster/v1/*`.

A publisher that also hints needs `iot:Publish` on `topic/roster/v1/<service>/*`.

Hints are untrusted by design. A forged hint costs one extra GET, and what the GET returns is controlled by the S3 grants. This is why hints name a key and never carry a command.

## Alternatives considered

- **SQS or SNS as the transport for state.** They carry events, not state. Proxies need current state and liveness, and standard queues reorder and redeliver. They are fine as hint channels only, and per controller queues need infrastructure for controllers that come and go.
- **Kinesis with readers replaying the log.** Works for a small controller fleet. Shared throughput limits (about 5 reads per second and 2 MB/s per shard) and the enhanced fan-out consumer cap make a fleet restart expensive, because every reader replays a full window. There is a 1 MiB record limit, no compaction, and IAM cannot scope by partition key.
- **SNS to HTTP(S) endpoints.** Endpoints must be publicly reachable, confirm a subscription handshake, and verify signatures. Dead subscriptions need a janitor.
- **A relay tier serving a stream to controllers.** It handles ephemerality but adds a component to run and secure. Consider it only if IoT Core is ruled out.
- **Keep DynamoDB and version a schema attribute.** Workable, but the table schema would still be the contract, and publishers would still write attributes. The change here is that the wire format becomes the contract.

## Where it lives

| Area | Code |
|---|---|
| The wire format | `wirev1`: stdlib only structs, `Encode`, `Decode`, and the frozen corpus in `testdata/` with its `corpus.sum` manifest and the tests that enforce it |
| Storage, reading, hints | `s3store`: `Store` (`PutService`, `DeleteService`, `ListOwned`), `Reader`, `Marker`, `MarkerPoller`, `Verify`. `s3fake` is an in-memory S3 for tests. |
| The publisher | The root package: `DocumentStore` is service level, `Publisher` works in services, `Snapshot.SetRevision`, `RejectedEndpoint`, `ValidSegment`. Nothing there knows about S3. |
| Controllers | The proxy's `app` package in the consuming repository (`RegistryConfig` has `bucket`, `key_prefix`, `path_style`, `endpoint`, `region`, `skip_verify`, `poll_interval`), reading through `roster.Reader` and `roster.HintSource`. The adapter (`backendregistry`) did not change except to log what the reader rejected. |
| Publishers | The Kubernetes and EC2 publisher programs in the consuming repository take `--bucket`, `--key-prefix`, `--s3-endpoint`, `--s3-path-style`. The owner ID of an EC2 instance is its instance ID. |
| Removed | `pkg/roster/dynamo`, `cmd/roster-spec`, `schema/roster-spec.json`, `docs/roster/dynamodb-schema.md`, `ScopedClusterID`, `ClusterGroup`, `checkScope`, per owner `Generation`, `Change.Generation`, `Diff`, `Operation`, the `Heartbeat` store method |

Details that differ from, or sharpen, the proposal:

- `ListOwned` reads what an owner stored. An object it fetched but cannot decode is logged and left out, because failing on it would make every later reconcile of the owner fail and leave the damaged object unrepairable. A failed LIST or GET is still an error.
- The store only stores. The writer marks, through `s3store.Marker` attached as a hint notifier, once per call that put or deleted something. It knows what it meant to store, so a refresh leaves the marker alone, and so does a restarted `Publisher` whose first `Reconcile` finds identical content. `Marker.Update` writes the single `marker` object, and is there for marking by hand.
- `MarkerPoller` polls the marker with `HeadObject` and compares the ETag locally, so no 304 handling is needed. It emits `HintRescan` for every change, because the marker names no owner.
- The reader's revision is pinned against the controller's snapshot acceptor in the proxy's `app/registry_s3_test.go` in the consuming repository, wiring the real reader, the real adapter and the real acceptor.
- The integration tests run against adobe/s3mock, since the public MinIO images can no longer be pulled. The mock is not S3. The one behavior the design depends on that a mock could get wrong is that an identical write moves `LastModified`, and that should be confirmed against real S3 once.

## Open questions

- Pricing and quotas for S3 requests, IoT Core, the enhanced fan-out consumer limit, and conditional writes, to confirm against current AWS documentation. Deliberately left for later.
- The identical write test against real S3 (see above), and whether SSE-KMS is acceptable: with it the ETag is not the MD5 of the content, so an identical rewrite would change the ETag and readers would fetch every object after every reconcile (`../s3-layout.md`).
- The owner ID prefix convention per writer kind. `i-` for EC2 instances is natural. The Kubernetes owner IDs need one for the IAM grants to keep fleets and clusters apart.
- Whether the single marker stays adequate. Any publisher can update it, which is harmless because a mark only causes a re-list, but a very high mark rate (many publishers changing at once) means many controllers re-list. Controllers could coalesce marks (at most one list per second).
- Whether refreshing every object every interval is the right liveness mechanism, or a sibling heartbeat object is preferable. The refresh approach was chosen because it needs no second object type and no pairing, and it keeps the stale owner semantics.
- Whether to add the MQTT hint transport. It would buy lower latency only, and only if the measured drain latency or poll cost shows a need.
