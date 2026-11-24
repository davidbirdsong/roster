# Architecture, API naming, and test strategy review

> A review dated 2026-11-15, kept as written. It predates the rename to roster, so its names, paths and line numbers are from then, and some of its findings are settled (see [TODO.md](TODO.md) and the analyses beside it).

**Subject:** `registry.tar` — Go module `github.com/davidbirdsong/registry`  
**Review date:** 2026-11-15  
**Review type:** source review, design critique, API redesign, prioritized implementation plan, and local integration-test scaffold  
**Evidence:** inspected the supplied archive's Go code, tests, README and design documents; checked current AWS and emulator documentation. No changes to the submitted production source are assumed.  
**Execution limitation:** this environment has Go 1.23.2, while the module requires Go 1.26.0; the requested Go toolchain cannot be downloaded here. Docker is not installed. Findings are source-grounded, but neither `go test` nor Compose was executed. The separately supplied testing scaffold is **proposed and unverified**, not a passing test result.

## 1. Executive judgment

**Keep the idea. Tighten the guarantees. Generalize the storage boundary without generalizing the domain.**

The useful product is **a small, polling-friendly, blobstore-backed registry of named services and their advertised endpoints**. It is for an operator who already has object storage and wants discovery without deploying Consul, etcd, ZooKeeper, or another always-on service. Its defining characteristics should be:

- Publishers **declare desired endpoint inventories**. One logically owned document is replaced atomically; the owner periodically reconciles it.
- Readers **obtain observed inventories**, with explicit completeness, observation time, and provenance. They may poll frequently and use optional change hints to reduce read latency.
- The library does **not** elect leaders, probe endpoint health, decide which publishers to trust, promise a globally atomic view, enforce freshness policies, or own application routing. Callers choose those policies.
- The storage engine implements an explicit **object/blob-store contract**, not the full S3 API. S3 is the first implementation, not a concept embedded in the root API.
- **No extra running infrastructure** beyond object storage is required. Even the change hint is an object being polled; native S3 event notifications are *not* part of today's implementation.

This is a credible niche. Consul is the better fit when a team needs membership/health/ACLs/watch behavior out of the box, consensus-backed updates, strong coordination, or sub-second discovery without polling. This library is better when deployment simplicity, smallish inventory, low write rates, high object-store availability, and caller-defined policy matter more than those features.

**Decision:** proceed, but **fix snapshot completeness during read races and corrupted-owned-object reconciliation before renaming the public API**. Rename before a stable v1 release if you can; otherwise support a compatibility period. Retain S3 as an adapter and as the benchmark for the contract, not in the project name.

### Strengths worth preserving

| Strength | Evidence | Why it matters |
|---|---|---|
| Dependency-light core | `registry.go`, `reader.go`, `publisher.go`; root has no AWS imports | The most important foundation for a generic backend |
| Desired-state, group-atomic writes | `Publisher.prepare/apply`, `Store.PutGroup`, `wirev1.Encode` | Easy to rebuild, idempotent, no delta-event history to replay |
| Separate `Audit` and `Sync` paths | `publisher.go:94-147,191-267` | Cheap change propagation plus periodic repair |
| Useful failure signals | `Snapshot.Complete`, `Skipped`, `Rejected`; `LastAuditAt` | Consumers can choose fail-closed or fail-open per use case |
| Self-contained polling hint | `s3store/bell.go`, `s3store/feed.go` | Avoids queues, WebSockets, event brokers, subscription setup |
| Bounded parallel GET | `s3store/reader.go:25-36,234-292` | Limits cold-start bursts |
| Deterministic wire and compatibility corpus | `wirev1/`, `wirev1/testdata/` | Essential to cross-version, multi-publisher compatibility |
| Unit doubles and some real-endpoint tests | `registrytest/`, `s3store/s3fake`, `s3store/integration_test.go` | Good base for a contract-test suite |

### Concise risk register

| Priority | Finding | Impact | Primary source |
|---|---|---|---|
| **P0** | A listed object that vanishes before GET is silently absent in a `Complete=true` snapshot | False deletion presented as certainty | `s3store/reader.go:141-145,268-301`; test `reader_test.go:291-307` |
| **P1** | Owned unreadable objects are dropped from `ListOwned` with no identity preserved | Full audit may fail to delete/repair a corrupt, no-longer-desired group | `s3store/store.go:114-145` |
| **P1** | Initial/recovery marker baseline race can miss a change notification | Drain/discovery latency extends to independent periodic rescan | `s3store/feed.go:91-132`; already identified in `docs/TODO.md:13-24` |
| **P1** | `ContentHash` encodes unbounded free-form fields with NUL separators; accepted values may contain NUL | Distinct valid inventories can share the same *serialized input to the hash*, suppressing a revision increment | `reader.go:121-142`, `registry.go:200-244` |
| **P1** | No storage-independent blob contract yet | Existing S3 client method signatures remain the actual storage abstraction | `s3store/s3store.go:27-35` |
| **P1** | A lease without fencing does not prevent an old publisher from writing late | Stale leadership can overwrite current state | `publisher.go:22-24`; `docs/design.md` scope decisions |
| **P2** | ETag rewrite stability is assumed for fetch-avoidance and needs backend-specific verification | Unexpected full refetch on audits; ETag semantics vary by implementation/encryption | `s3store/reader.go:234-302`, `docs/s3-layout.md` |
| **P2** | Invalid poll/retry options can make a hot spin loop | Excessive requests / log volume | `s3store/feed.go:36-79,159-168` |
| **P2** | Truncated LIST without a continuation token is treated as success | Partial inventory can look complete on a nonconforming store | `s3store/s3store.go:135-166` |
| **P2** | `Resolve` and `DropStale` encode default policy in the root package | Public API contradicts “the caller decides health, freshness, and selection” | `resolve.go`, `stale.go` |
| **P2** | Tests use an S3-shaped in-memory fake, not a generic provider contract | A new blob backend can compile but violate correctness expectations | `s3store/s3fake/fake.go` |

Priorities express architectural risk, **not observed production outages**. P0/P1 items require regression tests and a chosen behavior; the severity of a particular deployment depends on whether it treats snapshot absence as destructive.

## 2. Reframing the product and scope

### Public one-liner

> **A lightweight service registry backed by an object store.** Publishers advertise endpoint sets; consumers poll snapshots and decide freshness, health, trust, and routing. No registry server to operate.

Alternate short tagline: **“Service discovery using the blobstore you already run.”**

### Explicit non-goals

1. **Not a coordinator.** No distributed locks, elections, leases or fencing; writers bring an ownership strategy. The library should document that *application-level single-writer discipline is an assumption, not a guarantee*.
2. **Not a health authority.** `READY`, `TERMINATING`, and `Serving` are publisher observations, not measured reachability. A consumer can ignore them or require active checks.
3. **Not a trust engine.** Writers are authenticated/authorized by the blobstore's access-control model. Object provenance is metadata, not proof that an address is safe. A consumer validates the endpoints it accepts and the privileges of the identity that published them.
4. **Not an event bus.** Every hint is advisory. Snapshots are authoritative for current registered state; a full rescan is required after reconnect/errors and periodically in all cases.
5. **Not a globally serializable database.** A single object replacement is atomic on S3, but multiple objects, paginated LISTs and GETs do not form one transaction.
6. **Not a general-purpose schema registry.** V1 intentionally defines service/owner/endpoint identities and IP:port + HTTP/TLS attributes. If future users need arbitrary values or non-IP targets, define a different record type or deliberate schema extension, not unstructured blobs masquerading as endpoints.

The current model is *more than slightly* opinionated in some places: it requires IP-only endpoints, three HTTP variants, a required logical name and serving state, Kubernetes-shaped labels, and a specific resolution policy. These are legitimate for the current ingress workload but should be split into **core identity/advertisement** versus **optional endpoint-routing profile** if the audience is broader.

### A useful responsibility table

| Library guarantees | Blobstore guarantees/assumptions | Caller owns |
|---|---|---|
| Schema validation; canonical encoding; per-document replace/remove; snapshot diagnostics; retries/errors; optional change-marker watching | Atomic single-key overwrite; listed keys and metadata; read/list-after-write consistency sufficient for convergent reads; IAM | Leader election, writer fencing, trust, verification of destinations, health checks, health-based routing, stale cutoff, all-stale policy, jitter, scheduling, periodic refresh, metrics/alerts |

The middle column is a **required provider behavior to test**, not a universal truth of every product calling itself a blobstore.

## 3. Detailed code review and recommended fixes

### F1 — A disappearing GET is a falsely complete snapshot (**P0**)

**Where:** `s3store/reader.go:135-149,234-302`; current test `s3store/reader_test.go:291-307` explicitly expects `Complete=true` for this case.

**Mechanism:** The Reader lists key `v1/web/c2`, then a concurrent delete makes `GetObject` return `NoSuchKey`. `get` returns `(nil, nil)`, `fetch` omits the entry, and `Snapshot` uses `if !ok { continue }`. If nothing else was skipped, `NewSnapshot` marks it complete. The result can cause a consumer to retire an endpoint even though it did not obtain a coherent read of the inventory.

**Caveat:** Because the object really can have been deleted, a later complete snapshot *could* legitimately omit it. The bug is asserting completeness of a multi-operation observation that did not establish that fact for the read in progress.

**Fix:** On a missing GET for a listed object, either (a) restart the affected LIST/GET pass with a bounded number of retries and backoff/jitter, or (b) include `SkippedItem{Key, Err: ErrChangedDuringRead}` and `Complete=false`. Prefer bounded retry once or twice; return incomplete if the race persists. Cache must not silently fill the gap. Keep the previous accepted complete snapshot in the consumer when applying deletions. Add an explicit test checking the new invariant and an alternating-write/delete stress test.

### F2 — Corrupted owned groups are forgotten (**P1**)

**Where:** `s3store/store.go:114-145`, `registry.Store.ListOwned` contract in `registry.go:254-269`, `Publisher.Audit` in `publisher.go:134-162`, `s3store/store_test.go:224-245`.

`ListOwned` discards undecodable objects. But the publisher's full reconciliation operates from *decoded backends grouped by group*. If the stored object is corrupt and its service is no longer desired, the publisher has no evidence that the key exists and will not delete it. New readers may continue to mark themselves incomplete due to the abandoned object. The code comment says corruption can be repaired by rewriting a desired group; this is only half the lifecycle.

**Fix:** Change the full-audit storage contract to list **owned documents with their identities even when the payload is invalid**. E.g. `ListOwned(ctx, ownerID) ([]OwnedDocument, error)`, where each result includes `Service`, `Owner`, `Endpoints`, `DecodeError`, and `ObjectKey`. A desired group with bad data gets overwritten; an undesired bad group is deleted; a protected group is preserved and reported. A raw `ListOwnedKeys` or `ListOwnedGroups` plus optional decode result is an acceptable minimal alternative. Do not confuse a malformed object with a failed network read: the latter still aborts reconciliation before deletions.

A second subtlety: a valid JSON document with rejected individual records can also make ownership/state appear different. Decide whether **any rejected record** forces a rewrite/removal decision, rather than accepting only the decoded subset as the full stored state.

### F3 — Hint baseline race (**P1**)

**Where:** `s3store/feed.go:91-132`; independently documented in `docs/TODO.md` item 2.

`Subscribe` sends `ChangeResync` **before** the first HEAD that captures the marker's ETag. A client can start its LIST after this hint. If a writer updates a document and rings after that LIST but before the feed's first HEAD, the new marker value becomes the baseline and no further hint is generated. The same pattern recurs after feed error recovery. The caller's separate periodic snapshot read bounds the missed-hint delay, but it may violate a fast-drain expectation.

**Fix:** Establish a baseline HEAD **before** emitting the initial/recovery resync, then watch for differences. If the baseline HEAD fails, back off and retry; once it succeeds, emit a resync to cover everything up to that point. Test the precise interleaving with channels rather than sleeps. The caller must still perform periodic full reads: a marker is advisory and changes can coalesce.

### F4 — Non-injective content hashing (**P1**)

**Where:** `reader.go:121-142`, `registry.go:200-244`.

`ContentHash` concatenates string fields separated by `\x00`. `StableName` and `UniqueID` have no character-set restrictions; in particular NUL is permitted by `Backend.Validate()`. Consequently, `StableName="a\x00b", UniqueID="c"` and `StableName="a", UniqueID="b\x00c"` generate identical bytes across those two slots of the hash input, given otherwise identical fields. They are distinct valid records. The SHA-256 function is not broken; **the serialization before hashing is ambiguous**. Since `Revision` only increments when `ContentHash` changes, a consumer can mistake a real identity change for no change.

**Fix:** Use an unambiguous canonical serialization with field lengths, typed delimiters, and sorted map entries, or hash deterministic wire-compatible structured JSON including all relevant fields. Do not use a human-readable `fmt.Fprintf` format as a canonical identity scheme. Add regression fixtures for embedded NUL, newlines, colons and reordered labels. As defense in depth, bound and validate `LogicalID`, `InstanceID`, `Region`, and `Zone` (without needlessly banning interoperable ARN-style IDs).

### F5 — Root-level storage abstraction isn't actually a blobstore contract (**P1 design**)

The existing root `registry.Store` is **a domain-specific group store**, not a blob interface. This is fine as a publisher port. `s3store.API` uses AWS SDK input/output types, so alternative providers must mimic S3, including pagination and exception types. That makes the S3 adapter the de facto backend contract and leaks S3 assumptions into the storage logic.

**Fix:** Split two boundaries:

1. **Domain repository / document store:** `ListOwnedDocuments`, `PutService`, `DeleteService`; the reconciler speaks this.
2. **Generic blobstore:** key/prefix LIST, GET, PUT, DELETE, HEAD with an opaque object *fingerprint* and last-modified timestamp; the document-layout implementation speaks this.

The first change can keep `Publisher` mostly intact. The second allows another backend (filesystem, GCS, Azure Blob, in-memory) without asking it to implement AWS SDK interfaces. Keep S3-specific pagination, request signing, error interpretation, bucket naming and encryption behavior inside `blob/s3`.

A sample interface and constraints appear in §5.

### F6 — Single writer is assumed, not enforced (**P1 depending on use**)

**Where:** `publisher.go:20-24`, design scope in `docs/design.md`.

A Kubernetes Lease reduces overlapping writers; it does not prevent a delayed former leader from issuing a PUT/DELETE after the new leader has taken over. The assumption “two writers derive the same desired state” may be false across rollout, informer freshness, and service deletion. On an object store without compare-and-swap fencing, one stale writer can roll back or delete a newer inventory. This is **not** a request to embed a leader election protocol in the library.

**Fix:** Document the single-writer invariant prominently. Make ownership/lease management an application requirement. For use cases requiring stronger safety, offer an *optional* store capability for conditional writes (`If-Match`/generation) or embed owner-epoch metadata checked by an external coordinator. Do not promise that a check-then-write read of an epoch is sufficient; it is racy. Test two publishers with adversarial interleaving, and state whether late overwrites are acceptable for the targeted use case.

### F7 — Cache is keyed by ETag; storage contract should say what ETag means (**P2**)

**Where:** `s3store/reader.go:231-302`; `docs/TODO.md` item 4.

The reader skips GET when LIST's ETag matches the cache. The design additionally expects *identical content rewrites* to leave the ETag unchanged, so periodic liveness refresh does not trigger full GET. This is a useful optimization, not an inherent requirement of object storage. ETags are opaque and provider/encryption/multipart behavior differs; S3 documentation does **not** universally promise that ETag is the MD5 or that a rewrite will have an identical ETag under all configurations.

**Fix:** In generic code call the field `Fingerprint` or `RevisionToken`, explicitly *opaque* and only comparable for the same key within the same store. Define two levels of capability: **required** “a changed object becomes observable via a changed revision token or a conservative forced refetch” and **optimization** “identical rewrite usually keeps the token.” On backends that cannot provide a reliable fingerprint, fetch all listed objects, or use conditional GET with provider-native tokens. Provide a test mode with unstable/empty tokens and report how request cost changes. Avoid documenting SSE-S3 as a hard requirement if it is only a cache efficiency goal.

### F8 — LIST pagination should fail closed (**P2**)

**Where:** `s3store/s3store.go:135-166`.

`listAll` returns successful accumulated results when `IsTruncated==true` **or** the continuation token is nil — the code uses `if !IsTruncated || NextContinuationToken == nil { return ... }`. For a broken emulator or another backend this silently accepts an incomplete list. Separate the checks: stop only when not truncated; if truncated with absent/empty/repeated token, return an error. Test an incomplete page, a repeated token and a page with zero keys but non-empty common prefixes.

The in-memory `s3fake.ListObjectsV2` also has a **delimiter pagination gap**: it counts `Contents` but not `CommonPrefixes` toward page size (`s3fake/fake.go:155-178`). It therefore cannot validate the reader's top-level `v1/` prefix pagination behavior. Repair the fake and test the contract at boundaries 999/1000/1001 and with multiple wire majors.

### F9 — Errors and bad records should not silently imply deletion (**P2**) 

**Where:** `wirev1.Decode`, `Snapshot.Complete`, `Rejected`, `Skipped`.

The wire decoder correctly permits compatible new optional fields and separates invalid records from malformed whole documents. But `NewSnapshot` sets `Complete=false` only for `Skipped`. An object with invalid individual endpoints can be `Complete=true` with fewer backends than the prior snapshot. This is currently a conscious choice, and it might be right for an untrusted publisher: a bad endpoint should not freeze every good one. For a consumer performing destructive configuration replacement, however, silent loss can be dangerous.

**Fix:** Keep separate diagnostics but make policy explicit: `Complete` means every listed document was fetched and interpreted, **not** “all advertised endpoints were accepted”. Expose `Rejected` by owner/service and document a consumer mode that rejects or quarantines affected documents while retaining last good endpoints. Do not globally flip `Complete=false` merely because one endpoint is invalid unless you choose that semantic contract. Add a table of consumer reactions to `Skipped`, `Rejected`, storage errors, stale owners and all-stale conditions.

### F10 — Poll configuration and concurrency safety (**P2**)

`WithPollInterval(0)` or negative retry intervals can cause repeated immediate HEAD calls; `WithFeedRetry` accepts any min/max, including contradictory values. Validate intervals in `NewPollFeed` (`interval>0`, `retryMin>0`, `retryMax>=retryMin`) and avoid delaying cancellation while sending to a slow subscriber. Currently cancellation is handled, which is good. Consider jitter to prevent 80 consumers restarting and polling in lockstep, and minimal coalescing/debounce of redundant full reads in the caller. `WithClock(nil)` and `WithDecoder(..., nil)` should be rejected or guarded. Typed-nil interface arguments deserve either detection or explicit documented responsibility.

### F11 — Staleness helper is a policy, and its fail-open behavior is material (**P2**)

**Where:** `stale.go:17-67`.

`DropStale` defaults to keeping everything if **all** owners are stale, deliberately preventing a total zero-endpoint outage. This is an *application policy* with potentially severe failure/security tradeoffs: intentionally retired or compromised endpoints may be reactivated indefinitely by the all-stale exception. Conversely, dropping all stale backends can cause a blackout during a shared clock or storage failure. There is no universally correct choice.

**Fix:** Keep this as an **opt-in example** in `policy/` (or `examples/`) named something like `FilterExpiredOwners`, with explicit `AllStaleKeep`/`AllStaleDrop`/`AllStaleError` policies, or remove it from the stable public API. Keep `ClusterStatus` (rename `OwnerStatus`) in core as *observation*. `ReadAt` and `LastAuditAt` compare local reader clock against storage's clock, so a caller must allow skew and choose a threshold much longer than audit cadence.

### F12 — `Resolve` belongs to consumers (**P2**)

**Where:** `resolve.go:8-68`.

The shared tie-breaker preferring `READY`, then `TERMINATING`, then `NOT_READY` is sensible for a specific HAProxy naming collision; it does **not** consider the `Serving` field and is not an inherent registry rule. If this library's philosophy is “caller owns trust, health, routing,” move it into an optional `policy/` helper or a consumer example. Keep records unfiltered in snapshots. Consider requiring a consumer to choose between duplicates by `(owner, service, logical name, port)` and explicitly define behavior for cross-owner duplicates.

### F13 — Validation and schema should be intentional (**P2**)

`UniqueID` is limited to 512 bytes; labels have Kubernetes-like rules; `StableName` and `Region/AZ` have no length limits or escape rules. There is no hard document size limit or endpoint count per service. This is acceptable for private controlled writers, but a *general* registry library needs a predictable resource envelope. Implement caps on total document bytes, endpoints per document, string field lengths, and GET response bodies, and pick defaults high enough for the current EKS workload. Avoid silently assuming that hostnames are always forbidden; if IP-only is a deliberate v1 design, advertise it in the name/README, or add a future endpoint-address type with a versioned migration.

**Specific security note:** publisher-controlled `Writer`, `Labels`, `SNI`, IPs and logical names are *data*, not credentials. Storage ACLs define write authority; consumers must not grant behavioral authority based on a label or free-form writer string.

### F14 — Operational issues (**P2/P3**)

- **Cost model:** `N` readers each polling a marker every `T` seconds generate approximately `N/T` HEAD/s, or `86,400 × N / T` HEAD/day. For 80 readers at 2 seconds, **40 HEAD/s, 3,456,000 HEAD/day**, plus LISTs triggered by changes and periodic resyncs. This is plausible on S3; use measured cost/latency instead of “free”. AWS currently documents at least 5,500 GET/HEAD requests/s per partitioned prefix and at least 3,500 write requests/s per partitioned prefix (see references), which are *not* bucket-wide guarantees for every blob provider.
- **Burst control:** jitter poll interval and audits, bound fetch concurrency, retry transient 429/5xx with backoff, and add API latency/failed-poll counters.
- **Document growth:** one document per owner/service makes writes atomic but lets a very large service require a giant PUT/GET; enforce a size budget and decide what needs sharding later.
- **Observability:** expose snapshot duration, listed keys, fetched documents/cache hit ratio, skipped/rejected records, time since last completed audit, marker HEAD failures, overdue owners, changed endpoints, and end-to-end propagation delay. Never log full sensitive endpoint inventory by default.
- **Revision semantics:** `Revision` is a per-Reader process counter; it resets on a new instance. This is documented; keep it explicit in the public API to prevent consumers comparing revisions across restarts. Consider a named `ReaderEpoch` or an opaque `ChangeToken` to make the scope unmistakable.
- **Compatibility:** newest supported wire version wins per `(service, owner)`; test partial rollouts, unknown newer versions, old-version leftovers, deletes and mixed writer versions. A new backend adapter must use **the same canonical wire contract**, not reinterpret its fields.

## 4. Naming rethink: product, domain, API, packages

### Name selection criteria

A good name should make **service discovery/registry** obvious, avoid S3 and AWS brands, not promise automatic health or consensus, remain natural as a short Go module/package, and leave room for S3/GCS/Azure/local-filesystem backends. Names below are **suggestions, not domain/GitHub/package availability checks**.

| Name | Strength | Weakness | Verdict |
|---|---|---|---|
| **`blobdisco`** | Clearly hints at blob-backed discovery; memorable, provider-neutral | “disco” is informal and may imply richer discovery behavior | **Best distinctive product name** |
| **`serviceindex`** | Clear that it indexes advertised services, sounds read-oriented | Storage mechanism not in name; might sound like a search index | **Best sober/product-neutral name** |
| **`blobreg`** | Short, memorable, says “blob + registry” | Could be misunderstood as a *registry of blobs* | **Best compact Go package name**, with an explicit tagline |
| `serviceregistry` | Unambiguous domain | Generic, no hint of storage model | Good if discoverability beats distinctiveness |
| `objectdiscovery` | Provider-neutral and emphasizes discovery | Long and overbroad | Reasonable alternative |
| `bucketreg` | Familiar to S3 operators | Biases terminology toward bucket-based stores | Avoid if backend neutrality is a goal |
| `s3registry` | Mechanism obvious | Violates the user requirement and blocks backend abstraction | Reject |
| `lightconsul`, `miniconsul` | Explains niche by association | Implies compatibility/features not present | Reject |
| `servicedns`, `blobdns` | Memorable | It does not publish/resolve DNS records | Reject |

**My choice:** **`blobdisco`** for the repository/project and Go package *if an informal name is acceptable*; otherwise **`serviceindex`**. For a very concise API, **`blobreg`** is a strong alternative, provided the README's first line says “service registry backed by blob storage.” In all cases the customer-facing description should lead with *services*, not with the storage engine.

### Public domain vocabulary (recommended)

Keep terms tied to roles rather than Kubernetes or a particular proxy. Names are a coordinated family, not a search-and-replace exercise.

| Existing | Proposed | Rationale / compatibility note |
|---|---|---|
| `registry.Backend` | **`Endpoint`** | A published address/port and attributes, not necessarily a proxy backend |
| `Backend.Group` | **`Endpoint.Service`** (`ServiceName` also valid) | Names the logical advertised service/pool |
| `Backend.ClusterID` | **`Endpoint.OwnerID`** or `PublisherID` | Current “cluster” can be an EC2 instance; expresses writer ownership |
| `Backend.UniqueID` | **`InstanceID`** | One lifetime of a running instance; several ports share it |
| `Backend.StableName` | **`LogicalName`** | Distinguish stable-ish routing name from unique instance identity; duplicates during handoff allowed |
| `BackendID` | **`EndpointID`** | Derived from `(InstanceID, Port)`; no public implication that it is stored per endpoint |
| `ProtocolHTTP/HTTPS/H2C` | keep protocol values | These are wire values. Avoid changing without a wire version. Optionally rename Go constants to `ProtocolHTTP1`, etc., while keeping strings |
| `State*`, `Serving` | keep for v1 **endpoint profile** | Preserve observed states; do not imply automatic probes |
| `AZ` | **`Zone`** or `AvailabilityZone` | Cloud-neutral label; wire field remains `az` within v1 |
| `Publisher` | **`Publisher`** | Good role term and API already established |
| `Publisher.Audit` | **`Reconcile`** | Really reads stored state and repairs drift *and* refreshes liveness; “audit” sounds read-only |
| `Publisher.Sync` | **`Publish`** or `Update` | Takes a full desired set but skips unchanged groups using in-process cache. Recommend `Publish` |
| `ProtectGroups` | **`PreserveServices`** | A service whose authoritative desired inventory is temporarily unavailable should not be deleted |
| `GroupsOf` | **`ServicesOf`** | Matching helper |
| `registry.Store` | **`DocumentStore`** / `InventoryStore` | Distinguish from generic blobstore; `ListOwned` must preserve corrupt object identities |
| `Reader` | **`Reader`** | Precise and simple |
| `Snapshot.Backends` | **`Snapshot.Endpoints`** | Read-side domain normalization |
| `Snapshot.Clusters` | **`Snapshot.Owners`** | Liveness is per *owner*, not per compute cluster |
| `ClusterStatus.LastAuditAt` | **`OwnerStatus.LastReconciledAt`** or `LastRefreshAt` | Derived from oldest document write time; *not proof an audit method ran* |
| `ChangeFeed` | **`HintSource`** | Advisory nudge, not streaming authoritative state |
| `ChangeResync` | **`HintRescan`** | Clearly means “read again” |
| `Bell` / `Ringer` | **`ChangeMarker`** / `MarkerWriter` | Stops implying push notifications |
| `PollFeed` | **`MarkerPoller`** | The poller HEADs a marker blob; no event bus |
| `Resolve` | **`policy.PreferEndpoint`** or consumer-specific helper | Selection is an opt-in policy |
| `DropStale` | **`policy.FilterExpiredOwners`** | Opt-in with explicit all-stale behavior |
| `wirev1.GroupState` | **`wirev1.ServiceDocument`** | One document per owner/service; wire `backends` stays stable unless v2 chosen |
| `s3store` | **`blob/s3`** (implementation package) | S3 limited to provider adapter; no S3 name in core |
| `registrytest` | **`registrytest`/`testkit`** under renamed module | Stable in-memory test fixtures, generic interfaces |

A `Service` model type should be a named identifier or inventory envelope; **do not introduce a heavyweight `Service` object** just to satisfy naming. `[]Endpoint` plus an owner/service ID is sufficient.

### Schema policy: what should change, and what should stay stable

**Avoid changing JSON field names in `v1`.** Keep `unique_id`, `stable_name`, `backends`, `state`, `serving`, `az` and `v1/<group>/<clusterId>` *physically* stable during the **Go API rename**. Introduce new names only at the Go boundary and map to the old v1 wire with clear comments. This makes a rename an API migration rather than a dangerous mass rewrite of persisted objects and IAM grants.

If you want the *stored schema* renamed too (`services/<service>/<owner>`, `endpoints`, `owner_id`, `logical_name`), create **wire v2**, with a documented rollout: (1) readers support v1+v2, (2) writers move to v2, (3) monitor coverage and staleness, (4) delete v1 only after a safe period, (5) clean up IAM/lifecycle rules. Define canonical selection and deletion behavior for mixed majors. Do **not** silently point the new code at renamed keys.

### Package layout (target shape)

```text
blobdisco/                        # or serviceindex/ or blobreg/
  endpoint.go                    # Endpoint, IDs, validation, attributes
  publisher.go                   # desired-state reconciliation
  reader.go                      # Snapshot, completeness, observations
  hints.go                       # optional HintSource abstraction
  docstore.go                    # domain-level document repository interface
  wirev1/                        # stable existing JSON and frozen fixtures
  blob/
    blob.go                      # generic object store operations + capabilities
    s3/                          # AWS S3 adapter only; aws-sdk dependency
    memory/                      # conformance fake, not mock of AWS SDK types
  layout/                         # blob-backed document layout, read cache, change marker
  policy/                         # OPTIONAL staleness, dedup, state-based selection
  testkit/                        # in-memory publisher/reader/hint doubles
  testing/
    compose.yaml
    run-integration.sh
    README.md
    contract/                      # future backend conformance tests
  docs/
    architecture.md
    object-store-contract.md
    schema-v1.md
    migration.md
```

The layout does not need to appear in one disruptive PR. `blob/s3` can initially wrap the current `s3store.API` logic, with `layout` extracted after correctness tests are in place. A smaller initial design is better than a large interface introduced before a second implementation exists.

## 5. Proposed generic blobstore contract

This is a **conceptual Go API**, not a drop-in patch. It deliberately avoids exposing S3's `Bucket`, SDK request structs, `ETag`, pagination token shape, IAM, region, checksum or exception names at the root library boundary.

```go
package blob

import (
    "context"
    "errors"
    "time"
)

var ErrNotFound = errors.New("blob: not found")

type ObjectInfo struct {
    Key         string
    Fingerprint string    // opaque, same-key comparison only; may be empty
    ModifiedAt  time.Time // zero means unknown, NOT "fresh"
    Size        int64
}

type ListOptions struct {
    Prefix    string
    Delimiter string  // optional, for discovering child prefixes
}

type ListResult struct {
    Objects  []ObjectInfo
    Prefixes []string
    // Implementations must exhaust *all* physical pages or return an error.
    // Alternatively expose pagination consistently and have the layout iterate.
}

type Store interface {
    List(ctx context.Context, opts ListOptions) (ListResult, error)
    Read(ctx context.Context, key string) (data []byte, info ObjectInfo, err error)
    Stat(ctx context.Context, key string) (ObjectInfo, error)
    Write(ctx context.Context, key string, data []byte, contentType string) error
    Delete(ctx context.Context, key string) error // missing key is success
}
```

**Why include HEAD/Stat?** The default low-latency watcher polls a change marker. `Stat` lets the backend avoid full object downloads. **Why keep `ModifiedAt`?** The current design uses an object's write time to detect abandoned owners. If the new interface lacks it, the design must switch to a separate published heartbeat/lease object with a documented clock and failure model. **Why `Fingerprint` rather than ETag?** It is an opaque token useful for avoiding a download when a listed object is demonstrably unchanged; it is not necessarily a content hash.

### Contract invariants to document and test

| Capability | Contract | If a provider cannot supply it |
|---|---|---|
| Single-key replace | Atomic whole-object visibility; readers never observe a partial payload | Reject provider for this layout |
| Single-key visibility | A successful write/delete eventually becomes visible, and under preferred backends a subsequent LIST/GET sees it immediately | Specify weaker convergence/longer retry policy; do not claim S3-like consistency |
| Prefix enumeration | Lists all matching keys **or fails**; correct pagination/delimiter semantics; stable enough for recurring full scans | Reject provider or implement adapter-level complete scanning |
| Read missing key | Returns `ErrNotFound` distinctly from authorization/network errors | Adapter maps underlying error codes |
| Object version marker | Unchanged fingerprint permits cache reuse **only if adapter guarantees it denotes same body** | Force a GET on each scan; correctness beats cache |
| Modification time | Server/adapter supplied, comparable within reasonable skew and at known granularity | Return unknown and disable liveness inference, or add explicit per-owner liveness objects |
| Deletion | Idempotent; a missing object does not fail reconciliation | Adapter translates expected missing error |
| Durability/error handling | A successful write is committed according to the provider's own documentation; errors are surfaced; cancellation respected | State provider-specific weaker durability and retries |
| Access control | The adapter exposes credentials/storage scope but does not make application trust decisions | Caller must deploy an authorization boundary or reject unsafe configuration |

**Important design decision:** the proposed interface's `List` returns a fully collected listing. That makes it easy for the layout to enforce “no partial success.” For very large registries, a paginated iterator might be preferable, but never let the caller forget to exhaust it. A `ListPage` method is equally valid if the layout owns pagination and detects missing/repeated tokens.

Do **not** require S3-specific bucket notifications, object versioning, conditional writes or multipart uploads in the base contract. Make conditional writes an optional *capability* only if the project actually implements fenced writers.

### Data layout remains a separate layer

The generic `blob.Store` should know nothing about owners, endpoints, the v1 JSON or `/bell`. The `layout/` package knows how service IDs and owner IDs become keys, which wire version to write, how to decode stored objects, how to measure owner refresh age, and how to implement the marker. The S3 adapter knows how its SDK expresses HEAD, LIST, GET, PUT, DELETE and errors.

Conceptually:

```text
Publishers --> Publisher --> DocumentStore port ------+
                                                     |
Readers ----> SnapshotReader ---------------------+    |
                                                |    |
                                      blob document layout + marker
                                                |
                                         blob.Store contract
                                                |
                              +-----------------+------------------+
                              |                 |                  |
                           S3 adapter      Memory adapter     Future adapter
```

Keep the number of externally visible interfaces small. If you only need one real provider at first, write a *contract test* for a minimal memory adapter and postpone optional capabilities until there is demand.

## 6. Discovery, freshness and the polling design

### Polling is a first-class strategy, not a compromise to apologize for

S3's documented strong read-after-write consistency for PUT/DELETE and LIST is well suited to periodically enumerating an authoritative current-state registry. S3 also publishes request-throughput guidance of at least 5,500 GET/HEAD/s per partitioned prefix and at least 3,500 write/s per partitioned prefix; this makes dozens or hundreds of low-cost HEAD pollers plausible for the present scale. None of these properties automatically transfers to other object stores. See current AWS sources [1] and [2].

However, make the cost/latency options clear:

| Approach | Read path | Pros | Cons | Recommendation |
|---|---|---|---|---|
| **Direct periodic snapshots** | LIST keys + GET changed keys every `T` | Simplest; no hint object; no missed-marker race | More LIST requests; detection lag up to `T` + work | **Baseline supported mode; keep it first class** |
| **Marker-assisted polling** (current) | HEAD tiny marker every `P`; LIST + changed GET on marker change and periodically | Low steady-state LIST rate; easy on S3 | Initial HEAD baseline race; extra shared hot key; hint loss requires periodic rescan | **Optional optimization**, after race fix |
| **Per-service marker polling** | HEAD one marker per selected service | Targeted refresh | Multiplies HEAD requests by subscribed services | Only for truly large namespaces |
| **Native provider event notification** | S3 event -> SQS/SNS/EventBridge/Lambda -> consumer integration | Low potential latency | Adds services, bindings, permissions, delivery semantics, and consumer lifecycle | Explicitly **out of core scope** unless measurement requires it |

### Native S3 notifications are different from the current bell

The current `Bell` is a `PutObject` on a key. The current `PollFeed` observes it with `HeadObject`. **It does not call `PutBucketNotificationConfiguration`, receive SQS/SNS events, or subscribe to S3 changes.** S3 event notifications are a separate infrastructure feature that target other services; they are generally **at-least-once** and **not ordered**, so event delivery cannot replace current-state reconciliation [3]. This distinction matters for emulator choice: a local S3 object API is enough to test the present design, and it need not emulate native notifications.

### Recommended reader loop (caller-owned scheduling)

```text
on startup:
  read snapshot and apply if acceptable
  [optional] start marker poller, with marker baseline established before resync

forever:
  on hint:
     coalesce bursts; read fresh snapshot
  on scheduled full-rescan timer with jitter:
     read fresh snapshot even if no hint arrived
  on read error or incomplete snapshot:
     record failure; preserve last acceptable state; retry with backoff
  on rejected individual endpoints:
     apply caller-configured quarantine/reject/skip policy
  on staleness:
     apply caller-configured stale and all-stale policy
```

The library should return observations and good errors; consumers decide whether a snapshot is safe to apply. Avoid a claim of guaranteed “within two seconds” merely because the marker polling period is two seconds. Include HEAD delay, LIST+GET time, consumer debounce, proxy reload and fallbacks. Publish measured p50/p95/p99 results from local and real AWS testing.

### Useful request-budget formula

For `R` readers, marker poll period `P` seconds, full snapshot period `F` seconds, and average `C` change-driven snapshots per second across the fleet:

```text
marker HEAD/s  ≈ R / P
scheduled LIST/s ≈ R / F × LIST_pages_per_snapshot
change LIST/s ≈ C × LIST_pages_per_snapshot
GET/s ≈ changed_documents_fetched + cold_start_misses + noncacheable_objects
```

Example: `R=80`, `P=2s` means `40 marker HEAD/s`, or `3,456,000 HEAD/day`, before snapshot LISTs. This is far below cited S3 per-prefix request guidance, but still has costs, network latency, AWS regional dependencies and potential synchronized spikes. “Poll aggressively” should mean **tunable with jitter and observability**, not unbounded busy loops.

## 7. Integration testing: concrete `testing/` directory

I prepared a separate, additive directory in **`registry-testing-scaffold.tar.gz`** with the exact layout below. Unpack it into the module root. It does not change `s3store` code or rewrite existing unit tests.

```text
testing/
  compose.yaml
  run-integration.sh
  README.md
```

### Why `local-s3` now

The user's suggested [shyim/local-s3](https://github.com/shyim/local-s3) advertises a small Docker image, filesystem-backed objects, SigV4, path-style access, configurable accounts/buckets and a web UI. Its README documents `ghcr.io/shyim/local-s3:v1.0.0` and port 9000 [4]. Those are appropriate for the current library's GET/HEAD/LIST/PUT/DELETE contract. **Its documented feature list does not establish native S3 bucket-event notification delivery.** Do not assume that it has SQS/SNS/EventBridge implementations or that a local event matches AWS ordering/retry behavior.

`adobe/s3mock` is a viable existing fallback for CRUD/metadata testing and was already mentioned by the repository's `s3store/integration_test.go`. The current Adobe S3Mock operation matrix explicitly marks `PutBucketNotificationConfiguration` **unsupported** [5]. LocalStack supports S3 and has documented S3 event-to-SQS integrations, but brings additional services/configuration, which is not needed to test this project's current marker-polling feature [6].

### The provided Compose service

```yaml
name: registry-integration
services:
  objectstore:
    image: ghcr.io/shyim/local-s3:v1.0.0
    environment:
      S3_ACCOUNT_ADMIN: "${S3_ACCESS_KEY:-test}:${S3_SECRET_KEY:-test-secret}"
      S3_DATA_DIR: /data
    ports:
      - "127.0.0.1:${S3_TEST_PORT:-19000}:9000"
    tmpfs:
      - /data
```

The host listener is loopback-only; data disappears with the container, so test state is disposable. The script starts Compose, checks that the port becomes reachable, sets `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, and runs the **existing real-SDK integration test suite**:

```bash
./testing/run-integration.sh
# Internally:
# go test -count=1 -tags=integration ./s3store -run '^TestIntegration' -v
```

The test suite creates unique buckets, so no static bootstrap bucket is required. `KEEP_TEST_STORE=1` leaves Compose up after the script; by default the script tears everything down. This is a real scaffold, **not a claim that `local-s3` has already passed the tests**: Docker is absent in the review environment and the local Go compiler is older than this module's Go 1.26 requirement. Pay particular attention to paginated LIST, same-body rewrite time and ETag behavior during the first real execution.

### Test pyramid and required cases

| Layer | Harness | Tests | Must run when |
|---|---|---|---|
| **Unit: domain** | `registrytest`, no Docker | Validation, publisher diff/repair, owner bounds, protected services, state transitions, failure propagation | Every PR |
| **Unit: deterministic races** | controllable memory blobstore + barriers | LIST/GET delete race, marker startup race, failed refresh, two writers, corrupt object handling, content hash collision, cancellation | Every PR |
| **Blob contract** | same tests against memory, S3 adapter, future adapters | CRUD, prefix+delimiter lists, pagination, LastModified, fingerprints, error mapping, deletion semantics | Every adapter change |
| **Local integration** | provided Compose + real AWS SDK v2 | Existing `-tags integration` suite; marker HEAD polling and backend read/write flow | Every PR / CI |
| **AWS smoke** | scoped ephemeral test bucket | Real S3 consistency/ETag under intended encryption, IAM scoping, retries, lifecycle/versioning behavior, last-modified precision | Nightly / pre-release |
| **End-to-end latency/load** | publisher+reader+consumer harness | Burst updates, 80 pollers, startup thundering herd, p50/p95/p99 discovery delay and requests | Before production, periodically |

#### Must-add deterministic regression tests

1. **`TestListGetDisappearanceDoesNotProduceCompleteSnapshot`**: fake lists `v1/web/c2` but GET returns `ErrNotFound`. Assert retry or `Complete=false`, never a silent complete deletion.
2. **`TestAuditRemovesUndecodableUndesiredDocument`**: stored owned `v1/api/c1` has invalid JSON, desired inventory omits `api`. Audit must delete it, or explicitly report unresolved corruption; do not silently succeed with a dangling document.
3. **`TestMarkerBaselineCannotLoseWriteBetweenListAndHead`**: force the initial resync, interleave a writer's changed document and bell ring, then release baseline HEAD; reader must eventually be prompted without waiting for fallback period.
4. **`TestContentHashIsInjectiveAcrossDelimitedIdentityFields`**: use `LogicalName="a\x00b", InstanceID="c"` versus `LogicalName="a", InstanceID="b\x00c"`. Assert different canonical hash, revision increments.
5. **`TestPaginationTruncatedWithoutTokenReturnsError`** and **`TestPaginationCommonPrefixesCrossPageBoundary`**: ensure no partial complete result and proper fake contract behavior.
6. **`TestIncompleteToCompleteTransitionDoesNotGetLostToDedupe`**: an unreadable document becomes absent or repaired without a change to currently returned endpoint bytes. Ensure snapshot application can observe changed completeness and decide whether to remove previously known endpoints; `Revision` alone must not hide that transition. The current counter changes only with `ContentHash`, not `Complete` or diagnostics.
7. **`TestNoFingerprintForcesRefetch`** and **`TestUnstableFingerprintStillCorrect`**: correctness when a provider has empty or changing ETags, measuring extra traffic separately.
8. **`TestStaleWriterOverlapsNewWriter`**: demonstrate/document expected conflict under current single-writer assumption; if adding optional fencing, test the stale writer is rejected.
9. **`TestPollerRejectsNonPositiveIntervals`**, `TestPollerHasJitter`, `TestPollerCancelsPromptly`.
10. **`TestOwnedScopeResistsCrossOwnerDeletes`**: verify both high-level port logic and (in AWS smoke) resource policies.

For timing tests, use controllable clocks/barriers, not 400-ms sleeps. For integration time checks, the existing `TestIntegrationAnIdenticalWriteAdvancesLastModified` uses a 1.1-second delay to accommodate timestamp precision; this is reasonable in an external endpoint test but should not become a general unit-test pattern.

### How native event-notification testing would look *if added later*

Only introduce this once there is production code for an optional S3-specific event adapter. Run a **separate Compose profile** with a capable event emulator (for example LocalStack + S3 + SQS) and test `PutBucketNotificationConfiguration`, actual message deliveries, duplicates, reordering, deletion and filters. Then run AWS smoke tests as the authority. Do **not** design the generic `blob.Store` interface around that transport. Native notification tests are out of scope for the supplied `local-s3` scaffold, because **nothing in today's library consumes them**.

## 8. Step-by-step implementation plan

### Phase 0 — Baseline and guardrails (small PR)

**Goal:** no behavior changes; prove the existing contract.

- Import the `testing/` scaffold; run `./testing/run-integration.sh` on a machine with Docker and Go 1.26+.
- Repair emulator compatibility issues if any; confirm `HEAD`, pagination, `LastModified` and same-byte ETags. Never tailor core semantics solely to a mock quirk.
- Add a short `CONTRIBUTING.md` with `go test ./...`, `go test -race ./...`, `go test -tags=integration ...`, Compose commands, and AWS smoke instructions.
- Add CI jobs (unit/race + Compose integration) and an issue template for backend conformance failures.

**Exit:** local test runner works in CI; measured pass/fail outcomes documented. Do not claim all tests pass until executed.

### Phase 1 — Correctness repairs (**blocker before API rename**)

1. Fix disappearing-GET handling and change its existing unit test expectation.
2. Preserve corrupted owned document identities during full reconciliation; repair/delete deterministically.
3. Fix PollFeed marker baseline order and error-recovery resync sequence; add barrier tests.
4. Fix canonical content hashing and test incompleteness/revision transitions.
5. Fail on truncated LIST without continuation; repair fake delimiter pagination.
6. Validate feed intervals, retry limits and test zero/negative input.
7. Add bounded document-size/field limits where relevant and verify backwards compatibility against `wirev1/testdata`.

**Exit:** a failed or racing read cannot claim authoritative deletion; reconciliation can converge from damaged stored data; hint startup/recovery races are covered; revisions cannot collide due to ambiguous field serialization.

### Phase 2 — Contract extraction (no externally visible rename yet)

1. Introduce `blob.Store` and an in-memory implementation in `blob/memory`.
2. Move AWS SDK calls and error translation into `blob/s3`.
3. Move key-layout, v1 JSON, cache and marker algorithms into storage-neutral document-layout code.
4. Define required/optional provider capabilities, error mapping, pagination semantics and liveness timestamp behavior.
5. Run identical conformance tests for both memory and S3-backed implementations.
6. Provide one composition constructor or factory, so typical code still looks like one S3 client + config + Publisher/Reader, not five required adapters.

**Exit:** root package and layout compile without AWS imports; a hypothetical second backend would implement only generic blob operations, not the AWS SDK API.

### Phase 3 — Public naming and responsibility boundary

1. Choose repository name based on actual package/repository availability and consumer feedback. Recommended: `blobdisco` or `serviceindex`.
2. Rename public domain types/fields through wrappers or aliases where feasible; avoid a mass rename of v1 JSON/key names.
3. Rename `Audit` -> `Reconcile` and `Sync` -> `Publish` with exact semantics in docs; keep deprecated forwarding methods temporarily if already consumed externally.
4. Move `DropStale` and `Resolve` to `policy` or documented examples. Remove invisible assumptions that these are mandated by the registry.
5. Rewrite README around the use case, responsibilities/non-goals, polling modes, provider contract, and three minimal examples (publisher, reader, policy).
6. Add migration guide old->new APIs, module import paths, and wire-stability promise. Account for Go type/field alias limitations: struct field renames are source breaking and need migration edits or wrapper types.

**Exit:** a reader can understand what the library **does and does not do** from the README's first screen; S3 appears only under adapters/docs and not in project/package branding.

### Phase 4 — Prove deployability and performance

- Exercise intended S3 configuration (versioning, encryption, lifecycle and IAM) with a real bucket. Confirm `LastModified` on identical overwrites and ETag/refetch behavior empirically.
- Run high-poller tests (e.g., 80 readers at 1s and 2s) and capture rate, errors, request cost, memory and latency.
- Simulate publisher death, total owner staleness, storage outage, invalid records, producer restart and load spikes.
- Document least-privilege IAM, writer ownership, bucket lifecycle and operational alarms.
- Publish a simple decision table: **choose this vs choose Consul**; make the tradeoff visible rather than defensive.

**Exit:** published workload envelope with request estimates and tested recovery behavior, not a generalized performance promise.

### Phase 5 — Optional future enhancements (only if actual users need them)

- A second provider adapter (GCS or Azure Blob) to validate that the generic contract wasn't S3 in disguise.
- Optional conditional-write/fencing capability where needed; otherwise document single-writer assumptions.
- Optional native event/queue hint transport without changing snapshot authority.
- A versioned `EndpointAddress` type for DNS/Unix socket/non-HTTP workloads, only with a concrete use case.

**Do not** add background daemons, arbitrary service TTL eviction, a lease manager, health checker or central coordinator to “make it more like Consul.” That would eliminate the product's differentiator.

### Proposed ownership and review order

| PR | Change | Risk | Review focus |
|---|---|---|---|
| 1 | Compose harness + baseline CI | Low | Reproducible real-SDK execution |
| 2 | Read race, malformed owned groups, hash, marker ordering | **High** | Adversarial tests; no misleading completeness |
| 3 | Blobstore port + S3 adapter | Medium/high | No regressions in pagination, caching, refresh age |
| 4 | Public API vocabulary + docs | Medium | Backward compatibility and wire-v1 immutability |
| 5 | Consumer-policy examples + metrics/load test | Medium | Explicit responsibility boundaries and actual SLO |

## 9. Suggested README opening and minimal API narrative

```markdown
# blobdisco

A small service registry backed by an object store. It lets independent
publishers advertise endpoint inventories and independent readers discover
what has been advertised—without running a registry server.

S3 is the first blobstore adapter, not a dependency of the registry's public
model. Consumers poll current-state snapshots; an optional polled change marker
can reduce reaction latency.

**The library does not check endpoint health, expire or trust publishers for
you, elect leaders, or choose routing policy.** Those remain application
choices. A snapshot describes advertised state and whether the read was
complete, not a guarantee that endpoints are healthy or authorized.

Choose it when you already trust your object store and don't want to operate
Consul just to locate a modest number of changing endpoints. Choose Consul
or another dedicated discovery system if you need built-in health,
coordination, rich watches or stronger membership semantics.
```

Then present the following examples in this order: **(1) publisher** creates full desired inventory and calls `Reconcile` on startup + scheduled refresh, `Publish` on source changes; **(2) reader** fetches complete snapshots and optionally uses marker hints; **(3) caller policy** chooses stale/health/allowlist/duplicate resolution. Add a short diagram and a backend-contract table. Place wire format, IAM and migration under `docs/`, not above the getting-started path.

## 10. Decisions to make explicitly

There are a few honest product choices for which static review cannot establish one universal answer:

| Question | Recommended default | Tradeoff to document |
|---|---|---|
| Should missing GET make the whole snapshot error or incomplete? | Bounded retry, then **incomplete** | Simplicity of error-only path vs useful partial diagnostics |
| Should invalid individual endpoint records block a whole service? | Report by service and make **consumer policy** choose | Fail-open versus fail-closed use cases |
| Should the library retain old snapshots? | **No**, leave to consumers; provide a well-documented sample acceptor | A built-in cache of last accepted state starts to own policy |
| Keep a marker object? | Optional, **yes** for low-latency use; direct periodic LIST still supported | Extra key and complexity versus fewer routine LISTs |
| Is LastModified liveness mandatory? | **Yes for current layout**, but expose `Unknown` explicitly | Portability to backends with weaker metadata |
| Should storage keys be renamed? | **Not within v1** | A v2 migration has material operational cost |
| Should the project have a built-in freshness policy? | **No**; optional policy package/examples | More control for callers, less out-of-box convenience |
| Which main name? | `blobdisco` (distinctive), or `serviceindex` (conservative) | Brand tone and package availability |
| Should native S3 events be tested now? | **No**—not used by the present implementation | Add when there is an optional actual event adapter |

## 11. Outside references checked

These external references support the **S3 behavior and test-emulator discussion only**. All file-specific findings above come from the supplied archive, not from online code about a different project.

1. AWS, [Amazon S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html): strong read-after-write consistency for successful PUT/DELETE and LIST, single-key atomic update behavior. Accessed 2026-11-15.
2. AWS, [S3 performance guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html): baseline 3,500 write/s and 5,500 GET/HEAD/s per partitioned prefix. Accessed 2026-11-15.
3. AWS, [S3 event notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html): destinations and at-least-once, potentially out-of-order delivery. Accessed 2026-11-15.
4. [shyim/local-s3 README](https://github.com/shyim/local-s3): image/port/credentials/path-style and local development scope. Accessed 2026-11-15; feature list does not advertise event-notification delivery.
5. [Adobe S3Mock operation support](https://github.com/adobe/S3Mock): lists `PutBucketNotificationConfiguration` as unsupported. Accessed 2026-11-15.
6. [LocalStack S3 docs](https://docs.localstack.cloud/aws/services/s3/): S3 API emulation and path-style configuration; use only if an S3-specific optional notification adapter is developed. Accessed 2026-11-15.
7. AWS, [S3 lifecycle expiration considerations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html): lifecycle expiration is asynchronous and versioning changes deletion semantics. Accessed 2026-11-15.

## 12. Files delivered and verification status

- `registry-review.md` — this review, with source locations, naming proposal, storage contract and ordered work plan.
- `registry-testing-scaffold.tar.gz` — additive `testing/` directory containing Docker Compose, a local integration runner and a README.
- **Inspected:** the module's source, its `wirev1` corpus, unit tests, integration tests and design/TODO docs.
- **Not executed:** `go test` (toolchain mismatch and unavailable network), Docker Compose (Docker unavailable), AWS smoke tests (no credentials/bucket requested). Therefore do not treat timing, compatibility or pass-rate claims as verified by execution.

**Recommended next move:** land the test harness and four correctness fixes first; then cut a named release with a storage-neutral contract and renamed endpoint/owner/service API. The fundamental architectural choice—current state in reliable object storage, polled by independent readers—is a coherent one. The payoff comes from making **what a snapshot guarantees** and **what the user must decide** explicit.
