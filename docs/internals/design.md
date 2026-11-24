# Endpoint Roster: Design

## Goal

Maintain a regional roster of service endpoints (individually addressable network locations grouped into named services, or pools) in an S3 bucket, with any number of publishers and consumers.

The core case is a pool of independent members. Each member is a process that serves the pool, and it publishes only itself: it registers its own endpoints when it starts, refreshes them while it runs, marks them TERMINATING when it begins to shut down, and deregisters on exit. Members never coordinate, never write each other's records and never read the roster. A member joining or leaving is one write of one small object, at any time, from the member itself. Nothing funnels writes through a central publisher.

A second case is an aggregating publisher: one program that observes many instances and publishes all of them, such as a Kubernetes controller publishing the ingress Pods of a cluster. It is supported, and it is described under "Aggregating publishers" below, but it is not the model the roster is built around.

Consumers (for example a regional controller maintaining shard fanout endpoints) read snapshots and follow change hints. Their state machines are out of scope, and there is no coordination or drain acknowledgement between publishers and consumers.

## Scope decisions

These keep the design small. Revisit only if the stated assumption breaks.

- **Each publisher registers only itself.** The owner ID is the publishing process: an instance ID, a task ID, a pod UID, unique to the running process and stable for its life. A member owns exactly one object per pool, `v1/<service>/<owner>`, and knows its key without reading. There is no write fencing. Two processes that share an owner ID in one pool overwrite each other, which is a configuration error the library cannot detect without a read.
- **Latency and uptime are not critical.** Seconds of delay and short publisher outages are acceptable. A member that cannot write just ages until it can.
- **Dead members go stale, then expire.** A member rewrites its object every refresh interval, and a reader takes the write time as proof of life. A reader drops a stale owner after a timeout, and a bucket lifecycle rule removes objects that have not been rewritten for days. A crashed member cannot announce its departure, so its record is served until it is stale. That latency is the reader's stale timeout, and it is the price of having no coordinator.
- **TERMINATING is published.** It is a best effort signal, not a drain command. The roster itself never acknowledges consumption. A caller that needs confirmation gets it from a mirror layer in the consuming repository, which reads the roster and reports what its target reflects. The roster's semantics are unchanged by that.
- **The library does not interpret `serving` or health.** A member records what it observes and leaves meaning to consumers.

## Architecture

```text
Pool "web", N independent members, each its own process and owner ID:

  member i-1            member i-2            member i-3
  Registration          Registration          Registration
      |                     |                     |
  PUT v1/web/i-1        PUT v1/web/i-2        DELETE v1/web/i-3      (register, refresh, state, deregister)
      \                    |                    /
       +---- PUT marker after a content change -+
                            |
         Any reader: polls the marker, lists the objects,
         fetches only those whose ETag changed, drops stale owners
```

A member's life:

```text
start ---- Register ----> registered (READY or NOT_READY)
                              |   ^
                     Refresh  |   |  SetState (health change)
                   (interval) v   |
                          registered
                              |
              shutdown: SetState(TERMINATING), drain grace
                              |
                          Deregister ----> gone (object deleted, marker updated)

crash at any point: no more refreshes; readers drop the owner once it is
older than their stale timeout; the lifecycle rule deletes the object days later
```

None of these steps reads the roster, so a member's cost is one PUT per refresh, one PUT per state change and one DELETE on exit, whatever the size of the roster. The design and the reasons for it are in `design-s3-hints.md`, and the request model is in `../scaling.md`.

## Code layout and reuse

```text
(root package)      Public library. Endpoint, Registration, Publisher, the store and reader interfaces, Snapshot.
wirev1/             The JSON object a publisher stores, with its encoder, decoder and frozen corpus.
s3store/            The only code that knows the bucket layout: Store, Reader, Marker, MarkerPoller.
policy/             Opt in consumer policy: PreferEndpoint, StaleFilter, FilterStaleOwners.
rostertest/         In-memory Store, Reader and HintSource test doubles.
```

Publishers and consumers live in the repositories that use this module. They import the root package and `s3store`, and nothing else.

Readers use the same library: `Reader.Snapshot` for a complete read and `HintSource.Subscribe` for advisory changes, so a consumer can implement its own source interfaces without knowing the layout or the format. A snapshot carries a revision (a counter that increases exactly when the content changes) and is marked incomplete if any object could not be read, so a consumer that treats absence as deletion keeps its last good state instead.

A pool member needs only the root package and `s3store`: make a `roster.Registration` for its owner ID and pool, `Register` its endpoints at start, `Refresh` on a timer of its own, `SetState` on health changes and at shutdown, and `Deregister` on exit. It never sees a key or a JSON field. The safety rules live in `Registration`, so every member gets them: only its own object is written or deleted, invalid endpoints are rejected before any write, and nothing is read, so no failed read can turn into a delete. The library runs no timer.

An aggregating publisher uses `roster.Publisher` instead: build the complete `[]roster.Endpoint` for its owner, call `Reconcile` at startup and periodically, and `Publish` on changes. Its rules are the same plus the ones a read brings: only its own owner's objects are read, a failed roster read never causes deletes, and a reconcile rewrites every service so the owner does not look stale. See `../s3-layout.md` for the bucket layout.

## Data contract: one record per ingress endpoint

| Field | Purpose |
|---|---|
| `owner` | The writer. For a pool member, the member process itself. One path segment of the key of every object the owner writes. Not stored in the object. |
| `endpointId` | Derived as `<instanceId>:<port>`. Unique within an owner, never set by a publisher. |
| `service` | The publisher's name for the pool this endpoint belongs to (a shard, a service). Never a consumer's local name: consumers map it to theirs. The other path segment of the key. Not stored in the object. |
| `logicalName` | The identity a consumer hashes on, chosen by the publisher. Affinity is available on this value but not required: a replacement instance may reuse it (affinity) or it may change with every instance (none). Several records can share one (a draining instance and its successor). `roster.policy.PreferEndpoint` is the shared rule for picking one. |
| `labels` | Optional small string map a consumer can use to decide what an endpoint means to it. See below. |
| `instanceId` | One running instance. Unique and never reused, so old and new replicas are distinguishable during rollouts. |
| `protocol` | `http`, `https` or `h2c`. Whether to use TLS and which HTTP version. See below. |
| `sni` | DNS name for TLS (SNI and certificate verification). Required for `https`, optional otherwise. Not identity. |
| `address`, `port` | Individually routable ingress endpoint. `address` is always an IP. Connections go to the IP and verify TLS against `sni`, so the regional controller does no DNS. |
| `region`, `zone` | Region and endpoint zone, when available. Separate fields. |
| `state` | `NOT_READY`, `READY`, or `TERMINATING` |
| `serving` | EndpointSlice serving condition, informational |

`ABSENT` is never stored. It means the record is not in its service's object, or the object is deleted.

Protocol values:

| Value | TLS | HTTP version |
|---|---|---|
| `http` | no | 1.1 |
| `https` | yes | negotiated by ALPN |
| `h2c` | no | 2, by prior knowledge. Without TLS there is no ALPN, so the reader must be told. |

A value states a fact about the endpoint, and a consumer applies its own policy. `https` means any version the endpoint offers, so a consumer should offer both h2 and HTTP/1.1 by ALPN and use what the server picks. Only `h2c` pins a version, because plaintext HTTP/2 cannot be discovered. There is no value that requires HTTP/2 over TLS: a server that serves h2 by ALPN is reached with `https`, and one that serves h2 without ALPN is not supported.

The protocol is per port, read from the Service port `appProtocol` (copied into the EndpointSlice port). The accepted values are `http`, `https` and `kubernetes.io/h2c`. A port with no `appProtocol` uses a configured default, `https`. An unrecognized `appProtocol`, or a TLS port with no SNI, skips the whole service and leaves its published records untouched, because a wrong guess would break every connection and a configuration glitch must not empty a service.

Labels are opt in. The publisher copies the Service labels whose key starts with a configured prefix (default `ingress.davidbirdsong.io/label.`), with the prefix removed, onto every endpoint of that Service. Only labels meant for consumers should carry the prefix, because any change to a published label rewrites the service's object and marks the marker. Labels follow Kubernetes label syntax with a limit of 20, so any Service label fits, and an invalid one skips and protects the service instead of failing every publish. Choosing what an endpoint means to a consumer, for example mapping a service and labels to a local pool, is the consumer's job: the roster carries the facts and takes no side.

Records must not contain timestamps or other fields that change without a change in the source. An unchanged service encodes to identical bytes, so a refresh or a reconcile's rewrite changes nothing a reader can see except the object's write time, which is the proof of life. It does not touch the marker, so readers fetch nothing.

## Aggregating publishers

The rest of the model below the data contract describes an aggregating publisher, using the Kubernetes one as the worked case. It publishes the per instance inventory of an EKS cluster (many shards backed by 1 to M ingress controller Pods) under one owner ID, the cluster. It holds a Kubernetes Lease (client-go `leaderelection`) so one replica publishes at a time. If two replicas briefly overlap, they derive the same state from the same cluster and their writes are idempotent. Kubernetes is authoritative for what should be published, and the roster for what has been published.

An aggregator reads back its own owner's objects on every reconcile, and because the owner is the last segment of the key that read lists the whole of `v1/`. That is acceptable for tens of aggregators and is the reason a process that publishes only itself must use a `Registration`, which never reads.

### Kubernetes input

Observe EndpointSlices for the ingress-controller Service (not downstream application Services) and join Pods where needed. Relevant fields:

- Endpoint address, port, target reference (Pod name and UID), zone.
- EndpointSlice `ready`, `serving`, and `terminating` conditions. All are nullable.
- Pod `metadata.deletionTimestamp`.
- The Service label that gives the service, the Service annotation that gives the SNI, and the Service labels to publish as endpoint labels. The pod name becomes `logicalName` and the pod UID becomes `instanceId`.

Verify that the regional controller can address each ingress Pod independently before publishing Pod IPs.

### Per endpoint state machine

```text
ABSENT -- endpoint appears --> NOT_READY -- ready --> READY
                                 ^                     |
                                 +------ unready ------+
                                                       |
                        Pod deletion / terminating signal
                                                       v
                                                  TERMINATING
                                                       |
                                                 endpoint gone
                                                       v
                                                    ABSENT
```

Any present state can go directly to `ABSENT` after abrupt loss. Termination can start from `NOT_READY`. Precedence:

1. Not in the selected Service's endpoint inventory: `ABSENT`.
2. Endpoint `terminating` or Pod deletion timestamp set: `TERMINATING`.
3. Endpoint ready: `READY`.
4. Otherwise: `NOT_READY`.

Handle nullable conditions and `publishNotReadyAddresses` explicitly. A terminating endpoint can have `ready=false` and `serving=true`, so do not treat that as ordinary unready.

### Reconciliation

The publisher is a watch-driven controller with a rate-limited work queue. One state derivation and diff engine serves every trigger.

| Trigger | Scope | Default |
|---|---|---|
| Informer event | Affected endpoint or service | Brief debounce, retried by the work queue on failure |
| Startup | Complete owned inventory | Run before reporting ready |
| Periodic reconcile | Complete owned inventory | About every 10 minutes with jitter (5 to 15 acceptable) |

Informers already handle watch reconnects and relists, and the work queue handles retries after failed writes. The periodic reconcile is a safety net for what they do not cover: a roster item that disappeared, a stale item, or a reconciliation bug that skipped an endpoint. It is not the primary discovery path.

Full reconcile steps:

1. Confirm the informer caches report `HasSynced`. If not, skip the reconcile.
2. Read all objects for this `owner`, following pagination. S3 reads and lists are strongly consistent.
3. Derive the desired inventory from the caches and compare it with what is stored, service by service.
4. Write the services that changed, delete services that are no longer desired, and rewrite every unchanged service with identical bytes. A service the publisher could not derive is protected: its stored content is rewritten as it was.
5. A reader reads the oldest write time among the owner's objects as the owner's last reconcile, so a reconcile that could not rewrite a service leaves the owner looking stale.

The reconcile reads the synchronized informer cache, not a fresh API relist. Add an explicit relist only if the failure model later requires it.

Safety invariants:

- Never delete based on unsynced caches, a failed read, or an incomplete page sequence.
- Never touch another owner's items. Every query and write is scoped by `owner`.
- Run the reconcile and event reconciliation through the same worker so they are serialized.
- An unchanged reconcile changes no bytes and does not touch the marker.

## Interfaces (illustrative Go)

```go
type EndpointID string // <instanceId>:<port>, derived by Endpoint.ID(), unique within an owner

type Endpoint struct {
    OwnerID     string
    Service     string
    LogicalName string
    InstanceID  string
    Address     string
    Port        uint16
    Protocol    Protocol // http, https or h2c
    SNI         string
    Region      string
    Zone        string
    State       State
    Serving     bool
    Labels      map[string]string // compare with Equal, not ==
}

// The write boundary: one object per service for an owner, never read.
type EndpointWriter interface {
    PutService(ctx context.Context, ownerID, service string, endpoints []Endpoint) error
    DeleteService(ctx context.Context, ownerID, service string) error
}

// A pool member's own entry. Nothing here reads the roster.
func NewRegistration(w EndpointWriter, ownerID, service string, opts ...RegistrationOption) (*Registration, error)
func (r *Registration) Register(ctx context.Context, endpoints []Endpoint, opts ...CallOption) error
func (r *Registration) SetState(ctx context.Context, s State, opts ...CallOption) error
func (r *Registration) Refresh(ctx context.Context, opts ...CallOption) error
func (r *Registration) Deregister(ctx context.Context, opts ...CallOption) error

// Told what each write call stored, after the write (design-notifier.md).
type Notifier interface {
    Notify(ctx context.Context, n Notification, want Ack) Receipt
}

// An aggregator's storage boundary adds the read back of its own objects.
type DocumentStore interface {
    EndpointWriter
    ListOwned(ctx context.Context, ownerID string) ([]OwnedService, error)
}

// Pure logic in a Kubernetes aggregator. Kubernetes shaped observations are intentionally preserved.
func BuildDesiredInventory(snapshot KubernetesSnapshot) map[EndpointID]Endpoint
```

Keep Kubernetes and S3 I/O behind adapters. Do not build a generic multi-platform discovery framework. The reducer stays aware of Kubernetes lifecycle semantics.

## Change delivery

There is no stream, queue or fan out. Change detection is hints that point at state:

- After it changes an object, a publisher rewrites one tiny object, the marker, with content that is different every time. Several publishers marking at once overwrite each other harmlessly, because any write changes the marker's ETag.
- A reader polls the marker every couple of seconds (one request). When its ETag changes, the reader lists the objects and fetches only those whose ETag changed. It also reads a snapshot on a timer, which repairs any mark that was lost. Consumers can come and go freely, because nothing is created per consumer.
- A hint carries no state, so a late, duplicate or lost hint cannot corrupt anything, and no ordering protocol is needed. S3 reads are strongly consistent, so a read after the mark sees the write that came before it.
- A snapshot's revision is a counter that belongs to the reader. It increases exactly when the content changes, because the controller's snapshot acceptor treats an equal revision as a duplicate and a lower one as stale.
- The cost is latency of about one poll interval. A hint over a push transport (MQTT) is a possible later addition, see `design-s3-hints.md`.
- The regional controller independently reads snapshots and reconciles its own fanout state. That is a second anti-entropy boundary beyond Kubernetes to roster.
- `TERMINATING` is advance notice only. Abrupt removal remains possible, and a reader that polls every two seconds can miss a `TERMINATING` that lasted less than that. It sees the endpoint disappear instead.

## Tests

A `Registration` is covered against `rostertest.Store` (every call, every failure, the rule that nothing is written after `Deregister`) and against `s3fake` as a pool of members that join, refresh, drain, leave and crash, with a reader following them and the stale owner policy dropping the crashed one. That test also checks that a member makes no LIST, GET or HEAD.

For an aggregating publisher, the reducer is pure, so cover its lifecycle cases as table-driven unit tests: scale-up, readiness flap, graceful and abrupt removal, `InstanceID` replacement, duplicate or reshuffled endpoints, and address, port, or Zone changes.

Integration tests focus on the periodic reconcile:

| Case | Expected outcome |
|---|---|
| Roster is missing a desired endpoint | Reconcile recreates it |
| Roster holds an endpoint this owner no longer has | Reconcile deletes it |
| Roster already matches Kubernetes | Every service is rewritten with identical bytes, and no marker marks |

Also verify that an unsynced cache or a failed or partial roster read causes no deletes.

## Open decisions

1. Which Service, Deployment, and Pods belong to each service, and how that is labeled.
2. Whether routing uses Pod IPs or another individually reachable endpoint, and what `region` and `zone` represent.
3. Whether `service` spans owners (so one service covers every owner that serves it), and whether instances can move between services. `logicalName` is not unique by design, and a consumer that needs a name unique across owners combines it with the owner ID.
4. When an endpoint counts as `READY` under `publishNotReadyAddresses`.
5. Reconcile interval, termination grace expectations, and the orphaned-owner expiry policy (a reader's stale owner timeout, and the bucket lifecycle rule for objects nobody rewrites).
6. Where `sni` comes from (EndpointSlice endpoint `hostname`, a Pod or Service annotation, or derived from the service). Working assumption: one certificate per service (possibly a wildcard certificate), so the name is shared across the service and can be read once from the ingress Service rather than joined per Pod. The published name must be concrete (for example `ingress.shard-a.example.com`, which a `*.shard-a.example.com` certificate covers), because SNI cannot carry a wildcard and the roster rejects one. Still to confirm: which concrete name the operator publishes, and whether the reader should instead choose the SNI itself. The name is only needed for TLS protocols (`https`). When it is missing for a TLS port, the publisher skips and protects the service and logs the problem, since an unverifiable TLS endpoint is not usable.
7. The stale owner timeout in readers must be longer than the slowest writer's refresh interval (a pool member refreshes every 30 seconds or so, the Kubernetes publisher reconciles about every 10 minutes), because the write time of an owner's objects is its only liveness signal. A consumer that reads a pool of members and also an aggregator needs a timeout that suits the slower of the two, or a timeout per service. A `policy.StaleFilter` also takes a confirmation time, which must be longer than that same interval, so that members which have not refreshed yet after an outage are not taken for lost. The bucket lifecycle rule that expires objects nobody rewrites is set in days, as a backstop for publishers that are gone for good.
