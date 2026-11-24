# Publishing to the roster

For a process that should appear in the roster. The quick start is in the [README](../README.md).

The usual case is a pool: many independent processes serve one service, and each one registers only its own endpoints with a `Registration`. No process writes another's endpoints, and you never see a storage format. A program that publishes many instances it observes, rather than itself (a Kubernetes controller, for example), uses a `Publisher`.

## Configuration

The roster is a bucket. `Bucket` is required, and `KeyPrefix` (for example `roster/`) puts everything under a prefix if the bucket is shared. `Writer` names your publisher and its version in the objects it writes, for people reading them, and must stay the same for the life of the process. The constructors return an error if the configuration is unusable. Nothing is created for you: the bucket, its settings and the IAM grants are yours, and [s3-layout.md](s3-layout.md) says what they need to be.

Each service of each owner is one JSON object, `v1/<service>/<owner>`, holding the complete set of that service's endpoints. An owner ID and a service are each one path segment: neither may contain `/`, a wildcard (`*`, `?`), `$`, whitespace or control characters. Many publishers share a service, each with its own owner ID, so a pool member owns exactly one object and knows its key without reading anything.

## A member's life: `Registration`

None of these calls reads the roster. Each writes or deletes this member's one object, and nothing else.

| Call | Writes | Use it |
|---|---|---|
| `Register(ctx, endpoints, opts...)` | This member's object, replaced with these endpoints | At start, and whenever its endpoints change |
| `SetState(ctx, state, opts...)` | The object, only if the state of some endpoint changed | When health changes, and `StateTerminating` when shutdown begins |
| `Refresh(ctx, opts...)` | The object again, with identical bytes | Every interval, well under the readers' stale timeout |
| `Deregister(ctx, opts...)` | Deletes the object | At shutdown, after the drain |

Each call takes `roster.CallOption` values, of which `roster.Want` is the one that applies here (see [notifiers.md](notifiers.md)).

The owner ID must be unique to the running process and stable for its life: an instance ID, a task ID, a pod UID. Two live processes sharing an owner ID in one pool overwrite each other's object, and the library cannot tell, because it never reads. A process in several pools makes one `Registration` for each. A process with several ports registers one endpoint per port in its one object.

`Refresh` is the member's proof of life. A reader takes the object's write time as the owner's last refresh, so a member that crashes stops refreshing, and a consumer that keeps a `policy.StaleFilter` drops it. The filter takes a stale timeout and a confirmation time. A member is dropped when it is older than the stale timeout, is also that far behind the newest refresh in its service, and stays behind while the service's refreshes move on by the confirmation time. If a whole pool stops refreshing at once, nobody falls behind, so its members are kept and reported with `Dropped` false: that points at something systemic (the pool cannot write, or a clock is wrong) and emptying the pool would be an outage. The confirmation time covers the moment writes resume, when the first member to refresh would make the others look lost, so set it longer than the slowest refresh interval. The rule is per service. `policy.FilterStaleOwners` is the same without state or confirmation, so it does not cover that moment. A refresh changes nothing, so it does not mark the roster and readers fetch nothing for it. The library runs no timer: you choose the interval and add jitter.

What a `Registration` remembers changes only after a write succeeds, so a failed call can be retried. `Deregister` issues the delete even if no `Register` succeeded, and calling it twice is fine. After `Deregister`, `Refresh` and `SetState` return `roster.ErrDeregistered`, so a heartbeat that races shutdown cannot put the object back. `Register` again rejoins the pool. Before the first `Register`, they return `roster.ErrNotRegistered`.

## Aggregating publishers: `Publisher`

A `Publisher` is for a program that publishes many instances it observes, one owner with many services. It reads back what its owner has stored, so its cost grows with the whole roster (see [scaling.md](scaling.md)), and it needs a single writer per owner. A process that publishes only itself should use a `Registration`.

Both of its calls take the complete desired set of endpoints the owner has, not a delta.

| Call | Reads the roster | Writes | Use it |
|---|---|---|---|
| `Reconcile(ctx, endpoints, opts...)` | Yes, everything this owner has stored | Every service: changed ones with the new content, unchanged ones again with identical content | At startup, then periodically (about every 10 minutes with jitter) |
| `Publish(ctx, endpoints, opts...)` | No, compares against what this `Publisher` has already published | Only the services that changed | On every change, to publish quickly and cheaply |

`Publish` returns `roster.ErrNotInitialized` until one `Reconcile` has succeeded, because the `Publisher` does not know what is published before then.

A service is written as a whole, in one write, so a reader never sees half of it. A service that has no endpoints left is deleted. If some writes fail, the call returns an error, the writes that succeeded are kept, and calling again retries only the rest.

Rewriting an unchanged service with identical bytes is deliberate. A reader takes the write time of an object as proof that its publisher is alive, and takes the oldest write time among an owner's services as the owner's last reconcile. So a `Reconcile` that cannot refresh one service leaves the owner looking stale, and a publisher that dies stops refreshing and goes stale. A refresh changes nothing, so it does not touch the marker and readers fetch nothing. A restarted `Publisher` learns what is stored from its first `Reconcile`, so identical content does not mark after a restart either.

## Identity: three fields, three jobs

An endpoint carries three identifiers. They are easy to confuse, and they are not interchangeable.

| Field | Names | Reused? | Used for |
|---|---|---|---|
| `InstanceID` | One running instance | Never | The record's storage key (with the port), and grouping the ports of one instance |
| `LogicalName` | The identity a consumer hashes on, for example as an HAProxy server id | Your choice | Hashing, and affinity if you keep it stable (optional) |
| `Service` | The pool the endpoint belongs to | Constant | Which consumer pool, and `PreserveServices` |

You choose `LogicalName`. Affinity is available on this value but not required. If you can give a replacement instance the name of the one it replaces (a StatefulSet pod name, for example), a consumer that hashes on it keeps its position across the replacement. If you cannot (an ECS task, or a Deployment pod), use whatever identifies the instance. The name then changes with every replacement and consumers get no affinity from it, which is allowed.

The roster stores a record per `InstanceID` and port, so several records can share a `LogicalName`. The usual case is a draining instance and its successor, which both hold the name while the old one finishes. Nothing is overwritten, so a consumer can see both states. A consumer that hashes on `LogicalName` must still pick one, because HAProxy cannot hold two servers under one id. Call `policy.PreferEndpoint(endpoints)`: it keeps one endpoint per owner, service, `LogicalName` and port, preferring `StateReady`, then `StateTerminating`, then `StateNotReady` (a draining instance still carries connections and a starting one carries none), with ties going to the lowest key. Every consumer then picks the same one.

The storage key is derived from `InstanceID` and the port and is never set by a publisher. An endpoint with the same `InstanceID` and port listed twice is rejected.

## Fields that matter

| Field | Rules |
|---|---|
| `OwnerID` | Leave empty. The `Registration` or `Publisher` fills it in. A different owner ID is rejected. |
| `Service` | Your name for the pool this endpoint belongs to (a shard, a service). It is your own identifier and never a consumer's local name: consumers map it to theirs. A `Registration` fills it in and rejects a different one. A `Publisher` requires it. |
| `LogicalName` | The identity a consumer hashes on. Required, but affinity on it is optional. See above. |
| `InstanceID` | Any string unique to the instance and never reused for another: a Kubernetes pod UID, an ECS task ARN, a hostname that is unique per instance. All ports of one instance share it. Required, at most 512 bytes. |
| `Labels` | Optional small string map a consumer can use to decide what an endpoint means to it. At most 20 entries, with Kubernetes label syntax for keys and values (`roster.ValidateLabels` shows the exact limits). Nil and empty are the same. Data, never commands: a consumer must treat them as untrusted text. |
| `Address` | An IP address, never a hostname. Readers connect to the IP. |
| `Protocol` | `ProtocolHTTP` (plaintext HTTP/1.1), `ProtocolHTTPS` (TLS, HTTP version negotiated by ALPN) or `ProtocolH2C` (plaintext HTTP/2). Required. |
| `SNI` | The DNS name sent as the TLS server name and checked against the certificate. Required for `ProtocolHTTPS`, optional for plaintext protocols. It must be a concrete DNS name the certificate covers, never a wildcard pattern or an IP, because SNI cannot carry one. A wildcard certificate is fine: publish a name it matches. It is usually not the instance's own host name. |
| `State` | `StateNotReady`, `StateReady` or `StateTerminating`. Terminating is a best effort signal, not a drain command. |
| `Region` | Optional, for example `us-east-1`. |
| `Zone` | Optional, the full zone name, for example `us-east-1a`. |

Do not add anything to an endpoint that changes without a real change to the endpoint, such as a timestamp. It would change the bytes of the object on every reconcile, so every reconcile would mark the marker and every reader would fetch the object each time.

Compare endpoints with `Endpoint.Equal`, not `==`, because `Labels` is a map.

## Announcing an endpoint going away, then its removal

An endpoint's state is a field, and removal is the object going away. A pool member announces that it is leaving in two writes:

```go
err := reg.SetState(ctx, roster.StateTerminating) // 1. announce: same InstanceID and port, new state
// ... drain: until connections close, or the grace period ends
err = reg.Deregister(ctx)                         // 2. remove: delete this member's object
```

With a `Publisher` the same two steps are two ordinary `Publish` calls: one with the endpoint's state set to `StateTerminating`, then one with the endpoint left out of the desired set.

The states an endpoint can be in, and what a consumer should do with each:

| State | Meaning |
|---|---|
| `StateNotReady` | Present but not serving yet. Send no traffic. |
| `StateReady` | Serving. |
| `StateTerminating` | Going away. Keep existing connections, send no new ones. If `Serving` is false it can no longer take even the existing ones. |
| absent | Gone. The endpoint was removed, and consumers should treat that as immediate. |

Rules for publishers:

- **Keep the `InstanceID` and port the same** across state changes. A consumer then sees one endpoint change state, not one leave and another arrive.
- **Replace with a new `InstanceID`.** To replace an instance, publish the new one first, wait until it is `StateReady`, then mark the old one `StateTerminating`. In a pool this is the orchestrator's rollout order (start the new member, then stop the old one). A `Publisher` gets the ordering for free by publishing the complete desired set each time. If the new instance reuses the old one's `LogicalName`, both records exist for a while and consumers resolve them with `policy.PreferEndpoint`.
- **You own the timing.** The roster has no timer and no expiry on items. How long an endpoint stays `StateTerminating` is your decision, typically until your graceful shutdown finishes or the grace period ends.
- **Hold `StateTerminating` long enough to be seen.** Consumers poll, so a state that lasts less than the poll interval (2 seconds by default) is never observed, and the endpoint simply disappears. If consumers should see it, keep it for several poll intervals before removing it.
- **An abrupt death cannot be announced.** A crashed process or a lost node goes straight from `StateReady` to absent, and consumers must cope with removal without warning. `StateTerminating` is advance notice, not a drain handshake: nothing waits for a consumer to acknowledge it.
- **A crash leaves the record behind until it is stale.** A pool member that dies without deregistering stops refreshing, and consumers drop it by its `LastRefreshAt` (`policy.StaleFilter`). A bucket lifecycle rule removes the object days later. For a `Publisher`, a crash between the two writes is repaired by its next `Reconcile`.

A Kubernetes publisher would publish a Pod with a deletion timestamp, or an endpoint whose terminating condition is set, as `StateTerminating`, and leave the endpoint out once it disappears. Unless it also enforces a minimum visible time, a Pod that exits instantly is never seen draining.

## Validation and rejection

Before touching the roster, each call checks every endpoint with `Endpoint.Validate()`. If any endpoint is invalid, belongs to another owner, or repeats a `InstanceID` and port, the whole call fails and nothing is written or deleted. This is deliberate. A stored item that cannot be decoded later would make every subsequent reconcile of your own objects fail.

If one bad source object should not block the rest, validate and skip it yourself before calling `Reconcile` or `Publish`, and log the problem. A `Registration` writes one member, so an invalid endpoint there is a bug in that member and `Register` fails.

## Protecting a service

This applies to a `Publisher`. If you cannot work out the desired state for a service (a configuration error, for example), do not just omit its endpoints. An omitted endpoint looks like a removed endpoint, and its records would be deleted. Use `PreserveServices` to leave that service's published records untouched:

```go
err := pub.Reconcile(ctx, endpoints, roster.PreserveServices("web-b"))
```

Nothing under a protected service is changed or deleted, including any endpoints you pass for it. A `Reconcile` still rewrites the service with the content it has stored, so the service does not make the owner look stale.

## Safety guarantees

Every `Registration` gets these:

- Only this member's one object is written or deleted. An endpoint naming another owner or service is rejected.
- Nothing is read, so no failed read can become a delete. The only delete is the `Deregister` you ask for.
- Invalid input is rejected before any write.
- Calls on one `Registration` are serialized.

Every `Publisher` gets these:

- Only your owner's records are read or written. Records owned by another owner make `Reconcile` refuse to proceed.
- A failed or partial roster read never causes deletes.
- Invalid input is rejected before any write.
- Calls on one `Publisher` are serialized.

There is no write fencing. Give every pool member its own owner ID, and run exactly one `Publisher` per owner (for example by holding a Kubernetes Lease). Two overlapping publishers that agree on the desired state only produce redundant idempotent writes, but two that disagree will keep overwriting each other.

## Using a different store

`s3store.Store` implements `roster.EndpointWriter` (`PutService`, `DeleteService`), which is all a `Registration` needs, and `roster.DocumentStore`, which adds `ListOwned` for a `Publisher`. If you need another backend, implement them. Every method is scoped to one owner, and `ListOwned` must return an error rather than a partial list.
