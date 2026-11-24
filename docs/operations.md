# Operating a roster

There is no server to run. What you operate is a bucket, some IAM grants, and the timings of your own loops.

## What you provide

The roster needs a bucket and IAM grants. This module documents what the code needs in [s3-layout.md](s3-layout.md): the key layout, the bucket settings that matter (versioning, lifecycle expiry, public access), and the IAM statements for a pool member, an aggregating publisher and a reader. The code does not create or configure anything.

The service is part of the key, so a publisher's IAM role is limited to its own prefix. The members of pool `web`, whose owner IDs are instance IDs, are granted PUT and DELETE on `v1/web/i-*`, which cannot write a Kubernetes cluster's objects in the same service, and an aggregating publisher is granted `v1/*/<its owner ID>`.

Call `s3store.Verify` at startup. It checks that the bucket can be reached and listed under the prefix, so a wrong bucket or a missing permission stops the start and does not look like an empty roster.

## Setting it up

1. Create a bucket with the settings in [s3-layout.md](s3-layout.md): versioning off, SSE-S3, public access blocked, and a lifecycle rule on `v1/`.
2. Use one key prefix for each kind of writer whose refresh interval differs a lot. Each prefix is a separate roster with its own marker, grants and timings.
3. Choose owner IDs: unique to the running process, stable for its life, with a common start for each kind of writer so that a grant can name it.
4. Grant each role only what [s3-layout.md](s3-layout.md) lists, and call `s3store.Verify` at startup.
5. Choose the timings below, and add jitter to every loop you run.

## Choosing timings

None of these has to be exact, but they depend on each other.

| Timing | Set by | Pool members | Aggregators | Rule |
|---|---|---|---|---|
| Refresh or reconcile | your writer's loop | 30s | 10m | The others follow from it |
| `staleAfter` | `policy.NewStaleFilter` | 3m | 30m | Several refresh intervals, so a few failed writes are not a death |
| `confirmFor` | `policy.NewStaleFilter` | 1m | 15m | Longer than one refresh interval, with jitter and retries |
| Snapshot timer | your consumer's loop | 30s | 2m | Well under `staleAfter`. It notices crashes and repairs lost hints |
| Marker poll | `s3store.WithPollInterval` | 2s | 2s | The delay of an announced change |
| Drain wait | your writer's shutdown | 10s | 10s | Several poll intervals, or consumers may never see `TERMINATING` |
| Lifecycle expiry | the bucket | 3 to 7 days | 3 to 7 days | Cleanup only. Consumers do not wait for it |

These are starting points and have not been measured. A crashed member is dropped about `staleAfter` plus `confirmFor` after its last refresh, rounded up to a snapshot: between four and five minutes with the pool member values. For anything faster, let consumers check health themselves.

## What happens when something fails

| Failure | What consumers see | What to do |
|---|---|---|
| A member crashes | Its endpoints until the `StaleFilter` drops them | Nothing. Use health checks to cover the gap |
| Every member of a pool crashes | The endpoints stay, reported with `Dropped` false | Alert on it. Health checks have to stop the traffic |
| S3 refuses writes | Nothing changes. Owners age together and are kept | Fix the grant or wait. Members recover at their next refresh |
| S3 refuses reads | `Snapshot` returns an error | Keep the last good state. Never treat it as empty |
| An object cannot be decoded | `Complete` is false and `Skipped` names it | Keep the last good state for that service. Its writer repairs it at its next write |
| A record is invalid | It is left out and listed in `Rejected` | Fix the publisher. Nothing else is affected |
| The marker cannot be written or read | Changes arrive at the snapshot timer | Check the marker grant |
| Two processes share an owner ID | Their endpoints replace each other | Fix the owner ID. The library cannot detect it |

## What to watch

The library logs but exports no metrics. These are the ones worth making:

- Failures of `Refresh`, `Reconcile` and `Snapshot`, and the time since each last succeeded.
- `Skipped` and `Rejected` in a snapshot. Both should be empty.
- Owners reported stale, and separately those with `Dropped` false for longer than `staleAfter` plus `confirmFor`.
- Age of the snapshot a consumer is acting on.

For request volume and where the limits are, see [scaling.md](scaling.md).
