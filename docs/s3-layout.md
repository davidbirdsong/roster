# S3 layout and IAM

This is what the code needs from the bucket and from IAM. The code creates and configures nothing, so the bucket, its settings and the grants below are the operator's. The reasons for the design are in [design-s3-hints.md](internals/design-s3-hints.md).

## Layout

```text
<prefix>v1/<service>/<owner>     One JSON object per service for each owner.
<prefix>marker                       One tiny object, rewritten after a change so that readers notice it.
```

`<prefix>` is empty, or a key prefix that ends with `/` and does not start with one (for example `roster/`), so one bucket can hold more than one roster.

- A state object holds the complete set of one service's endpoints for one owner, in the format of `wirev1`. The service and the owner ID are in the key and not in the object. For a pool member the owner is the member itself, so the object holds only that member's endpoints, and the member writes and deletes it without reading anything.
- A key under `v1/` is exactly three segments. The service and the owner ID are each one segment: they are not empty, at most 255 bytes, never `.` or `..`, and contain no `/`, `*`, `?`, `$`, whitespace or control characters. A reader ignores and reports any other key.
- `v1` is the wire version. A breaking change to the format is a new major (`v2/`) written beside the old one, and readers support a window of majors.
- The marker has no meaning in its content, which is different every time it is rewritten. Any publisher may mark it. A forged mark costs readers one extra list, and what they then read is controlled by the grants below.

## One bucket per region

The roster is regional: publishers and consumers of a region use that region's bucket. A bucket name is unique
across AWS, but a bucket lives in one region, so create one bucket in each region and put the region in its name,
for example `myteam-roster-us-east-1`. Reads and writes then stay local, an outage stays
regional, and a role scoped to the bucket is scoped to the region. Nothing in the keys names the region. A key
prefix is for several rosters in one bucket, not for regions. Set the SDK region, with the usual
AWS settings, to the bucket's region.

The layout's `v1/` is added by the library, so a key prefix does not include it. With `KeyPrefix: "roster/"` the
objects are `roster/v1/<service>/<owner>` and the marker is `roster/marker`. The prefix must end in `/` and not
start with one. The IAM examples below assume no prefix, so with one, put it in front of every resource and every
`s3:prefix` condition, including the marker.

## Bucket settings

| Setting | Value | Why |
|---|---|---|
| Block public access | On, all four settings | Objects hold the addresses of endpoints |
| Versioning | Off, or expire noncurrent versions after a day | Every refresh rewrites an object, and each rewrite would otherwise store a new version |
| Default encryption | SSE-S3 (AES256) | An object's ETag is the MD5 of its content, so an identical rewrite keeps the same ETag and readers fetch nothing. With SSE-KMS the ETag is not the MD5, so each rewrite would change it and readers would fetch every object after every reconcile. Confirm this for your configuration before choosing KMS. |
| Lifecycle | Expire objects under `v1/` that have not been modified for 3 to 7 days; abort incomplete multipart uploads | A live publisher rewrites its objects every refresh, so an object that is days old belongs to a publisher that is gone, usually a pool member that crashed without deregistering. Expiry is in whole days. Readers hide stale owners within minutes, using their own stale timeout. |

## Liveness

A pool member rewrites its one object every refresh interval (`Registration.Refresh`), with identical bytes when nothing changed, and an aggregating publisher rewrites all of its objects on every reconcile. A reader takes an object's `LastModified` as proof that its publisher is alive, and an owner's last refresh as the oldest `LastModified` among its objects. The stale owner timeout in a reader must therefore be comfortably longer than the slowest writer's interval: a pool member refreshing every 30 seconds suits a timeout of a few minutes, while an aggregating publisher that reconciles every 10 minutes needs a good deal more.

A member that deregisters deletes its object, so readers see it gone at once. A member that crashes leaves its object until the reader's stale timeout hides it and the lifecycle rule deletes it.

## IAM

Replace `ROSTER_BUCKET` (the region's bucket) and the prefix. The examples use an empty key prefix.

### Pool member

The members of pool `web`, where every owner ID is an EC2 instance ID (`i-...`). Use whatever prefix your owner IDs share (a task ID has none, so a task role is granted `v1/web/*`):

```json
[
  {
    "Effect": "Allow",
    "Action": ["s3:PutObject", "s3:DeleteObject"],
    "Resource": "arn:aws:s3:::ROSTER_BUCKET/v1/web/i-*"
  },
  {
    "Effect": "Allow",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::ROSTER_BUCKET/marker"
  },
  {
    "Effect": "Allow",
    "Action": "s3:ListBucket",
    "Resource": "arn:aws:s3:::ROSTER_BUCKET",
    "Condition": { "StringLike": { "s3:prefix": ["v1/", "v1/*"] } }
  }
]
```

The service is enforced by the path, and the kind of writer by the owner ID prefix. Because instance IDs start with `i-`, this grant cannot write a Kubernetes cluster's objects in the same service, and the aggregating publisher's grant below cannot write an instance's. Choose an ID prefix for each kind of writer and put it in its grant. One limit remains: any member of the pool can overwrite or delete another member's object, because IAM cannot tie an object to one member without per member policies or session tags. The blast radius is one pool.

A `Registration` never reads, so a member needs no `s3:GetObject`. `s3:ListBucket` is only for `s3store.Verify` at startup, and a member that skips the check can drop it. An aggregating `Publisher` in the same position (one owner per instance) would also need `s3:GetObject` on its keys and `s3:ListBucket`, for its reconcile.

### Aggregating publisher

For example a Kubernetes controller that publishes a whole cluster.

One owner ID, many services. For owner `eks-a`:

```json
[
  {
    "Effect": "Allow",
    "Action": ["s3:PutObject", "s3:DeleteObject", "s3:GetObject"],
    "Resource": "arn:aws:s3:::ROSTER_BUCKET/v1/*/eks-a"
  },
  {
    "Effect": "Allow",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::ROSTER_BUCKET/marker"
  },
  {
    "Effect": "Allow",
    "Action": "s3:ListBucket",
    "Resource": "arn:aws:s3:::ROSTER_BUCKET",
    "Condition": { "StringLike": { "s3:prefix": ["v1/", "v1/*"] } }
  }
]
```

`*` in a resource also matches `/`, so `v1/*/eks-a` is any service, then the owner ID `eks-a`. Service names are an operator concern: a Kubernetes cluster may publish into the same service as a fleet.

### Reader

```json
[
  {
    "Effect": "Allow",
    "Action": "s3:GetObject",
    "Resource": ["arn:aws:s3:::ROSTER_BUCKET/v1/*", "arn:aws:s3:::ROSTER_BUCKET/marker"]
  },
  {
    "Effect": "Allow",
    "Action": "s3:ListBucket",
    "Resource": "arn:aws:s3:::ROSTER_BUCKET",
    "Condition": { "StringLike": { "s3:prefix": ["", "v1/", "v1/*"] } }
  }
]
```

The reader lists the root with a delimiter to find the wire versions present, so the empty prefix is part of its grant. `s3.HeadObject` on the marker needs `s3:GetObject`.

### What the grants do not do

Hints are untrusted by design. A forged mark costs readers one extra list, and what they read is controlled by these grants. IAM on S3 cannot look at the content of an object, so a publisher with the right to write `v1/web/i-*` can write anything into that object. The reader's validation (the wire decoder and `Endpoint.Validate`) is what stops a bad record from being served, and a bad record is reported in `Snapshot.Rejected`.

## Checks at startup

`s3store.Verify` lists one key under the prefix. Nothing calls it for you. Run it at start in publishers and readers, so that a wrong bucket or a missing permission stops the start. It needs `s3:ListBucket`, which all three roles above have.
