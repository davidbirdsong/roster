# Scaling the roster

How many readers and publishers turn into how many S3 requests, and which term breaks first.

The point is not the absolute numbers, which depend on pricing and on your intervals, but which term grows with what. Everything here is derived from the code and not measured against real S3. Where a number is also in another document, this file is the one that owns it.

## What generates a request

| You do this | Requests |
|---|---|
| Follow a `MarkerPoller` | one HEAD of the marker per poll interval |
| `Reader.Snapshot` | a LIST to find the wire versions, a LIST of each version present (one request per 1000 objects), and a GET for each object whose ETag changed since that `Reader`'s last snapshot |
| `Registration` `Register`, `Refresh`, or a `SetState` that changes something | one PUT |
| `Registration` `Deregister` | one DELETE |
| `Publisher` `Publish` | one PUT per service changed and one DELETE per service removed |
| `Publisher` `Reconcile` | a LIST of the whole version, a GET of every object the owner has, a PUT of every service, and a DELETE per service removed |
| Any of those writes, with a `Marker` attached | one more PUT if the call changed or deleted something, and none for a refresh |
| `s3store.Verify` | one LIST |

A pool member using a `Registration` makes no LIST and no GET, except the one LIST if it calls `Verify`, so its cost does not depend on the size of the roster.

Two parts of `Reconcile` are easy to underestimate. Its LIST covers the **whole roster**, because the owner is the last segment of the key and S3 cannot list by it, so a publisher's reconcile cost scales with everyone's objects. And its GETs are never cached, so a publisher that reconciles every 30 seconds reads its own objects 2,880 times a day.

## The variables

| Symbol | Meaning |
|---|---|
| `R` | readers (consumers) |
| `P` | poll interval, `WithPollInterval`, default 2s |
| `W` | publishers. With pool members, one per member, so `W = K` |
| `A` | refresh interval of a member, or reconcile interval of an aggregator |
| `G` | services per publisher, so documents that publisher owns |
| `K` | total documents in the roster, which is `W * G` when every publisher owns a disjoint set |
| `F` | the interval at which a reader reads a snapshot regardless of hints |
| `C` | content changing writes per second, fleet wide |

`A` and `F` are the caller's loops. The library ships no timer: it offers `Refresh`, `Reconcile`, `Publish` and `Snapshot`, and the program decides when to call them. `P` is the only interval the library owns.

## The formulas

```text
marker HEAD/s = R / P
snapshot/s    = R * ( min(C, 1/P) + 1/F )
LIST/s        = snapshot/s * ( 1 + ceil(K / 1000) )
GET/s         = snapshot/s * documents changed since that reader's last snapshot
marker PUT/s  = at most C (one per writer call with a change, however many services it wrote)
pool members  = per member 1/A PUT/s, nothing else
              = fleet wide K/A PUT/s, no LIST, no GET
aggregator    = per publisher G/A PUT/s and G/A GET/s, plus ceil(K/1000)/A LIST/s
              = fleet wide K/A PUT/s and K/A GET/s, plus W * ceil(K/1000) / A LIST/s
cold start    = K GETs per reader, DefaultGetConcurrency (16) in flight
fleet restart = R * K GETs
```

The `min(C, 1/P)` is the term worth understanding, and the one a naive reading gets wrong. A reader learns of a change by noticing the marker's ETag moved, and it polls only every `P`, so **it cannot be made to read more than once per interval** however fast publishers write. Marks collapse, which is correct because a read after the last mark sees everything before it. So the fan out of a change burst is bounded by `R / P`, exactly like the HEAD rate, and not by `R * C`.

The `1 + ceil(K/1000)` is one delimited LIST of the prefix to find the majors, which is a single page unless you have more than 1000 wire majors, plus one flat LIST of each major present, paged at 1000 keys. With only `v1` in the bucket that is `1 + ceil(K/1000)`.

A snapshot GETs only what changed, because the reader caches by ETag. This depends on an identical rewrite keeping its ETag, which holds with SSE-S3 and not with SSE-KMS. Under KMS every reconcile rewrite changes every ETag and every reader refetches everything, so `GET/s` becomes `snapshot/s * K`. See [s3-layout.md](s3-layout.md).

## Worked example: a pool of members

`R` = 80, `P` = 2s, `K` = `W` = 2,000 members across all pools, `A` = 30s, `F` = 120s. A rolling deploy touches a member every second or two, so `C` is about 1/P.

```text
marker HEAD/s = 80 / 2              = 40/s
snapshot/s    = 80 * (0.5 + 1/120)  = 40.7/s     during a rollout
LIST/s        = 40.7 * 3            = 122/s      the reader side term, as below
GET/s         = 40.7 * 1            = 41/s
member PUT    = 2,000 / 30          = 67/s       fleet wide, and the only publisher cost
member LIST   = 0
member GET    = 0
```

The same 2,000 owners driven through `Publisher.Reconcile` would add `2,000 * 2 / 30 = 133` LIST/s and 67 uncached GET/s, and the LIST term grows with `K` squared. That is why a process that publishes itself uses a `Registration`. With members, `K` grows with the number of processes, so the reader side LIST fan out below is the term to watch.

## Worked example: aggregators

`R` = 80, `P` = 2s, `K` = 1000, `W` = 80, `G` = 12.5 average, `A` = 30s, `F` = 120s.

```text
marker HEAD/s = 80 / 2            = 40/s        3.46M/day
snapshot/s    = 80 * (0.5 + 1/120) = 40.7/s     at a sustained change rate
LIST/s        = 40.7 * 2          = 81/s
GET/s         = 40.7 * 1          = 41/s        one document changed per snapshot
publisher PUT = 1000 / 30         = 33/s        fleet wide
publisher GET = 1000 / 30         = 33/s        fleet wide, uncached
publisher LIST= 80 * 1 / 30       = 2.7/s
fleet restart = 80 * 1000         = 80,000 GETs
```

Quiet state, with no content changing anywhere, is `80/120 = 0.67` snapshots a second, so about 1.3 LIST/s and no GETs, plus the 40 HEAD/s and the publisher's 33 PUT and 33 GET. Call it 110 requests a second for the whole system.

## Worked example: ten times bigger

`R` = 800, `K` = 10,000, `W` = 800, everything else the same.

```text
marker HEAD/s = 800 / 2             = 400/s      34.6M/day
snapshot/s    = 800 * (0.5 + 1/120) = 407/s
LIST/s        = 407 * 11            = 4,473/s    <-- the term that breaks
GET/s         = 407 * 1             = 407/s
publisher PUT = 10,000 / 30         = 333/s
publisher GET = 10,000 / 30         = 333/s
publisher LIST= 800 * 10 / 30       = 267/s
fleet restart = 800 * 10,000        = 8,000,000 GETs
```

Every term grew by ten except `LIST/s`, which grew by 55, because it is the only one that multiplies two variables that both grew: the snapshot rate scales with `R` and the pages per snapshot scale with `K`.

## Where the wall is

Not where the per prefix limits are. S3 allows 5,500 GET and HEAD requests a second and 3,500 write requests a second per partitioned prefix, and S3 splits prefixes automatically as request rates grow, so the document keys are not permanently one prefix. Three other things bite first.

**The LIST fan out, `R / P * ceil(K/1000)`.** This is the first wall and the ten times example hits it. A LIST request is also several times the price of a GET and returns up to 1000 keys whether or not anything changed, so it is the term to watch in a bill as well as in a rate limit. It is also the term with the most headroom available, because `F` and a consumer side debounce both cut the snapshot rate directly and neither changes correctness.

**The cold start burst, `R * K`.** 80,000 GETs at the current target and 8,000,000 at ten times. `WithGetConcurrency` bounds one reader to 16 in flight, so a fleet of `R` readers offers `16 * R` concurrent GETs: 1,280 at the current target, which at a 20ms GET is an offered rate of about 64,000 a second, already past the per prefix limit. S3 answers with 503 SlowDown and the SDK retries with backoff, so a simultaneous fleet restart is slower than it looks rather than broken. Stagger restarts, or serve the last known state from disk while the first snapshot builds. Keeping that state is up to the consumer.

**The marker, which is one key and cannot be partitioned.** All `R / P` HEADs land on it, so the ceiling is `R / P < 5,500`, which is 11,000 readers at a 2 second interval. That is 137 times the current target and 13 times the ten times case, so it is the most comfortable of the three despite being the only truly unsplittable thing here. Relieving it would need per service markers, which is not offered, because readers poll markers and a marker per service multiplies every reader's polling by the number of services it watches.

## Knobs, by leverage

1. **`F`, and a debounce in the consumer.** Cuts `snapshot/s`, which is the multiplier on both `LIST/s` and `GET/s`. A debounce of one second caps a reader at one snapshot a second no matter what the marker does. Costs hint latency only.
2. **Jitter.** Already applied to the poll wait of a `MarkerPoller`, which spreads each wait over `[d, d + d/4)`. It does not change the mean rate, only the synchronized peak, which is what trips 503 SlowDown after a deploy or a shared outage. Apply the same to your own `F` and `A` loops; the library cannot do it for you.
3. **`P`.** Trades hint latency against `R / P` directly, and that ratio sets both the HEAD rate and the ceiling on the fan out.
4. **`A`.** Cuts publisher PUT, GET and LIST together. It is bounded below by the staleness threshold a consumer uses: liveness is the oldest `LastModified` of an owner's documents, so `A` must stay comfortably under the threshold or healthy owners look stale.
5. **`K`.** Fewer, larger documents cut the LIST pages, which cuts the term that breaks first. The unit is one (service, owner), so with pool members `K` is the number of member processes, and it is set by the deployment rather than by how services are split.
6. **`WithGetConcurrency`.** Bounds one reader's burst, not the fleet's. Useful for one reader's own latency and memory, close to useless as a fleet wide control.

## Rules of thumb

- One reader costs `1/P` HEAD a second, so 0.5/s at the default, plus `1 + ceil(K/1000)` LIST requests per snapshot it takes.
- One content change costs at most `R` snapshots, delivered within `P`, and no more than that however many changes land in the same interval.
- One fleet restart costs `R * K` GETs.
- One `Refresh`, `SetState` or `Deregister` costs one request, whatever the size of the roster.
- One `Reconcile` costs the whole roster in LIST pages, not just that publisher's share, plus one uncached GET per document it owns.
- A quiet system is not free: `R / P` HEAD and `R / F` snapshots continue with nothing changing.

## Measured versus derived

Every number here is derived from the requests above and the stated intervals. Nothing is measured against real S3, so check current pricing and limits. Two assumptions are worth confirming before trusting a sizing based on this file:

1. That an identical rewrite keeps its ETag, which the whole `GET/s` term depends on. True with SSE-S3, false with SSE-KMS.
2. That a LIST page is 1000 keys. It is the S3 default, and `s3store` does not set `MaxKeys` except in `Verify`.
