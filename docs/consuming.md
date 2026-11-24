# Reading from the roster

Use the read side when you are building a consumer, such as a controller that routes to the published endpoints. It gives you a snapshot and a stream of change hints through two small interfaces, so you can implement your own source interfaces on top without knowing the bucket layout.

```go
type Reader interface {
	Snapshot(ctx context.Context) (Snapshot, error)
}

type HintSource interface {
	Subscribe(ctx context.Context) (<-chan Hint, error)
}
```

## How it works

A reader lists the roster's objects and fetches only those whose ETag changed since its last read, so an unchanged roster costs a list and no fetches. Change hints come from one tiny object, the marker, that a publisher rewrites after it changes anything. A reader polls it (one request) and reads a snapshot when it changes. There is no stream, queue or fan out, so nothing has to be created when consumers come and go, and a reader needs only S3 read access. A change reaches a reader within about one poll interval, 2 seconds by default.

## Quick start

```go
import (
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/davidbirdsong/roster/s3store"
)

client := s3.NewFromConfig(awsCfg)
loc := s3store.Config{Bucket: "myteam-roster-us-east-1"}
reader, err := s3store.NewReader(client, loc)
poller, err := s3store.NewMarkerPoller(client, loc)

snap, err := reader.Snapshot(ctx) // read once at startup

hints, err := poller.Subscribe(ctx)
for range hints {
	snap, err = reader.Snapshot(ctx) // cheap: only objects that changed are fetched
}
```

Use one `Reader` per consumer and keep it, because it caches each object by ETag. A fresh `Reader` fetches everything.

## A consumer loop

A consumer reads on every hint and also on a timer, because hints can be lost and because a crash produces no hint at all. This is the shape of a complete one:

```go
// follow calls apply with the endpoints to route to, until ctx is cancelled.
func follow(ctx context.Context, reader roster.Reader, hints roster.HintSource, apply func([]roster.Endpoint)) error {
	filter := policy.NewStaleFilter(3*time.Minute, time.Minute) // keep it as long as the Reader
	ch, err := hints.Subscribe(ctx)
	if err != nil {
		return err
	}
	tick := time.NewTicker(30 * time.Second) // jitter it in production
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ch:
			if !ok {
				return ctx.Err()
			}
		case <-tick.C:
		}
		snap, err := reader.Snapshot(ctx)
		if err != nil || !snap.Complete {
			continue // log it, and keep the last good endpoints: this is not an empty roster
		}
		kept, stale := filter.Filter(snap)
		_ = stale // report these
		apply(kept) // must be cheap when nothing changed
	}
}
```

- **Do not skip the filter because `Revision` is unchanged.** A crash changes no content, so an owner goes stale without the revision moving. Compare the filtered result instead.
- **Holding everything on an incomplete snapshot is the simplest safe rule.** `Skipped` names the service of each unreadable object, so you can hold only that service and follow the rest.
- **Keep your own last good copy on disk** if a consumer must be able to start while S3 is unavailable.
- **Choose the timings together.** See [operations.md](operations.md).

## Snapshots

`Snapshot` returns every owner's endpoints. You do not need to know owner IDs in advance: the listing is the index.

| Field | Meaning |
|---|---|
| `Endpoints` | All endpoints, sorted by owner ID then endpoint ID. `ByService()` groups them by service. |
| `Revision` | A counter that belongs to the `Reader`, as a decimal string. It increases exactly when the content of the snapshot changes and stays the same when it does not (a refresh is not a change), so a consumer that orders revisions can use it directly. It starts again when the `Reader` is created, so it is not comparable across readers or restarts. |
| `Complete` | False when any object could not be read: one that is not valid JSON, or one under a wire version the reader does not support when no supported object covers the same service and owner. Do not read absence as deletion from an incomplete snapshot, because the skipped object may hold live endpoints. Keep your last good state and mark it stale. |
| `Skipped` | The objects that could not be read, each naming its service, owner and key, with the error. Surface these. The service and owner come from the storage key, so they are known even for an object whose contents are not, which lets you hold back only the affected service instead of your whole view. |
| `Rejected` | Records left out because they are not acceptable: a key that is not `v1/<service>/<owner>`, a record with an unknown state or protocol, or one that fails validation. They do not make the snapshot incomplete, so one bad writer cannot freeze every reader. Surface these. |
| `Owners` | Each owner's `LastRefreshAt`: the oldest write time among the owner's objects. It is a freshness signal. How old is too old is your policy. |
| `ContentHash` | A hash of the endpoints, for diagnostics. Hashes have no order, so do not use it as a revision. |

A failed read returns an error and no snapshot. A snapshot is not a point in time: writes may land while it is being read.

The reader decodes each object with the decoder for its wire version (`wirev1` today). A newer version adds a decoder beside the old one, and the newest supported version wins when a service and owner are stored under several. See [internals/design-s3-hints.md](internals/design-s3-hints.md) for the version rules.

## Changes

`MarkerPoller` emits hints, not events. A hint says something moved, and your response is always to read a snapshot.

| Kind | Meaning |
|---|---|
| `HintRescan` | Something may have changed, or changes may have been missed, so read a fresh snapshot. The first value after every subscribe, every time the marker changes, and again after the poller recovers from a failed poll. |

`HintRescan` is the only kind, and a `Hint` carries no identity. A per owner kind existed once and was removed: no hint source can say which owner changed, because the marker names none.

Hints can be coalesced or delayed, so also read a snapshot on a timer. Set the poll interval with `s3store.WithPollInterval`. Polling cost grows with the number of consumers, at one request per poll, so 80 consumers polling every 2 seconds make about 40 requests a second. Check current S3 pricing. [scaling.md](scaling.md) has the full request model: what each call site costs, how readers and publishers turn into requests, and which term breaks first. The channel is unbuffered, so a slow consumer slows polling and nothing is dropped, and marks that arrive faster than the consumer reads collapse into one.

## Implementing another system's source interfaces

The adapter lives in the consumer, because only the consumer knows its own model. The library hands it everything schema specific. For a consumer with a snapshot port (a complete read with a revision and a completeness flag) and a hint port (advisory invalidations), the mapping is direct:

| Consumer concept | Library |
|---|---|
| Snapshot revision | `Snapshot.Revision` |
| Snapshot completeness | `Snapshot.Complete` |
| Observed time | `Snapshot.ReadAt` |
| Group of endpoints | `Snapshot.ByService()`, then your own mapping from the service and `Labels` to your local service |
| One server per hash identity | `policy.PreferEndpoint`, which picks one endpoint per `LogicalName` |
| Refresh everything | `HintRescan`, the only hint kind |

A sketch of a snapshot adapter, with an illustrative consumer model. Fetches must be serialized per scope, so arrival order is also freshness order:

```go
func (s *Source) Fetch(ctx context.Context, scope ScopeID) (Envelope, error) {
	snap, err := s.reader.Snapshot(ctx)
	if err != nil {
		return Envelope{}, err // an outage is not an empty roster
	}
	var members []Member
	for service, endpoints := range snap.ByService() {
		// One endpoint per LogicalName, so a draining instance and its successor
		// do not both claim the same server id.
		for _, b := range policy.PreferEndpoint(endpoints) {
			switch b.State {
			case roster.StateReady:
				// serve
			case roster.StateTerminating:
				// drain: keep existing connections, send no new ones
			default:
				continue // NOT_READY: leave it out
			}
			members = append(members, Member{Service: myGroupFor(service, endpoints), Name: b.LogicalName, Addr: netip.MustParseAddrPort(
				net.JoinHostPort(b.Address, strconv.Itoa(int(b.Port))))})
		}
	}
	return Envelope{Revision: snap.Revision, Complete: snap.Complete, Payload: members}, nil
}
```

`policy.PreferEndpoint` keeps one endpoint when two share a `LogicalName`, so a draining instance is dropped once its successor is `READY`, and kept while the successor is still `NOT_READY`. If you want to keep serving the drain, work from the unresolved endpoints instead and give the two different server ids yourself.

Mapping states to your own lifecycle is your policy. A reasonable default is: `READY` is serving, `TERMINATING` is draining, and `NOT_READY` is absent. Check `Serving` as well: a terminating endpoint with `Serving` false can no longer take traffic. A reader that polls every 2 seconds can miss a `TERMINATING` shorter than that, and sees the endpoint disappear instead.
