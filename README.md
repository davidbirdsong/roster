# roster

A roster of service endpoints: individually addressable network locations (an IP address, a port, a protocol and a state) grouped into named services. Any number of publishers write to it and any number of consumers read from it, through an S3 bucket. A publisher writes one small JSON object for each service it serves, and consumers poll one tiny object that publishers rewrite after a change, so there is no stream, queue or fan out to run.

This is a Go module, `github.com/davidbirdsong/roster`, and it holds only the library and its docs. Publishers and consumers live in the repositories that use it.

## Who this is for

You have services that need to find each other by address, and the usual answers do not quite fit. A load balancer in front of everything adds a hop you do not want. Consul would do the job, but someone has to run its servers, keep quorum, upgrade it and answer the page. So you look at what is already there and think: each process could just write its address to S3 (or DynamoDB), and everyone else could read it.

That is a sound idea, and it is the idea this library is built on. The first version takes an afternoon. The rest of the work shows up later, usually during a deploy or an outage. Roster is the rest of that work, as a library, with no servers of its own to run.

### What the afternoon version leaves out

| What happens | What roster does |
|---|---|
| Two writers update one shared file and one of them loses. | Each (service, owner) pair has its own object. Members never write each other's records. |
| A process crashes and its address stays forever. | `Refresh` is a heartbeat, every snapshot carries each owner's `LastRefreshAt`, and `policy.StaleFilter` drops owners that stopped. A bucket lifecycle rule clears up later. |
| Heartbeats look like changes, so every reader refetches everything. | A refresh writes identical bytes. With SSE-S3 the ETag does not move, so readers fetch nothing. |
| Noticing changes quickly means listing the bucket constantly. | Readers poll one tiny marker object, then fetch only the objects whose ETag changed. A periodic read repairs a missed mark. |
| A failed or partial read looks like an empty pool, and traffic stops. | A failed read is an error, never an empty snapshot. A snapshot with an unreadable object is marked incomplete, so you can keep your last good state. |
| One bad record breaks every reader. | A record that fails validation is left out and reported, and the rest are served. |
| Writers cannot reach the registry for a while, and live endpoints expire during the outage or as it ends. | `policy.StaleFilter` judges an owner against the rest of its service and waits for confirmation, so owners that stop together are kept and reported. This fails open on purpose, and you can apply a different policy. |
| A replacement starts before the old instance is gone, and they collide on one name. | `InstanceID`, `LogicalName` and `Service` are separate, and `policy.PreferEndpoint` gives every consumer the same answer. |
| A heartbeat races shutdown and puts a dead process back. | After `Deregister`, `Refresh` returns `ErrDeregistered`. |
| The format has to change while old writers are still running. | The wire format is versioned, changes only by addition within a major version, and is pinned by a frozen corpus. |
| Any writer can overwrite anything. | The service and the owner are in the key, so IAM can limit a writer by prefix. |

DynamoDB in place of S3 gives you conditional writes and item expiry, and leaves most of this table for you to write.

Polling is enough for most users: a published change reaches a reader within about one poll interval, 2 seconds by default. If you want planned changes (a new member, a drain, a removal) to arrive sooner, attach an optional `Notifier` that pushes each write over a transport you already have, such as MQTT, with a matching `HintSource` in the consumer. The library ships the interfaces and not the transport. It speeds up what a writer announces, so it does not detect a crash any sooner, and if it is down readers still see the change at their next poll. See [docs/notifiers.md](docs/notifiers.md).

### A worked example: an nginx pool in front of an app server pool

A team runs a pool of nginx servers in front of a pool of app servers. Both pools scale up and down and are redeployed often. Each nginx needs the current list of app servers, and whatever sends traffic to nginx (a load balancer's target list, a DNS updater) needs the current list of nginx servers. Nobody has time to operate a new distributed system.

```text
app server 1..N                       nginx server 1..M
  app + Registration                    nginx + agent (Registration, Reader)
      |                                     |                   ^
      | Register, Refresh,                  | the same calls    | Snapshot of
      | SetState, Deregister                |                   | pool "app"
      v                                     v                   |
  v1/app/<owner>                        v1/nginx/<owner>        |
      |                                     |                   |
      |                                     +--> whatever sends traffic to nginx
      +---------------------------------------------------------+
```

It is one roster in one bucket, with a service for each pool:

- **Pool `app`.** Each app server registers itself with a `Registration`, using its instance ID as the owner ID, and refreshes every 30 seconds or so. It writes one object and never reads the roster.
- **Pool `nginx`.** A small agent beside each nginx does two jobs. It registers that nginx the same way, and it follows the roster: when the `app` endpoints change, it rewrites the upstream list and reloads nginx. Each pool is granted only its own key prefix for writing.

During a deploy, a new app server registers as `NOT_READY` and becomes `READY` once it can serve. The old one calls `SetState(StateTerminating)`, the agents see it within a poll interval or so and stop sending it new requests, and after a drain period it calls `Deregister` and exits. The nginx pool drains in the same way, one hop further out. This is a bounded window for a planned stop. A crash or a lost host skips it, and the record ages out instead. Until then nginx's own failure handling, such as `proxy_next_upstream`, covers the gap.

What the team runs is a bucket, some IAM grants and the agent. There is no quorum and no new data path: if S3 is unavailable, traffic keeps flowing between endpoints nginx already knows, provided the agent leaves its last good upstream list in place. An nginx that starts during the outage needs a saved copy or has to wait.

If a pool cannot register itself, one program can publish it on its behalf with a `Publisher`. See [docs/publishing.md](docs/publishing.md).

### What is still yours

- **The loops.** The library runs no heartbeat or reconcile timer. You choose the intervals, add jitter and handle errors.
- **Freshness and health.** Roster reports when an owner last wrote. How old is too old, and whether an endpoint actually answers, are decided by the consumer.
- **Knowing that a drain is finished.** A stored `TERMINATING` does not prove that a consumer saw it, or that connections have closed. Give it a bounded wait, or attach a `Notifier` that can report evidence.

### When to use something else

- **You need failures noticed in a second or two.** A crash is seen when the heartbeat goes stale, which takes minutes with typical settings. Pair roster with health checks in the consumer, or use a system with active failure detection.
- **You need DNS, locks, leader election or a general key/value store.** Roster has none of them. Consul has most of them.
- **Everything is in one Kubernetes cluster.** EndpointSlices already do this.
- **Your platform's own registry is enough.** On AWS, Cloud Map is worth a look for plain service discovery.
- **You have thousands of readers.** Request cost grows with readers and objects. [docs/scaling.md](docs/scaling.md) shows which term breaks first.

## Publish

```go
import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/s3store"
)

// run registers this process in pool "web" until ctx is cancelled (for example by
// signal.NotifyContext on SIGTERM), then drains and deregisters it.
func run(ctx context.Context) error {
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	client := s3.NewFromConfig(awsCfg)
	loc := s3store.Config{Bucket: "myteam-roster-us-east-1", Writer: "my-service/1.0"}

	// The owner ID is this process: unique to it and stable for its life.
	const self = "i-0a1b2c3d4e5f60718"

	// The marker tells readers to look now. Without it they notice at their next poll.
	marker, err := s3store.NewMarker(client, loc, self)
	if err != nil {
		return err
	}
	store, err := s3store.NewStore(client, loc)
	if err != nil {
		return err
	}
	reg, err := roster.NewRegistration(store, self, "web", roster.WithRegistrationHintNotifier(marker))
	if err != nil {
		return err
	}

	if err := reg.Register(ctx, []roster.Endpoint{{
		LogicalName: self,
		InstanceID:  self,
		Address:     "10.0.4.17",
		Port:        8443,
		Protocol:    roster.ProtocolHTTPS,
		SNI:         "web.example.com",
		Region:      "us-east-1",
		Zone:        "us-east-1a",
		State:       roster.StateReady,
		Serving:     true,
	}}); err != nil {
		return err
	}

	tick := time.NewTicker(30 * time.Second) // well under the readers' stale timeout; jitter it in production
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			_ = reg.Refresh(ctx) // log a failure and keep going: a missed refresh only ages the record
		case <-ctx.Done():
			// ctx is cancelled, so shutdown needs a context of its own.
			stop, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = reg.SetState(stop, roster.StateTerminating) // announce
			time.Sleep(10 * time.Second)                     // drain grace, several reader poll intervals
			return reg.Deregister(stop)                      // leave the pool
		}
	}
}
```

Nothing is created for you: the bucket, its settings and the IAM grants are yours, and [docs/operations.md](docs/operations.md) says what they need to be. [docs/publishing.md](docs/publishing.md) covers the calls, the fields, draining, and publishing on behalf of other instances.

## Consume

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

This is the short form. A real consumer also reads on a timer, keeps its last good state when a read fails or is incomplete, and drops stale owners. [docs/consuming.md](docs/consuming.md) has a complete loop.

## Packages

| Package | Purpose |
|---|---|
| `roster` (the module root) | `Endpoint`, `Registration` for a process that publishes itself, `Publisher` for an aggregator, and the `EndpointWriter` and `DocumentStore` interfaces they write through |
| `wirev1` | The JSON document a publisher stores, with its encoder, decoder and frozen compatibility corpus. Standard library only. |
| `policy` | Opt in consumer policy: `PreferEndpoint`, `StaleFilter` and `FilterStaleOwners`. Nothing here is required, and the core API does not use it. |
| `s3store` | The S3 `Store`, `Reader`, and the marker and `MarkerPoller`. The only code that knows the bucket layout. |
| `s3store/s3fake` | An in-memory S3 for tests |
| `rostertest` | In-memory `DocumentStore`, `Reader` and `HintSource` test doubles, for your unit tests |

## Documentation

| If you want to | Read |
|---|---|
| Register a process, or publish what a controller observes | [publishing.md](docs/publishing.md) |
| Read endpoints and follow changes | [consuming.md](docs/consuming.md) |
| Set up, tune and watch a roster | [operations.md](docs/operations.md) |
| Configure the bucket and IAM | [s3-layout.md](docs/s3-layout.md) |
| Estimate requests and find the limits | [scaling.md](docs/scaling.md) |
| Push changes sooner, or collect evidence of delivery | [notifiers.md](docs/notifiers.md) |
| Test your code without AWS | [testing.md](docs/testing.md) |

You do not need anything else to use the library. [docs/internals](docs/internals/README.md) explains why it is built this way, for people changing it.
