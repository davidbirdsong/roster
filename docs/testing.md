# Testing code that uses the roster

## Testing your publisher

`rostertest` provides an in-memory store, so you can test without AWS:

```go
store := rostertest.NewStore()
reg, _ := roster.NewRegistration(store, "i-1", "web")
err := reg.Register(ctx, []roster.Endpoint{endpoint})

pub, _ := roster.NewPublisher(store, "c1")
err = pub.Reconcile(ctx, endpoints)

store.Puts           // each service write: owner, service and the endpoints written, in order
store.ServiceDeletes // each service deletion, in order
store.Writes()       // number of writes of any kind
store.Item(id)       // one stored endpoint
```

`rostertest.Notifier` records what a writer notifies, returns the `Receipt` you set, and can be held to test that a slow notifier holds nothing up:

```go
n := rostertest.NewNotifier()
reg, _ = roster.NewRegistration(store, "i-1", "web", roster.WithRegistrationNotifier(n))
n.Receipt = roster.Receipt{Reached: roster.AckReceived}
n.Hold()  // later calls block until n.Release() or their context ends
n.Calls() // each notification and the level asked for, in order
```

Inject failures to test your error handling:

```go
store.ListErr = errors.New("throttled")                                      // roster read fails
store.PutFails = func(owner, service string) bool { return service == "web-b" } // one service's write fails
store.DeleteFails = true
```

Set `store.Now` to stamp the writes, and read the owner's last reconcile back from `store.Snapshot`. The fields are not safe to change while a `Registration` or `Publisher` call is running.

## Testing a consumer

`rostertest.Store` implements `Reader` and `roster.DocumentStore`, and `rostertest.Hints` implements `HintSource`:

```go
store := rostertest.NewStore(endpoints...)
store.SnapshotErr = errors.New("throttled")          // the read fails
store.SkipOnSnapshot = []roster.SkippedItem{{...}}   // makes the snapshot incomplete

hints := rostertest.NewHints()
hints.Send(roster.Hint{Kind: roster.HintRescan})
```

`Hints` emits a `HintRescan` first, like the real one, and its channel closes when the subscribe context ends.

`Store.Snapshot` mints its revision the way a real `Reader` does: it moves only when the
content changes, so rewriting the same endpoints (what a periodic `Reconcile` does) advances
the owner's `LastRefreshAt` without producing a new revision. Code that acts on every new
revision can be tested against the double without seeing a change on every heartbeat.

## Running against an S3 compatible server

The integration tests run the store, reader and poller against a real S3 implementation. The public MinIO images can no longer be pulled, so use adobe/s3mock:

```text
docker run -d --rm --name s3store-it-s3 -p 19000:9090 adobe/s3mock
S3_ENDPOINT=http://localhost:19000 go test -tags integration ./s3store -run Integration -v
docker rm -f s3store-it-s3
```

The mock is not S3. In particular, the roster relies on an identical write moving an object's `LastModified`, and that test should be confirmed against real S3 once.
