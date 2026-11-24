# Request model

Every place the library calls S3, which is the basis for the formulas in [../scaling.md](../scaling.md). Update this file when a call site is added, removed, or made more or less frequent, and then update `../scaling.md` if a user would see the difference.

Nothing outside `s3store` calls S3, because it is the only package that knows the bucket layout. It has nine direct calls. One of them is the shared LIST helper (`listAll`), whose three callers are listed separately, since what controls their frequency is different. Call sites are named by function because line numbers go out of date.

| Call site | Request | Key | What controls the frequency |
|---|---|---|---|
| `MarkerPoller.poll` | HEAD | `<prefix>marker` | one per reader per poll interval |
| `Marker.Update` | PUT | `<prefix>marker` | one per writer call that put or deleted something, from `Marker.Notify` attached as a hint notifier; none for a refresh |
| `Reader.listMajors` | LIST (delimited) | `<prefix>` | one page set per snapshot, to find the wire majors |
| `Reader.listMajor` | LIST (flat) | `<prefix>v1/` | one page set per snapshot per major present |
| `Reader.get` | GET | one document | one per document whose ETag changed since that reader's last snapshot |
| `Reader.get`, retry | GET | one document | only when the first GET said the key was missing |
| `Store.ListOwned` | LIST (flat) | `<prefix>v1/` | one page set per `Reconcile` |
| `Store.get` | GET | one document | one per document the owner owns, per `Reconcile`, uncached |
| `Store.PutService` | PUT | one document | one per `Registration` `Register`, `SetState` that changes something, and `Refresh`; one per service written, per `Publish` or `Reconcile` |
| `Store.DeleteService` | DELETE | one document | one per `Deregister`; one per service a `Publisher` no longer desires |
| `Verify` | LIST | `<prefix>` | once, at startup |

## What the model rests on

- **Marks collapse.** A reader polls no more than once per interval and a read after the last mark sees everything before it, so a burst of writes costs a reader at most one snapshot per interval. See the `MarkerPoller` comment.
- **An identical rewrite keeps its ETag.** `Reader.fetch` depends on it, and it does not hold with SSE-KMS. See `TODO.md` item 4.
- **A LIST page is 1000 keys.** It is the S3 default, and `s3store` does not set `MaxKeys` except in `Verify`.
- **The owner is the last segment of the key.** Owner scoped listing is therefore impossible, which `design-s3-hints.md` chose deliberately. The same document rejects a marker per service, and records the open question on pricing.
