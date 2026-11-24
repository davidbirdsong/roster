# The content hash and field validation fix

A walkthrough of two changes that belong together: bounding and validating the text fields that nothing checked, and making `ContentHash` injective so that it does not depend on that validation holding.

The review treated these as one finding and put the hash first. That is backwards. The hash was the symptom, the unvalidated fields were the defect, and the fix for the hash is the one that matters anyway, because it removes the dependency between them.

## What the hash is for

`ContentHash` is not a checksum of bytes on the wire. It is a hash of the *meaning* of a snapshot: the set of endpoints, independent of the order they were read in. A reader uses it to decide whether anything changed, in `s3store/reader.go:215`:

```go
if r.counter == 0 || snap.ContentHash != r.lastHash {
	r.counter++
	r.lastHash = snap.ContentHash
}
snap.SetRevision(r.counter)
```

So `Revision` moves exactly when the hash moves. The contract a consumer is given, in `reader.go`:

> Generation and Revision are assigned by the reader (see `SetRevision`): a counter that belongs to that reader, increasing exactly when the content of the snapshot changes and equal when it does not.

Two snapshots with the same revision are promised to have the same content. A consumer is entitled to skip work on that basis: no reload, no HAProxy state change, no reconcile pass.

That makes the hash load bearing in one direction only. A hash that changes when nothing changed costs a redundant reload, which is cheap. A hash that **stays the same when something changed** is a lie the consumer acts on, and the change is invisible until something else happens to move the hash. That asymmetry is what makes injectivity worth caring about here.

## The bug

The old implementation joined fields with a NUL:

```go
_, _ = fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s\x00%d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%t\x00",
	b.OwnerID, b.Service, b.LogicalName, b.InstanceID, b.Address, b.Port,
	b.Protocol, b.SNI, b.Region, b.Zone, b.State, b.Serving)
for _, k := range slices.Sorted(maps.Keys(b.Labels)) {
	_, _ = fmt.Fprintf(h, "%s\x02%s\x01", k, b.Labels[k])
}
_, _ = fmt.Fprint(h, "\n")
```

A separator is unambiguous only while no value can contain it. These values could. Two different endpoints then produce identical bytes:

| | `LogicalName` | `InstanceID` | bytes fed to the hash |
|---|---|---|---|
| A | `a\0b` | `c` | `...a\0b` `\0` `c\0...` |
| B | `a` | `b\0c` | `...a` `\0` `b\0c\0...` |

Both are `a\0b\0c\0`. The hash cannot tell them apart, so a publisher replacing A with B moves no revision and every consumer believes nothing happened.

The same collision existed between `Region` and `Zone`, and between a label key and its value through the `\x02` separator.

## Why the fields were the real defect

A NUL in `LogicalName` is not a thing a sensible publisher writes. The reason it was reachable is that nothing checked. `Validate` required `LogicalName` to be non empty and said nothing about its contents, and `Region` and `Zone` were not validated at all. `InstanceID` had a length cap and no charset rule.

`ValidSegment` existed and was strict, but it only applied to `Service` and `OwnerID`, because those become path segments of a storage key and a `/` in them would make the key ambiguous. The fields that are not key segments had no equivalent.

So the entry points were:

- a publisher with a bug, writing a value it built by concatenating something unsanitised
- a hand edited object in the bucket, which is a supported thing to do for recovery
- anything else writing to the same keys

Note the third especially: `wirev1` is a documented format and the bucket layout is documented, so other writers are expected.

## The fix, part one: bound and validate

`ValidText` in `segment.go:30`:

```go
// ValidText reports whether s is usable as a plain text field value: at most max bytes, and free of control
// characters. kind names what s is, for the error.
//
// Control characters are refused for two reasons. They have no meaning in any of these fields, and a NUL in
// particular used to make two different records produce the same input to ContentHash, so a real change could
// look like no change. ContentHash no longer depends on this, because it length prefixes every value, but a
// value that cannot be logged, compared or written into a proxy configuration is still not worth storing.
//
// Unlike ValidSegment this allows "/", ":" and wildcards, because these values never form part of a storage
// key or an IAM pattern and identifiers like an ARN legitimately contain them. An empty s is allowed: a caller
// that requires a value checks for that separately.
func ValidText(kind, s string, max int) error {
	if len(s) > max {
		return fmt.Errorf("%s is %d bytes, at most %d are allowed", kind, len(s), max)
	}
	if i := strings.IndexFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }); i >= 0 {
		return fmt.Errorf("%s %q has a control character (%q) at byte %d", kind, s, s[i], i)
	}
	return nil
}
```

The deliberate choice is how much it does **not** forbid. `ValidSegment` rejects `/`, `*`, `?`, `$` and all whitespace, because those break keys and IAM patterns. Applying that here would reject this, which is a perfectly good instance ID:

```text
arn:aws:ecs:us-east-1:123456789012:task/cluster/0abc
```

The review warned against exactly that, and it was right to. These fields are identifiers chosen by whatever platform the publisher runs on, and the library does not get to dictate their alphabet. What it legitimately refuses is bytes that cannot be logged, compared, or written into a proxy configuration. That is control characters, which includes the NUL, and nothing else.

The caps, `segment.go:14`:

```go
const (
	MaxLogicalNameLen = 512
	MaxRegionLen      = 64
	MaxZoneLen        = 64
)
```

These are generous on purpose. The point is not to pick the correct maximum, which nobody knows, but that every field has **some** bound, so no publisher can write a document that is unbounded in a dimension nobody checked. 512 matches the existing `MaxInstanceIDLen`. Region and zone names are short in every cloud that has them.

Wired into `Validate` at `endpoint.go:220`:

```go
require("LogicalName", b.LogicalName)
if err := ValidText("LogicalName", b.LogicalName, MaxLogicalNameLen); err != nil {
	errs = append(errs, err)
}
require("InstanceID", b.InstanceID)
if err := ValidText("InstanceID", b.InstanceID, MaxInstanceIDLen); err != nil {
	errs = append(errs, err)
}
if err := ValidText("Region", b.Region, MaxRegionLen); err != nil {
	errs = append(errs, err)
}
if err := ValidText("Zone", b.Zone, MaxZoneLen); err != nil {
	errs = append(errs, err)
}
```

`Validate` accumulates into `errs` rather than returning on the first problem, so a caller fixing a record is told everything wrong with it at once. That was already the style; these follow it.

### Both boundaries enforce it

Worth being explicit, because it is the part that makes validation useful rather than decorative. `Validate` runs on the way in and on the way out.

**Writing**, `publisher.go:181`, inside `prepare`, which runs before storage is touched at all:

```go
if err := b.Validate(); err != nil {
	errs = append(errs, fmt.Errorf("endpoint %q: %w", b.ID(), err))
	continue
}
```

**Reading**, `wirev1/wirev1.go:190`, per record during decode:

```go
if err := b.Validate(); err != nil {
	return roster.Endpoint{}, err
}
```

and in `Decode` that failure becomes a `Rejection` rather than killing the document:

```go
b, err := toEndpoint(ownerID, service, w)
...
if err != nil {
	rejected = append(rejected, Rejection{Index: i, InstanceID: w.InstanceID, Err: err})
	continue
}
```

So a hand edited object with a NUL in one record is not a corrupt document. The other records of that document are still served, and the bad one is reported in `Snapshot.Rejected` with its index and instance ID. That is the right granularity: one bad record should not take out a whole service, and it should not be silent either.

## The fix, part two: an injective hash

Validation is a policy. The hash should not depend on a policy holding, because policies get relaxed, bypassed by a direct write, or applied one version behind. `reader.go:132`:

```go
// ContentHash returns a hash of the endpoints, independent of input order.
//
// Every value is written length prefixed rather than joined with a separator
// byte. A separator is only unambiguous while no value can contain it, and
// Revision moves exactly when this hash moves, so an ambiguity here is a real
// change that looks like no change. Length prefixing does not depend on
// Validate refusing any particular byte.
func ContentHash(endpoints []Endpoint) string {
	h := sha256.New()
	for _, b := range sortedEndpoints(endpoints) {
		hashText(h, b.OwnerID, b.Service, b.LogicalName, b.InstanceID, b.Address,
			string(b.Protocol), b.SNI, b.Region, b.Zone, string(b.State))
		// Writes to a hash never fail.
		_, _ = fmt.Fprintf(h, "%d;%t;%d;", b.Port, b.Serving, len(b.Labels))
		for _, k := range slices.Sorted(maps.Keys(b.Labels)) {
			hashText(h, k, b.Labels[k])
		}
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// hashText writes each value as its byte length, a colon, and the bytes, which
// no other sequence of values can reproduce.
func hashText(h io.Writer, values ...string) {
	for _, v := range values {
		_, _ = fmt.Fprintf(h, "%d:%s", len(v), v)
	}
}
```

This is netstring style framing. The encoding of a sequence of values is unambiguous because the length tells a reader exactly how many bytes belong to each value, so no value's content can be mistaken for structure. The collision above becomes:

| | encoding |
|---|---|
| A | `3:a\0b` `1:c` |
| B | `1:a` `3:b\0c` |

Different, and necessarily so, for any pair of distinct field assignments.

Three details in there are deliberate.

**`len(b.Labels)` is in the stream.** Labels are variable in number and come last for a record, so without a count the label pairs of one record could run into the next record's fields. With it, `{"a":"1","b":"2"}` and `{"a":"1b2"}` cannot collide. There is a test for exactly that.

**Numbers do not need framing.** `Port` is a `uint16` and `Serving` is a `bool`, so `%d;%t;` is already unambiguous: neither can contain the delimiter. Framing them would be noise.

**`Protocol` and `State` are converted with `string(...)` and framed.** They are string typed constants today, with a small fixed set of values, but they are still strings and a future value is not guaranteed to be delimiter free. Framing them costs nothing.

### Changing the hash was free, and that is not an accident

The output values changed, which would normally be a compatibility question. It is not one here, for a reason worth recording:

- `ContentHash` is never persisted. It lives in a `Reader`'s memory, in `lastHash`.
- `Revision` is explicitly a counter belonging to one reader, and consumers are told not to compare revisions across readers.
- It is not part of `wirev1`, so the frozen corpus is unaffected.

The one visible effect is that every reader's revision increments once on the first snapshot after upgrading, because `lastHash` starts empty anyway. A consumer sees one redundant reload, which is the cheap direction of the asymmetry at the top of this document.

## Tests

In `reader_test.go`, inside `TestContentHash`:

| Subtest | Asserts |
|---|---|
| `a separator byte inside a value cannot forge another record` | the three collisions, as table driven pairs: `LogicalName`/`InstanceID`, `Region`/`Zone`, label key/value |
| `a label count change is not hidden by the next record` | `{"a":"1","b":"2"}` differs from `{"a":"1b2"}` |
| `field boundaries are not ambiguous` | the pre existing `ab`/`c` against `a`/`bc` case, which the old separator already handled |

That last one is worth noting as a lesson. It looks like it covers this bug and it does not. It only exercises values with **no** separator in them, which is the case a separator handles correctly. Testing the shape of a defect is not the same as testing the defect.

In `endpoint_test.go`:

| Test | Asserts |
|---|---|
| `TestEndpointValidate_TextFieldsAreBoundedAndFreeOfControlCharacters` | NUL, newline and tab rejected in each field, and each cap enforced at one byte over |
| `TestEndpointValidate_TextFieldsAllowWhatAKeySegmentCannot` | an ECS task ARN as `InstanceID` and `shard-a/0` as `LogicalName` both pass |

The second test is the one that keeps the fix honest. Without it, someone tightening `ValidText` toward `ValidSegment` would break real identifiers and no test would notice.

Negative check against the old NUL joined hash:

```text
--- FAIL: TestContentHash/a_separator_byte_inside_a_value_cannot_forge_another_record
        	Messages:   	logicalName and instanceID
```

## What this does not fix

`ContentHash` is SHA-256 truncated to 128 bits, which is a collision resistance question rather than an injectivity one. The input encoding is now injective, so distinct content produces distinct input; whether distinct input can produce the same 128 bit digest is a cryptographic property and 128 bits is ample against accident. It is not a security boundary: a publisher that can write to the bucket can write whatever it likes, so there is nothing to be gained by forging a hash collision that a plain write would achieve directly.
