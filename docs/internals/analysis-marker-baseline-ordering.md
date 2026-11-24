# The marker baseline ordering fix

A walkthrough of one correctness fix in `s3store/poller.go`: why the poller could silently lose a mark, why the obvious fix was the wrong one, and what the ordering argument actually rests on. Written because the reasoning is not obvious from the diff, and because the fix widened the `HintSource` contract in a way consumers need to know about.

Settles `TODO.md` item 2.

## What the poller does

`MarkerPoller` tells a consumer "something moved, go read a snapshot." It learns that by issuing a HEAD against one object, `<prefix>marker`, and watching its ETag. A publisher rewrites that object with different content after every content changing write, so a new ETag means something, somewhere, changed. The hint names nothing, so the consumer's answer is always to read the whole roster, which is cheap because the reader refetches only documents whose ETag moved.

## The bug

The old loop:

```go
func (f *MarkerPoller) run(ctx context.Context, out chan<- roster.Hint) {
	defer close(out)

	if !send(ctx, out, roster.Hint{Kind: roster.HintRescan}) {   // (1) tell the consumer to read
		return
	}
	var (
		last   string
		seeded = false // the first poll after a rescan only records a baseline
		failed = false
		delay  = f.retryMin
	)
	for ctx.Err() == nil {
		etag, err := f.poll(ctx)                                 // (2) HEAD
		if err != nil {
			...
			failed, seeded = true, false
			...
			continue
		}
		delay = f.retryMin

		if failed {
			failed = false
			if !send(ctx, out, rescan) { return }
		}
		if seeded && etag != last {
			if !send(ctx, out, rescan) { return }
		}
		last, seeded = etag, true                                // (3) baseline, silently
		if !sleep(ctx, f.interval) { return }
	}
}
```

`seeded` is the defect. The first successful HEAD recorded `last` and emitted nothing, on the reasoning that the consumer was already told to read at step (1), so whatever the marker says at step (2) is already accounted for.

That reasoning is wrong, because (1) and (2) are not the same instant.

| Time | Event |
|---|---|
| T0 | rescan sent, consumer receives it |
| T1 | consumer LISTs, sees state `S1` |
| T1.5 | a publisher writes a `TERMINATING` endpoint and marks, so the ETag becomes `E1` |
| T2 | the poller's first HEAD reads `E1`, sets `last = E1`, `seeded = true`, and **emits nothing** |
| T3 and after | every poll sees `E1 == last`, so the poller stays silent |

The write at T1.5 is now invisible. It landed after the consumer's listing, so it is not in `S1`, and it landed before the baseline, so it has been declared already known. Nothing recovers it except the consumer's own periodic full read, which defaults to 2 minutes with 20% jitter in the consuming proxy. That is a drain signal delayed by about 2.4 minutes, against a design goal of a second or two.

The window is milliseconds wide per subscribe, which is why it survived this long. It also reopened after every recovery from a failed poll, which is worse, because an outage is exactly when marks pile up.

## Why the obvious fix was wrong

The obvious fix, and the one the plan called for, is to take the baseline HEAD before emitting the initial rescan. That does close the window. It also broke three tests, including this one:

```text
--- FAIL: TestAFailedPollIsRetriedAndFollowedByAResync (3.00s)
```

If the first rescan waits on a successful HEAD, then a marker that is missing, throttled, or permission denied means the consumer is never told to read at all.

The marker is advisory. A snapshot read is a LIST plus GETs and never touches the marker, so a consumer can serve the roster perfectly well while the marker is unreachable. Gating the first read on it trades a millisecond wide correctness window for a total availability failure. That is a bad trade, and it is the kind of thing a plan written ahead of the code does not catch.

## The fix

Two rescans per subscription. `s3store/poller.go:106`:

```go
func (f *MarkerPoller) run(ctx context.Context, out chan<- roster.Hint) {
	defer close(out)

	// Tell the consumer to read before polling anything. The marker is advisory and the snapshot read does
	// not depend on it, so a marker that is missing or unreachable must never hold up a consumer's first
	// read.
	if !send(ctx, out, roster.Hint{Kind: roster.HintRescan}) {         // line 112
		return
	}

	var (
		last         string
		haveBaseline bool
		delay        = f.retryMin
	)
	for ctx.Err() == nil {
		etag, err := f.poll(ctx)                                       // line 122
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			f.onError(fmt.Errorf("poll %s: %w", f.cfg.MarkerKey(), err))
			// Marks may land while polling is failing, so the baseline is unknown again and the
			// recovery is treated like a fresh start.
			haveBaseline = false                                       // line 130
			if !sleep(ctx, jittered(delay)) {
				return
			}
			delay = min(delay*2, f.retryMax)
			continue
		}
		delay = f.retryMin

		switch {
		case !haveBaseline:                                            // line 140
			last, haveBaseline = etag, true
			if !send(ctx, out, roster.Hint{Kind: roster.HintRescan}) { // line 152
				return
			}
		case etag != last:
			last = etag
			if !send(ctx, out, roster.Hint{Kind: roster.HintRescan}) {
				return
			}
		}

		if !sleep(ctx, jittered(f.interval)) {
			return
		}
	}
}
```

Three changes, in order of how much they matter.

**`seeded` became `haveBaseline`, and its branch now emits.** This is the whole fix. Where the old code recorded the baseline and stayed quiet, line 140 records it and sends.

**The send at line 112 is unconditional and happens before any HEAD.** An unreachable marker now costs hint latency and never a consumer's first read.

**`haveBaseline = false` at line 130.** A failed poll discards the baseline, so a recovery runs the same path as a fresh start: baseline first, then the rescan that covers it. The old code needed a separate `failed` flag for this and got it subtly differently, treating recovery as a rescan followed by a silent reseed, which is the same bug again.

## Why this closes the window

The channel is unbuffered, so `send` returns only once the consumer has received the value. That is what makes the ordering provable rather than merely likely:

```text
HEAD completes (line 122)  ->  send returns (line 152)  ->  consumer starts listing
```

The consumer's listing therefore starts strictly after the baseline HEAD. So for any write:

- **Before the HEAD.** It is visible to a listing that starts after the HEAD. Seen.
- **After the HEAD.** It moved the ETag, so a later poll takes `case etag != last`. Seen.

There is no third case. Compare the old code, where the consumer's listing started strictly before the baseline HEAD, which is exactly how the T1.5 write fell through.

The unbuffered channel is load bearing here. If anyone ever buffers it for throughput, this argument stops holding and the window comes back, because `send` would then return before the consumer had the hint.

## What it costs

One redundant read per subscribe and per recovery. A write that lands between the baseline and the consumer's listing produces a rescan the consumer did not strictly need, because its listing already saw that write.

That read is cheap: the reader caches by ETag and refetches only changed documents, so a redundant snapshot is two LIST requests and no GETs. A redundant read is always safer than a missed one, which is the trade this whole design makes repeatedly.

## The contract change

`MarkerPoller` now emits more hints than there were changes, which the `HintSource` interface did not promise. The interface in `reader.go` says so explicitly:

> A hint is advisory in both directions: a source may deliver more hints than there were changes, and a consumer must treat a redundant read as normal rather than as a signal. Sources use that latitude to avoid missing a change, which is the only failure that matters here.

This is worth checking on the consumer side. A consumer that counts rescans, or treats one as proof that something changed, is broken by this. It was already broken, because marks coalesce and any source may duplicate, but the breakage used to be rare and now happens on every subscribe.

`rostertest.Hints` still emits exactly one rescan, which is the contract minimum, so the double does not hide the problem by accident. Its doc comment now says to send an extra `HintRescan` to exercise that path.

## Tests

`TestTheBaselineIsTakenBeforeTheRescanThatCoversIt` asserts the invariant directly, counting HEADs through the fake's fault hook:

```go
require.Equal(t, roster.HintRescan, next(t, ch).Kind)
require.Equal(t, roster.HintRescan, next(t, ch).Kind)
require.NotZero(t, heads.Load(), "the second rescan follows a poll, it is not emitted before one")
```

A blocking barrier would be the tighter test: hold the first HEAD, land a mark, release, and assert the mark is not lost. It deadlocks. `s3fake` calls its fault hook while holding its own mutex, so a `mark()` from the test cannot proceed while a hook is blocked. Worth knowing before extending that double.

`TestARecoveryRebaselinesBeforeItsRescan` covers the recovery path, including a mark that lands while polling is down. `TestTheFirstRescanDoesNotWaitForTheMarker` pins the availability property, so that nobody later "fixes" this into the version that blocks the first read.

Reverting the fix to the silent baseline fails all three plus the subscription shape test:

```text
--- FAIL: TestASubscriptionStartsWithARescanAndThenAnUnchangedMarkerSaysNothing
--- FAIL: TestTheBaselineIsTakenBeforeTheRescanThatCoversIt
--- FAIL: TestARecoveryRebaselinesBeforeItsRescan
```
