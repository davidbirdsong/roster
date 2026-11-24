package rostertest

import (
	"context"
	"errors"
	"sync"

	"github.com/davidbirdsong/roster"
)

// Hints is a roster.HintSource you drive from a test: Send a HintRescan to
// deliver it. Like the real poller, it emits a HintRescan first, and its
// channel closes when the subscribe context ends.
//
// It emits exactly one rescan before what a test sends, which is the minimum
// the contract allows. MarkerPoller emits two, and a source may emit more, so a
// consumer that only works when each rescan matches a change is broken whatever
// this double does. Send an extra HintRescan to cover that path.
type Hints struct {
	// SubscribeErr makes Subscribe fail.
	SubscribeErr error

	mu sync.Mutex
	in chan roster.Hint
}

var _ roster.HintSource = (*Hints)(nil)

func NewHints() *Hints { return &Hints{in: make(chan roster.Hint, 64)} }

// Send queues a change for the subscriber. It panics if more than 64 are
// queued without being received, which means a test forgot to read.
func (f *Hints) Send(c roster.Hint) {
	select {
	case f.in <- c:
	default:
		panic("rostertest.Hints: buffer full, the test is not receiving")
	}
}

func (f *Hints) Subscribe(ctx context.Context) (<-chan roster.Hint, error) {
	if f.SubscribeErr != nil {
		return nil, f.SubscribeErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.in == nil {
		return nil, errors.New("rostertest: use NewHints")
	}
	out := make(chan roster.Hint)
	go func() {
		defer close(out)
		select {
		case out <- roster.Hint{Kind: roster.HintRescan}:
		case <-ctx.Done():
			return
		}
		for {
			select {
			case c := <-f.in:
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
