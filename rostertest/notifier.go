package rostertest

import (
	"context"
	"sync"

	"github.com/davidbirdsong/roster"
)

// NotifyCall is one call a Notifier received.
type NotifyCall struct {
	Notification roster.Notification
	Want         roster.Ack
	// CtxErr is the context's error when the call arrived, so a test can see that a primary was handed a
	// cancelled context or that a hint was not.
	CtxErr error
}

// Notifier is a roster.Notifier for testing writers: it records every call, returns Receipt, and can be held so
// that calls block until Release, to show that a slow notifier holds up nothing it must not.
//
// To test a notifier of your own, drive it from a real Registration or Publisher over a Store instead.
type Notifier struct {
	// Receipt is returned by every call that is not held.
	Receipt roster.Receipt

	mu    sync.Mutex
	calls []NotifyCall
	hold  chan struct{}
}

var _ roster.Notifier = (*Notifier)(nil)

func NewNotifier() *Notifier { return &Notifier{} }

// Notify records the call, then blocks while the Notifier is held. A held call that ends because its context is
// done returns AckNone with the context's error, as an implementation keeping the contract would.
func (n *Notifier) Notify(ctx context.Context, note roster.Notification, want roster.Ack) roster.Receipt {
	n.mu.Lock()
	n.calls = append(n.calls, NotifyCall{Notification: note, Want: want, CtxErr: ctx.Err()})
	hold, r := n.hold, n.Receipt
	n.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return roster.Receipt{Err: ctx.Err()}
		}
	}
	return r
}

// Hold makes calls block until Release, or until their context is done.
func (n *Notifier) Hold() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.hold == nil {
		n.hold = make(chan struct{})
	}
}

// Release lets every held call, and every later one, return.
func (n *Notifier) Release() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.hold != nil {
		close(n.hold)
		n.hold = nil
	}
}

// Calls returns the calls received so far, in order. A call is recorded on arrival, so a held call is included.
func (n *Notifier) Calls() []NotifyCall {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]NotifyCall, len(n.calls))
	copy(out, n.calls)
	return out
}
