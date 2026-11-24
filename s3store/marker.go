package s3store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/davidbirdsong/roster"
)

// Marker marks the doorbell: it rewrites one tiny object with content that is different every time, so its ETag
// changes and any reader polling it knows to look. It carries no state, and several publishers marking at once
// overwrite each other harmlessly, because any write changes the ETag.
//
// Marker implements roster.Notifier, so a writer marks by attaching it as a hint notifier. Update marks by hand.
type Marker struct {
	api API
	cfg Config
	id  string
}

var _ roster.Notifier = (*Marker)(nil)

// NewMarker returns a Marker for the roster in cfg. id says who marks (an owner ID), which helps a person
// reading the object, and nothing depends on it.
func NewMarker(api API, cfg Config, id string) (*Marker, error) {
	if api == nil {
		return nil, fmt.Errorf("s3store: an API is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("s3store: %w", err)
	}
	return &Marker{api: api, cfg: cfg, id: id}, nil
}

// Update writes the marker object.
func (b *Marker) Update(ctx context.Context) error {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("s3store: mark: %w", err)
	}
	body := fmt.Sprintf("%s %d %s\n", b.id, time.Now().UnixNano(), hex.EncodeToString(nonce[:]))
	if _, err := b.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.cfg.Bucket), Key: aws.String(b.cfg.MarkerKey()), Body: bytes.NewReader([]byte(body)), ContentType: aws.String("text/plain"),
	}); err != nil {
		return fmt.Errorf("s3store: mark %s: %w", b.cfg.MarkerKey(), err)
	}
	return nil
}

// Notify marks once when n holds a put or a delete, and does nothing for a notification that is all
// WriteUnchanged, so a refresh leaves readers alone. It reaches AckAccepted when the marker PUT returns,
// whatever want is: the PUT is the hand off, and nothing further can be known.
func (b *Marker) Notify(ctx context.Context, n roster.Notification, _ roster.Ack) roster.Receipt {
	if !slices.ContainsFunc(n.Writes, func(w roster.ServiceWrite) bool { return w.Kind != roster.WriteUnchanged }) {
		return roster.Receipt{}
	}
	if err := b.Update(ctx); err != nil {
		return roster.Receipt{Err: err}
	}
	return roster.Receipt{Reached: roster.AckAccepted}
}
