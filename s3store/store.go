package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/wirev1"
)

// Store writes the objects of one publisher. It is not safe to share between publishers of one owner: a
// owner has one writer.
//
// It only stores: it never marks. A Registration or Publisher marks by having a Marker attached as a hint
// notifier, because only the writer knows whether a write changed anything.
type Store struct {
	api API
	cfg Config
	log roster.Logger
}

var (
	_ roster.DocumentStore  = (*Store)(nil)
	_ roster.EndpointWriter = (*Store)(nil)
)

type StoreOption func(*Store)

// WithLogger sets the logger for objects that cannot be decoded. A nil logger is ignored.
func WithLogger(l roster.Logger) StoreOption {
	return func(s *Store) {
		if l != nil {
			s.log = l
		}
	}
}

func NewStore(api API, cfg Config, opts ...StoreOption) (*Store, error) {
	if api == nil {
		return nil, errors.New("s3store: an API is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("s3store: %w", err)
	}
	s := &Store{api: api, cfg: cfg, log: slog.Default()}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// PutService writes the full desired set of one service in one owner, replacing what was there. The endpoints must
// belong to that owner and service. Writing the same endpoints again rewrites identical bytes, which moves the
// object's LastModified (a reader reads that as proof the publisher is alive).
//
// It does not mark. A direct caller's write is seen by readers at their next snapshot, or sooner if it marks with
// Marker.Update. Writes that must reach readers at once go through a Registration or Publisher with the Marker
// attached as a hint notifier.
func (s *Store) PutService(ctx context.Context, ownerID, service string, endpoints []roster.Endpoint) error {
	key, err := s.cfg.DocumentKey(writeMajor, service, ownerID)
	if err != nil {
		return err
	}
	for _, b := range endpoints {
		if b.OwnerID != ownerID || b.Service != service {
			return fmt.Errorf("s3store: endpoint %q is in owner %q service %q, not owner %q service %q",
				b.ID(), b.OwnerID, b.Service, ownerID, service)
		}
	}
	data, err := wirev1.Encode(s.cfg.Writer, endpoints)
	if err != nil {
		return err
	}
	if _, err := s.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key), Body: bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}); err != nil {
		return fmt.Errorf("s3store: put %s: %w", key, err)
	}
	return nil
}

// DeleteService removes the object of one service in one owner. Removing one that is not there is not an error.
// Like PutService, it does not mark.
func (s *Store) DeleteService(ctx context.Context, ownerID, service string) error {
	key, err := s.cfg.DocumentKey(writeMajor, service, ownerID)
	if err != nil {
		return err
	}
	if _, err := s.api.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key)}); err != nil {
		return fmt.Errorf("s3store: delete %s: %w", key, err)
	}
	return nil
}

// ListOwned returns one roster.OwnedService for every object this owner has stored. A failed list or fetch is an
// error, never a partial result, so a publisher that reconciles against it cannot mistake a failed read for an
// empty service and delete.
//
// An object that was fetched but cannot be decoded (hand edited, or damaged) is not a failed read. It is
// returned with its DecodeErr set rather than left out, so the publisher can see that the object exists: it
// repairs the object if the service is still desired, deletes it if the service is not, and reports it if the
// service is protected. Leaving it out instead would strand the object in the bucket forever, where it also
// makes every reader's snapshot incomplete.
func (s *Store) ListOwned(ctx context.Context, ownerID string) ([]roster.OwnedService, error) {
	if err := roster.ValidSegment("owner ID", ownerID); err != nil {
		return nil, err
	}
	objects, _, err := listAll(ctx, s.api, s.cfg, s.cfg.majorPrefix(writeMajor), "")
	if err != nil {
		return nil, err
	}
	var out []roster.OwnedService
	for _, o := range objects {
		_, service, owner, err := s.cfg.parseKey(o.Key)
		if err != nil || owner != ownerID {
			continue
		}
		data, err := s.get(ctx, o.Key)
		if err != nil {
			return nil, err
		}
		bs, _, err := wirev1.Decode(owner, service, data)
		if err != nil {
			s.log.Warn("s3store: an object of this owner cannot be decoded, reporting it for repair or removal",
				"key", o.Key, "err", err)
			out = append(out, roster.OwnedService{Service: service, DecodeErr: err, Key: o.Key})
			continue
		}
		out = append(out, roster.OwnedService{Service: service, Endpoints: bs, Key: o.Key})
	}
	return out, nil
}

func (s *Store) get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.api.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("s3store: get %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3store: read %s: %w", key, err)
	}
	return data, nil
}

// isMissing reports whether err means the object does not exist.
func isMissing(err error) bool {
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	return errors.As(err, &nsk) || errors.As(err, &nf)
}
