// Package s3store keeps the roster in an S3 bucket: one JSON object per service in each publisher owner, at
//
//	<prefix>v1/<service>/<owner>
//
// and one tiny object, <prefix>marker, that a publisher rewrites after a change so that readers notice it. See
// docs/internals/design-s3-hints.md for the design and the wirev1 package for the object format.
//
// Store writes, Reader reads, and neither knows more of the other than the key layout and the wire format.
// The package is the only code that knows the bucket layout, so a change of layout is a change here.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/davidbirdsong/roster"
)

// API is the part of the S3 client this package uses. *s3.Client satisfies it, and s3fake.Fake is an in memory
// implementation for tests.
type API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

var _ API = (*s3.Client)(nil)

// Config says where the roster lives.
type Config struct {
	Bucket string
	// KeyPrefix is optional. When set it ends in "/" and does not start with one, for example "roster/", and
	// everything the roster stores is under it.
	KeyPrefix string
	// Writer names the publisher and its version in the objects it writes. It is for diagnostics and must stay
	// the same for the life of the process, because a change of it changes the bytes of every object.
	Writer string
}

// Validate reports what is wrong with the configuration, all of it at once.
func (c Config) Validate() error {
	var errs []error
	if c.Bucket == "" {
		errs = append(errs, errors.New("a bucket is required"))
	}
	if c.KeyPrefix != "" && (!strings.HasSuffix(c.KeyPrefix, "/") || strings.HasPrefix(c.KeyPrefix, "/")) {
		errs = append(errs, fmt.Errorf("key prefix %q must end with / and not start with one", c.KeyPrefix))
	}
	return errors.Join(errs...)
}

// MarkerKey is the key of the object a publisher rewrites after a change.
func (c Config) MarkerKey() string { return c.KeyPrefix + "marker" }

func (c Config) majorPrefix(major string) string { return c.KeyPrefix + major + "/" }

// DocumentKey is the key of the object for one service in one owner, in the current wire version.
func (c Config) DocumentKey(major, service, ownerID string) (string, error) {
	if !majorRe.MatchString(major) {
		return "", fmt.Errorf("%q is not a wire major version", major)
	}
	if err := roster.ValidSegment("service", service); err != nil {
		return "", err
	}
	if err := roster.ValidSegment("owner ID", ownerID); err != nil {
		return "", err
	}
	return c.majorPrefix(major) + service + "/" + ownerID, nil
}

var majorRe = regexp.MustCompile(`^v[0-9]+$`)

// parseKey splits a state key into its parts. A key that is not exactly <prefix><major>/<service>/<owner>
// with valid segments is an error.
func (c Config) parseKey(key string) (major, service, ownerID string, err error) {
	rest, ok := strings.CutPrefix(key, c.KeyPrefix)
	if !ok {
		return "", "", "", fmt.Errorf("key %q is outside the prefix %q", key, c.KeyPrefix)
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("key %q is not <major>/<service>/<owner>", key)
	}
	major, service, ownerID = parts[0], parts[1], parts[2]
	if !majorRe.MatchString(major) {
		return "", "", "", fmt.Errorf("key %q: %q is not a wire major version", key, major)
	}
	if err := roster.ValidSegment("service", service); err != nil {
		return "", "", "", fmt.Errorf("key %q: %w", key, err)
	}
	if err := roster.ValidSegment("owner ID", ownerID); err != nil {
		return "", "", "", fmt.Errorf("key %q: %w", key, err)
	}
	return major, service, ownerID, nil
}

// majorNumber orders wire majors: "v2" is greater than "v1", and "v10" is greater than "v9".
func majorNumber(major string) int {
	n, _ := strconv.Atoi(strings.TrimPrefix(major, "v"))
	return n
}

// Verify checks that the bucket can be reached and listed under the prefix. Run it once at start, so a wrong
// bucket or missing permission is reported at start and not as an empty roster.
func Verify(ctx context.Context, api API, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	_, err := api.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.Bucket),
		Prefix: aws.String(cfg.KeyPrefix), MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("s3store: cannot list s3://%s/%s: %w", cfg.Bucket, cfg.KeyPrefix, err)
	}
	return nil
}

// listed is one object as a LIST reports it.
type listed struct {
	Key      string
	ETag     string
	Modified time.Time
}

// listAll lists every object under prefix, following all pages. A delimiter, when set, also returns the common
// prefixes. It returns an error, never a partial listing.
func listAll(ctx context.Context, api API, cfg Config, prefix, delimiter string) (objects []listed, prefixes []string, err error) {
	var token *string
	for {
		in := &s3.ListObjectsV2Input{
			Bucket: aws.String(cfg.Bucket),
			Prefix: aws.String(prefix), ContinuationToken: token,
		}
		if delimiter != "" {
			in.Delimiter = aws.String(delimiter)
		}
		out, err := api.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, nil, fmt.Errorf("list s3://%s/%s: %w", cfg.Bucket, prefix, err)
		}
		for _, o := range out.Contents {
			objects = append(objects, listed{
				Key:  aws.ToString(o.Key),
				ETag: aws.ToString(o.ETag), Modified: aws.ToTime(o.LastModified),
			})
		}
		for _, p := range out.CommonPrefixes {
			prefixes = append(prefixes, aws.ToString(p.Prefix))
		}
		// Stop only when the store says there is nothing more. A truncated page without a usable token is a
		// store that is not following the protocol, and treating it as the end would silently return a short
		// listing. The publisher reconciles against this listing and the reader builds snapshots from it, so a
		// short listing that looks complete is how a service gets deleted or declared gone by mistake.
		if !aws.ToBool(out.IsTruncated) {
			return objects, prefixes, nil
		}
		next := aws.ToString(out.NextContinuationToken)
		switch {
		case next == "":
			return nil, nil, fmt.Errorf(
				"list s3://%s/%s: truncated listing with no continuation token, refusing to return a partial listing",
				cfg.Bucket, prefix)
		case token != nil && next == aws.ToString(token):
			return nil, nil, fmt.Errorf(
				"list s3://%s/%s: continuation token %q repeated, refusing to loop or return a partial listing",
				cfg.Bucket, prefix, next)
		}
		token = out.NextContinuationToken
	}
}
