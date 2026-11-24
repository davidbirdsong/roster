//go:build integration

// These tests run the real code against a real S3 implementation. The public MinIO images can no longer be
// pulled, so the default is adobe/s3mock. Start it and point the tests at it:
//
//	docker run -d --rm --name s3store-it-s3 -p 19000:9090 adobe/s3mock
//	S3_ENDPOINT=http://localhost:19000 go test -tags integration ./s3store -run Integration -v
//	docker rm -f s3store-it-s3
//
// Any S3 compatible endpoint works. Set S3_ACCESS_KEY and S3_SECRET_KEY for one that checks credentials. Each
// test creates its own bucket. The S3 mock is not S3: confirm the LastModified test against real S3 once.
package s3store_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/davidbirdsong/roster"
	"github.com/davidbirdsong/roster/s3store"
)

func newBucket(t *testing.T) (*s3.Client, s3store.Config) {
	t.Helper()
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set S3_ENDPOINT to run against an S3 compatible endpoint")
	}
	access, secret := os.Getenv("S3_ACCESS_KEY"), os.Getenv("S3_SECRET_KEY")
	if access == "" {
		access, secret = "test", "test-secret"
	}
	client := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: access, SecretAccessKey: secret}, nil
		}),
	})
	bucket := fmt.Sprintf("it-%d", time.Now().UnixNano())
	_, err := client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx := context.Background()
		var token *string
		for {
			out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(bucket), ContinuationToken: token,
			})
			if err != nil {
				return
			}
			for _, o := range out.Contents {
				_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucket), Key: o.Key,
				})
			}
			if !aws.ToBool(out.IsTruncated) {
				break
			}
			token = out.NextContinuationToken
		}
		_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
	return client, s3store.Config{Bucket: bucket, Writer: "it/1"}
}

func TestIntegrationReaderReadsWhatTheStoreWrote(t *testing.T) {
	client, c := newBucket(t)
	require.NoError(t, s3store.Verify(t.Context(), client, c))
	s, err := s3store.NewStore(client, c)
	require.NoError(t, err)
	require.NoError(t, s.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "web", "a", roster.StateReady),
	}))
	require.NoError(t, s.PutService(t.Context(), "c1", "api", []roster.Endpoint{
		be("c1", "api", "b", roster.StateTerminating),
	}))
	require.NoError(t, s.PutService(t.Context(), "c2", "web", []roster.Endpoint{
		be("c2", "web", "c", roster.StateReady),
	}))

	r, err := s3store.NewReader(client, c)
	require.NoError(t, err)
	snap, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Complete)
	require.Len(t, snap.Endpoints, 3)
	require.Len(t, snap.Owners, 2)

	owned, err := s.ListOwned(t.Context(), "c1")
	require.NoError(t, err)
	require.Len(t, owned, 2)

	require.NoError(t, s.DeleteService(t.Context(), "c1", "api"))
	snap, err = r.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.Endpoints, 2)
}

// Liveness is an object's LastModified, so the roster only works if writing identical bytes advances it.
func TestIntegrationAnIdenticalWriteAdvancesLastModified(t *testing.T) {
	client, c := newBucket(t)
	s, err := s3store.NewStore(client, c)
	require.NoError(t, err)
	b := []roster.Endpoint{be("c1", "web", "a", roster.StateReady)}
	r, err := s3store.NewReader(client, c)
	require.NoError(t, err)

	require.NoError(t, s.PutService(t.Context(), "c1", "web", b))
	first, err := r.Snapshot(t.Context())
	require.NoError(t, err)

	time.Sleep(1100 * time.Millisecond) // LastModified has a resolution of a second
	require.NoError(t, s.PutService(t.Context(), "c1", "web", b))
	second, err := r.Snapshot(t.Context())
	require.NoError(t, err)

	require.True(t, second.Owners[0].LastRefreshAt.After(first.Owners[0].LastRefreshAt),
		"an identical write must move LastModified: %v then %v", first.Owners[0].LastRefreshAt, second.Owners[0].LastRefreshAt)
	require.Equal(t, first.Revision, second.Revision, "and it is not a change of content")
}

func TestIntegrationAListingOfMoreThanOnePageIsReadWhole(t *testing.T) {
	client, c := newBucket(t)
	s, err := s3store.NewStore(client, c)
	require.NoError(t, err)
	const services = 1100 // the page size of a real LIST is 1000
	for i := 0; i < services; i++ {
		g := fmt.Sprintf("g%04d", i)
		require.NoError(t, s.PutService(t.Context(), "c1", g, []roster.Endpoint{
			be("c1", g, "a", roster.StateReady),
		}))
	}
	r, err := s3store.NewReader(client, c)
	require.NoError(t, err)
	snap, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.Endpoints, services)

	before := snap.Revision
	again, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, again.Revision)
}

func TestIntegrationAKeyPrefixAndUnknownMajorsAreHandledByTheRealListing(t *testing.T) {
	client, c := newBucket(t)
	c.KeyPrefix = "roster/"
	s, err := s3store.NewStore(client, c)
	require.NoError(t, err)
	require.NoError(t, s.PutService(t.Context(), "c1", "web", []roster.Endpoint{
		be("c1", "web", "a", roster.StateReady),
	}))
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String("roster/v2/api/c9"), Body: strings.NewReader("{}"),
	})
	require.NoError(t, err)

	r, err := s3store.NewReader(client, c)
	require.NoError(t, err)
	snap, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	require.False(t, snap.Complete, "a v2 object with no v1 counterpart")
	require.Len(t, snap.Endpoints, 1)
}

func TestIntegrationThePollerSeesAMarkOnARealBucketIncludingTheFirstMarker(t *testing.T) {
	client, c := newBucket(t)
	poller, err := s3store.NewMarkerPoller(client, c, s3store.WithPollInterval(50*time.Millisecond),
		s3store.WithPollErrorHandler(func(err error) { t.Logf("poller error: %v", err) }))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ch, err := poller.Subscribe(ctx)
	require.NoError(t, err)
	recv := func() roster.Hint {
		select {
		case ch := <-ch:
			return ch
		case <-time.After(10 * time.Second):
			t.Fatal("no change arrived")
			return roster.Hint{}
		}
	}
	require.Equal(t, roster.HintRescan, recv().Kind) // the start, with no marker yet (a HEAD that finds nothing)
	// A mark that lands before the poller's first poll is covered by that first rescan, so let the poller take its
	// baseline of "no marker" before marking.
	time.Sleep(400 * time.Millisecond)

	marker, err := s3store.NewMarker(client, c, "c1")
	require.NoError(t, err)
	require.NoError(t, marker.Update(t.Context()))
	require.Equal(t, roster.HintRescan, recv().Kind, "the first marker")
	require.NoError(t, marker.Update(t.Context()))
	require.Equal(t, roster.HintRescan, recv().Kind,
		"a second mark, with different content")
}

func TestIntegrationPoolMembersRegisterAndDeregisterIndependently(t *testing.T) {
	client, c := newBucket(t)
	var members []*roster.Registration
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("i-%d", i)
		s, err := s3store.NewStore(client, c)
		require.NoError(t, err)
		r, err := roster.NewRegistration(s, id, "web")
		require.NoError(t, err)
		require.NoError(t, r.Register(t.Context(), []roster.Endpoint{be("", "", id, roster.StateReady)}))
		members = append(members, r)
	}
	reader, err := s3store.NewReader(client, c)
	require.NoError(t, err)
	snap, err := reader.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.ByService()["web"], 3)

	require.NoError(t, members[1].SetState(t.Context(), roster.StateTerminating))
	require.NoError(t, members[1].Deregister(t.Context()))
	require.NoError(t, members[1].Deregister(t.Context()), "a second deregister of a missing key succeeds")
	snap, err = reader.Snapshot(t.Context())
	require.NoError(t, err)
	var owners []string
	for _, e := range snap.ByService()["web"] {
		owners = append(owners, e.OwnerID)
	}
	require.Equal(t, []string{"i-1", "i-3"}, owners)
}
