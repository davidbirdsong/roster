// Package s3fake is an in memory implementation of the S3 calls s3store makes, for tests. It behaves the way S3
// does where the roster depends on it: reads and lists are strongly consistent, an ETag is the MD5 of the
// content, and writing identical bytes again still moves LastModified.
package s3fake

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type object struct {
	data     []byte
	etag     string
	modified time.Time
}

// Fake is a single bucket. The zero value is not usable, use New.
type Fake struct {
	mu       sync.Mutex
	objs     map[string]*object
	now      func() time.Time
	pageSize int
	fault    func(op, key string) error
	calls    map[string]int
}

// New returns an empty bucket whose clock is time.Now.
func New() *Fake {
	return &Fake{objs: map[string]*object{}, now: time.Now, pageSize: 1000, calls: map[string]int{}}
}

// SetClock sets the clock that stamps LastModified.
func (f *Fake) SetClock(now func() time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = now
}

// SetPageSize sets how many keys a LIST returns before it needs a continuation token.
func (f *Fake) SetPageSize(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pageSize = n
}

// SetFault makes every call ask fn first, with the operation name (ListObjectsV2, GetObject, HeadObject,
// PutObject, DeleteObject) and the key or prefix. A non nil error is returned instead of doing the call.
func (f *Fake) SetFault(fn func(op, key string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault = fn
}

// Calls returns how many times an operation was called, including calls that failed.
func (f *Fake) Calls(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[op]
}

// Put stores an object directly, as a writer outside the code under test would.
func (f *Fake) Put(key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.put(key, data)
}

// Get returns an object's bytes.
func (f *Fake) Get(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs[key]
	if !ok {
		return nil, false
	}
	return bytes.Clone(o.data), true
}

// Keys returns every key, sorted.
func (f *Fake) Keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k := range f.objs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Modified returns an object's LastModified.
func (f *Fake) Modified(key string) (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs[key]
	if !ok {
		return time.Time{}, false
	}
	return o.modified, true
}

func (f *Fake) put(key string, data []byte) *object {
	sum := md5.Sum(data)
	o := &object{data: bytes.Clone(data), etag: `"` + hex.EncodeToString(sum[:]) + `"`, modified: f.now()}
	f.objs[key] = o
	return o
}

// begin records the call and returns an injected fault, if any. The caller holds f.mu.
func (f *Fake) begin(op, key string) error {
	f.calls[op]++
	if f.fault != nil {
		return f.fault(op, key)
	}
	return nil
}

func (f *Fake) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := aws.ToString(in.Prefix)
	if err := f.begin("ListObjectsV2", prefix); err != nil {
		return nil, err
	}
	delimiter := aws.ToString(in.Delimiter)
	after := ""
	if in.ContinuationToken != nil {
		after = aws.ToString(in.ContinuationToken)
	}
	limit := f.pageSize
	if in.MaxKeys != nil && int(*in.MaxKeys) < limit {
		limit = int(*in.MaxKeys)
	}

	keys := make([]string, 0, len(f.objs))
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	// Page size counts contents and common prefixes together, as S3 counts both against MaxKeys. Counting
	// only contents would make a delimited listing unable to truncate at all, so the paging of the top level
	// prefix listing could not be tested.
	//
	// The continuation token is the point to resume after, which is this fake's own convention: real tokens
	// are opaque, so nothing in the library may read it. For a common prefix the resume point is the prefix
	// followed by 0xFF, so the next page starts past every key underneath it and no prefix is returned twice.
	out := &s3.ListObjectsV2Output{}
	seenPrefix := map[string]bool{}
	count, resume, truncated := 0, "", false
	for _, k := range keys {
		var isPrefix bool
		var item, next string
		if delimiter != "" {
			if i := strings.Index(k[len(prefix):], delimiter); i >= 0 {
				item = k[:len(prefix)+i+len(delimiter)]
				if seenPrefix[item] {
					continue
				}
				isPrefix, next = true, item+"\xff"
			}
		}
		if !isPrefix {
			item, next = k, k
		}
		if count == limit {
			truncated = true
			break
		}
		if isPrefix {
			seenPrefix[item] = true
			out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: aws.String(item)})
		} else {
			o := f.objs[k]
			out.Contents = append(out.Contents, types.Object{Key: aws.String(k), ETag: aws.String(o.etag), LastModified: aws.Time(o.modified), Size: aws.Int64(int64(len(o.data)))})
		}
		count++
		resume = next
	}
	// A truncated page with nothing on it would hand back an empty token, which is what a real store must
	// never do. Only a zero page size can cause it, so refuse that rather than emit a listing the library is
	// right to reject.
	if truncated && resume == "" {
		return nil, fmt.Errorf("s3fake: page size %d cannot make progress", limit)
	}
	if truncated {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(resume)
	}
	return out, nil
}

func (f *Fake) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(in.Key)
	if err := f.begin("GetObject", key); err != nil {
		return nil, err
	}
	o, ok := f.objs[key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(o.data)), ETag: aws.String(o.etag), LastModified: aws.Time(o.modified), ContentLength: aws.Int64(int64(len(o.data)))}, nil
}

func (f *Fake) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(in.Key)
	if err := f.begin("HeadObject", key); err != nil {
		return nil, err
	}
	o, ok := f.objs[key]
	if !ok {
		return nil, &types.NotFound{}
	}
	return &s3.HeadObjectOutput{ETag: aws.String(o.etag), LastModified: aws.Time(o.modified), ContentLength: aws.Int64(int64(len(o.data)))}, nil
}

func (f *Fake) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(in.Key)
	if err := f.begin("PutObject", key); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, fmt.Errorf("s3fake: read body: %w", err)
	}
	o := f.put(key, data)
	return &s3.PutObjectOutput{ETag: aws.String(o.etag)}, nil
}

func (f *Fake) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(in.Key)
	if err := f.begin("DeleteObject", key); err != nil {
		return nil, err
	}
	delete(f.objs, key)
	return &s3.DeleteObjectOutput{}, nil
}
