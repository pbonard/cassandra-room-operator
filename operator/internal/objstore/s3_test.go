package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/require"
)

// NewFakeS3 serves an in-memory S3 with a bucket; it returns the store
// options to reach it.
func newFakeS3(t *testing.T, bucket string) S3Options {
	backend := s3mem.New()
	require.NoError(t, backend.CreateBucket(bucket))
	srv := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(srv.Close)
	return S3Options{Bucket: bucket, Prefix: "backups/", Endpoint: srv.URL, PathStyle: true, AccessKey: "a", SecretKey: "b", Region: "us-east-1"}
}

func TestS3(t *testing.T) {
	ctx := context.Background()
	for name, mk := range map[string]func() Store{
		"memory": func() Store { return NewMemory() },
		"s3": func() Store {
			s, err := NewS3(ctx, newFakeS3(t, "b1"))
			require.NoError(t, err)
			return s
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := mk()
			_, err := s.Stat(ctx, "a/x")
			require.True(t, errors.Is(err, ErrNotFound), "%v", err)
			_, err = s.Get(ctx, "a/x")
			require.True(t, errors.Is(err, ErrNotFound), "%v", err)
			data := bytes.Repeat([]byte("0123456789"), 1000)
			require.NoError(t, s.Put(ctx, "a/x", bytes.NewReader(data), int64(len(data))))
			require.NoError(t, s.Put(ctx, "a/y", bytes.NewReader([]byte("y")), 1))
			require.NoError(t, s.Put(ctx, "b/z", bytes.NewReader([]byte("z")), 1))
			o, err := s.Stat(ctx, "a/x")
			require.NoError(t, err)
			require.Equal(t, int64(len(data)), o.Size)
			r, err := s.Get(ctx, "a/x")
			require.NoError(t, err)
			got, _ := io.ReadAll(r)
			r.Close()
			require.Equal(t, data, got)
			list, err := s.List(ctx, "a/")
			require.NoError(t, err)
			require.Len(t, list, 2)
			require.Equal(t, "a/x", list[0].Key)
			require.NoError(t, s.Delete(ctx, "a/x"))
			list, _ = s.List(ctx, "a/")
			require.Len(t, list, 1)
		})
	}
}
