package objstore

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3Options configure an S3 store.
type S3Options struct {
	Bucket, Prefix, Region, Endpoint string
	PathStyle                        bool
	// AccessKey and SecretKey; empty uses the default credential chain.
	AccessKey, SecretKey string
}

// S3 is a Store in an S3 bucket (or a compatible store), under a prefix.
type S3 struct {
	c      *s3.Client
	up     *manager.Uploader
	bucket string
	prefix string
}

// NewS3 connects to a bucket.
func NewS3(ctx context.Context, o S3Options) (*S3, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if o.Region != "" {
		opts = append(opts, awsconfig.WithRegion(o.Region))
	} else {
		opts = append(opts, awsconfig.WithRegion("us-east-1"))
	}
	if o.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(o.AccessKey, o.SecretKey, "")))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	c := s3.NewFromConfig(cfg, func(so *s3.Options) {
		if o.Endpoint != "" {
			so.BaseEndpoint = aws.String(o.Endpoint)
		}
		so.UsePathStyle = o.PathStyle
	})
	prefix := strings.Trim(o.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &S3{c: c, up: manager.NewUploader(c), bucket: o.Bucket, prefix: prefix}, nil
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := s.up.Upload(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key), Body: r})
	return err
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.c.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key)})
	if err != nil {
		return nil, notFound(err)
	}
	return out.Body, nil
}

func (s *S3) Stat(ctx context.Context, key string) (Object, error) {
	out, err := s.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key)})
	if err != nil {
		return Object{}, notFound(err)
	}
	return Object{Key: key, Size: aws.ToInt64(out.ContentLength), LastModified: aws.ToTime(out.LastModified)}, nil
}

func (s *S3) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	p := s3.NewListObjectsV2Paginator(s.c, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: aws.String(s.prefix + prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			out = append(out, Object{Key: strings.TrimPrefix(aws.ToString(o.Key), s.prefix), Size: aws.ToInt64(o.Size),
				LastModified: aws.ToTime(o.LastModified)})
		}
	}
	return out, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	_, err := s.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key)})
	return err
}

// notFound maps S3's missing-object errors to ErrNotFound.
func notFound(err error) error {
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	var re *smithyhttp.ResponseError
	if errors.As(err, &nsk) || errors.As(err, &nf) || errors.As(err, &re) && re.HTTPStatusCode() == 404 {
		return ErrNotFound
	}
	return err
}
