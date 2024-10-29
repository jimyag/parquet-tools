package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var (
	_                parquet.ReaderAtSeeker = (*S3Reader)(nil)
	ErrInvalidS3Path                        = errors.New("path is not a valid s3 path")
)

type S3Reader struct {
	client s3iface.S3API
	ctx    context.Context
	bucket string
	key    string
	size   int64
	offset int64
	body   io.ReadCloser
}

func parsePath(path string) (bucket, key string, err error) {
	u, err := url.Parse(path)
	if err != nil {
		return
	}
	if u.Scheme != "s3" && u.Scheme != "s3a" {
		err = ErrInvalidS3Path
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	return
}

type fileInfo struct {
	Name    string
	Size    int64
	ModTime time.Time
	IsDir   bool
}

func head(ctx context.Context, bucket, key string, client s3iface.S3API) (*s3.HeadObjectOutput, error) {
	return client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
}

func stat(ctx context.Context, uri string, client s3iface.S3API) (*fileInfo, error) {
	bucket, key, err := parsePath(uri)
	if err != nil {
		return nil, err
	}
	h, err := head(ctx, bucket, key, client)
	if err != nil {
		return nil, err
	}
	return &fileInfo{
		Name:    path.Base(key),
		Size:    *h.ContentLength,
		ModTime: *h.LastModified,
		IsDir:   false,
	}, nil
}

func NewS3Reader(ctx context.Context, filepath string, client s3iface.S3API) (*S3Reader, error) {
	info, err := stat(ctx, filepath, client)
	if err != nil {
		return nil, err
	}
	bucket, key, err := parsePath(filepath)
	if err != nil {
		return nil, err
	}
	return &S3Reader{
		client: client,
		ctx:    ctx,
		bucket: bucket,
		key:    key,
		size:   info.Size,
	}, nil
}

func (r *S3Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.offset
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, errors.New("s3io.Reader.Seek: invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("s3io.Reader.Seek: negative position")
	}
	if offset == r.offset {
		return offset, nil
	}
	r.offset = offset
	if r.body != nil {
		r.body.Close()
		r.body = nil
	}
	return r.offset, nil
}

func (r *S3Reader) Read(p []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}
request:
	if r.body == nil {
		body, err := r.makeRequest(r.offset, r.size-r.offset)
		if err != nil {
			return 0, err
		}
		r.body = body
	}

	n, err := r.body.Read(p)
	if errors.Is(err, syscall.ECONNRESET) {
		// See: https://github.com/aws/aws-sdk-go/issues/1242
		r.body = nil
		goto request
	}
	if err == io.EOF {
		err = nil
	}
	if err == nil {
		r.offset += int64(n)
	}
	return n, err
}

func (r *S3Reader) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.size {
		return 0, io.EOF
	}
	count := int64(len(p))
	if off+count >= r.size {
		count = r.size - off
	}
	b, err := r.makeRequest(off, count)
	if err != nil {
		return 0, err
	}
	defer b.Close()
	return io.ReadAtLeast(b, p, int(count))
}

func (r *S3Reader) Close() error {
	var err error
	if r.body != nil {
		err = r.body.Close()
		r.body = nil
	}
	return err
}

func (r *S3Reader) makeRequest(off int64, count int64) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+count-1)),
	}
	res, err := r.client.GetObjectWithContext(r.ctx, input)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

func (r *S3Reader) Size() (int64, error) {
	return r.size, nil
}
