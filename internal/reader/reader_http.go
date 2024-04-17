package reader

import (
	"fmt"
	"io"
	"net/http"

	"github.com/apache/arrow/go/v16/parquet"
)

var _ parquet.ReaderAtSeeker = (*HttpReader)(nil)

type HttpReader struct {
	url      string
	fileSize int64
	data     []byte
	offset   int64
}

func NewHttpReader(url string) (*HttpReader, error) {
	resp, err := http.Head(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned non-OK status: %v", resp.Status)
	}

	fileSize := resp.ContentLength
	if fileSize < 0 {
		return nil, fmt.Errorf("server did not specify Content-Length")
	}

	return &HttpReader{
		url:      url,
		fileSize: fileSize,
	}, nil
}

func (fd *HttpReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= fd.fileSize {
		return 0, io.EOF
	}
	if len(fd.data) == 0 {
		if err := fd.download(); err != nil {
			return 0, err
		}
	}

	end := off + int64(len(p))
	if end > fd.fileSize {
		end = fd.fileSize
	}

	copy(p, fd.data[off:end])
	return int(end - off), nil
}

func (fd *HttpReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		fd.offset = offset
	case io.SeekCurrent:
		fd.offset += offset
	case io.SeekEnd:
		fd.offset = fd.fileSize + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	if fd.offset < 0 {
		fd.offset = 0
	} else if fd.offset > fd.fileSize {
		fd.offset = fd.fileSize
	}

	return fd.offset, nil
}

func (fd *HttpReader) download() error {
	resp, err := http.Get(fd.url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned non-OK status: %v", resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fd.data = data
	return nil
}
