package reader

import (
	"fmt"
	"io"
	"net/http"

	"github.com/apache/arrow/go/v17/parquet"
)

var _ parquet.ReaderAtSeeker = (*HttpReader)(nil)

type HttpReader struct {
	url      string
	fileSize int64
	offset   int64
}

func NewHttpReader(url string) (*HttpReader, error) {
	resp, err := http.Head(url)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
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

// ReadAt fetches only the requested byte range via HTTP Range requests,
// avoiding loading the entire file into memory.
func (fd *HttpReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= fd.fileSize {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= fd.fileSize {
		end = fd.fileSize - 1
	}

	req, err := http.NewRequest(http.MethodGet, fd.url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("server returned non-OK status: %v", resp.Status)
	}

	return io.ReadFull(resp.Body, p[:end-off+1])
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
