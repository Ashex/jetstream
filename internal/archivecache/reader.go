// Package archivecache optionally releases local archive pages after reading.
package archivecache

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/vfs"
)

const releaseBatchBytes = 8 << 20

type descriptor interface {
	io.ReadCloser
	io.ReaderAt
	Fd() uintptr
}

// Reader tracks bytes read from one open archive descriptor. It owns Close.
type Reader struct {
	file   descriptor
	advise func(fd uintptr, offset, length int64)

	mu     sync.Mutex
	pos    int64
	start  int64
	end    int64
	closed bool
}

// NewReader takes ownership of a read-only descriptor at offset zero.
func NewReader(file descriptor) *Reader {
	return &Reader{file: file, advise: dropPages}
}

func (r *Reader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, err := r.file.Read(p)
	r.record(r.pos, n)
	r.pos += int64(n)
	return n, err
}

func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.file.ReadAt(p, off)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.record(off, n)
	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	file, ok := r.file.(io.Seeker)
	if !ok {
		return 0, errors.ErrUnsupported
	}
	pos, err := file.Seek(offset, whence)
	if err == nil {
		r.pos = pos
	}
	return pos, err
}

func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.release(r.end)
		r.closed = true
	}
	return r.file.Close()
}

func (r *Reader) record(off int64, n int) {
	if n == 0 || r.closed {
		return
	}
	end := off + int64(n)
	if r.start == r.end || end < r.start || off > r.end {
		r.release(r.end)
		r.start, r.end = off, end
	} else {
		r.start, r.end = min(r.start, off), max(r.end, end)
	}
	if r.end-r.start >= releaseBatchBytes {
		// Retain the partial ending page in case the next read completes it.
		through := r.end - r.end%int64(os.Getpagesize())
		r.release(through)
		r.start = through
	}
}

func (r *Reader) release(end int64) {
	pageSize := int64(os.Getpagesize())
	first, last := r.start/pageSize, end/pageSize
	if r.start%pageSize != 0 {
		first++
	}
	// A length of zero would advise through EOF. Omit partial pages so
	// readahead and unread bytes outside these ranges are never requested.
	if first < last {
		if fd := r.file.Fd(); fd != vfs.InvalidFd {
			r.advise(fd, first*pageSize, (last-first)*pageSize)
		}
	}
}
