package archivecache

import (
	"strings"

	"github.com/cockroachdb/pebble/vfs"
)

// ReadFS decorates only read-only segment opens. Writes and all other file
// operations use the original filesystem.
func ReadFS(fs vfs.FS, drop bool) vfs.FS {
	if !drop || !Supported {
		return fs
	}
	if fs == nil {
		fs = vfs.Default
	}
	return readFS{fs}
}

type readFS struct{ vfs.FS }

func (fs readFS) Open(path string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := fs.FS.Open(path, opts...)
	if err != nil || !strings.HasSuffix(path, ".jss") || file.Fd() == vfs.InvalidFd {
		return file, err
	}
	return &readFile{File: file, reader: NewReader(file)}, nil
}

type readFile struct {
	vfs.File
	reader *Reader
}

func (f *readFile) Read(p []byte) (int, error)             { return f.reader.Read(p) }
func (f *readFile) ReadAt(p []byte, at int64) (int, error) { return f.reader.ReadAt(p, at) }
func (f *readFile) Close() error                           { return f.reader.Close() }
