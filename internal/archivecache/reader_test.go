package archivecache

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type advice struct {
	fd             uintptr
	offset, length int64
}

func newTestReader(t *testing.T, size int64) (*Reader, *[]advice) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "segment.jss")
	require.NoError(t, os.WriteFile(path, make([]byte, size), 0o600))
	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	r := NewReader(file)
	var calls []advice
	r.advise = func(fd uintptr, offset, length int64) {
		// The original descriptor must still be live when advice is sent.
		require.Equal(t, file.Fd(), fd)
		_, err := file.Stat()
		require.NoError(t, err)
		calls = append(calls, advice{fd, offset, length})
	}
	return r, &calls
}

func TestReaderAdvisesOnlyCompleteReadPages(t *testing.T) {
	t.Parallel()
	page := int64(os.Getpagesize())
	r, calls := newTestReader(t, 12*page+9)
	fd := r.file.Fd()

	_, err := r.Seek(1, io.SeekStart)
	require.NoError(t, err)

	// Adjacent short reads complete a page across the internal boundary.
	n, err := r.Read(make([]byte, page+17))
	require.NoError(t, err)
	require.EqualValues(t, page+17, n)
	n, err = r.Read(make([]byte, page+11))
	require.NoError(t, err)
	require.EqualValues(t, page+11, n)
	require.Empty(t, *calls)

	// A seek alone adds no bytes. A separate range must not fill the hole.
	_, err = r.Seek(8*page, io.SeekStart)
	require.NoError(t, err)
	n, err = r.Read(make([]byte, 2*page))
	require.NoError(t, err)
	require.EqualValues(t, 2*page, n)

	// ReadAt records bytes actually returned, including a partial EOF.
	n, err = r.ReadAt(make([]byte, 2*page), 11*page)
	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, page+9, n)
	require.NoError(t, r.Close())
	require.Equal(t, []advice{{fd, page, page}, {fd, 8 * page, 2 * page}, {fd, 11 * page, page}}, *calls)
}

func TestReaderReleasesLongReadInBatches(t *testing.T) {
	t.Parallel()
	page := int64(os.Getpagesize())
	r, calls := newTestReader(t, 2*releaseBatchBytes)
	fd := r.file.Fd()

	// An unaligned batch end must carry across to the next read.
	chunk := make([]byte, page)
	for off := int64(1); off < releaseBatchBytes; off += page {
		n, err := r.ReadAt(chunk, off)
		require.NoError(t, err)
		require.Len(t, chunk, n)
	}
	require.Equal(t, []advice{{fd, page, releaseBatchBytes - page}}, *calls)

	n, err := r.ReadAt(chunk, releaseBatchBytes+1)
	require.NoError(t, err)
	require.Len(t, chunk, n)
	require.NoError(t, r.Close())
	require.Equal(t, []advice{{fd, page, releaseBatchBytes - page}, {fd, releaseBatchBytes, page}}, *calls)
}

func TestReadFSScopesDecoration(t *testing.T) {
	t.Parallel()
	require.Nil(t, ReadFS(nil, false))
	mem := vfs.NewMem()
	require.Same(t, mem, ReadFS(mem, false))

	root := t.TempDir()
	segment := filepath.Join(root, "seg_0000000000.jss")
	other := filepath.Join(root, "metadata")
	require.NoError(t, os.WriteFile(segment, nil, 0o600))
	require.NoError(t, os.WriteFile(other, nil, 0o600))

	for _, tc := range []struct {
		path string
		want bool
	}{
		{path: segment, want: Supported},
		{path: other},
	} {
		fs := ReadFS(vfs.Default, true)
		f, err := fs.Open(tc.path)
		require.NoError(t, err)
		_, decorated := f.(*readFile)
		require.Equal(t, tc.want, decorated)
		require.NoError(t, f.Close())

		f, err = fs.OpenReadWrite(tc.path)
		require.NoError(t, err)
		_, decorated = f.(*readFile)
		require.False(t, decorated)
		require.NoError(t, f.Close())
	}

	f, err := mem.Create("/segment.jss")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	f, err = ReadFS(mem, true).Open("/segment.jss")
	require.NoError(t, err)
	_, decorated := f.(*readFile)
	require.False(t, decorated)
	require.NoError(t, f.Close())
}
