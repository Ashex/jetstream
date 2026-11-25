package archivecache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestReaderAdvisesOriginalInodeAfterRename(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "segment.jss")
	page := os.Getpagesize()
	require.NoError(t, os.WriteFile(path, make([]byte, page), 0o600))
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	var original unix.Stat_t
	require.NoError(t, unix.Fstat(int(f.Fd()), &original))
	r := NewReader(f)
	var advised bool
	r.advise = func(fd uintptr, off, len int64) {
		var st unix.Stat_t
		require.NoError(t, unix.Fstat(int(fd), &st))
		require.Equal(t, original.Ino, st.Ino)
		require.EqualValues(t, 0, off)
		require.EqualValues(t, page, len)
		advised = true
	}

	tmp := path + ".tmp"
	require.NoError(t, os.WriteFile(tmp, make([]byte, page), 0o600))
	require.NoError(t, os.Rename(tmp, path))
	n, err := r.ReadAt(make([]byte, page), 0)
	require.NoError(t, err)
	require.Equal(t, page, n)
	require.NoError(t, r.Close())
	require.True(t, advised)
}
