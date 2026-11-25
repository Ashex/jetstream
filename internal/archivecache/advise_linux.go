package archivecache

import "golang.org/x/sys/unix"

// Supported reports whether the platform offers archive read-cache advice.
const Supported = true

func dropPages(fd uintptr, offset, length int64) {
	_ = unix.Fadvise(int(fd), offset, length, unix.FADV_DONTNEED)
}
