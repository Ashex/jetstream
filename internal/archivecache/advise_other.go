//go:build !linux

package archivecache

// Supported reports whether the platform offers archive read-cache advice.
const Supported = false

func dropPages(uintptr, int64, int64) {}
