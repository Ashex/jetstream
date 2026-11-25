package xrpcapi

import (
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/archivecache"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestArchiveReadCacheKeepsSegmentAndBlockResponses(t *testing.T) {
	t.Parallel()
	base, dir := newTestServer(t, 1)
	keep := FileOpener{Src: base.src}
	drop := FileOpener{Src: base.src, DropReadCache: true}
	for _, opener := range []FileOpener{keep, drop} {
		f, err := opener.OpenSegment(t.Context(), 0)
		require.NoError(t, err)
		_, tracked := f.Content().(*archivecache.Reader)
		require.Equal(t, opener.DropReadCache && archivecache.Supported, tracked)
		require.NoError(t, f.Close())
	}

	srv := New(Config{Src: base.src, Opener: drop, Logger: base.logger})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	name := ingest.SegmentFilename(0)
	path := filepath.Join(dir, name)
	raw := rawFile(t, path)
	full := doGet(t, getSegURL(ts.URL, name))
	require.Equal(t, http.StatusOK, full.StatusCode)
	etag := full.Header.Get("ETag")
	body, err := io.ReadAll(full.Body)
	require.NoError(t, err)
	require.NoError(t, full.Body.Close())
	require.Equal(t, raw, body)

	ranged := doGetWith(t, getSegURL(ts.URL, name), func(req *http.Request) {
		req.Header.Set("Range", "bytes=1-9,32-47")
		req.Header.Set("If-Range", etag)
	})
	require.Equal(t, http.StatusPartialContent, ranged.StatusCode)
	media, params, err := mime.ParseMediaType(ranged.Header.Get("Content-Type"))
	require.NoError(t, err)
	require.Equal(t, "multipart/byteranges", media)
	parts := multipart.NewReader(ranged.Body, params["boundary"])
	for _, want := range [][]byte{raw[1:10], raw[32:48]} {
		part, err := parts.NextPart()
		require.NoError(t, err)
		got, err := io.ReadAll(part)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, err = parts.NextPart()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, ranged.Body.Close())

	conditional := doGetWith(t, getSegURL(ts.URL, name), func(req *http.Request) {
		req.Header.Set("If-None-Match", etag)
	})
	require.Equal(t, http.StatusNotModified, conditional.StatusCode)
	require.NoError(t, conditional.Body.Close())

	onDisk, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = onDisk.Close() }()
	hdr, err := segment.ReadSealedHeader(onDisk)
	require.NoError(t, err)
	want, err := segment.ReadBlockFrame(onDisk, hdr, 0)
	require.NoError(t, err)
	block := doGet(t, blockURL(ts.URL, name, 0))
	require.Equal(t, http.StatusOK, block.StatusCode)
	got, err := io.ReadAll(block.Body)
	require.NoError(t, err)
	require.NoError(t, block.Body.Close())
	require.Equal(t, want, got)
}
