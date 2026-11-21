package local_test

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func writeSealedSegment(t *testing.T, fs vfs.FS, idx, firstSeq uint64) {
	t.Helper()
	require.NoError(t, fs.MkdirAll(segDir, 0o755))
	w, err := segment.New(segment.Config{Path: fs.PathJoin(segDir, ingest.SegmentFilename(idx)), FS: fs, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for seq := firstSeq; seq < firstSeq+2; seq++ {
		_, err := w.Append(segment.Event{Seq: seq, WitnessedAt: int64(seq), Kind: segment.KindCreate, DID: "did:plc:lain", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("wired")})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)
}

func TestRemoveSegmentFiles_DropsFromTheCatalogAndStalesOldRefs(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	writeSealedSegment(t, fs, 0, 1)
	writeSealedSegment(t, fs, 1, 3)
	cat := newCatalog(t, fs)
	require.NoError(t, cat.Refresh(t.Context()))

	before := cat.Snapshot()
	require.Len(t, before.Segments(catalog.Main), 2)
	var ref catalog.BlockRef
	for r := range before.RefsFrom(catalog.Main, 0) {
		ref = r
		break
	}
	require.Equal(t, uint64(0), ref.Segment)

	require.NoError(t, cat.RemoveSegmentFiles(catalog.Main, []uint64{0}))
	require.NoError(t, cat.SyncSegmentsDir(catalog.Main))

	require.Len(t, before.Segments(catalog.Main), 2, "a snapshot taken earlier still names the segment")
	_, statErr := fs.Stat(cat.Path(catalog.Main, 0))
	require.True(t, oserror.IsNotExist(statErr))

	_, err := cat.Fetcher().Fetch(t.Context(), ref)
	require.ErrorIs(t, err, catalog.ErrStaleRef)
	require.ErrorContains(t, err, "is gone")

	after := cat.Snapshot().Segments(catalog.Main)
	require.Len(t, after, 1)
	require.Equal(t, uint64(1), after[0].Index)

	require.NoError(t, cat.RemoveSegmentFiles(catalog.Main, []uint64{0}), "removing twice is fine")
}
