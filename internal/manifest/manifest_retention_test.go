package manifest

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

const retentionTestDir = "/segs"

func writeRetentionSegment(t *testing.T, fs vfs.FS, idx, firstSeq uint64) {
	t.Helper()
	require.NoError(t, fs.MkdirAll(retentionTestDir, 0o755))
	w, err := segment.New(segment.Config{Path: fs.PathJoin(retentionTestDir, ingest.SegmentFilename(idx)), FS: fs, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for seq := firstSeq; seq < firstSeq+2; seq++ {
		_, err := w.Append(segment.Event{Seq: seq, WitnessedAt: time.Now().UnixMicro(), Kind: segment.KindCreate, DID: "did:plc:rei", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("x")})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)
}

func retentionTestOptions(fs vfs.FS) Options {
	return Options{SegmentsDir: retentionTestDir, FS: fs, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestRemoveSegment_MovesTheViewsPastTheRemovedSegment(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	writeRetentionSegment(t, fs, 0, 1)
	writeRetentionSegment(t, fs, 1, 3)
	m, err := Open(retentionTestOptions(fs))
	require.NoError(t, err)

	genBefore := m.Generation()
	floorSeq, _ := m.LookbackFloor(time.Hour)
	require.Equal(t, uint64(1), floorSeq)

	require.NoError(t, m.RemoveSegment(0))
	require.Greater(t, m.Generation(), genBefore)
	_, ok := m.SegmentByIdx(0)
	require.False(t, ok)
	_, ok = m.SegmentByIdx(1)
	require.True(t, ok)
	floorSeq, _ = m.LookbackFloor(time.Hour)
	require.Equal(t, uint64(3), floorSeq)

	gen := m.Generation()
	require.NoError(t, m.RemoveSegment(0), "an absent segment is a no-op")
	require.Equal(t, gen, m.Generation())
}

// blockingListFS holds the first List of dir until released, so a test can
// keep a background manifest load in flight.
type blockingListFS struct {
	vfs.FS
	dir     string
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *blockingListFS) List(dir string) ([]string, error) {
	if dir == b.dir {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return b.FS.List(dir)
}

func TestRemoveSegment_DuringBackgroundLoadWaitsAndStaysGone(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	writeRetentionSegment(t, fs, 0, 1)
	writeRetentionSegment(t, fs, 1, 3)
	blocking := &blockingListFS{FS: fs, dir: retentionTestDir, entered: make(chan struct{}), release: make(chan struct{})}
	m, err := OpenBackground(t.Context(), retentionTestOptions(blocking))
	require.NoError(t, err)
	<-blocking.entered

	removed := make(chan error, 1)
	go func() { removed <- m.RemoveSegment(0) }()
	select {
	case err := <-removed:
		t.Fatalf("RemoveSegment returned before the load finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(blocking.release)
	require.NoError(t, <-removed)
	require.NoError(t, m.Wait(t.Context()))
	_, ok := m.SegmentByIdx(0)
	require.False(t, ok, "the load must not bring the removed segment back")
	_, ok = m.SegmentByIdx(1)
	require.True(t, ok)
}

func TestRemoveSegment_PlanSnapshotNeverNamesTheRemovedSegment(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	writeRetentionSegment(t, fs, 0, 1)
	writeRetentionSegment(t, fs, 1, 3)
	m, err := Open(retentionTestOptions(fs))
	require.NoError(t, err)
	require.NoError(t, m.RemoveSegment(0))

	got, err := m.PlanSnapshot(PlanSnapshotRequest{WholeSegmentThreshold: 0.5})
	require.NoError(t, err)
	for _, seg := range got.Segments {
		require.NotEqual(t, uint64(0), seg.Idx)
	}
	require.Len(t, got.Segments, 1)
}
