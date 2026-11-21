package subscribe_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/stretchr/testify/require"
)

// retentionColdFixture has three sealed segments and a writer-published
// floor. prune raises the floor past segment 0 and unlinks its file the way
// the retention pass does.
type retentionColdFixture struct {
	cat    *withholdingCatalog
	holder *retention.Holder
	seg0   catalog.SegmentView
	seg1   catalog.SegmentView
	prune  func(t *testing.T)
	reader func(fetch func(catalog.BlockRef), withWriterFloor bool) *subscribe.ColdReader
}

func newRetentionColdFixture(t *testing.T) *retentionColdFixture {
	t.Helper()
	cat, w := openFloorReplayFixture(t, nil)
	for w.ActiveIndex() < 3 {
		appendReplayEvent(t, w, "did:plc:lain")
	}
	for range 3 {
		appendReplayEvent(t, w, "did:plc:lain")
	}
	require.NoError(t, w.Flush(context.Background()))
	sealed := cat.Snapshot().Segments(catalog.Main)
	require.GreaterOrEqual(t, len(sealed), 3)
	seg0, seg1 := sealed[0], sealed[1]
	require.Greater(t, seg0.MaxSeq(), seg0.MinSeq(), "segment 0 holds more than one event")

	fx := &retentionColdFixture{cat: cat, holder: &retention.Holder{}, seg0: seg0, seg1: seg1}
	fx.prune = func(t *testing.T) {
		t.Helper()
		fx.holder.Raise(retention.Floor{Seq: seg1.MinSeq(), LastIndex: seg0.Index, Pruned: true})
		require.NoError(t, cat.RemoveSegmentFiles(catalog.Main, []uint64{seg0.Index}))
	}
	fx.reader = func(fetch func(catalog.BlockRef), withWriterFloor bool) *subscribe.ColdReader {
		cfg := subscribe.ColdReaderConfig{Catalog: cat, Fetcher: cat.Fetcher(), Retention: fx.holder}
		if fetch != nil {
			cfg.Fetcher = hookFetcher{Fetcher: cat.Fetcher(), before: fetch}
		}
		if withWriterFloor {
			ref := &atomic.Pointer[ingest.Writer]{}
			ref.Store(w)
			cfg.WriterRef = ref
		} else {
			cfg.Floor = func() (uint64, bool) { return 0, true }
		}
		return subscribe.NewColdReader(cfg)
	}
	return fx
}

func TestColdReader_RetentionFloorOvertakesAReaderBetweenBatches(t *testing.T) {
	t.Parallel()
	fx := newRetentionColdFixture(t)
	rd := fx.reader(nil, true)

	batch, next, err := rd.Read(t.Context(), fx.seg0.MinSeq(), 1)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Less(t, next, fx.seg1.MinSeq())

	fx.prune(t)
	batch, _, err = rd.Read(t.Context(), next, 10)
	require.ErrorIs(t, err, subscribe.ErrBelowRetention)
	require.Empty(t, batch)
}

func TestColdReader_RetentionFileUnlinkedMidWalkIsBelowRetentionNotAHole(t *testing.T) {
	t.Parallel()
	fx := newRetentionColdFixture(t)
	var once sync.Once
	rd := fx.reader(func(catalog.BlockRef) { once.Do(func() { fx.prune(t) }) }, true)

	batch, _, err := rd.Read(t.Context(), fx.seg0.MinSeq(), 100)

	require.ErrorIs(t, err, subscribe.ErrBelowRetention, "got %v", err)
	require.Empty(t, batch, "the batch is discarded")
}

func TestColdReader_RetentionFloorRaisedDuringAWalkIsBelowRetention(t *testing.T) {
	t.Parallel()
	fx := newRetentionColdFixture(t)
	var once sync.Once
	rd := fx.reader(func(catalog.BlockRef) {
		once.Do(func() {
			fx.holder.Raise(retention.Floor{Seq: fx.seg1.MinSeq(), LastIndex: fx.seg0.Index, Pruned: true})
		})
	}, true)

	batch, _, err := rd.Read(t.Context(), fx.seg0.MinSeq(), 100)

	require.ErrorIs(t, err, subscribe.ErrBelowRetention)
	require.Empty(t, batch)
}

func TestColdReader_RetentionUnboundedWalkThatJumpsTheHoleIsStillRefused(t *testing.T) {
	t.Parallel()
	fx := newRetentionColdFixture(t)
	var once sync.Once
	rd := fx.reader(func(catalog.BlockRef) { once.Do(func() { fx.prune(t) }) }, false)

	batch, _, err := rd.Read(t.Context(), fx.seg0.MinSeq(), 100)

	require.ErrorIs(t, err, subscribe.ErrBelowRetention, "StopSeq 0 would silently skip the hole; got %v", err)
	require.Empty(t, batch)
}
