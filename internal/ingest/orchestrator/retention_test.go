package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	retentionTestWindow = time.Hour
	retentionDataDir    = "/data"
)

var retentionTestNow = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

// witnessedMicros is a witnessed time age before retentionTestNow.
func witnessedMicros(age time.Duration) int64 {
	return retentionTestNow.Add(-age).UnixMicro()
}

var (
	oldWitnessed   = witnessedMicros(2 * time.Hour)
	youngWitnessed = witnessedMicros(10 * time.Minute)
)

type retentionFixture struct {
	t         *testing.T
	fs        vfs.FS
	store     metastore.Store
	o         *Orchestrator
	segDir    string
	log       []string
	pruned    []uint64
	floors    []retention.Floor
	crashInj  crashpoint.Injector
	waitCalls int
}

func newRetentionFixture(t *testing.T) *retentionFixture {
	t.Helper()
	fx := &retentionFixture{t: t, fs: vfs.NewMem(), store: memstore.New()}
	fx.setUp(fx.fs, fx.store, nil)
	return fx
}

// setUp builds the orchestrator literal the way the compaction tests do,
// with recording hooks.
func (fx *retentionFixture) setUp(fs vfs.FS, store metastore.Store, inj crashpoint.Injector) {
	fx.t.Helper()
	fx.fs, fx.store, fx.crashInj = fs, store, inj
	fx.segDir = fs.PathJoin(retentionDataDir, "segments")
	require.NoError(fx.t, ingest.MkdirAllSyncedFS(fs, fx.segDir, 0o755, "retention-test"))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fx.o = &Orchestrator{
		cfg: Config{
			DataDir:            retentionDataDir,
			FS:                 fs,
			Store:              store,
			Logger:             logger,
			CompactionInterval: time.Hour,
			RetentionWindow:    retentionTestWindow,
			CrashInjector:      inj,
			OnRetentionFloor: func(f retention.Floor) {
				fx.floors = append(fx.floors, f)
				fx.log = append(fx.log, "floor")
			},
			OnSegmentPruned: func(idx uint64) error {
				fx.pruned = append(fx.pruned, idx)
				fx.log = append(fx.log, "pruned")
				return nil
			},
			WaitForArchiveLoad: func(context.Context) error {
				fx.waitCalls++
				fx.log = append(fx.log, "wait")
				return nil
			},
		},
		logger: logger,
	}
}

// writeSegment seals segment idx with events at the given seqs, all witnessed
// at witnessed. No seqs seals an empty segment.
func (fx *retentionFixture) writeSegment(idx uint64, witnessed int64, seqs ...uint64) {
	fx.t.Helper()
	w, err := segment.New(segment.Config{Path: fx.fs.PathJoin(fx.segDir, ingest.SegmentFilename(idx)), FS: fx.fs, MaxEventsPerBlock: 2})
	require.NoError(fx.t, err)
	for _, seq := range seqs {
		full, err := w.Append(segment.Event{Seq: seq, WitnessedAt: witnessed, Kind: segment.KindCreate, DID: "did:plc:lain", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("wired")})
		require.NoError(fx.t, err)
		if full {
			require.NoError(fx.t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(fx.t, err)
}

func (fx *retentionFixture) setWatermark(seq uint64) {
	fx.t.Helper()
	require.NoError(fx.t, saveCompactionWatermark(fx.store, seq))
}

func (fx *retentionFixture) exists(idx uint64) bool {
	fx.t.Helper()
	_, err := fx.fs.Stat(fx.fs.PathJoin(fx.segDir, ingest.SegmentFilename(idx)))
	if err == nil {
		return true
	}
	require.True(fx.t, oserror.IsNotExist(err), "stat: %v", err)
	return false
}

func (fx *retentionFixture) floor() retention.Floor {
	fx.t.Helper()
	f, err := retention.Load(context.Background(), fx.store)
	require.NoError(fx.t, err)
	return f
}

func (fx *retentionFixture) pass() error {
	return fx.o.runRetentionPass(context.Background(), retentionTestNow)
}

func (fx *retentionFixture) catalogIndexes() []uint64 {
	fx.t.Helper()
	require.NoError(fx.t, fx.o.segments().Refresh(context.Background()))
	var idxs []uint64
	for _, v := range fx.o.segments().Snapshot().Segments(catalog.Main) {
		idxs = append(idxs, v.Index)
	}
	return idxs
}

func TestRetentionPass_PrunesTheOldPrefixAndCommitsTheFloor(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, oldWitnessed, 3, 4)
	fx.writeSegment(2, oldWitnessed, 5, 6)
	fx.writeSegment(3, youngWitnessed, 7, 8)
	fx.setWatermark(8)

	require.NoError(t, fx.pass())

	// Seqs 1-6 were written above; the youngest kept segment starts at 7.
	require.Equal(t, retention.Floor{Seq: 7, WitnessedAt: oldWitnessed, LastIndex: 2, Pruned: true}, fx.floor())
	for idx, want := range map[uint64]bool{0: false, 1: false, 2: false, 3: true} {
		require.Equal(t, want, fx.exists(idx), "segment %d", idx)
	}
	require.Equal(t, []uint64{0, 1, 2}, fx.pruned)
	require.Equal(t, []uint64{3}, fx.catalogIndexes())
}

func TestRetentionPass_StopsAtASegmentAboveTheCompactionWatermark(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, oldWitnessed, 3, 4)
	fx.writeSegment(2, oldWitnessed, 5, 6)
	fx.writeSegment(3, youngWitnessed, 7, 8)
	fx.setWatermark(4)

	require.NoError(t, fx.pass())

	require.Equal(t, uint64(5), fx.floor().Seq)
	require.Equal(t, []uint64{0, 1}, fx.pruned)
	require.True(t, fx.exists(2))
}

func TestRetentionPass_KeepsAnOldSegmentThatFollowsAYoungOne(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, youngWitnessed, 3, 4)
	fx.writeSegment(2, oldWitnessed, 5, 6)
	fx.writeSegment(3, youngWitnessed, 7, 8)
	fx.setWatermark(8)

	require.NoError(t, fx.pass())

	require.Equal(t, []uint64{0}, fx.pruned)
	require.True(t, fx.exists(1))
	require.True(t, fx.exists(2), "no hole above the floor")
}

func TestRetentionPass_NeverPrunesTheNewestSealedSegment(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, oldWitnessed, 3, 4)
	fx.setWatermark(4)

	require.NoError(t, fx.pass())

	require.Equal(t, []uint64{0}, fx.pruned)
	require.True(t, fx.exists(1))
}

func TestRetentionPass_WindowZeroAndNoWatermarkAreNoOps(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, oldWitnessed, 3, 4)

	require.NoError(t, fx.pass(), "no watermark stored")
	require.Empty(t, fx.pruned)

	fx.setWatermark(4)
	fx.o.cfg.RetentionWindow = 0
	require.NoError(t, fx.pass())
	require.Empty(t, fx.pruned)
	require.False(t, fx.floor().Pruned)
	require.True(t, fx.exists(0))
}

func TestRetentionPass_SecondPassIsANoOp(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, youngWitnessed, 3, 4)
	fx.setWatermark(4)

	require.NoError(t, fx.pass())
	first := fx.floor()
	require.NoError(t, fx.pass())

	require.Equal(t, first, fx.floor())
	require.Equal(t, []uint64{0}, fx.pruned)
	require.Len(t, fx.floors, 1)
}

func TestRetentionPass_PublishesTheFloorBeforeRemovingAnything(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, oldWitnessed, 3, 4)
	fx.writeSegment(2, youngWitnessed, 5, 6)
	fx.setWatermark(6)

	require.NoError(t, fx.pass())

	require.Equal(t, []string{"wait", "floor", "pruned", "pruned"}, fx.log)
}

func TestRecoverRetention_FinishesARemovalThatCrashedAfterTheFloor(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	sentinel := errors.New("kill point: floor before remove")
	fx.o.cfg.CrashInjector = pointErrorInjector{point: crashpoint.AfterRetentionFloorBeforeRemove, err: sentinel}
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, oldWitnessed, 3, 4)
	fx.writeSegment(2, youngWitnessed, 5, 6)
	fx.setWatermark(6)

	require.ErrorIs(t, fx.pass(), sentinel)

	require.Equal(t, uint64(1), fx.floor().LastIndex)
	require.True(t, fx.exists(0) && fx.exists(1) && fx.exists(2), "the crash left every file")
	require.Empty(t, fx.pruned)

	fx.o.cfg.CrashInjector = nil
	require.NoError(t, fx.o.recoverRetention(context.Background()))
	require.False(t, fx.exists(0))
	require.False(t, fx.exists(1))
	require.True(t, fx.exists(2))
	require.Equal(t, []uint64{0, 1}, fx.pruned, "recovery goes through OnSegmentPruned too")
}

func TestRetentionPass_EmptySegmentsRideAlongWithoutLoweringTheFloorSeq(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, 0)
	fx.writeSegment(1, oldWitnessed, 1, 2)
	fx.writeSegment(2, youngWitnessed, 3, 4)
	fx.setWatermark(4)

	require.NoError(t, fx.pass())
	require.Equal(t, retention.Floor{Seq: 3, WitnessedAt: oldWitnessed, LastIndex: 1, Pruned: true}, fx.floor())
	require.Equal(t, []uint64{0, 1}, fx.pruned)
}

func TestRetentionPass_AnEmptyOnlyCandidateRaisesLastIndexAndKeepsSeq(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, 0)
	fx.writeSegment(1, youngWitnessed, 1, 2)
	fx.writeSegment(2, youngWitnessed, 3, 4)
	fx.setWatermark(4)

	require.NoError(t, fx.pass())

	require.Equal(t, retention.Floor{Seq: 0, WitnessedAt: 0, LastIndex: 0, Pruned: true}, fx.floor())
	require.False(t, fx.exists(0))
	require.True(t, fx.exists(1))
}

func TestRecoverRetention_KeepsAnEmptySegmentAboveTheLastIndex(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, 0)
	fx.writeSegment(2, youngWitnessed, 3, 4)
	fx.writeSegment(3, youngWitnessed, 5, 6)
	require.NoError(t, retention.Save(t.Context(), fx.store, retention.Floor{Seq: 3, WitnessedAt: oldWitnessed, LastIndex: 0}))

	require.NoError(t, fx.o.recoverRetention(t.Context()))

	require.False(t, fx.exists(0))
	require.True(t, fx.exists(1), "an empty segment above LastIndex survives")
	require.Equal(t, []uint64{0}, fx.pruned)
}

func TestRetentionPass_ReselectsAResurrectedSegmentWithoutLoweringTheFloor(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, oldWitnessed, 3, 4)
	fx.writeSegment(2, youngWitnessed, 5, 6)
	fx.writeSegment(3, youngWitnessed, 7, 8)
	fx.setWatermark(8)
	require.NoError(t, retention.Save(t.Context(), fx.store, retention.Floor{Seq: 5, WitnessedAt: oldWitnessed, LastIndex: 1, Pruned: true}))

	require.NoError(t, fx.pass())

	require.Equal(t, retention.Floor{Seq: 5, WitnessedAt: oldWitnessed, LastIndex: 1, Pruned: true}, fx.floor())
	require.False(t, fx.exists(0))
	require.False(t, fx.exists(1))
}

func TestRetentionPass_MetricsFollowThePass(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	reg := prometheus.NewRegistry()
	fx.o.cfg.Metrics = NewMetrics(reg)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, oldWitnessed, 3, 4)
	fx.writeSegment(2, youngWitnessed, 5, 6)
	fx.setWatermark(6)

	require.NoError(t, fx.pass())

	m := fx.o.cfg.Metrics.retention
	require.Equal(t, 5.0, testutil.ToFloat64(m.floorSeq))
	require.Equal(t, 2.0, testutil.ToFloat64(m.segmentsPruned))
	require.Equal(t, 1.0, testutil.ToFloat64(m.passes.WithLabelValues("pruned")))
	require.Positive(t, testutil.ToFloat64(m.bytesPruned))

	require.NoError(t, fx.pass())
	require.Equal(t, 1.0, testutil.ToFloat64(m.passes.WithLabelValues("noop")))
}

// listBlockingFS holds the first List of dir until released, standing in for a
// manifest whose background load is still in flight.
type listBlockingFS struct {
	vfs.FS
	dir     string
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *listBlockingFS) List(dir string) ([]string, error) {
	if dir == b.dir {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return b.FS.List(dir)
}

func TestRecoverRetention_WaitsForTheManifestLoadAndLeavesNoGhostEntry(t *testing.T) {
	t.Parallel()
	fx := newRetentionFixture(t)
	fx.writeSegment(0, oldWitnessed, 1, 2)
	fx.writeSegment(1, oldWitnessed, 3, 4)
	fx.writeSegment(2, youngWitnessed, 5, 6)
	require.NoError(t, retention.Save(t.Context(), fx.store, retention.Floor{Seq: 5, WitnessedAt: oldWitnessed, LastIndex: 1, Pruned: true}))

	segDir := fx.segDir
	blocking := &listBlockingFS{FS: fx.fs, dir: segDir, entered: make(chan struct{}), release: make(chan struct{})}
	mft, err := manifest.OpenBackground(t.Context(), manifest.Options{SegmentsDir: segDir, FS: blocking, Logger: fx.o.logger})
	require.NoError(t, err)
	<-blocking.entered

	fx.o.cfg.WaitForArchiveLoad = func(ctx context.Context) error {
		fx.log = append(fx.log, "wait")
		return mft.Wait(ctx)
	}
	fx.o.cfg.OnSegmentPruned = func(idx uint64) error {
		fx.log = append(fx.log, "pruned")
		return mft.RemoveSegment(idx)
	}

	done := make(chan error, 1)
	go func() { done <- fx.o.recoverRetention(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("recovery finished before the manifest load did: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.True(t, fx.exists(0), "nothing is removed while the load is in flight")

	close(blocking.release)
	require.NoError(t, <-done)
	require.NoError(t, mft.Wait(t.Context()))

	require.Equal(t, "wait", fx.log[0], "recovery waits for the load before touching anything")
	for idx, want := range map[uint64]bool{0: false, 1: false, 2: true} {
		_, ok := mft.SegmentByIdx(idx)
		require.Equal(t, want, ok, "manifest entry %d", idx)
		require.Equal(t, want, fx.exists(idx), "file %d", idx)
	}
}

// retentionRunConfig is a Config that can run Run against a recording relay
// with retention settings of the caller's choosing.
func retentionRunConfig(t *testing.T, relay *recordingRelay, st metastore.Store) Config {
	t.Helper()
	cfg := liveOnlyConfig(t, relay, st)
	cfg.LiveOnly = false
	cfg.RetentionWindow = 2160 * time.Hour
	return cfg
}

func storeKeyCount(t *testing.T, st metastore.Store) int {
	t.Helper()
	it, err := st.NewIter(t.Context(), nil, nil)
	require.NoError(t, err)
	defer func() { _ = it.Close() }()
	n := 0
	for it.Next() {
		n++
	}
	require.NoError(t, it.Err())
	return n
}

func TestRetentionRun_DefaultedWindowRefusesAFreshDataDirBeforeWritingAnything(t *testing.T) {
	t.Parallel()
	relay := newRecordingRelay(t)
	st := newOrchestratorTestStore(t)

	err := runLiveOnlyUntilSubscribed(t, retentionRunConfig(t, relay, st), relay)

	require.ErrorContains(t, err, "start with --live-only, or set --retention-window explicitly")
	phase, err := lifecycle.ReadPhase(t.Context(), st)
	require.NoError(t, err)
	require.Empty(t, phase, "the phase key is still absent")
	require.Zero(t, storeKeyCount(t, st), "the store holds no key at all")
	require.Empty(t, relay.seen())
}

func TestRetentionRun_DefaultedWindowStartsAFreshLiveOnlyDataDir(t *testing.T) {
	t.Parallel()
	relay := newRecordingRelay(t)
	st := newOrchestratorTestStore(t)
	cfg := retentionRunConfig(t, relay, st)
	cfg.LiveOnly = true

	require.NoError(t, runLiveOnlyUntilSubscribed(t, cfg, relay))

	phase, err := lifecycle.ReadPhase(t.Context(), st)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, phase)
}

// seedOldArchive makes cfg.DataDir an existing steady_state archive with two
// sealed segments, the first witnessed long before any window.
func seedOldArchive(t *testing.T, cfg Config) {
	t.Helper()
	segDir := vfs.Default.PathJoin(cfg.DataDir, "segments")
	require.NoError(t, vfs.Default.MkdirAll(segDir, 0o755))
	long := time.Now().Add(-5000 * time.Hour).UnixMicro()
	for idx, firstSeq := range []uint64{1, 3} {
		w, err := segment.New(segment.Config{Path: vfs.Default.PathJoin(segDir, ingest.SegmentFilename(uint64(idx))), FS: vfs.Default, MaxEventsPerBlock: 2})
		require.NoError(t, err)
		for seq := firstSeq; seq < firstSeq+2; seq++ {
			_, err := w.Append(segment.Event{Seq: seq, WitnessedAt: long, Kind: segment.KindCreate, DID: "did:plc:asuka", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("x")})
			require.NoError(t, err)
		}
		_, err = w.Seal()
		require.NoError(t, err)
	}
	require.NoError(t, lifecycle.WritePhase(t.Context(), cfg.Store, lifecycle.PhaseSteadyState, time.Now().UTC()))
}

func TestRetentionRun_DefaultedWindowRefusesAnExistingArchiveThatWasNotStartedLiveOnly(t *testing.T) {
	t.Parallel()
	relay := newRecordingRelay(t)
	st := newOrchestratorTestStore(t)
	cfg := retentionRunConfig(t, relay, st)
	seedOldArchive(t, cfg)

	err := runLiveOnlyUntilSubscribed(t, cfg, relay)
	require.ErrorContains(t, err, "2160h")
	require.ErrorContains(t, err, "would delete 2 sealed segments")
	require.FileExists(t, vfs.Default.PathJoin(cfg.DataDir, "segments", ingest.SegmentFilename(0)))
	require.FileExists(t, vfs.Default.PathJoin(cfg.DataDir, "segments", ingest.SegmentFilename(1)))

	explicit := retentionRunConfig(t, relay, st)
	explicit.DataDir = cfg.DataDir
	explicit.RetentionWindowExplicit = true
	require.NoError(t, runLiveOnlyUntilSubscribed(t, explicit, relay))
}

func TestRetentionRun_DefaultedWindowPrunesAnArchiveStartedLiveOnlyOrAlreadyPruned(t *testing.T) {
	t.Parallel()
	for name, mark := range map[string]func(*testing.T, metastore.Store){
		"live-only marker": func(t *testing.T, st metastore.Store) {
			require.NoError(t, retention.SaveLiveOnlySinceIfAbsent(t.Context(), st, time.Now()))
		},
		"stored floor": func(t *testing.T, st metastore.Store) {
			require.NoError(t, retention.Save(t.Context(), st, retention.Floor{}))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			relay := newRecordingRelay(t)
			st := newOrchestratorTestStore(t)
			cfg := retentionRunConfig(t, relay, st)
			seedOldArchive(t, cfg)
			mark(t, st)
			require.NoError(t, runLiveOnlyUntilSubscribed(t, cfg, relay))
		})
	}
}
