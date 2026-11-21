package orchestrator

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestRetentionPass_StrictMemPowerLossAtEachCrashpoint(t *testing.T) {
	t.Parallel()

	for _, point := range []crashpoint.Point{
		crashpoint.AfterRetentionFloorBeforeRemove,
		crashpoint.AfterRetentionRemoveBeforeDirSync,
	} {
		t.Run(point.String(), func(t *testing.T) {
			t.Parallel()
			runStrictMemRetentionPowerLossCase(t, point)
		})
	}
}

func retentionEvents(witnessed int64, seqs ...uint64) []segment.Event {
	events := make([]segment.Event, 0, len(seqs))
	for _, seq := range seqs {
		events = append(events, segment.Event{Seq: seq, WitnessedAt: witnessed, Kind: segment.KindCreate, DID: "did:plc:misato", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("nerv")})
	}
	return events
}

func runStrictMemRetentionPowerLossCase(t *testing.T, point crashpoint.Point) {
	t.Helper()

	fs := vfs.NewStrictMem()
	syncStrictMemDir(t, fs, "/")
	const dataDir = "/data"
	segmentsDir := fs.PathJoin(dataDir, "segments")
	require.NoError(t, ingest.MkdirAllSyncedFS(fs, segmentsDir, 0o755, "orchestrator-test"))

	segPath := func(idx uint64) string { return fs.PathJoin(segmentsDir, ingest.SegmentFilename(idx)) }
	writeStrictMemSegment(t, fs, segmentsDir, 0, retentionEvents(oldWitnessed, 1, 2))
	writeStrictMemSegment(t, fs, segmentsDir, 1, retentionEvents(oldWitnessed, 3, 4))
	writeStrictMemSegment(t, fs, segmentsDir, 2, retentionEvents(youngWitnessed, 5, 6))
	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	newOrch := func(st *pebblestore.Store, inj crashpoint.Injector) *Orchestrator {
		o := newStrictMemCompactionOrchestrator(dataDir, fs, st, nil, inj)
		o.cfg.RetentionWindow = retentionTestWindow
		return o
	}

	st := openStrictMemStore(t, fs, dataDir)
	require.NoError(t, saveCompactionWatermark(st, 6))
	inj := &strictFSPowerLossPointInjector{point: point, fs: fs}
	err := newOrch(st, inj).runRetentionPass(context.Background(), retentionTestNow)
	require.ErrorIs(t, err, errStrictFSPowerLoss)
	require.True(t, inj.fired.Load(), "crashpoint %s did not fire", point)
	require.NoError(t, st.Close())

	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	st = openStrictMemStore(t, fs, dataDir)
	t.Cleanup(func() { _ = st.Close() })
	floorAfterCrash, err := retention.Load(context.Background(), st)
	require.NoError(t, err)
	require.Equal(t, retention.Floor{Seq: 5, WitnessedAt: oldWitnessed, LastIndex: 1, Pruned: true}, floorAfterCrash, "the floor was durable before any removal")
	if point == crashpoint.AfterRetentionRemoveBeforeDirSync {
		for _, idx := range []uint64{0, 1} {
			_, statErr := fs.Stat(segPath(idx))
			require.NoError(t, statErr, "the power loss resurrected segment %d", idx)
		}
	}

	o := newOrch(st, nil)
	require.NoError(t, o.recoverRetention(context.Background()))
	require.NoError(t, o.runRetentionPass(context.Background(), retentionTestNow))

	for _, idx := range []uint64{0, 1} {
		_, statErr := fs.Stat(segPath(idx))
		require.Truef(t, oserror.IsNotExist(statErr), "segment %d at or below the floor must be gone: %v", idx, statErr)
	}
	floorNow, err := retention.Load(context.Background(), st)
	require.NoError(t, err)
	require.GreaterOrEqual(t, floorNow.Seq, floorAfterCrash.Seq)
	require.GreaterOrEqual(t, floorNow.LastIndex, floorAfterCrash.LastIndex)

	kept := readStrictMemSegment(t, fs, segPath(2))
	require.Len(t, kept, 2)
	require.Equal(t, uint64(5), kept[0].Seq)
	require.Equal(t, uint64(6), kept[1].Seq)
}
