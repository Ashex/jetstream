package jetstreamd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// seedFloor stores a retention floor in dataDir's metastore before Build opens it.
func seedFloor(t *testing.T, dataDir string, f retention.Floor) {
	t.Helper()
	st, err := pebblestore.Open(dataDir, nil)
	require.NoError(t, err)
	require.NoError(t, retention.Save(t.Context(), st, f))
	require.NoError(t, st.Close())
}

func TestRetentionBuildRefusesRetentionSettingsThatCannotWork(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		mutate func(*Options)
		want   string
	}{
		"a window without compaction": {func(o *Options) {
			o.RetentionWindow = time.Hour
			o.CompactionInterval = 0
		}, "--retention-window needs compaction: --compaction-interval is 0"},
		"a negative window": {func(o *Options) { o.RetentionWindow = -time.Hour }, "--retention-window must be >= 0"},
		"a window with the migrator": {func(o *Options) {
			o.RetentionWindow = time.Hour
			o.CompactionInterval = time.Hour
			o.Migration = DefaultMigrationConfig()
			o.Migration.Enabled = true
			o.Storage.PG.URL, o.Storage.S3.Region, o.Storage.S3.Bucket = "postgres://x", "r", "b"
		}, "--retention-window=0 to migrate"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opts := testOptions(t)
			tc.mutate(&opts)
			_, err := Build(t.Context(), opts)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestRetentionBuildRefusesToMigrateAPrunedArchive(t *testing.T) {
	t.Parallel()

	for name, floor := range map[string]retention.Floor{
		"a floor with a seq": {Seq: 100, WitnessedAt: 5, LastIndex: 3, Pruned: true},
		"a floor of seq 0":   {LastIndex: 0, Pruned: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opts := testOptions(t)
			opts.Migration = DefaultMigrationConfig()
			opts.Migration.Enabled = true
			opts.Storage.PG.URL, opts.Storage.S3.Region, opts.Storage.S3.Bucket = "postgres://x", "r", "b"
			seedFloor(t, opts.DataDir, floor)

			_, err := Build(t.Context(), opts)
			require.ErrorContains(t, err, "pruned by the retention window")
		})
	}
}

func TestRetentionMigrationDryRunSaysAPrunedArchiveCannotMigrate(t *testing.T) {
	t.Parallel()

	logs := &bytes.Buffer{}
	opts := testOptions(t)
	opts.LogLevel = "info"
	opts.LogOutput = logs
	opts.Migration = DefaultMigrationConfig()
	opts.Migration.DryRun = true

	segmentsDir := filepath.Join(opts.DataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	for idx, firstSeq := range map[uint64]uint64{3: 7, 4: 9} {
		w, err := segment.New(segment.Config{Path: filepath.Join(segmentsDir, ingest.SegmentFilename(idx)), MaxEventsPerBlock: 2})
		require.NoError(t, err)
		for seq := firstSeq; seq < firstSeq+2; seq++ {
			_, err := w.Append(segment.Event{Seq: seq, WitnessedAt: int64(seq), Kind: segment.KindCreate, DID: "did:plc:shinji", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("x")})
			require.NoError(t, err)
		}
		_, err = w.Seal()
		require.NoError(t, err)
	}
	seedFloor(t, opts.DataDir, retention.Floor{Seq: 7, WitnessedAt: 6, LastIndex: 2, Pruned: true})

	rt, err := Build(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { rt.closeWithLogging(context.Background()) })
	rt.steadyWriterOpen = func() bool { return true }
	require.NoError(t, rt.inventory(t.Context()))

	out := logs.String()
	require.Contains(t, out, "migration dry run: this archive was pruned below segment 3; --migrate-to-disaggregated refuses it")
	require.Contains(t, out, "ready_to_migrate=false")
}
