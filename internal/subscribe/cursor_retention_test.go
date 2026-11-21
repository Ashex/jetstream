package subscribe_test

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/stretchr/testify/require"
)

func TestResolveCursor_RetentionFloorSeqMatrix(t *testing.T) {
	t.Parallel()
	floor := retention.Floor{Seq: 100, Pruned: true}

	for _, tc := range []struct {
		name         string
		cursor       string
		v2           bool
		floor        retention.Floor
		wantErr      bool
		wantStart    uint64
		wantClamped  bool
		wantErrFloor string
	}{
		{name: "v2 below the floor is refused", cursor: "99", v2: true, floor: floor, wantErr: true, wantErrFloor: "retention floor 100"},
		{name: "v2 at the floor streams from it", cursor: "100", v2: true, floor: floor, wantStart: 100},
		{name: "v2 seq 0 is too old", cursor: "0", v2: true, floor: floor, wantErr: true, wantErrFloor: "retention floor 100"},
		{name: "v1 below the floor clamps silently", cursor: "50", floor: floor, wantStart: 100, wantClamped: true},
		{name: "no floor leaves v2 seq 99 alone", cursor: "99", v2: true, wantStart: 99},
		{name: "no floor leaves v1 seq 50 alone", cursor: "50", wantStart: 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, err := subscribe.ResolveCursor(tc.cursor, subscribe.CursorEnv{NextSeq: 300, RejectBelowFloor: tc.v2, RetentionFloor: tc.floor})
			if tc.wantErr {
				require.ErrorIs(t, err, subscribe.ErrBelowRetention)
				require.ErrorIs(t, err, subscribe.ErrCursorTooOld)
				require.ErrorContains(t, err, tc.wantErrFloor)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantStart, p.StartSeq)
			require.Equal(t, tc.wantClamped, p.Clamped)
		})
	}
}

func TestResolveCursor_RetentionFloorTimestampNotices(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000003.jss"), sealedFixture{
		minSeq: 100, maxSeq: 109,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	m := mustOpenManifest(t, dir)
	beforeArchive := strconv.FormatInt(now-int64(20*time.Hour/time.Microsecond), 10)
	env := subscribe.CursorEnv{Manifest: m, NextSeq: 110, Lookback: 36 * time.Hour}

	p, err := subscribe.ResolveCursor(beforeArchive, env)
	require.NoError(t, err)
	require.Equal(t, subscribe.NoticeBeforeArchive, p.Notice, "no floor: upstream's notice")

	env.RetentionFloor = retention.Floor{Seq: 100, Pruned: true}
	p, err = subscribe.ResolveCursor(beforeArchive, env)
	require.NoError(t, err)
	require.Equal(t, subscribe.NoticeBelowRetention, p.Notice, "the archive knows it pruned")
	require.Equal(t, uint64(100), p.StartSeq)

	env.RetentionFloor = retention.Floor{Seq: 105, Pruned: true}
	p, err = subscribe.ResolveCursor(beforeArchive, env)
	require.NoError(t, err)
	require.Equal(t, uint64(105), p.StartSeq)
	require.True(t, p.Clamped)
	require.Equal(t, subscribe.NoticeBelowRetention, p.Notice)
}
