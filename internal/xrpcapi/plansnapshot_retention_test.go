package xrpcapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newRetentionPlanServer serves segments 0 (seqs 1-4) and 1 (seqs 5-8) with
// the retention floor at floorSeq.
func newRetentionPlanServer(t *testing.T, floorSeq uint64) (*httptest.Server, *manifest.Manifest, string, *Metrics) {
	t.Helper()
	dir := t.TempDir()
	writeSealedSegment(t, dir, 0, 1)
	writeSealedSegment(t, dir, 1, 5)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	holder := &retention.Holder{}
	if floorSeq > 0 {
		holder.Raise(retention.Floor{Seq: floorSeq, Pruned: true})
	}
	metrics := NewMetrics(prometheus.NewRegistry())
	srv := New(Config{Src: m, Logger: slog.Default(), Plan: defaultPlanTestConfig(), Retention: holder, Metrics: metrics})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, m, dir, metrics
}

func requireCursorTooOld(t *testing.T, ts *httptest.Server, body map[string]any, wantInMessage ...string) {
	t.Helper()
	resp := doPostJSON(t, ts.URL+planURLPath, body)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var envelope struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	require.Equal(t, "CursorTooOld", envelope.Error)
	for _, want := range wantInMessage {
		require.Contains(t, envelope.Message, want)
	}
}

func TestPlanSnapshot_RetentionRefusesToPlanBelowTheFloor(t *testing.T) {
	t.Parallel()
	ts, _, _, metrics := newRetentionPlanServer(t, 5)

	requireCursorTooOld(t, ts, map[string]any{"afterSeq": 1}, "retention floor 5", "afterSeq=4")
	requireCursorTooOld(t, ts, map[string]any{}, "retention floor 5")
	require.Equal(t, 2.0, testutil.ToFloat64(metrics.belowRetention))

	status, plan := postPlan(t, ts, map[string]any{"afterSeq": 4})
	require.Equal(t, http.StatusOK, status)
	require.Len(t, plan.Segments, 1)
	require.EqualValues(t, 1, plan.Segments[0].Index)
}

func TestPlanSnapshot_RetentionFloorZeroIsUpstreamBehaviour(t *testing.T) {
	t.Parallel()
	ts, _, _, _ := newRetentionPlanServer(t, 0)

	status, plan := postPlan(t, ts, map[string]any{})
	require.Equal(t, http.StatusOK, status)
	require.Len(t, plan.Segments, 2)
}

func TestGetSegment_RetentionPrunedSegmentIsSegmentNotFound(t *testing.T) {
	t.Parallel()
	ts, m, dir, _ := newRetentionPlanServer(t, 5)

	require.NoError(t, m.RemoveSegment(0))
	require.NoError(t, os.Remove(filepath.Join(dir, ingest.SegmentFilename(0))))

	resp := doGet(t, getSegURL(ts.URL, ingest.SegmentFilename(0)))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	var envelope struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	require.Equal(t, "SegmentNotFound", envelope.Error)
}

func TestGetSegment_RetentionDownloadInProgressCompletesAfterPruning(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSealedSegment(t, dir, 0, 1)
	// A segment far larger than the socket buffers, of incompressible bytes,
	// so the response is still streaming when the prune lands.
	path := filepath.Join(dir, ingest.SegmentFilename(1))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	rng := rand.New(rand.NewPCG(7, 8))
	for seq := uint64(5); seq < 25; seq++ {
		payload := make([]byte, 400_000)
		for i := range payload {
			payload[i] = byte(rng.Uint32())
		}
		full, err := w.Append(segment.Event{Seq: seq, WitnessedAt: int64(1_730_000_000_000_000 + seq), Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.bsky.feed.post", Rkey: "rkey", Rev: "rev", Payload: payload})
		require.NoError(t, err)
		if full && seq < 24 {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	want, err := os.ReadFile(path)
	require.NoError(t, err)

	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	ts := httptest.NewServer(New(Config{Src: m, Logger: slog.Default()}).Handler())
	t.Cleanup(ts.Close)

	resp := doGet(t, getSegURL(ts.URL, ingest.SegmentFilename(1)))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	head := make([]byte, 4096)
	_, err = io.ReadFull(resp.Body, head)
	require.NoError(t, err)

	require.NoError(t, m.RemoveSegment(1))
	require.NoError(t, os.Remove(path))

	rest, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, want, append(head, rest...), "the open download completes with the original bytes")
}
