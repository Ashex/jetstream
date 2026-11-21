package xrpcapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// gatedOpener opens segments like FileOpener, but the file's content reader
// serves one chunk and then blocks until released, and records Close. That
// holds the response in flight, with the file open, while the test prunes.
type gatedOpener struct {
	FileOpener
	blocked chan struct{}
	release chan struct{}
	closed  atomic.Bool
}

func (o *gatedOpener) OpenSegment(ctx context.Context, idx uint64) (SegmentFile, error) {
	f, err := o.FileOpener.OpenSegment(ctx, idx)
	if err != nil {
		return nil, err
	}
	return &gatedFile{SegmentFile: f, o: o}, nil
}

type gatedFile struct {
	SegmentFile
	o *gatedOpener
}

func (f *gatedFile) Close() error {
	f.o.closed.Store(true)
	return f.SegmentFile.Close()
}

func (f *gatedFile) Content() io.ReadSeeker {
	return &gatedReader{ReadSeeker: f.SegmentFile.Content(), o: f.o}
}

type gatedReader struct {
	io.ReadSeeker
	o    *gatedOpener
	once sync.Once
	read int
}

const gatedChunk = 16 << 10

func (r *gatedReader) Read(p []byte) (int, error) {
	if r.read >= gatedChunk {
		r.once.Do(func() { close(r.o.blocked) })
		<-r.o.release
	}
	n, err := r.ReadSeeker.Read(p[:min(len(p), gatedChunk)])
	r.read += n
	return n, err
}

func TestGetSegment_RetentionDownloadInProgressCompletesAfterPruning(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSealedSegment(t, dir, 0, 1)
	// Incompressible bytes, so the segment is well past one gated chunk.
	path := filepath.Join(dir, ingest.SegmentFilename(1))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	rng := rand.New(rand.NewPCG(7, 8))
	for seq := uint64(5); seq < 10; seq++ {
		payload := make([]byte, 100_000)
		for i := range payload {
			payload[i] = byte(rng.Uint32())
		}
		full, err := w.Append(segment.Event{Seq: seq, WitnessedAt: int64(1_730_000_000_000_000 + seq), Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.bsky.feed.post", Rkey: "rkey", Rev: "rev", Payload: payload})
		require.NoError(t, err)
		if full && seq < 9 {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	want, err := os.ReadFile(path)
	require.NoError(t, err)

	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	opener := &gatedOpener{FileOpener: FileOpener{Src: m}, blocked: make(chan struct{}), release: make(chan struct{})}
	ts := httptest.NewServer(New(Config{Src: m, Opener: opener, Logger: slog.Default()}).Handler())
	t.Cleanup(ts.Close)

	type result struct {
		body []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/xrpc/network.bsky.jetstream.getSegment?name="+ingest.SegmentFilename(1), nil)
		if err != nil {
			done <- result{err: err}
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		done <- result{body: body, err: err}
	}()

	select {
	case <-opener.blocked:
	case got := <-done:
		t.Fatalf("the response ended before it was held in flight: err=%v bytes=%d", got.err, len(got.body))
	case <-time.After(5 * time.Second):
		t.Fatal("the response never reached the gated chunk")
	}
	require.False(t, opener.closed.Load(), "the response is in flight with its file open")
	require.NoError(t, m.RemoveSegment(1))
	require.NoError(t, os.Remove(path))
	close(opener.release)

	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, want, got.body, "the open download completes with the original bytes")
	require.Eventually(t, opener.closed.Load, 2*time.Second, 5*time.Millisecond)
}

// pruningSource prunes segment 0 inside PlanSnapshot the way a retention pass
// can land between the handler's floor check and its plan: floor first, then
// the manifest entry.
type pruningSource struct {
	*manifest.Manifest
	holder *retention.Holder
}

func (p pruningSource) PlanSnapshot(req manifest.PlanSnapshotRequest) (manifest.PlanSnapshotResult, error) {
	p.holder.Raise(retention.Floor{Seq: 5, Pruned: true})
	if err := p.RemoveSegment(0); err != nil {
		return manifest.PlanSnapshotResult{}, err
	}
	return p.Manifest.PlanSnapshot(req)
}

func TestPlanSnapshot_RetentionPruneLandingMidRequestIsRefusedNotSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSealedSegment(t, dir, 0, 1)
	writeSealedSegment(t, dir, 1, 5)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	metrics := NewMetrics(prometheus.NewRegistry())
	holder := &retention.Holder{}
	srv := New(Config{
		Src: pruningSource{Manifest: m, holder: holder}, Logger: slog.Default(),
		Plan: defaultPlanTestConfig(), Retention: holder, Metrics: metrics,
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	requireCursorTooOld(t, ts, map[string]any{"afterSeq": 0}, "retention floor 5")
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.belowRetention))
}
