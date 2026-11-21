package subscribe_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type retentionServer struct {
	srv     *httptest.Server
	metrics *subscribe.Metrics
}

// newRetentionServer serves a recent single-segment archive of seqs 200, 210,
// ... 299 with the retention floor set. A nil read uses the real cold reader.
func newRetentionServer(t *testing.T, v2 bool, floor retention.Floor, read func(context.Context, uint64, int) ([]*subscribe.Entry, uint64, error)) *retentionServer {
	t.Helper()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	recentSealedSegment(t, segDir, 200, 299)
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 300)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	makeSteadyState(t, st)

	holder := &retention.Holder{}
	holder.Raise(floor)
	cat := mustCatalog(t, segDir, w)
	if read == nil {
		var writerPtr atomic.Pointer[ingest.Writer]
		writerPtr.Store(w)
		read = subscribe.NewColdReader(subscribe.ColdReaderConfig{
			Catalog: cat, Fetcher: cat.Fetcher(), WriterRef: &writerPtr, BlockCacheBytes: 1 << 20, Retention: holder,
		}).Read
	}
	tail, err := subscribe.New(subscribe.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, read, w.NextSeq)
	require.NoError(t, err)

	metrics := subscribe.NewMetrics(prometheus.NewRegistry())
	srv := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{
		Tail: tail, Store: st, Manifest: m, Writer: w, Catalog: cat, Fetcher: cat.Fetcher(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:   metrics,
		Lookback:  36 * time.Hour,
		V2:        v2,
		Retention: holder,
	}))
	t.Cleanup(srv.Close)
	return &retentionServer{srv: srv, metrics: metrics}
}

func dialRetention(t *testing.T, srv *httptest.Server, cursor string) (*websocket.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/?cursor="+cursor, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn, ctx
}

func TestHandler_RetentionV2SeqCursorBelowTheFloorIsRefused(t *testing.T) {
	t.Parallel()
	rs := newRetentionServer(t, true, retention.Floor{Seq: 220, Pruned: true}, nil)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rs.srv.URL+"/?cursor=1", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	var envelope struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope), "body %q", body)
	require.Equal(t, "CursorTooOld", envelope.Error)
	require.Contains(t, envelope.Message, "retention floor 220")
	require.Equal(t, 1.0, testutil.ToFloat64(rs.metrics.CursorRequests.WithLabelValues("retention_too_old")))
	require.Zero(t, testutil.ToFloat64(rs.metrics.CursorRequests.WithLabelValues("too_old")))
}

func TestHandler_RetentionV1CursorBelowTheFloorStartsAtIt(t *testing.T) {
	t.Parallel()
	rs := newRetentionServer(t, false, retention.Floor{Seq: 220, Pruned: true}, nil)

	conn, ctx := dialRetention(t, rs.srv, "1")
	_, body, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Contains(t, string(body), `"cursor":220`)
	require.Equal(t, 1.0, testutil.ToFloat64(rs.metrics.CursorRequests.WithLabelValues("clamped")))
}

func TestHandler_RetentionV2TimestampCursorGetsAnOutdatedCursorInfoNamingTheFloor(t *testing.T) {
	t.Parallel()
	rs := newRetentionServer(t, true, retention.Floor{Seq: 220, Pruned: true}, nil)

	tooOld := time.Now().Add(-20 * time.Hour).UnixMicro()
	conn, ctx := dialRetention(t, rs.srv, strconv.FormatInt(tooOld, 10))
	_, frame, err := conn.Read(ctx)
	require.NoError(t, err)
	var envelope struct {
		Payload struct {
			Name    string `json:"name"`
			Message string `json:"message"`
		} `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(frame, &envelope))
	require.Equal(t, "OutdatedCursor", envelope.Payload.Name)
	require.Contains(t, envelope.Payload.Message, "retention floor")
	require.Contains(t, envelope.Payload.Message, "starting at seq 220")
}

func TestHandler_RetentionOvertakenV2ReaderGetsAnErrorFrameAndIsCounted(t *testing.T) {
	t.Parallel()
	overtaken := func(_ context.Context, cursor uint64, _ int) ([]*subscribe.Entry, uint64, error) {
		return nil, cursor, fmt.Errorf("%w: cursor %d, floor 260", subscribe.ErrBelowRetention, cursor)
	}
	rs := newRetentionServer(t, true, retention.Floor{Seq: 260, Pruned: true}, overtaken)

	conn, ctx := dialRetention(t, rs.srv, "270")
	_, frame, err := conn.Read(ctx)
	require.NoError(t, err)
	var envelope struct {
		Type    string `json:"$type"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(frame, &envelope), "frame %s", frame)
	require.Equal(t, "error", envelope.Type)
	require.Equal(t, "InternalError", envelope.Error)
	require.Contains(t, envelope.Message, "retention floor 260")
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(rs.metrics.Disconnects.WithLabelValues("below_retention")) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, testutil.ToFloat64(rs.metrics.Disconnects.WithLabelValues("read_error")))
}
