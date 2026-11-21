package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// recordingRelay holds subscribeRepos open and answers 500 to everything
// else, remembering every request so a test can assert on them after Run
// returns (t.Fatal is not allowed from a server goroutine).
type recordingRelay struct {
	srv        *httptest.Server
	subscribed chan struct{}
	once       sync.Once

	mu       sync.Mutex
	requests []string
}

func newRecordingRelay(t *testing.T) *recordingRelay {
	t.Helper()
	r := &recordingRelay{subscribed: make(chan struct{})}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.requests = append(r.requests, req.URL.RequestURI())
		r.mu.Unlock()
		if req.URL.Path != "/xrpc/com.atproto.sync.subscribeRepos" {
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		conn, err := websocket.Accept(w, req, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		r.once.Do(func() { close(r.subscribed) })
		<-req.Context().Done()
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *recordingRelay) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.requests...)
}

// runLiveOnlyUntilSubscribed starts the orchestrator with cfg and cancels it
// once the live consumer has dialed the relay, returning Run's error.
func runLiveOnlyUntilSubscribed(t *testing.T, cfg Config, relay *recordingRelay) error {
	t.Helper()
	o, err := New(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	select {
	case <-relay.subscribed:
		cancel()
		err := <-done
		require.True(t, err == nil || errors.Is(err, context.Canceled), "unexpected Run error: %v", err)
		return nil
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run neither subscribed nor returned")
		return nil
	}
}

func liveOnlyConfig(t *testing.T, relay *recordingRelay, st metastore.Store) Config {
	t.Helper()
	return Config{
		DataDir:    t.TempDir(),
		Store:      st,
		RelayURL:   relay.srv.URL,
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Directory:  testIdentityDirectory(),
		Verifier:   newTestVerifier(t, relay.srv.URL),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		LiveOnly:   true,
	}
}

func TestLiveOnly_FreshStoreStartsSteadyAtTheTipWithoutBackfill(t *testing.T) {
	t.Parallel()
	relay := newRecordingRelay(t)
	st := newOrchestratorTestStore(t)

	require.NoError(t, runLiveOnlyUntilSubscribed(t, liveOnlyConfig(t, relay, st), relay))

	phase, err := lifecycle.ReadPhase(t.Context(), st)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, phase)

	watermark, ok, err := loadCompactionWatermark(st)
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, watermark)

	_, ok, err = retention.LoadLiveOnlySince(t.Context(), st)
	require.NoError(t, err)
	require.True(t, ok)

	requests := relay.seen()
	require.Len(t, requests, 1, "only the subscribeRepos dial; got %v", requests)
	require.Equal(t, "/xrpc/com.atproto.sync.subscribeRepos", requests[0], "a live-only start dials with no cursor")
}

func TestLiveOnly_RefusesADataDirMidBootstrap(t *testing.T) {
	t.Parallel()
	for _, phase := range []lifecycle.Phase{lifecycle.PhaseBootstrap, lifecycle.PhaseMerging} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			relay := newRecordingRelay(t)
			st := newOrchestratorTestStore(t)
			require.NoError(t, lifecycle.WritePhase(t.Context(), st, phase, time.Now().UTC()))

			err := runLiveOnlyUntilSubscribed(t, liveOnlyConfig(t, relay, st), relay)
			require.ErrorContains(t, err, string(phase))
			require.ErrorContains(t, err, "live-only")

			got, err := lifecycle.ReadPhase(t.Context(), st)
			require.NoError(t, err)
			require.Equal(t, phase, got)
			require.Empty(t, relay.seen())
		})
	}
}

func TestLiveOnly_SteadyStateRunsAsWithoutIt(t *testing.T) {
	t.Parallel()
	relay := newRecordingRelay(t)
	st := newOrchestratorTestStore(t)
	require.NoError(t, lifecycle.WritePhase(t.Context(), st, lifecycle.PhaseSteadyState, time.Now().UTC()))

	require.NoError(t, runLiveOnlyUntilSubscribed(t, liveOnlyConfig(t, relay, st), relay))

	_, ok, err := retention.LoadLiveOnlySince(t.Context(), st)
	require.NoError(t, err)
	require.False(t, ok, "an existing steady_state dir is not retroactively live-only")
}

func TestLiveOnly_SecondStartKeepsTheFirstMarker(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)

	first := newRecordingRelay(t)
	require.NoError(t, runLiveOnlyUntilSubscribed(t, liveOnlyConfig(t, first, st), first))
	since, ok, err := retention.LoadLiveOnlySince(t.Context(), st)
	require.NoError(t, err)
	require.True(t, ok)

	time.Sleep(5 * time.Millisecond)
	second := newRecordingRelay(t)
	require.NoError(t, runLiveOnlyUntilSubscribed(t, liveOnlyConfig(t, second, st), second))
	again, _, err := retention.LoadLiveOnlySince(t.Context(), st)
	require.NoError(t, err)
	require.True(t, since.Equal(again))
}

func TestLiveOnly_ConfigRefusesBackfillAndDisaggregated(t *testing.T) {
	t.Parallel()
	relay := newRecordingRelay(t)
	st := newOrchestratorTestStore(t)

	cfg := liveOnlyConfig(t, relay, st)
	cfg.MaxBackfillRepos = 5
	_, err := New(cfg)
	require.ErrorIs(t, err, ErrInvalidConfig)
}
