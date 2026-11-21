package oracle

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

const (
	liveOnlyWindow        = 30 * time.Minute
	liveOnlyPassInterval  = 5 * time.Minute
	liveOnlyRounds        = 24 // two hours of fake time
	liveOnlyEventsPerStep = 16
	liveOnlyRerunCommand  = "go test ./internal/oracle -run '^TestOracle_LiveOnlyRetention$' -count=1 -v"
)

// TestOracle_LiveOnlyRetention drives the Northsky fork's live-only start and
// window pruning through the whole runtime in a synctest bubble, with the
// simulator fed in memory and a fake clock, so two hours pass instantly.
//
// Contract: a fresh data dir starts in steady_state at the relay tip with no
// bootstrap; sealed segments older than the window are deleted behind a
// durable floor; every surface below the floor refuses rather than skips; the
// archive above the floor still folds to the simulator's ground truth
// restricted to that window; and a restart keeps the floor.
//
// The process has one synctest bubble and TestOracle_DefaultLifecycle claims
// it when both run together, so this test skips unless it runs on its own:
//
//	go test ./internal/oracle -run '^TestOracle_LiveOnlyRetention$' -count=1 -v
//
// nolint:paralleltest // synctest.Test forbids t.Parallel inside the bubble.
func TestOracle_LiveOnlyRetention(t *testing.T) {
	if synctestBubbleUsed.Swap(true) {
		t.Skip("the oracle's one synctest bubble is already claimed; run this test as its own invocation: " + liveOnlyRerunCommand)
	}
	synctest.Test(t, testOracleLiveOnlyRetention)
}

// liveOnlySim is the in-process simulator relay and PLC, counting the
// requests a live-only start must not make.
type liveOnlySim struct {
	listHosts, listRepos, getRepo atomic.Int64

	mu             sync.Mutex
	subscribeQuery []string
	cleanup        func()
	transport      *http.Client
	url            string
}

func (s *liveOnlySim) subscribes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.subscribeQuery...)
}

func newLiveOnlySim(t *testing.T, w *world.World) *liveOnlySim {
	t.Helper()
	const simURL = "http://sim.invalid"
	s := &liveOnlySim{url: simURL}
	inner := simhttp.NewHandlerWithOptions(w, simURL, simhttp.HandlerOptions{})
	counting := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "com.atproto.sync.listHosts"):
			s.listHosts.Add(1)
		case strings.HasSuffix(r.URL.Path, "com.atproto.sync.listRepos"):
			s.listRepos.Add(1)
		case strings.HasSuffix(r.URL.Path, "com.atproto.sync.getRepo"):
			s.getRepo.Add(1)
		case strings.HasSuffix(r.URL.Path, "com.atproto.sync.subscribeRepos"):
			s.mu.Lock()
			s.subscribeQuery = append(s.subscribeQuery, r.URL.RawQuery)
			s.mu.Unlock()
		}
		inner.ServeHTTP(rw, r)
	})
	ln := newPipeListener()
	srv := &http.Server{Handler: counting}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ln)
	}()
	s.transport = ln.httpClient()
	s.cleanup = func() {
		s.transport.CloseIdleConnections()
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(closeCtx)
		_ = ln.Close()
		<-done
	}
	return s
}

// liveOnlyRuntime is one jetstreamd runtime over the shared data dir.
type liveOnlyRuntime struct {
	rt        *jetstreamd.Runtime
	run       *runtimeRun
	cancel    context.CancelFunc
	publicURL string
	obs       *http.Client
	debug     *http.Client
	stopped   bool
}

func startLiveOnlyRuntime(t *testing.T, cfg Config, sim *liveOnlySim, dataDir string, onEvent func(*segment.Event)) *liveOnlyRuntime {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	publicLn, debugLn := newPipeListener(), newPipeListener()
	lr := &liveOnlyRuntime{cancel: cancel, obs: publicLn.httpClient(), debug: debugLn.httpClient()}

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		UnlimitedResyncs:               true,
		DataDir:                        dataDir,
		RelayURL:                       sim.url,
		PLCURL:                         sim.url,
		OTelServiceName:                "jetstream-oracle",
		LogLevel:                       "warn",
		LogFormat:                      "text",
		LogOutput:                      testWriter{t: t},
		ShutdownTimeout:                5 * time.Second,
		ClientDrainTimeout:             time.Second,
		PublicListener:                 publicLn,
		DebugListener:                  debugLn,
		LiveDial:                       subscribeReposDial(sim.transport),
		HTTPTransport:                  sim.transport.Transport,
		BackfillRetryBaseDelay:         time.Millisecond,
		FailedRepoRetryInterval:        time.Hour,
		CursorLookback:                 36 * time.Hour,
		PlanMaxDIDs:                    xrpcapi.DefaultPlanMaxDIDs,
		PlanMaxCollections:             xrpcapi.DefaultPlanMaxCollections,
		PlanMaxEntries:                 xrpcapi.DefaultPlanMaxEntries,
		PlanWholeSegmentThreshold:      xrpcapi.DefaultPlanWholeSegmentThreshold,
		SubscribeReadLogRetentionBytes: 16 << 20,
		SubscribeBlockCacheBytes:       16 << 20,
		SubscribeReadBatch:             1024,
		SubscribeSlowWindow:            time.Second,
		SubscribeSlowMinRate:           1,
		CursorBlockIndexCacheSize:      32,
		CompactionInterval:             liveOnlyPassInterval,
		CompactionTombstoneCap:         1_000_000,
		LiveOnly:                       true,
		RetentionWindow:                liveOnlyWindow,
		RetentionWindowExplicit:        true,
		OnSteadyStateEvent:             onEvent,
	})
	require.NoError(t, err)
	lr.rt = rt
	lr.run = &runtimeRun{exited: make(chan struct{})}
	go func() {
		lr.run.err = rt.Run(ctx)
		close(lr.run.exited)
	}()
	lr.publicURL = waitForRuntimePublicURL(t, cfg, rt, lr.run)
	return lr
}

func (lr *liveOnlyRuntime) stop(t *testing.T, cfg Config) {
	t.Helper()
	if lr.stopped {
		return
	}
	lr.stopped = true
	lr.cancel()
	waitForRuntimeExit(t, cfg, lr.run)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	require.NoError(t, lr.rt.Close(closeCtx))
	lr.obs.CloseIdleConnections()
	lr.debug.CloseIdleConnections()
}

// scrapeGauges returns every unlabeled sample of the named families, and the
// sum of each family's labeled ones, from the runtime's debug listener.
func (lr *liveOnlyRuntime) scrape(t *testing.T, name string) float64 {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://debug.invalid/metrics", nil)
	require.NoError(t, err)
	resp, err := lr.debug.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)
	family, ok := families[name]
	require.Truef(t, ok, "metric %s is not exposed", name)
	var sum float64
	for _, m := range family.GetMetric() {
		switch {
		case m.GetGauge() != nil:
			sum += m.GetGauge().GetValue()
		case m.GetCounter() != nil:
			sum += m.GetCounter().GetValue()
		}
	}
	return sum
}

type httpOutcome struct {
	status  int
	errName string
	message string
}

func (lr *liveOnlyRuntime) do(t *testing.T, method, path string, body []byte) httpOutcome {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, lr.publicURL+path, bytes.NewReader(body))
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := lr.obs.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var envelope struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(raw, &envelope)
	return httpOutcome{status: resp.StatusCode, errName: envelope.Error, message: envelope.Message}
}

func (lr *liveOnlyRuntime) requireRefusesBelowTheFloor(t *testing.T, floorSeq uint64) {
	t.Helper()
	sub := lr.do(t, http.MethodGet, "/xrpc/network.bsky.jetstream.subscribeEvents?cursor=1", nil)
	require.Equal(t, http.StatusBadRequest, sub.status)
	require.Equal(t, "CursorTooOld", sub.errName)
	require.Contains(t, sub.message, "below the retention floor")

	plan := lr.do(t, http.MethodPost, "/xrpc/network.bsky.jetstream.planSnapshot", []byte(`{"afterSeq":0}`))
	require.Equal(t, http.StatusBadRequest, plan.status)
	require.Equal(t, "CursorTooOld", plan.errName)
	require.Contains(t, plan.message, "retention floor")

	atFloor := lr.do(t, http.MethodPost, "/xrpc/network.bsky.jetstream.planSnapshot", []byte(`{"afterSeq":`+itoa(floorSeq-1)+`}`))
	require.Equal(t, http.StatusOK, atFloor.status, "planning from the floor is allowed")
}

func itoa(n uint64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// windowModel maps archived seqs to upstream relay cursors and decides which
// upstream frames belong to the retained window.
type windowModel struct {
	cursorOfSeq map[uint64]int64
	inWindow    map[int64]bool
	excluded    map[int64]bool // frames whose rows straddle the floor
}

func buildWindowModel(t *testing.T, events []segment.Event, floorSeq uint64) *windowModel {
	t.Helper()
	wm := &windowModel{cursorOfSeq: map[uint64]int64{}, inWindow: map[int64]bool{}, excluded: map[int64]bool{}}
	below, atOrAbove := map[int64]bool{}, map[int64]bool{}
	for _, ev := range events {
		if ev.UpstreamRelayCursor <= 0 {
			continue
		}
		wm.cursorOfSeq[ev.Seq] = ev.UpstreamRelayCursor
		if ev.Seq >= floorSeq {
			atOrAbove[ev.UpstreamRelayCursor] = true
		} else {
			below[ev.UpstreamRelayCursor] = true
		}
	}
	for c := range atOrAbove {
		if below[c] {
			wm.excluded[c] = true
			continue
		}
		wm.inWindow[c] = true
	}
	return wm
}

// expectedWindowFold folds the simulator's own firehose frames whose upstream
// cursor is in the window. It never reads what Jetstream stored, so it can see
// what Jetstream lost.
func (wm *windowModel) expectedWindowFold(t *testing.T, w *world.World) map[RecordKey]uint64 {
	t.Helper()
	frames, err := w.FirehoseRange(0, int(w.CurrentSeq())+1)
	require.NoError(t, err)
	var observed []ObservedEvent
	for _, frame := range frames {
		evt, err := decodeOracleFirehoseFrame(frame)
		require.NoError(t, err)
		if !wm.inWindow[evt.Seq] {
			continue
		}
		segEvents, err := expectedSegmentEventsFromFirehoseEvent(w, evt)
		require.NoError(t, err)
		for i := range segEvents {
			segEvents[i].Seq = uint64(evt.Seq)
		}
		observed = append(observed, observedEventsFromSegments(segEvents)...)
	}
	require.NotEmpty(t, observed, "the window holds no simulator events: the scenario is vacuous")
	live, err := groundTruthLive(observed)
	require.NoError(t, err)
	return live
}

// foldArchived folds events read back from Jetstream, translated to upstream
// cursors, keeping only the window.
func (wm *windowModel) foldArchived(t *testing.T, events []ObservedEvent) map[RecordKey]uint64 {
	t.Helper()
	var mapped []ObservedEvent
	for _, ev := range events {
		cursor, ok := wm.cursorOfSeq[ev.Seq]
		require.Truef(t, ok, "archived seq %d has no upstream cursor", ev.Seq)
		if !wm.inWindow[cursor] {
			continue
		}
		ev.Seq = uint64(cursor)
		mapped = append(mapped, ev)
	}
	live, err := groundTruthLive(mapped)
	require.NoError(t, err)
	return live
}

func testOracleLiveOnlyRetention(t *testing.T) {
	cfg, err := defaultLifecycleConfig(os.LookupEnv, true)
	require.NoError(t, err)

	advanceClockToSimulatorEpoch()
	bubbleDrain = synctest.Wait
	defer func() { bubbleDrain = nil }()

	// The simulator relay replays its whole retained history to a cursorless
	// subscriber, where bsky.network starts at the live tip. The world has made
	// no firehose history before the runtime starts, so the replay is empty and
	// this is a live start. Every expected set below is by mapped seq anyway.
	simCfg := world.DefaultConfig()
	simCfg.DataDir = t.TempDir()
	simCfg.Seed = cfg.Seed
	simCfg.Accounts = cfg.Accounts
	simCfg.InitialRecords = 0
	simCfg.InitialRecordsMin = cfg.MinInitialRecords
	simCfg.InitialRecordsMax = cfg.MaxInitialRecords
	simCfg.FirehoseHistory = 10_000
	w, err := world.New(t.Context(), simCfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(t.Context(), slog.Default()))
	fan := fanout.New(16384)
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(cfg.Seed^0xfeedf00d, cfg.Seed^0xc0ffee)), fan))
	require.Zero(t, w.CurrentSeq(), "the world must have no firehose history before the runtime starts")

	sim := newLiveOnlySim(t, w)
	defer sim.cleanup()

	dataDir := t.TempDir()
	events := newEventLogRecorder()
	acked := newSeqAck()
	onEvent := func(ev *segment.Event) {
		events.Observe(ev)
		acked.Observe(ev)
	}

	lr := startLiveOnlyRuntime(t, cfg, sim, dataDir, onEvent)
	defer lr.stop(t, cfg)

	// 1. Live start.
	require.Equal(t, 3.0, lr.scrape(t, "jetstream_orchestrator_phase"), "steady_state from the first start")
	drain()
	queries := sim.subscribes()
	require.NotEmpty(t, queries)
	require.NotContains(t, queries[0], "cursor", "a live-only start subscribes with no cursor, at the relay tip")

	// Two hours of live traffic, a pass interval apart.
	firstCursor := w.CurrentSeq() + 1
	for range liveOnlyRounds {
		generateN(t, w, liveOnlyEventsPerStep)
		require.NoError(t, acked.WaitContiguousFrom(t.Context(), firstCursor, w.CurrentSeq(), oracleWaitTimeout(cfg)))
		time.Sleep(liveOnlyPassInterval)
	}
	drain()

	require.Zero(t, sim.listHosts.Load(), "no listHosts")
	require.Zero(t, sim.listRepos.Load(), "no listRepos")
	syncRows := 0
	for _, ev := range events.snapshotEvents() {
		if ev.Kind == segment.KindSync {
			syncRows++
		}
	}
	require.LessOrEqual(t, int(sim.getRepo.Load()), syncRows, "getRepo only for #sync resyncs the world generated")

	// 2. Pruning happened, and by a margin the test can see.
	floorSeq := uint64(lr.scrape(t, "jetstream_retention_floor_seq"))
	require.Greater(t, floorSeq, uint64(1), "the floor never rose: pruning is vacuous")
	require.GreaterOrEqual(t, lr.scrape(t, "jetstream_retention_segments_pruned_total"), 2.0, "at least two segments were pruned")
	require.Positive(t, lr.scrape(t, "jetstream_retention_bytes_pruned_total"))

	// 3. Public surfaces refuse below the floor.
	lr.requireRefusesBelowTheFloor(t, floorSeq)

	archived := events.snapshotEvents()
	wm := buildWindowModel(t, archived, floorSeq)
	want := wm.expectedWindowFold(t, w)
	require.Less(t, len(wm.inWindow), int(w.CurrentSeq()), "the window must be a strict part of the history")

	// The bundled client from just below the floor folds to the window model.
	client, err := jetstream.Subscribe(lr.publicURL,
		jetstream.WithHTTPClient(lr.obs), jetstream.WithAfterSeq(floorSeq-1), jetstream.WithBatchSize(64))
	require.NoError(t, err)
	clientCtx, cancelClient := context.WithTimeout(t.Context(), oracleWaitTimeout(cfg))
	var clientEvents []ObservedEvent
	converged := false
	for batch, err := range client.Events(clientCtx) {
		if err != nil {
			continue
		}
		for _, ev := range batch.Events() {
			clientEvents = append(clientEvents, observedEventFromClient(t, ev))
		}
		got := wm.foldArchived(t, EventsSortedBySeq(clientEvents))
		if foldsEqual(want, got) {
			converged = true
			break
		}
	}
	cancelClient()
	_ = client.Close()
	require.True(t, converged, "the client from afterSeq=%d did not fold to the model restricted to the window", floorSeq-1)

	// From afterSeq 0 the client ends with a fatal CursorTooOld and delivers nothing.
	fromZero, err := jetstream.Subscribe(lr.publicURL, jetstream.WithHTTPClient(lr.obs), jetstream.WithAfterSeq(0))
	require.NoError(t, err)
	zeroCtx, cancelZero := context.WithTimeout(t.Context(), oracleWaitTimeout(cfg))
	delivered := 0
	var zeroErrs []error
	for batch, err := range fromZero.Events(zeroCtx) {
		if err != nil {
			zeroErrs = append(zeroErrs, err)
			continue
		}
		delivered += len(batch.Events())
	}
	cancelZero()
	_ = fromZero.Close()
	require.Zero(t, delivered)
	require.NotEmpty(t, zeroErrs)
	require.ErrorContains(t, zeroErrs[len(zeroErrs)-1], "CursorTooOld")

	// Shut down, then read the durable state directly.
	lr.stop(t, cfg)
	store, err := pebblestore.Open(dataDir, nil)
	require.NoError(t, err)
	floor, err := retention.Load(t.Context(), store)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	require.True(t, floor.Pruned)
	require.Equal(t, floorSeq, floor.Seq)

	files, err := ingest.SegmentFilesFS(vfs.Default, dataDir+"/segments")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		require.Greaterf(t, f.Idx, floor.LastIndex, "segment %d at or below the floor's last index is still on disk", f.Idx)
	}
	onDisk, err := ObserveSegments(dataDir)
	require.NoError(t, err)
	for _, ev := range onDisk {
		require.GreaterOrEqualf(t, ev.Seq, floor.Seq, "seq %d below the floor is still on disk", ev.Seq)
	}
	require.True(t, foldsEqual(want, wm.foldArchived(t, EventsSortedBySeq(onDisk))),
		"the retained segments do not fold to the simulator's ground truth restricted to the window")

	// 4. Restart: the floor and the refusals hold, and nothing re-bootstraps.
	restarted := startLiveOnlyRuntime(t, cfg, sim, dataDir, onEvent)
	defer restarted.stop(t, cfg)
	require.Equal(t, 3.0, restarted.scrape(t, "jetstream_orchestrator_phase"))
	require.GreaterOrEqual(t, uint64(restarted.scrape(t, "jetstream_retention_floor_seq")), floor.Seq)
	restarted.requireRefusesBelowTheFloor(t, floor.Seq)
	require.Zero(t, sim.listHosts.Load())
	require.Zero(t, sim.listRepos.Load())
}

func foldsEqual(want, got map[RecordKey]uint64) bool {
	if len(want) != len(got) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
