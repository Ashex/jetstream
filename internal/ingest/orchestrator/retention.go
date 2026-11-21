package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/retention"
	"github.com/prometheus/client_golang/prometheus"
)

const retentionMetricsSubsystem = "retention"

// retentionMetrics are the window-pruning gauges and counters. The zero
// value (a nil *Metrics) records nothing.
type retentionMetrics struct {
	floorSeq       prometheus.Gauge
	floorWitnessed prometheus.Gauge
	window         prometheus.Gauge
	passes         *prometheus.CounterVec
	segmentsPruned prometheus.Counter
	bytesPruned    prometheus.Counter
}

func newRetentionMetrics() retentionMetrics {
	return retentionMetrics{
		floorSeq: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: retentionMetricsSubsystem,
			Name: "floor_seq",
			Help: "First seq the archive may still hold; 0 until a segment was pruned.",
		}),
		floorWitnessed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: retentionMetricsSubsystem,
			Name: "floor_witnessed_timestamp_seconds",
			Help: "Witnessed time of the newest pruned event, unix seconds; 0 until a segment was pruned.",
		}),
		window: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: retentionMetricsSubsystem,
			Name: "window_seconds",
			Help: "Configured retention window in seconds; 0 keeps everything.",
		}),
		passes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: retentionMetricsSubsystem,
			Name: "passes_total",
			Help: "Retention passes by result: pruned, noop, failed.",
		}, []string{"result"}),
		segmentsPruned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: retentionMetricsSubsystem,
			Name: "segments_pruned_total",
			Help: "Sealed segments deleted by the retention window.",
		}),
		bytesPruned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: retentionMetricsSubsystem,
			Name: "bytes_pruned_total",
			Help: "Bytes of sealed segment files deleted by the retention window.",
		}),
	}
}

func (r retentionMetrics) register(reg prometheus.Registerer) {
	reg.MustRegister(r.floorSeq, r.floorWitnessed, r.window, r.passes, r.segmentsPruned, r.bytesPruned)
}

// SetRetention publishes the configured window and the floor loaded at start.
func (m *Metrics) SetRetention(window time.Duration, floor retention.Floor) {
	if m == nil {
		return
	}
	m.retention.window.Set(window.Seconds())
	m.setRetentionFloor(floor)
}

func (m *Metrics) setRetentionFloor(f retention.Floor) {
	if m == nil {
		return
	}
	m.retention.floorSeq.Set(float64(f.Seq))
	m.retention.floorWitnessed.Set(float64(f.WitnessedAt) / 1e6)
}

func (m *Metrics) incRetentionPass(result string) {
	if m != nil {
		m.retention.passes.WithLabelValues(result).Inc()
	}
}

func (m *Metrics) addRetentionPruned(segments int, bytes int64) {
	if m == nil {
		return
	}
	m.retention.segmentsPruned.Add(float64(segments))
	if bytes > 0 {
		m.retention.bytesPruned.Add(float64(bytes))
	}
}

func (o *Orchestrator) waitForArchiveLoad(ctx context.Context) error {
	if o.cfg.WaitForArchiveLoad == nil {
		return nil
	}
	if err := o.cfg.WaitForArchiveLoad(ctx); err != nil {
		return fmt.Errorf("orchestrator: retention: wait for archive load: %w", err)
	}
	return nil
}

// sealedMainSegments returns the catalog's sealed main segments in index
// order. A list out of order is corrupt internal state, never pruned around.
func (o *Orchestrator) sealedMainSegments() ([]catalog.SegmentView, error) {
	var sealed []catalog.SegmentView
	for _, v := range o.segments().Snapshot().Segments(catalog.Main) {
		if v.State == catalog.Sealed {
			sealed = append(sealed, v)
		}
	}
	if !slices.IsSortedFunc(sealed, func(a, b catalog.SegmentView) int {
		switch {
		case a.Index < b.Index:
			return -1
		case a.Index > b.Index:
			return 1
		}
		return 0
	}) {
		return nil, fmt.Errorf("orchestrator: retention: sealed segments are not in index order")
	}
	return sealed, nil
}

// checkRetentionAllowed refuses a start that would let the defaulted window
// prune an archive the operator never said to prune: only a data dir started
// live-only, or one that already has a floor, is pruned by default. It reads
// phase before upstream's first write, so a refused fresh data dir is left
// untouched.
func (o *Orchestrator) checkRetentionAllowed(ctx context.Context, phase lifecycle.Phase, now time.Time) error {
	// An interrupted live-only start leaves its marker without a phase. A full
	// bootstrap on top of it would later read as "started live-only" and let
	// the defaulted window prune a full archive, so finish it live-only.
	if phase == "" && !o.cfg.LiveOnly && o.cfg.Disaggregated == nil {
		if _, ok, err := retention.LoadLiveOnlySince(ctx, o.cfg.Store); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("orchestrator: a live-only start was interrupted; restart with --live-only")
		}
	}
	if o.cfg.RetentionWindow <= 0 || o.cfg.RetentionWindowExplicit || o.cfg.Disaggregated != nil {
		return nil
	}
	if phase == "" {
		if o.cfg.LiveOnly {
			return nil
		}
		return fmt.Errorf("orchestrator: a fresh data dir with the default --retention-window %s: start with --live-only, or set --retention-window explicitly", o.cfg.RetentionWindow)
	}
	if _, ok, err := retention.LoadLiveOnlySince(ctx, o.cfg.Store); err != nil || ok {
		return err
	}
	floor, err := retention.Load(ctx, o.cfg.Store)
	if err != nil || floor.Pruned {
		return err
	}

	if err := o.waitForArchiveLoad(ctx); err != nil {
		return err
	}
	if err := o.segments().Refresh(ctx); err != nil {
		return fmt.Errorf("orchestrator: retention: refresh segment catalog: %w", err)
	}
	sealed, err := o.sealedMainSegments()
	if err != nil {
		return err
	}
	cutoff := now.Add(-o.cfg.RetentionWindow).UnixMicro()
	var older int
	for _, v := range sealed {
		if v.Header.MaxWitnessedAt < cutoff {
			older++
		}
	}
	oldest := "none"
	if len(sealed) > 0 {
		oldest = time.UnixMicro(sealed[0].Header.MinWitnessedAt).UTC().Format(time.RFC3339)
	}
	return fmt.Errorf("orchestrator: the default --retention-window %s would delete %d sealed segments of this archive (the oldest witnessed %s) because it was not started live-only; set --retention-window explicitly to prune it, or to 0 to keep everything",
		o.cfg.RetentionWindow, older, oldest)
}

// retentionCandidates is the longest prefix of sealed segments that may be
// pruned: older than the window, at or below the compaction watermark (so the
// session-start tombstone rebuild, which reads above it, stays whole), and
// never the newest sealed segment. A prefix only, so the floor never has a
// hole above it. A segment already at or below the stored floor (a power loss
// resurrected its file) qualifies again, since the conditions only get easier
// to meet. An empty segment has no seqs to protect and rides along when
// it sits inside the prefix.
func retentionCandidates(sealed []catalog.SegmentView, watermark uint64, cutoffMicros int64) []catalog.SegmentView {
	var candidates []catalog.SegmentView
	for i, v := range sealed {
		if i == len(sealed)-1 {
			break
		}
		if v.Header.MaxWitnessedAt >= cutoffMicros {
			break
		}
		if v.Header.EventCount > 0 && v.Header.MaxSeq > watermark {
			break
		}
		candidates = append(candidates, v)
	}
	return candidates
}

// runRetentionPass deletes sealed segments older than the window. The new
// floor is made durable before any file goes, so a crash anywhere after it
// leaves a floor every read surface already honours, and recovery finishes
// the removal.
func (o *Orchestrator) runRetentionPass(ctx context.Context, now time.Time) (retErr error) {
	if o.cfg.Disaggregated != nil || o.cfg.RetentionWindow <= 0 {
		return nil
	}
	defer func() {
		if retErr != nil {
			o.cfg.Metrics.incRetentionPass("failed")
		}
	}()
	if err := o.waitForArchiveLoad(ctx); err != nil {
		return err
	}
	if err := o.segments().Refresh(ctx); err != nil {
		return fmt.Errorf("orchestrator: retention: refresh segment catalog: %w", err)
	}
	watermark, ok, err := loadCompactionWatermark(o.cfg.Store)
	if err != nil {
		return err
	}
	if !ok {
		o.cfg.Metrics.incRetentionPass("noop")
		return nil
	}
	floor, err := retention.Load(ctx, o.cfg.Store)
	if err != nil {
		return err
	}
	sealed, err := o.sealedMainSegments()
	if err != nil {
		return err
	}
	candidates := retentionCandidates(sealed, watermark, now.Add(-o.cfg.RetentionWindow).UnixMicro())
	if len(candidates) == 0 {
		o.cfg.Metrics.incRetentionPass("noop")
		return nil
	}

	next := floor
	next.Pruned = true
	next.LastIndex = max(floor.LastIndex, candidates[len(candidates)-1].Index)
	var bytes int64
	idxs := make([]uint64, 0, len(candidates))
	for _, v := range candidates {
		idxs = append(idxs, v.Index)
		bytes += v.Size
		if v.Header.EventCount > 0 && v.Header.MaxSeq+1 > next.Seq {
			next.Seq = v.Header.MaxSeq + 1
			next.WitnessedAt = v.Header.MaxWitnessedAt
		}
	}

	if err := retention.Save(ctx, o.cfg.Store, next); err != nil {
		return err
	}
	if err := o.simulateCrash(ctx, crashpoint.AfterRetentionFloorBeforeRemove); err != nil {
		return err
	}
	if o.cfg.OnRetentionFloor != nil {
		o.cfg.OnRetentionFloor(next)
	}
	o.cfg.Metrics.setRetentionFloor(next)

	if err := o.removePrunedSegments(ctx, idxs); err != nil {
		return err
	}
	o.cfg.Metrics.incRetentionPass("pruned")
	o.cfg.Metrics.addRetentionPruned(len(idxs), bytes)
	o.logger.InfoContext(ctx, "retention pass complete",
		"segments", len(idxs), "bytes", bytes, "floor_seq", next.Seq, "floor_witnessed_at", time.UnixMicro(next.WitnessedAt).UTC())
	return nil
}

// removePrunedSegments takes segments out of the per-process readers, then
// the catalog and disk, then makes the unlinks durable.
func (o *Orchestrator) removePrunedSegments(ctx context.Context, idxs []uint64) error {
	if o.cfg.OnSegmentPruned != nil {
		for _, idx := range idxs {
			if err := o.cfg.OnSegmentPruned(idx); err != nil {
				return fmt.Errorf("orchestrator: retention: forget segment %d: %w", idx, err)
			}
		}
	}
	if err := o.segments().RemoveSegmentFiles(catalog.Main, idxs); err != nil {
		return fmt.Errorf("orchestrator: retention: %w", err)
	}
	if err := o.simulateCrash(ctx, crashpoint.AfterRetentionRemoveBeforeDirSync); err != nil {
		return err
	}
	if err := o.segments().SyncSegmentsDir(catalog.Main); err != nil {
		return fmt.Errorf("orchestrator: retention: %w", err)
	}
	return nil
}

// recoverRetention finishes a pass that crashed between committing the floor
// and removing the files, and removes files a power loss resurrected. Segments
// are matched by index: an empty segment has no seq to compare.
func (o *Orchestrator) recoverRetention(ctx context.Context) error {
	if o.cfg.Disaggregated != nil {
		return nil
	}
	floor, err := retention.Load(ctx, o.cfg.Store)
	if err != nil {
		return err
	}
	if !floor.Pruned {
		return nil
	}
	if err := o.waitForArchiveLoad(ctx); err != nil {
		return err
	}
	if err := o.segments().Refresh(ctx); err != nil {
		return fmt.Errorf("orchestrator: retention: refresh segment catalog: %w", err)
	}
	sealed, err := o.sealedMainSegments()
	if err != nil {
		return err
	}
	var idxs []uint64
	for _, v := range sealed {
		if v.Index <= floor.LastIndex {
			idxs = append(idxs, v.Index)
		}
	}
	if len(idxs) == 0 {
		return nil
	}
	if err := o.removePrunedSegments(ctx, idxs); err != nil {
		return err
	}
	o.logger.InfoContext(ctx, "retention recovery removed segments below the floor", "segments", len(idxs), "floor_seq", floor.Seq)
	return nil
}
