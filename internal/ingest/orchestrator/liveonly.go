package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/retention"
)

// startLiveOnly prepares a fresh data dir to start in steady_state at the
// relay's live tip. The compaction watermark and the live-only marker are
// written first and the phase last, so a crash before the phase write leaves
// a data dir that is still fresh.
func (o *Orchestrator) startLiveOnly(ctx context.Context) error {
	now := time.Now().UTC()
	if err := initCompactionWatermarkFloor(o.cfg.Store, 1); err != nil {
		return err
	}
	if err := retention.SaveLiveOnlySinceIfAbsent(ctx, o.cfg.Store, now); err != nil {
		return fmt.Errorf("orchestrator: live-only: %w", err)
	}
	if err := lifecycle.WritePhase(ctx, o.cfg.Store, lifecycle.PhaseSteadyState, now); err != nil {
		return fmt.Errorf("orchestrator: write phase=steady_state: %w", err)
	}
	o.cfg.Metrics.incTransition("", lifecycle.PhaseSteadyState)
	o.logger.InfoContext(ctx, "starting live-only at the relay tip")
	return nil
}
