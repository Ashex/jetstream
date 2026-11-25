package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/urfave/cli/v3"
)

const defaultRetentionWindow = 90 * 24 * time.Hour

// northskyServeFlags are the serve flags the Northsky fork adds to upstream's.
// They live here, not in main.go, so an upstream sync touches one wiring line.
func northskyServeFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "live-only",
			Usage:   "Start a fresh data dir at the relay's live tip: no listHosts/listRepos/getRepo bootstrap and no merge; the archive holds only what it sees from then on. No effect once the data dir is in steady_state; refused on a data dir mid-bootstrap.",
			Sources: cli.EnvVars("JETSTREAM_LIVE_ONLY"),
		},
		&cli.StringFlag{
			Name:    "archive-read-cache",
			Usage:   "Local archive read page cache policy: keep preserves caching; drop asks Linux to release fully read pages.",
			Sources: cli.EnvVars("JETSTREAM_ARCHIVE_READ_CACHE"),
			Value:   "keep",
		},
		&cli.DurationFlag{
			Name:    "retention-window",
			Usage:   "Delete sealed segments whose newest event is older than this, at the end of each compaction pass. Requests below the resulting floor are refused, never skipped. 0 keeps everything. Local storage only. The default prunes only a data dir started with --live-only.",
			Sources: cli.EnvVars("JETSTREAM_RETENTION_WINDOW"),
			Value:   defaultRetentionWindow,
		},
	}
}

func archiveReadCacheFromCommand(cmd *cli.Command) (bool, error) {
	switch policy := cmd.String("archive-read-cache"); policy {
	case "keep":
		return false, nil
	case "drop":
		return true, nil
	default:
		return false, fmt.Errorf("serve: --archive-read-cache must be keep or drop, got %q", policy)
	}
}

// retentionWindowFromCommand resolves the window and whether the operator set
// it. A defaulted window is switched off for disaggregated storage (which has
// no pruning), and an explicit one there, or alongside a migration, is refused.
func retentionWindowFromCommand(cmd *cli.Command, disaggregated, migrating bool) (window time.Duration, explicit bool, err error) {
	window = cmd.Duration("retention-window")
	explicit = cmd.IsSet("retention-window")
	switch {
	case window > 0 && disaggregated && !explicit:
		slog.Warn("--retention-window is ignored with --storage=disaggregated: pruning is local storage only", "default", window.String())
		return 0, false, nil
	case window > 0 && disaggregated:
		return 0, false, fmt.Errorf("serve: --retention-window is local storage only; set --retention-window=0 with --storage=disaggregated")
	case window > 0 && migrating:
		return 0, false, fmt.Errorf("serve: --migrate-to-disaggregated cannot run with a retention window: set --retention-window=0 to migrate")
	}
	return window, explicit, nil
}
