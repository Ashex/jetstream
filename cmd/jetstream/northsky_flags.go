package main

import (
	"github.com/urfave/cli/v3"
)

// northskyServeFlags are the serve flags the Northsky fork adds to upstream's.
// They live here, not in main.go, so an upstream sync touches one wiring line.
func northskyServeFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "live-only",
			Usage:   "Start a fresh data dir at the relay's live tip: no listHosts/listRepos/getRepo bootstrap and no merge; the archive holds only what it sees from then on. No effect once the data dir is in steady_state; refused on a data dir mid-bootstrap.",
			Sources: cli.EnvVars("JETSTREAM_LIVE_ONLY"),
		},
	}
}
