package main

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// serveOptionsFor runs `jetstream serve <args>` against the test app and
// returns what serveOptionsFromCommand built.
func serveOptionsFor(t *testing.T, args ...string) (jetstreamd.Options, error) {
	t.Helper()
	app := newTestApp()
	var opts jetstreamd.Options
	for _, cmd := range app.Commands {
		if cmd.Name != "serve" {
			continue
		}
		cmd.Action = func(_ context.Context, cmd *cli.Command) error {
			var err error
			opts, err = serveOptionsFromCommand(cmd)
			return err
		}
		break
	}
	err := app.Run(t.Context(), append([]string{"jetstream", "serve"}, args...))
	return opts, err
}

// nolint:paralleltest // mutates the process environment
func TestNorthskyFlags_LiveOnlyDefaultsOff(t *testing.T) {
	withClearedEnv(t)
	opts, err := serveOptionsFor(t)
	require.NoError(t, err)
	require.False(t, opts.LiveOnly)
}

// nolint:paralleltest // mutates the process environment
func TestNorthskyFlags_LiveOnlyFromEnvAndFlag(t *testing.T) {
	withClearedEnv(t)
	t.Setenv("JETSTREAM_LIVE_ONLY", "true")
	opts, err := serveOptionsFor(t)
	require.NoError(t, err)
	require.True(t, opts.LiveOnly)

	t.Setenv("JETSTREAM_LIVE_ONLY", "false")
	opts, err = serveOptionsFor(t, "--live-only")
	require.NoError(t, err)
	require.True(t, opts.LiveOnly)
}

// nolint:paralleltest // mutates the process environment
func TestNorthskyFlags_LiveOnlyRefusesBackfillFlags(t *testing.T) {
	withClearedEnv(t)
	for _, flag := range []string{"--backfill-repos", "--max-backfill-repos"} {
		value := "did:plc:aaa"
		if flag == "--max-backfill-repos" {
			value = "3"
		}
		_, err := serveOptionsFor(t, "--live-only", flag+"="+value)
		require.ErrorContains(t, err, "--live-only", flag)
		require.ErrorContains(t, err, flag, flag)
	}
}
