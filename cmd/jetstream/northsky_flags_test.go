package main

import (
	"context"
	"testing"
	"time"

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

// nolint:paralleltest // mutates the process environment
func TestNorthskyFlags_RetentionWindowDefaultsToNinetyDaysNotExplicit(t *testing.T) {
	withClearedEnv(t)
	opts, err := serveOptionsFor(t)
	require.NoError(t, err)
	require.Equal(t, 2160*time.Hour, opts.RetentionWindow)
	require.False(t, opts.RetentionWindowExplicit)
}

// nolint:paralleltest // mutates the process environment
func TestNorthskyFlags_RetentionWindowSetByFlagOrEnvIsExplicit(t *testing.T) {
	withClearedEnv(t)
	opts, err := serveOptionsFor(t, "--retention-window=48h")
	require.NoError(t, err)
	require.Equal(t, 48*time.Hour, opts.RetentionWindow)
	require.True(t, opts.RetentionWindowExplicit)

	t.Setenv("JETSTREAM_RETENTION_WINDOW", "0")
	opts, err = serveOptionsFor(t)
	require.NoError(t, err)
	require.Zero(t, opts.RetentionWindow)
	require.True(t, opts.RetentionWindowExplicit, "an env source counts as set")
}

// nolint:paralleltest // mutates the process environment
func TestNorthskyFlags_RetentionWindowRefusals(t *testing.T) {
	withClearedEnv(t)
	disaggregated := []string{"--storage=disaggregated", "--pg-url=postgres://127.0.0.1/jetstream", "--s3-region=us-east-1", "--s3-bucket=b"}

	_, err := serveOptionsFor(t, append(disaggregated, "--retention-window=48h")...)
	require.ErrorContains(t, err, "--retention-window is local storage only")

	opts, err := serveOptionsFor(t, disaggregated...)
	require.NoError(t, err)
	require.Zero(t, opts.RetentionWindow, "a defaulted window is off for disaggregated storage")
	require.False(t, opts.RetentionWindowExplicit)

	_, err = serveOptionsFor(t, "--migrate-to-disaggregated")
	require.ErrorContains(t, err, "--retention-window=0 to migrate")
	_, err = serveOptionsFor(t, "--migrate-to-disaggregated", "--retention-window=0")
	require.NoError(t, err)
}

// nolint:paralleltest // mutates the process environment
func TestNorthskyFlags_ArchiveReadCache(t *testing.T) {
	withClearedEnv(t)

	opts, err := serveOptionsFor(t)
	require.NoError(t, err)
	require.False(t, opts.DropArchiveReadCache)

	opts, err = serveOptionsFor(t, "--archive-read-cache=drop")
	require.NoError(t, err)
	require.True(t, opts.DropArchiveReadCache)

	t.Setenv("JETSTREAM_ARCHIVE_READ_CACHE", "drop")
	opts, err = serveOptionsFor(t)
	require.NoError(t, err)
	require.True(t, opts.DropArchiveReadCache)

	opts, err = serveOptionsFor(t, "--archive-read-cache=keep")
	require.NoError(t, err)
	require.False(t, opts.DropArchiveReadCache)

	_, err = serveOptionsFor(t, "--archive-read-cache=invalid")
	require.ErrorContains(t, err, "--archive-read-cache must be keep or drop")
	t.Setenv("JETSTREAM_ARCHIVE_READ_CACHE", "invalid")
	_, err = serveOptionsFor(t)
	require.ErrorContains(t, err, "--archive-read-cache must be keep or drop")
}
