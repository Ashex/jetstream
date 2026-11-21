package web_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/internal/web"
	"github.com/stretchr/testify/require"
)

func renderStatus(t *testing.T, snap *status.Snapshot) string {
	t.Helper()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: snap},
		Now:         func() time.Time { return time.Date(2026, 5, 25, 12, 0, 5, 0, time.UTC) },
	})
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	return rr.Body.String()
}

func TestHandler_RendersTheRetentionFloorAndWindow(t *testing.T) {
	t.Parallel()
	snap := newFixtureSnap()
	snap.Retention = status.RetentionStats{
		Window:        90 * 24 * time.Hour,
		FloorSeq:      123456,
		FloorAt:       time.Date(2026, 2, 24, 8, 30, 0, 0, time.UTC),
		LiveOnlySince: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	}

	body := renderStatus(t, snap)

	require.Contains(t, body, "Retention")
	require.Contains(t, body, "keeps 90d")
	require.Contains(t, body, "holds seqs from 123,456")
	require.Contains(t, body, "2026-02-24 08:30:00")
	require.Contains(t, body, "started live-only at 2026-02-01 00:00:00")
}

func TestHandler_RendersKeepsEverythingForWindowZero(t *testing.T) {
	t.Parallel()
	snap := newFixtureSnap()
	snap.Retention = status.RetentionStats{}

	body := renderStatus(t, snap)

	require.Contains(t, body, "keeps everything")
	require.Contains(t, body, "nothing pruned yet")
	require.NotContains(t, body, "started live-only")
}
