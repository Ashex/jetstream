package jetstream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// drain runs Events to its end or ctx's, returning every event seq and error.
func drain(ctx context.Context, c *Client) (seqs []uint64, errs []error) {
	for batch, err := range c.Events(ctx) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, ev := range batch.Events() {
			seqs = append(seqs, ev.Seq)
		}
	}
	return seqs, errs
}

func TestClientRetention_PlannerRefusalBelowTheFloorIsFatalAndDeliversNothing(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/network.bsky.jetstream.planSnapshot" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"error":"CursorTooOld","message":"afterSeq 0 is below the retention floor 100: plan from afterSeq=99"}`)
	}))
	t.Cleanup(srv.Close)

	c, err := Subscribe(srv.URL, WithAfterSeq(0), WithZstdCompression(false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	seqs, errs := drain(ctx, c)

	require.Empty(t, seqs, "nothing is delivered after the refusal")
	require.NotEmpty(t, errs)
	require.ErrorContains(t, errs[len(errs)-1], "CursorTooOld")
}

func TestClientRetention_APlannedSegmentPrunedBeforeDownloadEndsFatally(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		status int
		body   string
	}{
		"404 SegmentNotFound":        {http.StatusNotFound, `{"error":"SegmentNotFound","message":"segment not found"}`},
		"500 in the opener's window": {http.StatusInternalServerError, `{"error":"InternalError","message":"open segment"}`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/xrpc/network.bsky.jetstream.planSnapshot":
					_, _ = fmt.Fprint(w, `{"plannedThroughSeq":2,"sealedTipSeq":2,"segments":[{"name":"seg_0000000000.jss","index":0,"checksum":"aaaaaaaaaaaaaaaa","minSeq":1,"maxSeq":2,"mode":"segment"}],"stats":{"segmentsExamined":1,"segmentsMatched":1,"blocksMatched":0,"entries":2}}`)
				case "/xrpc/network.bsky.jetstream.getSegment":
					w.WriteHeader(tc.status)
					_, _ = fmt.Fprint(w, tc.body)
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(srv.Close)

			c, err := Subscribe(srv.URL, WithAfterSeq(0), WithSnapshotOnly(), WithZstdCompression(false), WithMaxDownloadAttempts(2), WithSegmentStripes(1))
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			started := time.Now()
			seqs, errs := drain(ctx, c)
			t.Logf("%s: ended after %s with %d error(s), ctx err %v", name, time.Since(started).Round(time.Millisecond), len(errs), ctx.Err())

			require.Empty(t, seqs, "no event is delivered from a pruned segment")
			require.NotEmpty(t, errs, "the client must surface the failure, not end silently")
			require.NoError(t, ctx.Err(), "the client must end by itself, not by this test's timeout")
		})
	}
}
