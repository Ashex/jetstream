package tombstone

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/bluesky-social/jetstream/segment"
)

func TestTombstoneReplacementOwnsEventKeys(t *testing.T) {
	t.Parallel()
	for name, kind := range map[string]segment.Kind{"update": segment.KindUpdate, "delete": segment.KindDelete, "sync": segment.KindSync, "account": segment.KindAccount} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			raw := "did:plc:rei|app.bsky.feed.post|key"
			fields := strings.Split(strings.Clone(raw), "|")
			next := segment.Event{Seq: 2, Kind: kind, DID: fields[0], Collection: fields[1], Rkey: fields[2]}
			if kind == segment.KindAccount {
				next.Payload = accountPayload(t, false, "deleted")
			}
			first := next
			first.Seq = 1
			set := New()
			if err := set.Observe(&first); err != nil {
				t.Fatal(err)
			}
			before := set.ApproxBytes()
			if err := set.Observe(&next); err != nil {
				t.Fatal(err)
			}
			if set.ApproxBytes() != before || set.Len() != 1 {
				t.Fatal("replacement changed accounting")
			}
			snap := set.Snapshot()
			for k, v := range snap.Records {
				if v != 2 {
					t.Fatal("wrong sequence")
				}
				for i, stored := range []string{k.DID, k.Collection, k.Rkey} {
					if unsafe.StringData(stored) == unsafe.StringData(fields[i]) {
						t.Fatalf("stored field %d aliases input", i)
					}
				}
			}
			for k, v := range snap.DIDs {
				if v.Seq != 2 || v.Reason != name {
					t.Fatal("wrong DID state")
				}
				if unsafe.StringData(k) == unsafe.StringData(next.DID) {
					t.Fatal("stored DID aliases input")
				}
			}
		})
	}
}
