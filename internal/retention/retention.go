// Package retention holds the Northsky fork's archive retention floor and the
// live-only start marker.
//
// The floor is the durable statement "the archive may no longer hold seqs
// below Seq". It is committed before any sealed segment is removed, so every
// read surface can refuse or announce a request below it instead of silently
// skipping pruned data. It only rises.
package retention

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

const (
	floorKey     = "retention/floor"
	floorVersion = 0x01
	floorLen     = 1 + 8 + 8 + 8

	liveOnlySinceKey     = "retention/live_only_since"
	liveOnlySinceVersion = 0x01
)

// Floor is the retention floor. The zero value means nothing was pruned.
type Floor struct {
	// Seq is the first seq the archive may still hold; 0 means nothing was pruned.
	Seq uint64
	// WitnessedAt is the newest pruned non-empty event's witnessed time, unix µs.
	WitnessedAt int64
	// LastIndex is the index of the last pruned segment. It is meaningful only
	// when Pruned, because index 0 is a real segment. Recovery works by index
	// since an empty sealed segment has MaxSeq 0.
	LastIndex uint64
	// Pruned reports that a floor is stored, even one with Seq 0 (a first
	// pass that pruned only empty segments).
	Pruned bool
}

// Load reads the stored floor; an absent key is the zero floor.
func Load(ctx context.Context, s metastore.Store) (Floor, error) {
	val, err := s.Get(ctx, []byte(floorKey))
	if errors.Is(err, metastore.ErrNotFound) {
		return Floor{}, nil
	}
	if err != nil {
		return Floor{}, fmt.Errorf("retention: load floor: %w", err)
	}
	return decodeFloor(val)
}

// Save stores f durably. It refuses a floor with a lower Seq or a lower
// LastIndex than the stored one: the floor only rises.
func Save(ctx context.Context, s metastore.Store, f Floor) error {
	cur, err := Load(ctx, s)
	if err != nil {
		return err
	}
	if cur.Pruned {
		if f.Seq < cur.Seq {
			return fmt.Errorf("retention: floor seq %d is below the stored floor %d", f.Seq, cur.Seq)
		}
		if f.LastIndex < cur.LastIndex {
			return fmt.Errorf("retention: floor last index %d is below the stored last index %d", f.LastIndex, cur.LastIndex)
		}
	}
	buf := make([]byte, floorLen)
	buf[0] = floorVersion
	binary.LittleEndian.PutUint64(buf[1:], f.Seq)
	binary.LittleEndian.PutUint64(buf[9:], uint64(f.WitnessedAt)) //nolint:gosec // round-trips a signed unix µs
	binary.LittleEndian.PutUint64(buf[17:], f.LastIndex)
	if err := s.Set(ctx, []byte(floorKey), buf); err != nil {
		return fmt.Errorf("retention: save floor: %w", err)
	}
	return nil
}

func decodeFloor(val []byte) (Floor, error) {
	if len(val) != floorLen {
		return Floor{}, fmt.Errorf("retention: %s has wrong length %d (want %d)", floorKey, len(val), floorLen)
	}
	if val[0] != floorVersion {
		return Floor{}, fmt.Errorf("retention: %s has unknown version 0x%02x", floorKey, val[0])
	}
	return Floor{
		Seq:         binary.LittleEndian.Uint64(val[1:]),
		WitnessedAt: int64(binary.LittleEndian.Uint64(val[9:])), //nolint:gosec // round-trips a signed unix µs
		LastIndex:   binary.LittleEndian.Uint64(val[17:]),
		Pruned:      true,
	}, nil
}

// LoadLiveOnlySince returns when this data dir was first started live-only.
func LoadLiveOnlySince(ctx context.Context, s metastore.Store) (time.Time, bool, error) {
	v, ok, err := metastore.GetVersionedUint64LE(ctx, s, liveOnlySinceKey, liveOnlySinceVersion)
	if err != nil || !ok {
		return time.Time{}, false, err
	}
	return time.UnixMicro(int64(v)).UTC(), true, nil //nolint:gosec // unix µs written by SaveLiveOnlySinceIfAbsent
}

// SaveLiveOnlySinceIfAbsent records t as the live-only start; the first write wins.
func SaveLiveOnlySinceIfAbsent(ctx context.Context, s metastore.Store, t time.Time) error {
	_, ok, err := LoadLiveOnlySince(ctx, s)
	if err != nil || ok {
		return err
	}
	return metastore.SetVersionedUint64LE(ctx, s, liveOnlySinceKey, liveOnlySinceVersion, uint64(t.UnixMicro())) //nolint:gosec // post-1970 time
}

// Source is what read surfaces consult for the current floor.
type Source interface {
	Floor() Floor
}

// Holder is the per-process, concurrency-safe copy of the floor. A nil
// *Holder answers the zero floor.
type Holder struct {
	cur atomic.Pointer[Floor]
}

// Floor returns the current floor.
func (h *Holder) Floor() Floor {
	if h == nil {
		return Floor{}
	}
	if p := h.cur.Load(); p != nil {
		return *p
	}
	return Floor{}
}

// Raise publishes f unless it is lower in Seq or LastIndex than the current floor.
func (h *Holder) Raise(f Floor) {
	for {
		old := h.cur.Load()
		if old != nil && (f.Seq < old.Seq || (f.Pruned && old.Pruned && f.LastIndex < old.LastIndex)) {
			return
		}
		next := f
		if h.cur.CompareAndSwap(old, &next) {
			return
		}
	}
}
