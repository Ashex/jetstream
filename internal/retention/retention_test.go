package retention

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
)

func TestFloorRoundTripAgainstHandWrittenBytes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := memstore.New()

	got, err := Load(ctx, s)
	require.NoError(t, err)
	require.Equal(t, Floor{}, got)
	require.False(t, got.Pruned)

	require.NoError(t, Save(ctx, s, Floor{Seq: 0x0102, WitnessedAt: 0x0304, LastIndex: 5}))
	raw, err := s.Get(ctx, []byte("retention/floor"))
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x01,
		0x02, 0x01, 0, 0, 0, 0, 0, 0,
		0x04, 0x03, 0, 0, 0, 0, 0, 0,
		0x05, 0, 0, 0, 0, 0, 0, 0,
	}, raw)

	got, err = Load(ctx, s)
	require.NoError(t, err)
	require.Equal(t, Floor{Seq: 0x0102, WitnessedAt: 0x0304, LastIndex: 5, Pruned: true}, got)
}

func TestSaveRefusesALowerFloor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := memstore.New()
	require.NoError(t, Save(ctx, s, Floor{Seq: 100, WitnessedAt: 9, LastIndex: 4}))

	require.Error(t, Save(ctx, s, Floor{Seq: 99, WitnessedAt: 9, LastIndex: 4}))
	require.Error(t, Save(ctx, s, Floor{Seq: 100, WitnessedAt: 9, LastIndex: 3}))

	got, err := Load(ctx, s)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got.Seq)
	require.Equal(t, uint64(4), got.LastIndex)
}

func TestSaveAcceptsEqualSeqWithHigherLastIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := memstore.New()
	require.NoError(t, Save(ctx, s, Floor{Seq: 100, WitnessedAt: 9, LastIndex: 4}))
	require.NoError(t, Save(ctx, s, Floor{Seq: 100, WitnessedAt: 9, LastIndex: 6}))
	got, err := Load(ctx, s)
	require.NoError(t, err)
	require.Equal(t, uint64(6), got.LastIndex)
}

func TestSaveAcceptsASeqZeroFloorFirst(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := memstore.New()
	require.NoError(t, Save(ctx, s, Floor{LastIndex: 0}))
	got, err := Load(ctx, s)
	require.NoError(t, err)
	require.True(t, got.Pruned)
	require.Zero(t, got.Seq)
}

func TestLoadRejectsCorruptValues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, val := range map[string][]byte{
		"short":   {0x01, 1, 2},
		"version": append([]byte{0x02}, make([]byte, 24)...),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := memstore.New()
			require.NoError(t, s.Set(ctx, []byte("retention/floor"), val))
			_, err := Load(ctx, s)
			require.Error(t, err)
		})
	}
}

func TestLiveOnlySinceKeepsTheFirstTime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := memstore.New()

	_, ok, err := LoadLiveOnlySince(ctx, s)
	require.NoError(t, err)
	require.False(t, ok)

	first := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	require.NoError(t, SaveLiveOnlySinceIfAbsent(ctx, s, first))
	require.NoError(t, SaveLiveOnlySinceIfAbsent(ctx, s, first.Add(time.Hour)))

	got, ok, err := LoadLiveOnlySince(ctx, s)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, first.Equal(got))
}

func TestHolderRaiseIsMonotonicUnderConcurrentRaises(t *testing.T) {
	t.Parallel()
	var h Holder
	require.Equal(t, Floor{}, h.Floor())

	var wg sync.WaitGroup
	for i := uint64(1); i <= 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Raise(Floor{Seq: i, LastIndex: i, Pruned: true})
		}()
	}
	wg.Wait()
	require.Equal(t, uint64(200), h.Floor().Seq)

	h.Raise(Floor{Seq: 5, LastIndex: 5, Pruned: true})
	require.Equal(t, uint64(200), h.Floor().Seq)
}

func TestNilHolderIsTheZeroFloor(t *testing.T) {
	t.Parallel()
	var h *Holder
	require.Equal(t, Floor{}, h.Floor())
}
