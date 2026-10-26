package jobserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTheCapOverrideWinsUntilCleared(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 4})
	s.OverrideMaxConcurrent(1)
	st := s.Status()
	assert.Equal(t, LimitInfo{Declared: 4, Effective: 1, Overridden: true}, st.Cap)
	assert.Equal(t, 1, st.MaxConcurrent)
	s.SetMaxConcurrent(6)
	assert.Equal(t, 1, s.MaxConcurrent(), "a new declared cap does not beat the override")
	s.ClearMaxConcurrentOverride()
	assert.Equal(t, LimitInfo{Declared: 6, Effective: 6}, s.Status().Cap)
}

func TestAGroupOverrideWinsForADeclaredGroupOnly(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	s.SetGroups(map[string]int{"a": 3, "b": 2})
	s.SetGroupOverrides(map[string]int{"a": 1, "ghost": 9, "b": 0})
	got := map[string]GroupInfo{}
	for _, g := range s.Status().Groups {
		got[g.Name] = g
	}
	assert.Equal(t, GroupInfo{Name: "a", Limit: 1, Declared: 3, Overridden: true}, got["a"])
	assert.Equal(t, GroupInfo{Name: "b", Limit: 1, Declared: 2, Overridden: true}, got["b"], "a limit below one is one")
	_, ghost := got["ghost"]
	assert.False(t, ghost, "an override never declares a group")
	s.SetGroups(map[string]int{"a": 5})
	limit, ok := s.HasGroup("a")
	require.True(t, ok)
	assert.Equal(t, 1, limit, "the override survives a new declaration")
	s.SetGroupOverrides(nil)
	limit, _ = s.HasGroup("a")
	assert.Equal(t, 5, limit)
}

func TestStatusListsBatches(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	a := admit(t, s, LeaseRequest{ID: "a", Batch: "b1"})
	_, ok, err := s.TryAcquire(LeaseRequest{ID: "b", Batch: "b1"})
	require.NoError(t, err)
	assert.False(t, ok)
	st := s.Status()
	require.Len(t, st.Batches, 1)
	assert.Equal(t, "b1", st.Batches[0].Key)
	assert.True(t, st.Batches[0].Open)
	assert.Equal(t, []string{"a"}, st.Batches[0].Holding)
	a.Release()
	st = s.Status()
	require.Len(t, st.Batches, 1)
	assert.True(t, st.Batches[0].Open, "a batch stays open for BatchGrace after its last member")
	assert.Empty(t, st.Batches[0].Holding)
}

func TestPriorityTableRanksTheSchedulersWay(t *testing.T) {
	p, err := NewPriorityTable([]PriorityKey{{Name: "infra", Default: 5}, {Name: "hook"}})
	require.NoError(t, err)
	a, err := p.Rank(map[string]float64{"infra": 1})
	require.NoError(t, err)
	b, err := p.Rank(nil)
	require.NoError(t, err)
	assert.Equal(t, []float64{1, 0, 1}, a, "open-batch is added last, at its default")
	assert.Equal(t, -1, CompareRanks(a, b))
	_, err = p.Rank(map[string]float64{"nope": 1})
	assert.ErrorIs(t, err, ErrUnknownPriorityKey)
	_, err = NewPriorityTable([]PriorityKey{{Name: "Bad Name"}})
	assert.Error(t, err)
}
