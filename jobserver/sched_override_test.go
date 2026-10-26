package jobserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapOverrideReplacesTheDeclaredCapUntilCleared(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	_, err := s.Acquire(context.Background(), LeaseRequest{ID: "a"}, nil)
	require.NoError(t, err)
	b := acquireAsync(s, context.Background(), LeaseRequest{ID: "b"}, nil)
	waitQueued(t, s, 1)

	require.NoError(t, s.OverrideMaxConcurrent(2))
	require.NotNil(t, <-b, "a raised override admits the queued lease")
	assert.Equal(t, 2, s.MaxConcurrent())
	st := s.Status()
	assert.Equal(t, 2, st.MaxConcurrent)
	assert.Equal(t, 1, st.MaxConcurrentDeclared)
	require.NotNil(t, st.MaxConcurrentOverride)
	assert.Equal(t, 2, *st.MaxConcurrentOverride)

	s.SetMaxConcurrent(5)
	assert.Equal(t, 2, s.MaxConcurrent(), "the override stays over a new declared cap")

	s.ClearMaxConcurrentOverride()
	st = s.Status()
	assert.Equal(t, 5, st.MaxConcurrent)
	assert.Equal(t, 5, st.MaxConcurrentDeclared)
	assert.Nil(t, st.MaxConcurrentOverride)
}

func TestCapOverrideOfZeroLiftsTheCap(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	require.NoError(t, s.OverrideMaxConcurrent(0))
	for _, id := range []string{"a", "b", "c"} {
		_, ok, err := s.TryAcquire(LeaseRequest{ID: id})
		require.NoError(t, err)
		assert.True(t, ok, "lease %s must start with no cap", id)
	}
	st := s.Status()
	require.NotNil(t, st.MaxConcurrentOverride, "an override of zero is still an override")
	assert.Equal(t, 0, *st.MaxConcurrentOverride)
}

func TestCapOverrideRefusesANegativeCap(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 3})
	require.ErrorIs(t, s.OverrideMaxConcurrent(-1), ErrInvalidLimit)
	assert.Nil(t, s.Status().MaxConcurrentOverride)
}

func TestGroupOverrideReplacesTheDeclaredLimitUntilCleared(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{Groups: map[string]int{"g": 1}})
	_, err := s.Acquire(context.Background(), LeaseRequest{ID: "a", Group: "g"}, nil)
	require.NoError(t, err)
	b := acquireAsync(s, context.Background(), LeaseRequest{ID: "b", Group: "g"}, nil)
	waitQueued(t, s, 1)

	require.NoError(t, s.OverrideGroup("g", 2))
	require.NotNil(t, <-b, "a raised group override admits the queued lease")
	limit, ok := s.HasGroup("g")
	require.True(t, ok)
	assert.Equal(t, 2, limit)
	assert.Equal(t, []GroupInfo{{Name: "g", Limit: 2, Declared: 1, Override: new(2), Active: 2}}, s.Status().Groups)

	s.SetGroups(map[string]int{"g": 4})
	limit, _ = s.HasGroup("g")
	assert.Equal(t, 2, limit, "the override stays over a new declared limit")

	s.ClearGroupOverride("g")
	assert.Equal(t, []GroupInfo{{Name: "g", Limit: 4, Declared: 4, Active: 2}}, s.Status().Groups)
}

func TestGroupOverrideForAnUndeclaredGroupWaitsForItsDeclaration(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	require.NoError(t, s.OverrideGroup("later", 3))
	_, ok := s.HasGroup("later")
	assert.False(t, ok, "an override does not declare a group")
	assert.Empty(t, s.Status().Groups)

	s.SetGroups(map[string]int{"later": 1})
	limit, ok := s.HasGroup("later")
	require.True(t, ok)
	assert.Equal(t, 3, limit)
}

func TestGroupOverrideRefusesALimitBelowOne(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{Groups: map[string]int{"g": 2}})
	require.ErrorIs(t, s.OverrideGroup("g", 0), ErrInvalidLimit)
	limit, _ := s.HasGroup("g")
	assert.Equal(t, 2, limit)
}

func TestConfigGroupsAreDeclaredAndClamped(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{Groups: map[string]int{"g": 0}})
	limit, ok := s.HasGroup("g")
	require.True(t, ok)
	assert.Equal(t, 1, limit)
}

func TestStatusReportsOpenBatchesWithTheirMembers(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	clock := time.Now()
	s.now = func() time.Time { return clock }

	first, err := s.Acquire(context.Background(), LeaseRequest{ID: "w1", Batch: "run"}, nil)
	require.NoError(t, err)
	acquireAsync(s, context.Background(), LeaseRequest{ID: "w2", Batch: "run"}, nil)
	acquireAsync(s, context.Background(), LeaseRequest{ID: "lone", Batch: "other"}, nil)
	waitQueued(t, s, 2)

	assert.Equal(t, []BatchInfo{{Key: "run", Holding: []string{"w1"}, Waiting: []string{"w2"}}}, s.Status().Batches,
		"a batch with no member that holds a slot is not open")

	require.NoError(t, s.OverrideMaxConcurrent(0))
	waitQueued(t, s, 0)
	first.Release()
	for _, l := range s.holders() {
		l.Release()
	}

	ended := clock
	clock = clock.Add(time.Minute)
	closes := ended.Add(BatchGrace)
	assert.Equal(t, []BatchInfo{
		{Key: "other", Holding: []string{}, Waiting: []string{}, Closes: &closes},
		{Key: "run", Holding: []string{}, Waiting: []string{}, Closes: &closes},
	}, s.Status().Batches, "a batch with no members says when it closes")

	clock = ended.Add(BatchGrace)
	assert.Empty(t, s.Status().Batches, "a batch past its grace is gone")
}
