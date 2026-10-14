package jobserver

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestScheduler(t *testing.T, cfg SchedulerConfig) *Scheduler {
	t.Helper()
	cfg.DisableCPU = true
	s := NewScheduler(cfg)
	t.Cleanup(s.Close)
	return s
}

// acquireAsync starts an Acquire and reports its result on a channel.
func acquireAsync(s *Scheduler, ctx context.Context, req LeaseRequest, onQueue func(QueueState)) <-chan *Lease {
	out := make(chan *Lease, 1)
	go func() {
		l, err := s.Acquire(ctx, req, onQueue)
		if err != nil {
			l = nil
		}
		out <- l
	}()
	return out
}

func waitQueued(t *testing.T, s *Scheduler, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return len(s.Status().Waiting) == n },
		5*time.Second, 5*time.Millisecond, "expected %d queued leases", n)
}

func TestSchedulerCapQueuesAndAdmitsByPriorityThenArrival(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	first, err := s.Acquire(context.Background(), LeaseRequest{ID: "first"}, nil)
	require.NoError(t, err)

	var mu sync.Mutex
	states := map[string]QueueState{}
	note := func(id string) func(QueueState) {
		return func(q QueueState) {
			mu.Lock()
			defer mu.Unlock()
			states[id] = q
		}
	}
	low := acquireAsync(s, context.Background(), LeaseRequest{ID: "low"}, note("low"))
	waitQueued(t, s, 1)
	high := acquireAsync(s, context.Background(), LeaseRequest{ID: "high", Priority: 5}, note("high"))
	waitQueued(t, s, 2)

	mu.Lock()
	assert.Equal(t, WaitCap, states["high"].Kind)
	assert.Equal(t, 1, states["high"].Position, "the higher priority goes to the front")
	assert.Equal(t, 2, states["low"].Position)
	assert.Equal(t, []string{"first"}, states["low"].Holders)
	assert.Equal(t, "run cap: 1 of 1 running", states["low"].Reason)
	mu.Unlock()

	first.Release()
	got := <-high
	require.NotNil(t, got)
	assert.Equal(t, "high", got.ID())
	select {
	case <-low:
		t.Fatal("the cap must still hold the lower priority back")
	case <-time.After(50 * time.Millisecond):
	}
	got.Release()
	require.NotNil(t, <-low)
}

func TestSchedulerRaisingTheCapAdmitsQueuedLeases(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	_, err := s.Acquire(context.Background(), LeaseRequest{ID: "a"}, nil)
	require.NoError(t, err)
	b := acquireAsync(s, context.Background(), LeaseRequest{ID: "b"}, nil)
	waitQueued(t, s, 1)
	s.SetMaxConcurrent(2)
	require.NotNil(t, <-b)
	assert.Equal(t, 2, s.Status().Running)
}

func TestSchedulerGroupsLimitTheirMembersOnly(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	s.SetGroups(map[string]int{"deploy": 1})

	_, err := s.Acquire(context.Background(), LeaseRequest{ID: "x", Group: "nope"}, nil)
	require.ErrorIs(t, err, ErrUnknownGroup)

	held, err := s.Acquire(context.Background(), LeaseRequest{ID: "d1", Group: "deploy"}, nil)
	require.NoError(t, err)
	var state QueueState
	var mu sync.Mutex
	d2 := acquireAsync(s, context.Background(), LeaseRequest{ID: "d2", Group: "deploy"}, func(q QueueState) {
		mu.Lock()
		defer mu.Unlock()
		state = q
	})
	waitQueued(t, s, 1)
	free, err := s.Acquire(context.Background(), LeaseRequest{ID: "free"}, nil)
	require.NoError(t, err, "a lease outside the group must not wait behind it")
	free.Release()

	mu.Lock()
	assert.Equal(t, QueueState{Kind: WaitGroup, Key: "deploy", Holders: []string{"d1"}, Position: 1,
		Reason: "group deploy: 1 of 1 running"}, state)
	mu.Unlock()

	held.Release()
	require.NotNil(t, <-d2)
}

func TestSchedulerRemovingAGroupFailsItsQueuedLeases(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	s.SetGroups(map[string]int{"g": 1})
	_, err := s.Acquire(context.Background(), LeaseRequest{ID: "a", Group: "g"}, nil)
	require.NoError(t, err)
	errs := make(chan error, 1)
	go func() {
		_, err := s.Acquire(context.Background(), LeaseRequest{ID: "b", Group: "g"}, nil)
		errs <- err
	}()
	waitQueued(t, s, 1)
	s.SetGroups(nil)
	require.ErrorIs(t, <-errs, ErrGroupRemoved)
}

func TestSchedulerGroupOnlyLeasesIgnoreTheCapAndTheBudget(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1, CPUBudget: 1, DefaultCost: 1})
	s.SetGroups(map[string]int{"m": 2})
	_, err := s.Acquire(context.Background(), LeaseRequest{ID: "run"}, nil)
	require.NoError(t, err)
	_, ok, err := s.TryAcquire(LeaseRequest{ID: "manager", Group: "m", GroupOnly: true})
	require.NoError(t, err)
	assert.True(t, ok, "a group-only lease must not wait on the run cap")
	assert.Equal(t, 1, s.Status().Running, "a group-only lease is not counted")
}

func TestSchedulerCPUBudgetHoldsWhatDoesNotFit(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 2})
	big, err := s.Acquire(context.Background(), LeaseRequest{ID: "big", Policy: JobPolicy{Cost: 4}}, nil)
	require.NoError(t, err, "work larger than the whole budget still runs alone")

	_, ok, err := s.TryAcquire(LeaseRequest{ID: "small", Policy: JobPolicy{Cost: 0.5}})
	require.NoError(t, err)
	assert.False(t, ok, "nothing fits beside work that already fills the budget")
	assert.Empty(t, s.Status().Waiting, "a refused TryAcquire leaves nothing queued")

	var state QueueState
	var mu sync.Mutex
	queued := acquireAsync(s, context.Background(), LeaseRequest{ID: "small", Policy: JobPolicy{Cost: 0.5}}, func(q QueueState) {
		mu.Lock()
		defer mu.Unlock()
		state = q
	})
	waitQueued(t, s, 1)
	mu.Lock()
	assert.Equal(t, WaitCPU, state.Kind)
	assert.Equal(t, "cpu budget: 4 of 2 CPUs reserved, this run needs 0.5", state.Reason)
	mu.Unlock()
	big.Release()
	require.NotNil(t, <-queued)
}

func TestSchedulerOpenBatchGoesFirst(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	member, err := s.Acquire(context.Background(), LeaseRequest{ID: "w1", Batch: "workflow"}, nil)
	require.NoError(t, err)
	other := acquireAsync(s, context.Background(), LeaseRequest{ID: "other", Priority: 10}, nil)
	waitQueued(t, s, 1)
	next := acquireAsync(s, context.Background(), LeaseRequest{ID: "w2", Batch: "workflow"}, nil)
	waitQueued(t, s, 2)
	assert.True(t, s.BatchOpen("workflow"))

	member.Release()
	got := <-next
	require.NotNil(t, got)
	assert.Equal(t, "w2", got.ID(), "a started batch outranks any priority")
	got.Release()
	require.NotNil(t, <-other)
}

func TestSchedulerCancelledWaitLeavesTheQueue(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	_, err := s.Acquire(context.Background(), LeaseRequest{ID: "a"}, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := acquireAsync(s, ctx, LeaseRequest{ID: "b"}, nil)
	waitQueued(t, s, 1)
	cancel()
	assert.Nil(t, <-done)
	assert.Empty(t, s.Status().Waiting)
}

func TestSchedulerRefusesADuplicateLiveID(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	l, err := s.Acquire(context.Background(), LeaseRequest{ID: "same"}, nil)
	require.NoError(t, err)
	_, err = s.Acquire(context.Background(), LeaseRequest{ID: "same"}, nil)
	require.ErrorIs(t, err, ErrDuplicateLease)
	l.Release()
	l.Release()
	_, err = s.Acquire(context.Background(), LeaseRequest{ID: "same"}, nil)
	require.NoError(t, err, "a released ID is free again")
}

func TestSchedulerClosedFailsQueuedAndNewLeases(t *testing.T) {
	s := NewScheduler(SchedulerConfig{MaxConcurrent: 1, DisableCPU: true})
	_, err := s.Acquire(context.Background(), LeaseRequest{ID: "a"}, nil)
	require.NoError(t, err)
	errs := make(chan error, 1)
	go func() {
		_, err := s.Acquire(context.Background(), LeaseRequest{ID: "b"}, nil)
		errs <- err
	}()
	waitQueued(t, s, 1)
	s.Close()
	require.ErrorIs(t, <-errs, ErrSchedulerClosed)
	_, err = s.Acquire(context.Background(), LeaseRequest{ID: "c"}, nil)
	require.ErrorIs(t, err, ErrSchedulerClosed)
}

func TestClassHistoryPricesTheNextRunAndSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "classes.json")
	s := newTestScheduler(t, SchedulerConfig{DefaultCost: 1, HistoryPath: path})
	assert.InDelta(t, 1, s.costOf(LeaseRequest{Class: "lint"}), 1e-9, "a class with no history gets the default")

	s.classes.observe("lint", 1, 4*time.Second, false)
	assert.InDelta(t, 0.25, s.costOf(LeaseRequest{Class: "lint"}), 1e-9)
	s.classes.observe("lint", 4, 4*time.Second, true)
	assert.InDelta(t, 0.25+classWeight*0.75, s.costOf(LeaseRequest{Class: "lint"}), 1e-9)
	assert.InDelta(t, 3, s.costOf(LeaseRequest{Class: "lint", Policy: JobPolicy{Cost: 3}}), 1e-9, "a declared cost wins")

	again := newTestScheduler(t, SchedulerConfig{HistoryPath: path})
	list := again.Classes()
	require.Len(t, list, 1)
	assert.Equal(t, "lint", list[0].Class)
	assert.Equal(t, 2, list[0].Runs)
	assert.Equal(t, 1, list[0].Revoked)
	assert.InDelta(t, 1, list[0].Peak, 1e-9)
	assert.InDelta(t, 5, list[0].CPUSeconds, 1e-9)
}
