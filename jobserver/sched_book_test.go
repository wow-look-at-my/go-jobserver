package jobserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startedSoon returns the lease an Acquire from acquireAsync got.
func startedSoon(t *testing.T, got <-chan *Lease) *Lease {
	t.Helper()
	select {
	case l := <-got:
		return l
	case <-time.After(5 * time.Second):
		t.Fatal("the job did not start")
		return nil
	}
}

func TestBookableIsTheBudgetPlusTwoCPUsOrDoubleASmallBudget(t *testing.T) {
	for budget, want := range map[float64]float64{16: 18, 4: 6, 2: 4, 1: 2, 0.5: 1, 0.001: 0.002} {
		assert.InDelta(t, want, bookable(budget), 1e-9, "budget %v", budget)
	}
}

func TestJobsStartUntilTheCostsPassTheBudgetPlusTwoCPUs(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 10})
	admit(t, s, LeaseRequest{ID: "a", Policy: JobPolicy{Cost: 5}})
	admit(t, s, LeaseRequest{ID: "b", Policy: JobPolicy{Cost: 6.9}})
	assert.InDelta(t, 11.9, s.Status().Reserved, 1e-9, "two CPUs more than the budget are booked")

	_, ok, err := s.TryAcquire(LeaseRequest{ID: "c", Policy: JobPolicy{Cost: 0.5}})
	require.NoError(t, err)
	assert.False(t, ok, "past the budget plus two CPUs, a job waits")
}

func TestNoJobStartsWhileMoreTasksWantACPUThanTheHostHasPlusTwo(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 16})
	host := &scriptedHost{cpus: 4}
	s.gov.sampler.Host = hostOf(host)
	host.setRunning(5)
	l := admit(t, s, LeaseRequest{ID: "a", Policy: JobPolicy{Cost: 1}})
	l.AttachActuator(newFakeActuator(1))
	s.gov.tick()
	require.Equal(t, 4, s.Readings().HostCPUs)
	require.Equal(t, 5, s.Readings().HostRunnable)

	_, ok, err := s.TryAcquire(LeaseRequest{ID: "b", Policy: JobPolicy{Cost: 1}})
	require.NoError(t, err)
	assert.True(t, ok, "five tasks and this job fit the four CPUs plus two")

	got := acquireAsync(s, context.Background(), LeaseRequest{ID: "c", Policy: JobPolicy{Cost: 1}}, nil)
	waitQueued(t, s, 1)
	wait := s.Status().Waiting[0].Wait
	require.NotNil(t, wait)
	assert.Equal(t, WaitCPU, wait.Kind)
	assert.Equal(t, "host load: 5 tasks want a CPU on 4 CPUs, this run needs 1", wait.Reason,
		"the job just started is not measured yet, so it counts at its cost")

	host.setRunning(3)
	s.gov.tick()
	require.NotNil(t, startedSoon(t, got), "the next sample finds room")
}

func TestAJobStartsOnABusyHostWhenNoneRuns(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 16})
	host := &scriptedHost{cpus: 4}
	s.gov.sampler.Host = hostOf(host)
	host.setRunning(100)
	s.gov.tick()

	_, ok, err := s.TryAcquire(LeaseRequest{ID: "a", Policy: JobPolicy{Cost: 1}})
	require.NoError(t, err)
	assert.True(t, ok, "a busy host never starves the scheduler")
	_, ok, err = s.TryAcquire(LeaseRequest{ID: "b", Policy: JobPolicy{Cost: 1}})
	require.NoError(t, err)
	assert.False(t, ok, "with one job running, the busy host holds the next back")
}

func TestAnUnreadableHostLoadHoldsNoJobBack(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 16})
	s.gov.tick()
	for _, id := range []string{"a", "b", "c"} {
		_, ok, err := s.TryAcquire(LeaseRequest{ID: id, Policy: JobPolicy{Cost: 1}})
		require.NoError(t, err)
		assert.True(t, ok, id)
	}
	assert.NotEmpty(t, s.Readings().HostError, "the missing counters are an error, not a quiet host")
}

func TestAGroupOnlyLeaseIgnoresTheHostLoad(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 16})
	s.SetGroups(map[string]int{"managers": 2})
	host := &scriptedHost{cpus: 4}
	s.gov.sampler.Host = hostOf(host)
	host.setRunning(100)
	admit(t, s, LeaseRequest{ID: "a", Policy: JobPolicy{Cost: 1}})
	s.gov.tick()

	_, ok, err := s.TryAcquire(LeaseRequest{ID: "manager", Group: "managers", GroupOnly: true})
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestControlsGoOnOnlyAboveTheBudgetPlusTwoCPUs(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 4})
	l := admit(t, s, hogRequest)
	a := newFakeActuator(5.9)
	l.AttachActuator(a)

	s.gov.tick()
	assert.True(t, s.Readings().OverBudget, "the report still says when the jobs use more than the budget")
	assert.False(t, a.isApplied(MechPriority), "the two booked CPUs are on purpose, so they are no reason to throttle")
	a.set(6.1)
	s.gov.tick()
	assert.True(t, a.isApplied(MechPriority), "above the budget plus two CPUs, the control goes on")
}
