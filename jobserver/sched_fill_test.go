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
		require.NotNil(t, l)
		return l
	case <-time.After(5 * time.Second):
		t.Fatal("the job did not start")
		return nil
	}
}

// measuredJob starts a job and gives the governor a fake actuator that reports cpus.
func measuredJob(t *testing.T, s *Scheduler, id string, cost, cpus float64) (*Lease, *fakeActuator) {
	t.Helper()
	l := admit(t, s, LeaseRequest{ID: id, Policy: JobPolicy{Cost: cost}})
	a := newFakeActuator(cpus)
	l.AttachActuator(a)
	return l, a
}

func TestJobsStartUntilTheCostsPassTheBudgetPlusATenth(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 10})
	admit(t, s, LeaseRequest{ID: "a", Policy: JobPolicy{Cost: 5}})
	admit(t, s, LeaseRequest{ID: "b", Policy: JobPolicy{Cost: 5.9}})
	assert.InDelta(t, 10.9, s.Status().Reserved, 1e-9, "a tenth more than the budget is booked")

	_, ok, err := s.TryAcquire(LeaseRequest{ID: "c", Policy: JobPolicy{Cost: 0.5}})
	require.NoError(t, err)
	assert.False(t, ok, "past the budget plus a tenth, a job waits")
}

func TestControlsGoOnOnlyAboveTheBudgetPlusATenth(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 2})
	l := admit(t, s, hogRequest)
	a := newFakeActuator(2.15)
	l.AttachActuator(a)

	s.gov.tick()
	assert.True(t, s.Readings().OverBudget, "the report still says when the jobs use more than the budget")
	assert.False(t, a.isApplied(MechPriority), "a tenth past the budget is the target, not a reason to throttle")
	a.set(2.3)
	s.gov.tick()
	assert.True(t, a.isApplied(MechPriority), "above the target, the control goes on")
}

func TestAnIdleHostStartsOneMoreJobAtEachSample(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 4})
	host := &scriptedHost{cpus: 4}
	s.gov.sampler.host = host
	_, aa := measuredJob(t, s, "a", 2, 0.5)
	_, ba := measuredJob(t, s, "b", 2, 0.5)
	assert.InDelta(t, 4, s.Status().Reserved, 1e-9, "the costs of two jobs fill the booked CPUs")
	cost := JobPolicy{Cost: 2}
	// One at a time, so the jobs wait in the order of their names.
	c := acquireAsync(s, context.Background(), LeaseRequest{ID: "c", Policy: cost}, nil)
	waitQueued(t, s, 1)
	d := acquireAsync(s, context.Background(), LeaseRequest{ID: "d", Policy: cost}, nil)
	waitQueued(t, s, 2)
	e := acquireAsync(s, context.Background(), LeaseRequest{ID: "e", Policy: cost}, nil)
	waitQueued(t, s, 3)

	host.set(0.25)
	s.gov.tick()
	assert.Equal(t, 2, s.Status().Running, "the first sample has no host load to go by")
	s.gov.tick()
	assert.Equal(t, 3, s.Status().Running, "one job starts for the idle CPUs, and only one")
	cl := startedSoon(t, c)
	ca := newFakeActuator(0.5)
	cl.AttachActuator(ca)

	host.set(0.375)
	s.gov.tick()
	assert.Equal(t, 4, s.Status().Running, "the next sample starts the next job")
	dl := startedSoon(t, d)
	dl.AttachActuator(newFakeActuator(0.5))

	host.set(0.5)
	for range 3 {
		s.gov.tick()
	}
	assert.Equal(t, 4, s.Status().Running, "the booked costs stop at twice the budget")
	st := s.Status()
	require.Len(t, st.Waiting, 1)
	assert.Equal(t, "e", st.Waiting[0].ID)
	assert.InDelta(t, 8, st.Reserved, 1e-9)
	assert.Zero(t, aa.count("apply priority")+ba.count("apply priority"), "starting a job throttles none")
	select {
	case <-e:
		t.Fatal("the last job started past twice the budget")
	default:
	}
}

func TestNoJobStartsForIdleCPUsWhenTheHostIsFull(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 4})
	host := &scriptedHost{cpus: 4}
	s.gov.sampler.host = host
	measuredJob(t, s, "a", 2, 1)
	measuredJob(t, s, "b", 2, 1)
	acquireAsync(s, context.Background(), LeaseRequest{ID: "c", Policy: JobPolicy{Cost: 2}}, nil)
	waitQueued(t, s, 1)

	// The jobs use CPUs. Other work uses the rest.
	host.set(1)
	for range 3 {
		s.gov.tick()
	}
	assert.Equal(t, 2, s.Status().Running)
}

func TestNoJobStartsForIdleCPUsWhenTheHostLoadIsUnknown(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 4})
	measuredJob(t, s, "a", 2, 0.5)
	measuredJob(t, s, "b", 2, 0.5)
	acquireAsync(s, context.Background(), LeaseRequest{ID: "c", Policy: JobPolicy{Cost: 2}}, nil)
	waitQueued(t, s, 1)

	for range 3 {
		s.gov.tick()
	}
	assert.Equal(t, 2, s.Status().Running, "without the host's counters, the scheduler cannot see idle CPUs")
	assert.NotEmpty(t, s.Readings().HostError)
}

func TestNoJobStartsForIdleCPUsOutsideTheBudget(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 2})
	host := &scriptedHost{cpus: 4}
	s.gov.sampler.host = host
	measuredJob(t, s, "a", 1, 0.95)
	measuredJob(t, s, "b", 1, 0.95)
	acquireAsync(s, context.Background(), LeaseRequest{ID: "c", Policy: JobPolicy{Cost: 1}}, nil)
	waitQueued(t, s, 1)

	// Some of the host's CPUs are idle, and the budget has no room.
	host.set(0.475)
	for range 3 {
		s.gov.tick()
	}
	assert.Equal(t, 2, s.Status().Running)
}

func TestAJobNotMeasuredYetCountsAtItsCostWhenTheSchedulerLooksForIdleCPUs(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 4})
	host := &scriptedHost{cpus: 4}
	s.gov.sampler.host = host
	measuredJob(t, s, "a", 2, 0.5)
	measuredJob(t, s, "b", 2, 0.5)
	// The new job has no actuator, so no sample measures it.
	acquireAsync(s, context.Background(), LeaseRequest{ID: "c", Policy: JobPolicy{Cost: 2}}, nil)
	acquireAsync(s, context.Background(), LeaseRequest{ID: "d", Policy: JobPolicy{Cost: 2}}, nil)
	waitQueued(t, s, 2)

	host.set(0.25)
	for range 4 {
		s.gov.tick()
	}
	assert.Equal(t, 3, s.Status().Running, "the unmeasured job counts at its cost, so no more start")
}
