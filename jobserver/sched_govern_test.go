package jobserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeActuator reports a set CPU use and records what the governor asks of it.
type fakeActuator struct {
	mu      sync.Mutex
	cpus    float64
	noRead  bool
	capOut  Outcome
	applied map[Mechanism]bool
	calls   []string
	capped  float64
}

func newFakeActuator(cpus float64) *fakeActuator {
	return &fakeActuator{cpus: cpus, capOut: OutcomeApplied, applied: map[Mechanism]bool{}}
}

func (f *fakeActuator) Usage() (float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cpus, !f.noRead
}

func (f *fakeActuator) Apply(m Mechanism, _ JobPolicy) (Outcome, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied[m] = true
	f.calls = append(f.calls, "apply "+string(m))
	return OutcomeApplied, "ok"
}

func (f *fakeActuator) Revert(m Mechanism) (Outcome, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.applied, m)
	f.calls = append(f.calls, "revert "+string(m))
	return OutcomeReverted, "ok"
}

func (f *fakeActuator) Cap(cpus float64) (Outcome, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "cap")
	if f.capOut != OutcomeApplied {
		return f.capOut, "this target has no CPU quota"
	}
	f.capped = cpus
	return OutcomeApplied, "ok"
}

func (f *fakeActuator) set(cpus float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cpus = cpus
}

func (f *fakeActuator) isApplied(m Mechanism) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied[m]
}

func (f *fakeActuator) capAt() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.capped
}

var throttlePolicy = JobPolicy{Priority: PrioritySettings{Enabled: true, Nice: 10}}

func TestGovernorThrottlesOnlyTheLeaseAboveItsCost(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 2})
	quiet, err := s.Acquire(context.Background(), LeaseRequest{ID: "quiet", Policy: JobPolicy{Cost: 1, Priority: throttlePolicy.Priority}}, nil)
	require.NoError(t, err)
	hog, err := s.Acquire(context.Background(), LeaseRequest{ID: "hog", Policy: JobPolicy{Cost: 0.5, Priority: throttlePolicy.Priority}}, nil)
	require.NoError(t, err)
	qa, ha := newFakeActuator(0.9), newFakeActuator(3)
	quiet.AttachActuator(qa)
	hog.AttachActuator(ha)

	s.gov.tick()
	report := s.Readings()
	assert.True(t, report.OverBudget)
	assert.InDelta(t, 3.9, report.Measured, 1e-9)
	assert.True(t, ha.isApplied(MechPriority), "the lease above its cost is the one throttled")
	assert.False(t, qa.isApplied(MechPriority), "a lease within its cost is left alone")
	require.Len(t, s.Controls(), 1)
	assert.Equal(t, "hog", s.Controls()[0].Job)

	ha.set(0.5)
	s.gov.tick()
	assert.False(t, ha.isApplied(MechPriority), "back under budget, the control is reverted")
	assert.Empty(t, s.Controls())

	hog.Release()
	assert.Greater(t, hog.CPUSeconds(), 0.0)
	_, ok := s.classes.cost("")
	assert.False(t, ok, "a lease without a class records no history")
}

func TestGovernorThrottlesEveryLeaseWhenNoneIsAboveItsCost(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 1})
	var acts []*fakeActuator
	for _, id := range []string{"a", "b"} {
		l, err := s.Acquire(context.Background(), LeaseRequest{ID: id, Policy: JobPolicy{Cost: 1, Priority: throttlePolicy.Priority}}, nil)
		require.NoError(t, err)
		a := newFakeActuator(0.8)
		l.AttachActuator(a)
		acts = append(acts, a)
	}
	s.gov.tick()
	for _, a := range acts {
		assert.True(t, a.isApplied(MechPriority))
	}
}

func TestGovernorCapsALeaseAboveItsCPULimit(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	l, err := s.Acquire(context.Background(), LeaseRequest{ID: "busy", Limits: Limits{MaxCPUs: 1.5}}, nil)
	require.NoError(t, err)
	a := newFakeActuator(3)
	l.AttachActuator(a)
	s.gov.tick()
	assert.InDelta(t, 1.5, a.capAt(), 1e-9)
	controls := s.Controls()
	require.Len(t, controls, 1)
	assert.Equal(t, MechCap, controls[0].Mechanism)
	select {
	case <-l.Revoked():
		t.Fatal("a capped lease is not revoked")
	default:
	}
	l.Release()
	assert.InDelta(t, 0, a.capAt(), 1e-9, "release lifts the cap")
	assert.Empty(t, s.Controls())
}

func TestGovernorRevokesAnUncappableLeaseAfterGrace(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	clock := time.Now()
	s.now = func() time.Time { return clock }
	l, err := s.Acquire(context.Background(), LeaseRequest{ID: "spin", Class: "spinner",
		Limits: Limits{MaxCPUs: 1, Grace: 10 * time.Second}}, nil)
	require.NoError(t, err)
	a := newFakeActuator(2)
	a.capOut = OutcomeNotApplicable
	l.AttachActuator(a)

	s.gov.tick()
	clock = clock.Add(5 * time.Second)
	s.gov.tick()
	assert.Empty(t, l.RevokeReason(), "inside the grace the lease goes on")

	clock = clock.Add(6 * time.Second)
	s.gov.tick()
	select {
	case <-l.Revoked():
	default:
		t.Fatal("a lease over its limit past the grace must be revoked")
	}
	assert.Equal(t, "used 2 CPUs for 11s, over its limit of 1", l.RevokeReason())
	l.Release()
	list := s.Classes()
	require.Len(t, list, 1)
	assert.Equal(t, 1, list[0].Revoked)
}

func TestGovernorRevokesALeasePastItsCPUSeconds(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	clock := time.Now()
	s.now = func() time.Time { return clock }
	l, err := s.Acquire(context.Background(), LeaseRequest{ID: "long", Limits: Limits{MaxCPUSeconds: 10}}, nil)
	require.NoError(t, err)
	l.AttachActuator(newFakeActuator(2))
	clock = clock.Add(4 * time.Second)
	s.gov.tick()
	assert.Empty(t, l.RevokeReason())
	clock = clock.Add(2 * time.Second)
	s.gov.tick()
	assert.Equal(t, "used 12 CPU-seconds, over its limit of 10", l.RevokeReason())
	assert.InDelta(t, 12, l.CPUSeconds(), 1e-9)
}

func TestGovernorSkipsAnActuatorWithoutAReading(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 0.1})
	l, err := s.Acquire(context.Background(), LeaseRequest{ID: "new", Policy: throttlePolicy}, nil)
	require.NoError(t, err)
	a := newFakeActuator(5)
	a.noRead = true
	l.AttachActuator(a)
	s.gov.tick()
	assert.False(t, l.Measured())
	assert.False(t, s.Readings().OverBudget)
	assert.False(t, a.isApplied(MechPriority))
}

func TestSchedulerStatusShowsHoldersWaitersAndGroups(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1, DefaultCost: 0.25})
	s.SetGroups(map[string]int{"g": 3})
	l, err := s.Acquire(context.Background(), LeaseRequest{ID: "a", Class: "hook-a", Group: "g"}, nil)
	require.NoError(t, err)
	acquireAsync(s, context.Background(), LeaseRequest{ID: "b", Group: "g", Priority: 2}, nil)
	waitQueued(t, s, 1)

	st := s.Status()
	assert.Equal(t, 1, st.MaxConcurrent)
	assert.Equal(t, 1, st.Running)
	assert.InDelta(t, 0.25, st.Reserved, 1e-9)
	require.Len(t, st.Holding, 1)
	assert.Equal(t, "hook-a", st.Holding[0].Class)
	assert.Nil(t, st.Holding[0].Wait)
	require.Len(t, st.Waiting, 1)
	require.NotNil(t, st.Waiting[0].Wait)
	assert.Equal(t, WaitCap, st.Waiting[0].Wait.Kind)
	assert.Equal(t, []GroupInfo{{Name: "g", Limit: 3, Active: 1, Waiting: 1}}, st.Groups)

	assert.True(t, s.SetPolicy("a", throttlePolicy))
	assert.False(t, s.SetPolicy("missing", throttlePolicy))
	l.Revoke("first")
	l.Revoke("second")
	assert.Equal(t, "first", l.RevokeReason(), "the first reason wins")
}

func TestServerRevokedJobFailsWithTheReason(t *testing.T) {
	hold := make(chan struct{})
	runner := &fakeRunner{hold: hold}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	j, _, err := srv.Enqueue(Spec{Name: "stuck", Command: []string{"stuck"}})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	var lease *Lease
	require.Eventually(t, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		if h := srv.running[j.ID]; h != nil {
			lease = h.lease
		}
		return lease != nil
	}, 5*time.Second, 5*time.Millisecond)
	lease.Revoke("used too much")
	waitState(t, srv, j.ID, StateFailed)
	got, err := srv.Get(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "revoked by the scheduler: used too much", got.Error)
	assert.Equal(t, 0, srv.SchedulerStatus().Running, "the slot is given back")
}
