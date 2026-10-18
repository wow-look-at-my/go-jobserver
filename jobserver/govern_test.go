package jobserver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// admit acquires a lease the plan grants at once.
func admit(t *testing.T, s *Scheduler, req LeaseRequest) *Lease {
	t.Helper()
	l, err := s.Acquire(context.Background(), req, nil)
	require.NoError(t, err)
	return l
}

// testClock is a clock a test moves by hand.
type testClock struct{ at time.Time }

func (c *testClock) now() time.Time      { return c.at }
func (c *testClock) add(d time.Duration) { c.at = c.at.Add(d) }

// blockingActuator is an actuator whose Apply hangs until the test lets it
// go, as a docker call does on a wedged daemon.
type blockingActuator struct {
	*fakeActuator
	entered chan struct{}
	gate    chan struct{}
	enter   sync.Once
	open    sync.Once
}

func newBlockingActuator(cpus float64) *blockingActuator {
	return &blockingActuator{fakeActuator: newFakeActuator(cpus),
		entered: make(chan struct{}), gate: make(chan struct{})}
}

func (b *blockingActuator) Apply(m Mechanism, p JobPolicy) (Outcome, string) {
	b.enter.Do(func() { close(b.entered) })
	<-b.gate
	return b.fakeActuator.Apply(m, p)
}

// release lets every Apply through.
func (b *blockingActuator) release() { b.open.Do(func() { close(b.gate) }) }

// within fails the test unless fn returns before the timeout.
func within(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	closedSoon(t, done, what+" did not return while a control call was in flight")
}

// closedSoon fails the test unless ch closes before the timeout.
func closedSoon(t *testing.T, ch <-chan struct{}, why string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(why)
	}
}

// logLines collects what a scheduler logs.
type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (g *logLines) logf(format string, args ...any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lines = append(g.lines, fmt.Sprintf(format, args...))
}

// matching returns the lines that hold every one of the words.
func (g *logLines) matching(words ...string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []string
	for _, line := range g.lines {
		all := true
		for _, w := range words {
			all = all && strings.Contains(line, w)
		}
		if all {
			out = append(out, line)
		}
	}
	return out
}

var hogRequest = LeaseRequest{ID: "hog", Policy: JobPolicy{Cost: 0.5, Priority: throttlePolicy.Priority}}

func TestASlowControlCallHoldsNoLockTheRestOfTheSchedulerNeeds(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 1})
	hog := admit(t, s, hogRequest)
	quiet := admit(t, s, LeaseRequest{ID: "quiet", Policy: JobPolicy{Cost: 0.5}})
	slow := newBlockingActuator(3)
	t.Cleanup(slow.release)
	hog.AttachActuator(slow)
	quiet.AttachActuator(newFakeActuator(0.1))

	ticked := make(chan struct{})
	go func() {
		defer close(ticked)
		s.gov.tick()
	}()
	closedSoon(t, slow.entered, "the tick never reached the actuator")

	within(t, "Release of another lease", quiet.Release)
	within(t, "Readings", func() { s.Readings() })
	within(t, "Controls", func() { s.Controls() })
	within(t, "Events", func() { s.Events() })

	slow.release()
	closedSoon(t, ticked, "the tick never finished")
	controls := s.Controls()
	require.Len(t, controls, 1)
	assert.Equal(t, "hog", controls[0].Job)
	assert.Equal(t, MechPriority, controls[0].Mechanism)
	assert.True(t, slow.isApplied(MechPriority))
	report := s.Readings()
	assert.Contains(t, report.Jobs, "hog")
	assert.NotContains(t, report.Jobs, "quiet", "a released lease leaves the report")
	assert.Equal(t, 1, s.Status().Running)
}

func TestALeaseReleasedDuringItsApplyEndsWithNothingInEffect(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 1})
	hog := admit(t, s, hogRequest)
	slow := newBlockingActuator(3)
	t.Cleanup(slow.release)
	hog.AttachActuator(slow)

	ticked := make(chan struct{})
	go func() {
		defer close(ticked)
		s.gov.tick()
	}()
	closedSoon(t, slow.entered, "the tick never reached the actuator")
	released := make(chan struct{})
	go func() {
		defer close(released)
		hog.Release()
	}()
	slow.release()
	closedSoon(t, ticked, "the tick never finished")
	closedSoon(t, released, "the release never finished")

	assert.Empty(t, s.Controls(), "a released lease leaves nothing in effect")
	assert.Equal(t, []string{"apply priority", "revert priority"}, slow.callList(),
		"the control that landed while the lease ended is reverted")
	assert.False(t, slow.isApplied(MechPriority))
	assert.Equal(t, 0, s.Status().Running)
}

func TestControlsComeOffOnlyOnceTheLoadHasStayedLowForReleaseAfter(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 2})
	clock := &testClock{at: time.Now()}
	s.now = clock.now
	hog := admit(t, s, hogRequest)
	a := newFakeActuator(3)
	hog.AttachActuator(a)
	step := func(by time.Duration, cpus float64) {
		clock.add(by)
		a.set(cpus)
		s.gov.tick()
	}

	step(0, 3)
	require.True(t, a.isApplied(MechPriority), "over the budget, the control goes on")
	// Throttled work uses less and then more: across the release mark, and across the budget.
	step(time.Second, 1.5)
	step(time.Second, 1.9)
	step(time.Second, 2.5)
	step(time.Second, 1)
	step(7*time.Second, 1)
	step(7*time.Second, 1)
	assert.Equal(t, 1, a.count("apply priority"))
	assert.Zero(t, a.count("revert priority"), "the control stays on until the load has stayed low for ReleaseAfter")

	step(time.Second, 1)
	assert.Equal(t, 1, a.count("revert priority"), "the calm has held for ReleaseAfter")
	step(time.Second, 1)
	assert.Equal(t, 1, a.count("revert priority"))
	assert.Equal(t, 1, a.count("apply priority"))
	assert.Empty(t, s.Controls())
}

func TestANegativeReleaseAfterRevertsAtTheFirstCalmSample(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 2, ReleaseBelow: 0.5, ReleaseAfter: -1})
	hog := admit(t, s, hogRequest)
	a := newFakeActuator(3)
	hog.AttachActuator(a)

	s.gov.tick()
	require.True(t, a.isApplied(MechPriority))
	a.set(1.5)
	s.gov.tick()
	assert.True(t, a.isApplied(MechPriority), "within the budget but above its release mark, the control stays")
	a.set(0.9)
	s.gov.tick()
	assert.False(t, a.isApplied(MechPriority), "with no wait, the first sample at the release mark reverts")
	assert.Equal(t, 1, a.count("revert priority"))
}

func TestASaturatedHostThrottlesWorkThatIsWithinItsBudget(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 8})
	clock := &testClock{at: time.Now()}
	s.now = clock.now
	host := &scriptedHost{cpus: 4}
	s.gov.sampler.host = host
	ci := admit(t, s, LeaseRequest{ID: "ci", Policy: JobPolicy{Cost: 1, Priority: throttlePolicy.Priority}})
	a := newFakeActuator(1)
	ci.AttachActuator(a)
	step := func(by time.Duration, share float64) {
		clock.add(by)
		host.set(share)
		s.gov.tick()
	}

	step(0, 0.2)
	assert.False(t, s.Readings().HostSaturated, "the first host read is only a baseline")
	step(time.Second, 0.95)
	report := s.Readings()
	assert.True(t, report.HostSaturated)
	assert.Equal(t, 4, report.HostCPUs)
	assert.InDelta(t, 3.8, report.HostBusy, 1e-9)
	assert.False(t, report.OverBudget, "the work itself is within its budget")
	assert.True(t, a.isApplied(MechPriority), "a saturated host throttles as if over budget")

	step(time.Second, 0.8)
	assert.True(t, s.Readings().HostSaturated, "the host stays saturated until it falls below the low mark")
	step(time.Second, 0.5)
	assert.False(t, s.Readings().HostSaturated)
	assert.True(t, a.isApplied(MechPriority), "the control waits for the calm to hold")
	step(DefaultReleaseAfter, 0.5)
	assert.False(t, a.isApplied(MechPriority), "after the calm has held, the control comes off")
	assert.Equal(t, 1, a.count("apply priority"))
}

func TestANegativeHostBusyHighTurnsTheHostCheckOff(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 8, HostBusyHigh: -1})
	host := &scriptedHost{cpus: 4}
	s.gov.sampler.host = host
	ci := admit(t, s, LeaseRequest{ID: "ci", Policy: JobPolicy{Cost: 1, Priority: throttlePolicy.Priority}})
	a := newFakeActuator(1)
	ci.AttachActuator(a)

	host.set(1)
	s.gov.tick()
	s.gov.tick()
	report := s.Readings()
	assert.InDelta(t, 4, report.HostBusy, 1e-9, "the host is still measured")
	assert.False(t, report.HostSaturated)
	assert.False(t, a.isApplied(MechPriority))
}

func TestNewSchedulerFillsTheGovernorDefaults(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	assert.InDelta(t, DefaultReleaseBelow, s.cfg.ReleaseBelow, 1e-9)
	assert.Equal(t, DefaultReleaseAfter, s.cfg.ReleaseAfter)
	assert.InDelta(t, DefaultHostBusyHigh, s.cfg.HostBusyHigh, 1e-9)
	assert.InDelta(t, DefaultHostBusyLow, s.cfg.HostBusyLow, 1e-9)
	assert.Equal(t, DefaultRetryInterval, s.cfg.RetryInterval)

	off := newTestScheduler(t, SchedulerConfig{ReleaseAfter: -1, HostBusyHigh: -1, RetryInterval: -1})
	assert.Negative(t, off.cfg.ReleaseAfter, "a negative value is kept: it means no wait")
	assert.Negative(t, off.cfg.HostBusyHigh, "a negative value is kept: it turns the host check off")
	assert.Negative(t, off.cfg.RetryInterval, "a negative value is kept: it means every sample")
}

func TestAHostBusyLowAtOrAboveTheHighMarkIsMovedBelowIt(t *testing.T) {
	var logs logLines
	s := newTestScheduler(t, SchedulerConfig{HostBusyHigh: 0.6, HostBusyLow: 0.8, Logf: logs.logf})
	assert.InDelta(t, 0.6, s.cfg.HostBusyHigh, 1e-9)
	assert.Less(t, s.cfg.HostBusyLow, s.cfg.HostBusyHigh)
	assert.Len(t, logs.matching("low mark 0.8", "high mark 0.6"), 1, "the change is logged once")

	var quiet logLines
	newTestScheduler(t, SchedulerConfig{Logf: quiet.logf})
	assert.Empty(t, quiet.matching("low mark"), "the defaults need no change")
}

func TestAHolderTheMeterCannotSeeCountsAtItsCost(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 3})
	seen := admit(t, s, LeaseRequest{ID: "seen", Policy: JobPolicy{Cost: 0.5, Priority: throttlePolicy.Priority}})
	blind := admit(t, s, LeaseRequest{ID: "blind", Policy: JobPolicy{Cost: 2, Priority: throttlePolicy.Priority}})
	sa, ba := newFakeActuator(0.5), newFakeActuator(5)
	ba.noRead = true
	seen.AttachActuator(sa)
	blind.AttachActuator(ba)
	// Both were admitted under the larger budget.
	s.SetCPUBudget(1)

	s.gov.tick()
	report := s.Readings()
	assert.InDelta(t, 0.5, report.Measured, 1e-9, "a holder with no reading is not part of what was measured")
	assert.Equal(t, 1, report.Unmeasured)
	assert.NotContains(t, report.Jobs, "blind")
	assert.False(t, report.OverBudget, "what was measured is within the budget")
	assert.True(t, ba.isApplied(MechPriority), "at its cost, the blind holder puts the load over the budget")
	assert.True(t, sa.isApplied(MechPriority), "with no holder above its cost, every holder is throttled")
	assert.False(t, blind.Measured())
	assert.Zero(t, blind.CPUSeconds())
}

func TestALoneHolderLargerThanTheBudgetIsNotOverItBeforeItsFirstReading(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 2})
	big := admit(t, s, LeaseRequest{ID: "big", Policy: JobPolicy{Cost: 5, Priority: throttlePolicy.Priority}})
	a := newFakeActuator(1)
	a.noRead = true
	big.AttachActuator(a)

	s.gov.tick()
	assert.False(t, a.isApplied(MechPriority), "admission lets work larger than the budget run alone, so it is not throttled for being unread")
}

func TestARefusedControlWaitsRetryIntervalAndLogsOnce(t *testing.T) {
	var logs logLines
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 1, Logf: logs.logf})
	clock := &testClock{at: time.Now()}
	s.now = clock.now
	l := admit(t, s, LeaseRequest{ID: "stubborn", Policy: JobPolicy{Cost: 0.5, Priority: throttlePolicy.Priority}})
	a := newFakeActuator(3)
	a.applyOut = OutcomeRefused
	l.AttachActuator(a)
	step := func(by time.Duration) {
		clock.add(by)
		s.gov.tick()
	}

	step(0)
	step(time.Second)
	step(8 * time.Second)
	assert.Equal(t, 1, a.count("apply priority"), "a refused control is not tried again before RetryInterval")
	step(time.Second)
	assert.Equal(t, 2, a.count("apply priority"), "RetryInterval after the last try, it is tried again")
	assert.Len(t, s.Events(), 2, "each try is one history entry")
	lines := logs.matching("stubborn", "priority", "refused", "the target said no")
	assert.Len(t, lines, 1, "a streak of refusals logs once: %v", lines)
	assert.Empty(t, s.Controls())

	a.setOutcomes(OutcomeApplied, OutcomeReverted)
	step(DefaultRetryInterval)
	assert.True(t, a.isApplied(MechPriority))
	s.gov.mu.Lock()
	assert.Empty(t, s.gov.failed, "a control that took ends its streak")
	s.gov.mu.Unlock()
}

func TestANegativeRetryIntervalTriesAtEverySample(t *testing.T) {
	var logs logLines
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 1, RetryInterval: -1, Logf: logs.logf})
	l := admit(t, s, hogRequest)
	a := newFakeActuator(3)
	a.applyOut = OutcomeNotApplicable
	l.AttachActuator(a)
	for range 3 {
		s.gov.tick()
	}
	assert.Equal(t, 3, a.count("apply priority"))
	assert.Len(t, logs.matching("priority", "at the next sample"), 1)
}

func TestARefusedRevertKeepsTheControlInEffectUntilItTakes(t *testing.T) {
	var logs logLines
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 2, ReleaseAfter: -1, Logf: logs.logf})
	clock := &testClock{at: time.Now()}
	s.now = clock.now
	l := admit(t, s, LeaseRequest{ID: "sticky", Policy: JobPolicy{Cost: 0.5, Priority: throttlePolicy.Priority}})
	a := newFakeActuator(3)
	a.revertOut = OutcomeRefused
	l.AttachActuator(a)
	step := func(by time.Duration) {
		clock.add(by)
		s.gov.tick()
	}

	step(0)
	require.True(t, a.isApplied(MechPriority))
	a.set(0.5)
	step(time.Second)
	controls := s.Controls()
	require.Len(t, controls, 1, "a control the target would not revert is still in effect")
	assert.True(t, controls[0].Active)
	step(time.Second)
	assert.Equal(t, 1, a.count("revert priority"), "a refused revert waits RetryInterval")
	step(DefaultRetryInterval - time.Second)
	assert.Equal(t, 2, a.count("revert priority"))
	assert.Len(t, logs.matching("the revert of priority", "sticky"), 1)

	a.setOutcomes(OutcomeApplied, OutcomeReverted)
	step(DefaultRetryInterval)
	assert.Empty(t, s.Controls())
	assert.False(t, a.isApplied(MechPriority))
}

func TestAnEndingLeaseTriesItsRefusedRevertOnceAndDropsIt(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 2, ReleaseAfter: -1})
	l := admit(t, s, hogRequest)
	a := newFakeActuator(3)
	a.revertOut = OutcomeRefused
	l.AttachActuator(a)
	s.gov.tick()
	a.set(0.5)
	s.gov.tick()
	require.Len(t, s.Controls(), 1)

	l.Release()
	assert.Equal(t, 2, a.count("revert priority"), "the lease's end tries the revert once more")
	assert.Empty(t, s.Controls(), "and then forgets the control")
	s.gov.mu.Lock()
	assert.Empty(t, s.gov.failed)
	s.gov.mu.Unlock()
}

func TestTheGovernorReadsNoProcessTableForActuatorWork(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	src := &countingSource{}
	s.gov.sampler.src = src
	s.gov.sampler.host = &scriptedHost{cpus: 4}
	l := admit(t, s, LeaseRequest{ID: "container"})
	l.AttachActuator(newFakeActuator(1))
	for range 3 {
		s.gov.tick()
	}
	assert.Zero(t, src.count(), "no holder is a process tree, so the process table is never read")
	assert.InDelta(t, 1, s.Readings().Measured, 1e-9)

	s.gov.sampler.host = nil
	s.gov.tick()
	assert.Equal(t, 1, src.count(), "with no host counters, HostBusy comes from the table")
}

func TestAClassCostCountsOnlyTheTimeALeaseWasMeasured(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	clock := &testClock{at: time.Now()}
	s.now = clock.now
	l := admit(t, s, LeaseRequest{ID: "half", Class: "half-seen"})
	a := newFakeActuator(2)
	a.noRead = true
	l.AttachActuator(a)
	step := func() {
		clock.add(5 * time.Second)
		s.gov.tick()
	}

	step()
	step()
	a.setBlind(false)
	step()
	step()
	assert.InDelta(t, 20, l.CPUSeconds(), 1e-9)
	l.Release()
	cost, ok := s.classes.cost("half-seen")
	require.True(t, ok)
	assert.InDelta(t, 2, cost, 1e-9, "the class cost leaves out the half of the lease that no reading covered")
}
