package jobserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The monitor policy goes on at the first sample, under budget too, and stays on.
func TestAMonitorIsThrottledFromItsFirstSampleAndNeverReverted(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 8, RevertAfter: -1, MonitorPolicy: JobPolicy{
		Priority: PrioritySettings{Enabled: true, Nice: 19},
		Affinity: AffinitySettings{Enabled: true, CPUs: []int{0}},
		Freeze:   FreezeSettings{Enabled: true},
	}})
	m := admit(t, s, LeaseRequest{ID: "m", Monitor: true})
	a := newFakeActuator(0.01)
	m.AttachActuator(a)

	s.gov.tick()
	assert.True(t, a.isApplied(MechPriority), "a monitor runs at low priority from its first sample")
	assert.True(t, a.isApplied(MechAffinity))
	assert.False(t, a.isApplied(MechFreeze), "one quiet monitor is under the monitor budget")
	for range 5 {
		s.gov.tick()
	}
	assert.Equal(t, 1, a.count("apply priority"), "the control goes on once")
	assert.Zero(t, a.count("revert priority"), "a host far under budget never reverts a monitor's controls")
}

// Monitors that together use more than their budget take turns: the heaviest freezes for a sample, then thaws.
func TestMonitorsOverTheirBudgetFreezeInRotationAndAreNeverRevoked(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 8, MonitorBudget: 0.5, MonitorPolicy: JobPolicy{
		Priority: PrioritySettings{Enabled: true, Nice: 19},
		Freeze:   FreezeSettings{Enabled: true},
	}})
	heavy := admit(t, s, LeaseRequest{ID: "heavy", Monitor: true, Limits: Limits{MaxCPUs: 0.1, Grace: 1}})
	light := admit(t, s, LeaseRequest{ID: "light", Monitor: true})
	ha, la := newFakeActuator(0.6), newFakeActuator(0.2)
	heavy.AttachActuator(ha)
	light.AttachActuator(la)

	// The first sample of an actuator has no rate yet.
	for i := 0; i < 3 && !ha.isApplied(MechFreeze); i++ {
		s.gov.tick()
	}
	require.True(t, ha.isApplied(MechFreeze), "the heaviest monitor freezes while the monitors use 0.8 of 0.5")
	assert.False(t, la.isApplied(MechFreeze))
	s.gov.tick()
	assert.False(t, ha.isApplied(MechFreeze), "a frozen monitor thaws at the next sample")
	assert.True(t, la.isApplied(MechFreeze), "and the next heaviest takes its turn")
	s.gov.tick()
	assert.True(t, ha.isApplied(MechFreeze), "the rotation comes back to it")
	assert.False(t, la.isApplied(MechFreeze))
	la.set(0)
	ha.set(0.3)
	s.gov.tick()
	s.gov.tick()
	assert.False(t, ha.isApplied(MechFreeze), "under the monitor budget, nothing stays frozen")
	assert.False(t, la.isApplied(MechFreeze))
	select {
	case <-heavy.Revoked():
		t.Fatal("a monitor is never revoked, whatever its limits say")
	default:
	}
	req, _, _ := heavy.snapshot()
	assert.Zero(t, req.Limits, "a monitor lease drops the limits it asked for")
}
