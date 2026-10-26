// The governor's monitor policy: steady controls from the first sample, and a freeze in rotation while the monitors together use more than their budget.

package jobserver

import (
	"runtime"
	"time"

	"github.com/wow-look-at-my/simple-stats-api/cpusample"
)

// DefaultMonitorBudget is how many CPUs every monitor together may use before the governor freezes the heaviest one in turn.
const DefaultMonitorBudget = 0.5

// monitorPolicy is the policy every monitor lease runs under: the configured one, else a later nice on the host's last CPU. That monitorPolicy is with
// freeze for the rotation.
func (s *Scheduler) monitorPolicy() JobPolicy {
	if !s.cfg.MonitorPolicy.Zero() {
		return s.cfg.MonitorPolicy.clone()
	}
	p := JobPolicy{
		Priority: PrioritySettings{Enabled: true, Nice: 19},
		Freeze:   FreezeSettings{Enabled: true},
	}
	if n := runtime.NumCPU(); n > 1 {
		p.Affinity = AffinitySettings{Enabled: true, CPUs: []int{n - 1}}
	}
	return p
}

func (s *Scheduler) monitorBudget() float64 {
	if s.cfg.MonitorBudget > 0 {
		return s.cfg.MonitorBudget
	}
	return DefaultMonitorBudget
}

// splitMonitors separates the monitor targets from the job targets.
func splitMonitors(live []target) (jobs, mons []target) {
	for _, t := range live {
		if t.req.Monitor {
			mons = append(mons, t)
		} else {
			jobs = append(jobs, t)
		}
	}
	return jobs, mons
}

// planMonitorsLocked keeps affinity and priority on every monitor process at all times. It thaws each frozen
// monitor, and freezes the heaviest other one while the monitors together use more than the monitor budget. Each
// monitor still gets turns. A monitor is never revoked.
func (g *governor) planMonitorsLocked(mons []target, reading Reading, now time.Time) []action {
	var plan []action
	var index *cpusample.ProcIndex
	procs := func(t target, m Mechanism) []Proc {
		if !reading.OK || t.pid <= 0 {
			return nil
		}
		if index == nil {
			index = cpusample.NewProcIndex(reading.Table)
		}
		return t.req.Policy.settings(m).selected(index.Tree(t.pid))
	}
	apply := func(t target, m Mechanism) {
		if !t.req.Policy.settings(m).Enabled {
			return
		}
		if t.a != nil {
			if g.shouldApplyLocked(t.l, controlKey{m: m}, now) {
				plan = append(plan, action{t: t, kind: doApply, m: m})
			}
			return
		}
		for _, p := range procs(t, m) {
			if g.shouldApplyLocked(t.l, controlKey{m: m, pid: p.PID}, now) {
				plan = append(plan, action{t: t, kind: doApply, m: m, proc: p})
			}
		}
	}
	total := 0.0
	var heaviest *target
	heaviestRate := 0.0
	for i, t := range mons {
		apply(t, MechAffinity)
		apply(t, MechPriority)
		rate := g.rates[t.l]
		total += rate
		frozen := false
		for _, c := range g.active[t.l] {
			if c.Mechanism == MechFreeze {
				frozen = true
				if g.mayTryLocked(t.l, controlKey{m: MechFreeze, pid: c.PID}, now) {
					plan = append(plan, action{t: t, kind: doRevert, m: MechFreeze, c: c})
				}
			}
		}
		if !frozen && rate > heaviestRate {
			heaviest, heaviestRate = &mons[i], rate
		}
	}
	if heaviest != nil && total > g.sched.monitorBudget() {
		apply(*heaviest, MechFreeze)
	}
	return plan
}
