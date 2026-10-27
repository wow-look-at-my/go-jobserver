// A limited target is above its MaxCPUs at this sample.

package jobserver

import (
	"fmt"
	"time"

	"github.com/wow-look-at-my/simple-stats-api/cpusample"
)

// A limited target is above its MaxCPUs at this sample.
type limited struct {
	t    target
	rate float64
}

// planLimitsLocked checks each measured lease against its hard limits. It
// returns the caps to try, the leases above MaxCPUs that no cap bounds yet,
// and the leases to revoke.
func (g *governor) planLimitsLocked(live []target, now time.Time) ([]action, []limited, map[*Lease]string) {
	var plan []action
	var over []limited
	revoke := map[*Lease]string{}
	for _, t := range live {
		rate, ok := g.rates[t.l]
		if !ok {
			continue
		}
		lim := t.req.Limits
		t.l.mu.Lock()
		used := t.l.cpuSeconds
		t.l.mu.Unlock()
		if lim.MaxCPUSeconds > 0 && used > lim.MaxCPUSeconds {
			revoke[t.l] = fmt.Sprintf("used %s CPU-seconds, over its limit of %s", fmtCPUs(used), fmtCPUs(lim.MaxCPUSeconds))
			continue
		}
		if lim.MaxCPUs <= 0 || rate <= lim.MaxCPUs {
			t.l.mu.Lock()
			t.l.overSince = time.Time{}
			t.l.mu.Unlock()
			continue
		}
		if _, capped := g.caps[t.l]; capped {
			continue
		}
		over = append(over, limited{t: t, rate: rate})
		if t.a != nil && g.mayTryLocked(t.l, controlKey{m: MechCap}, now) {
			plan = append(plan, action{t: t, kind: doCap, m: MechCap, cpus: lim.MaxCPUs})
		}
	}
	return plan, over, revoke
}

// graceLocked gives each lease above MaxCPUs that no cap bounds Grace to come
// back under the limit, and adds the ones past it to revoke.
func (g *governor) graceLocked(over []limited, now time.Time, revoke map[*Lease]string) {
	for _, o := range over {
		if _, capped := g.caps[o.t.l]; capped || o.t.l.ended {
			continue
		}
		lim := o.t.req.Limits
		o.t.l.mu.Lock()
		if o.t.l.overSince.IsZero() {
			o.t.l.overSince = now
		}
		since := now.Sub(o.t.l.overSince)
		o.t.l.mu.Unlock()
		if since >= lim.grace() {
			revoke[o.t.l] = fmt.Sprintf("used %s CPUs for %s, over its limit of %s",
				fmtCPUs(o.rate), since.Round(time.Second), fmtCPUs(lim.MaxCPUs))
		}
	}
}

// planBudgetLocked weighs the load against the budget. Above bookable, it
// plans the controls. Once the load has stayed at or below the budget for
// RevertAfter, it plans the reverts. In between, the controls stay as they are.
func (g *governor) planBudgetLocked(live []target, reading Reading, budget float64, now time.Time) []action {
	load := 0.0
	for _, t := range live {
		rate, ok := g.rates[t.l]
		if !ok {
			// A job not measured yet counts at its cost. A job that costs more than the budget runs alone, so it counts at the budget.
			rate = min(t.l.cost, budget)
		}
		load += rate
	}
	switch {
	case load > bookable(budget):
		g.lowSince = time.Time{}
		return g.planThrottleLocked(live, reading, now)
	case load <= budget:
		if g.lowSince.IsZero() {
			g.lowSince = now
		}
		if wait := g.sched.cfg.RevertAfter; wait < 0 || now.Sub(g.lowSince) >= wait {
			return g.planRevertLocked(now)
		}
	default:
		g.lowSince = time.Time{}
	}
	return nil
}

// planThrottleLocked plans the enabled mechanisms for the jobs that use more
// than their cost. When no job does, it plans them for all of the jobs.
func (g *governor) planThrottleLocked(live []target, reading Reading, now time.Time) []action {
	var offenders []target
	for _, t := range live {
		if rate, ok := g.rates[t.l]; ok && rate > t.l.cost+offenderSlack {
			offenders = append(offenders, t)
		}
	}
	if len(offenders) == 0 {
		offenders = live
	}
	var plan []action
	var index *cpusample.ProcIndex
	for _, t := range offenders {
		for _, m := range t.req.Policy.Enabled() {
			if t.a != nil {
				if g.shouldApplyLocked(t.l, controlKey{m: m}, now) {
					plan = append(plan, action{t: t, kind: doApply, m: m})
				}
				continue
			}
			if !reading.OK {
				continue
			}
			if index == nil {
				index = cpusample.NewProcIndex(reading.Table)
			}
			for _, p := range t.req.Policy.settings(m).selected(index.Tree(t.pid)) {
				if g.shouldApplyLocked(t.l, controlKey{m: m, pid: p.PID}, now) {
					plan = append(plan, action{t: t, kind: doApply, m: m, proc: p})
				}
			}
		}
	}
	return plan
}

// planRevertLocked plans a revert of every budget control in effect.
func (g *governor) planRevertLocked(now time.Time) []action {
	var plan []action
	for l, cs := range g.active {
		req, _, a := l.snapshot()
		// A monitor keeps its controls for its whole life. planMonitorsLocked thaws its freezes.
		if req.Monitor {
			continue
		}
		for _, c := range cs {
			if g.mayTryLocked(l, controlKey{m: c.Mechanism, pid: c.PID}, now) {
				plan = append(plan, action{t: target{l: l, a: a}, kind: doRevert, m: c.Mechanism, c: c})
			}
		}
	}
	return plan
}
