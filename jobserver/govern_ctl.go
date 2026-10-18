package jobserver

import (
	"runtime"
	"strconv"
	"time"
)

// A controlKey names one control of a lease: a mechanism, on one process or,
// with pid zero, on the lease's actuator.
type controlKey struct {
	m   Mechanism
	pid int
}

// An actionKind is what one planned control call does.
type actionKind string

const (
	doApply  actionKind = "apply"
	doRevert actionKind = "revert"
	doCap    actionKind = "cap"
)

// An action is one control call a tick planned.
type action struct {
	t    target
	kind actionKind
	m    Mechanism
	// proc is the process a process control acts on.
	proc Proc
	// c is the control a revert undoes.
	c Control
	// cpus is the bound a cap sets.
	cpus float64
}

// perform makes one planned control call with the lease's ctl held, then
// files it. A lease that ended after the plan is left alone.
func (g *governor) perform(a action, facts hostFacts, now time.Time) {
	l := a.t.l
	l.ctl.Lock()
	defer l.ctl.Unlock()
	if l.ended {
		return
	}
	c, k, took := callControl(a, facts)
	g.mu.Lock()
	switch {
	case !took:
	case a.kind == doCap:
		g.caps[l] = c
	case a.kind == doApply:
		g.active[l] = append(g.active[l], c)
	default:
		g.dropLocked(l, k)
	}
	first := g.settleLocked(l, k, took, c, now)
	g.mu.Unlock()
	if first {
		g.sched.cfg.Logf("go-jobserver: %s on %s%s did not take (%s: %s); trying again %s",
			a.kind.what(k.m), c.Job, pidSuffix(k.pid), c.Outcome, c.Detail, g.retryWords())
	}
}

// callControl makes one control call, and reports whether it did what it set out to.
func callControl(a action, facts hostFacts) (Control, controlKey, bool) {
	switch a.kind {
	case doCap:
		out, detail := a.t.a.Cap(a.cpus)
		c := Control{Job: a.t.l.key(), Mechanism: MechCap, Outcome: out, Detail: detail,
			Active: out == OutcomeApplied, At: time.Now()}
		return c, controlKey{m: MechCap}, c.Active
	case doApply:
		c := applyControl(a, facts)
		return c, controlKey{m: c.Mechanism, pid: c.PID}, c.Active
	}
	c := revertControl(a.t.a, a.c, facts.CPUs)
	// A refused revert left the control in effect.
	return c, controlKey{m: a.c.Mechanism, pid: a.c.PID}, c.Outcome != OutcomeRefused
}

// applyControl turns one mechanism on, through the lease's actuator or on one
// of its processes. Only a mechanism that took effect is in effect.
func applyControl(a action, facts hostFacts) Control {
	job := a.t.l.key()
	if a.t.a != nil {
		out, detail := a.t.a.Apply(a.m, a.t.req.Policy)
		return Control{Job: job, Mechanism: a.m, Outcome: out, Detail: detail,
			Active: out == OutcomeApplied, At: time.Now()}
	}
	res := applyMechanism(a.m, a.proc.PID, a.t.req.Policy, facts)
	return Control{
		Job: job, Mechanism: a.m, PID: a.proc.PID, Name: a.proc.Name,
		Outcome: res.outcome, Detail: res.detail,
		Prior: res.prior, HadPrior: res.hadPrior,
		Active: res.outcome == OutcomeApplied, At: time.Now(),
	}
}

// revertControl turns one control off again, through the actuator or on its
// process.
func revertControl(a Actuator, c Control, cpus int) Control {
	var res controlResult
	if a != nil {
		out, detail := a.Revert(c.Mechanism)
		res = appliedResult(out, detail)
	} else {
		res = revertMechanism(c, cpus)
	}
	return Control{Job: c.Job, Mechanism: c.Mechanism, PID: c.PID, Name: c.Name,
		Outcome: res.outcome, Detail: res.detail, At: time.Now()}
}

// settleLocked files one control call. A call that did not take starts or
// continues a streak, and waits RetryInterval for the next try. It reports
// whether the call starts a streak, which is the failure to log.
func (g *governor) settleLocked(l *Lease, k controlKey, took bool, c Control, now time.Time) bool {
	g.appendHistoryLocked(c)
	tries := g.failed[l]
	if took {
		delete(tries, k)
		if len(tries) == 0 {
			delete(g.failed, l)
		}
		return false
	}
	if tries == nil {
		tries = map[controlKey]time.Time{}
		g.failed[l] = tries
	}
	_, streak := tries[k]
	tries[k] = now
	return !streak
}

// what names a control call in a log line.
func (k actionKind) what(m Mechanism) string {
	switch k {
	case doApply:
		return string(m)
	case doRevert:
		return "the revert of " + string(m)
	}
	return "the CPU cap"
}

func pidSuffix(pid int) string {
	if pid <= 0 {
		return ""
	}
	return " pid " + strconv.Itoa(pid)
}

// retryWords says when a control that did not take is tried again.
func (g *governor) retryWords() string {
	if every := g.sched.cfg.RetryInterval; every > 0 {
		return "in " + every.String()
	}
	return "at the next sample"
}

// mayTryLocked reports whether a control may be tried now: it has no failed
// try, or its last one is RetryInterval old.
func (g *governor) mayTryLocked(l *Lease, k controlKey, now time.Time) bool {
	at, failed := g.failed[l][k]
	every := g.sched.cfg.RetryInterval
	return !failed || every < 0 || now.Sub(at) >= every
}

// wantsLocked reports whether a budget control should be applied now: it is
// not in effect yet, and it may be tried.
func (g *governor) wantsLocked(l *Lease, k controlKey, now time.Time) bool {
	return !g.isActiveLocked(l, k) && g.mayTryLocked(l, k, now)
}

// isActiveLocked reports whether a control is already in effect, so a sample
// that is still over budget does not apply it again.
func (g *governor) isActiveLocked(l *Lease, k controlKey) bool {
	for _, c := range g.active[l] {
		if c.Mechanism == k.m && c.PID == k.pid {
			return true
		}
	}
	return false
}

// dropLocked takes one control off a lease's budget controls.
func (g *governor) dropLocked(l *Lease, k controlKey) {
	var kept []Control
	for _, c := range g.active[l] {
		if c.Mechanism != k.m || c.PID != k.pid {
			kept = append(kept, c)
		}
	}
	if len(kept) == 0 {
		delete(g.active, l)
		return
	}
	g.active[l] = kept
}

func (g *governor) appendHistoryLocked(c Control) {
	g.history = append(g.history, c)
	if len(g.history) > historyLimit {
		g.history = append([]Control(nil), g.history[len(g.history)-historyLimit:]...)
	}
}

// finish ends a lease for the governor. It forgets the lease, then tries once
// to revert every control in effect on it.
func (g *governor) finish(l *Lease) {
	_, _, a := l.snapshot()
	l.ctl.Lock()
	defer l.ctl.Unlock()
	g.mu.Lock()
	if l.ended {
		g.mu.Unlock()
		return
	}
	l.ended = true
	controls := g.active[l]
	capped, hadCap := g.caps[l]
	delete(g.active, l)
	delete(g.caps, l)
	delete(g.rates, l)
	delete(g.lastAt, l)
	delete(g.failed, l)
	cpus := g.sample.CPUs
	g.mu.Unlock()
	if cpus <= 0 {
		cpus = runtime.NumCPU()
	}
	var done []Control
	for _, c := range controls {
		done = append(done, revertControl(a, c, cpus))
	}
	if hadCap && a != nil {
		out, detail := a.Cap(0)
		done = append(done, Control{Job: capped.Job, Mechanism: MechCap, Outcome: out, Detail: detail, At: time.Now()})
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, c := range done {
		g.appendHistoryLocked(c)
	}
}

// releaseAll reverts every control this daemon has applied.
func (g *governor) releaseAll() {
	g.mu.Lock()
	leases := make([]*Lease, 0, len(g.active)+len(g.caps))
	for l := range g.active {
		leases = append(leases, l)
	}
	for l := range g.caps {
		leases = append(leases, l)
	}
	g.mu.Unlock()
	for _, l := range leases {
		g.finish(l)
	}
}
