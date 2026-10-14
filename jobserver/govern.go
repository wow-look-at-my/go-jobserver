package jobserver

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"
)

// CPUReport is what the daemon last measured, and what it did about it.
type CPUReport struct {
	At   time.Time `json:"at,omitempty"`
	CPUs int       `json:"cpus"`
	// Budget is how many CPUs the running work may use together.
	Budget float64 `json:"budget"`
	// Measured is how many CPUs the running work is using.
	Measured float64 `json:"measured"`
	// HostBusy is how many CPUs were busy across the whole host.
	HostBusy float64 `json:"host_busy"`
	// OverBudget reports whether Measured is above Budget.
	OverBudget bool `json:"over_budget"`
	// Jobs is each running lease's CPU use, by lease ID.
	Jobs map[string]float64 `json:"jobs,omitempty"`
	// Note explains a report that carries no measurement.
	Note string `json:"note,omitempty"`
}

// offenderSlack is how far above its cost a lease may run before it counts as the cause of an oversubscription.
const offenderSlack = 0.05

// MechCap is the control a Limits.MaxCPUs bound applies to an actuator.
const MechCap Mechanism = "cap"

// A governor samples what the holders use. It enforces each lease's limits.
type governor struct {
	sched   *Scheduler
	mu      sync.Mutex
	sampler *sampler
	sample  Reading
	// rates is what each lease used at the latest sample.
	rates map[*Lease]float64
	// lastAt is when each actuator lease was read last.
	lastAt map[*Lease]time.Time
	// active holds the budget controls in effect, by lease.
	active map[*Lease][]Control
	// caps holds the MaxCPUs bound in effect, by lease.
	caps map[*Lease]Control
	// history is every control this daemon has applied or reverted, newest last.
	history []Control
	// probe is what this host answered about process control, asked once.
	probe hostProbe
}

// historyLimit bounds the control log the status surface carries.
const historyLimit = 64

func newGovernor(s *Scheduler) *governor {
	return &governor{
		sched:   s,
		sampler: newSampler(),
		rates:   map[*Lease]float64{},
		lastAt:  map[*Lease]time.Time{},
		active:  map[*Lease][]Control{},
		caps:    map[*Lease]Control{},
	}
}

// loop samples on a cadence until the scheduler closes.
func (g *governor) loop() {
	defer g.sched.wg.Done()
	t := time.NewTicker(g.sched.cfg.SampleInterval)
	defer t.Stop()
	for {
		select {
		case <-g.sched.quit:
			return
		case <-t.C:
			g.tick()
		}
	}
}

// A target is one holder the governor can see.
type target struct {
	l   *Lease
	req LeaseRequest
	pid int
	a   Actuator
}

// key names a lease in reports. A lease without an ID is named by its place in line.
func (l *Lease) key() string {
	if l.req.ID != "" {
		return l.req.ID
	}
	return fmt.Sprintf("#%d", l.seq)
}

// tick takes one sample, enforces limits, and brings the budget controls in line.
func (g *governor) tick() {
	var targets []target
	roots := map[string]int{}
	for _, l := range g.sched.holders() {
		req, pid, a := l.snapshot()
		if pid <= 0 && a == nil {
			continue
		}
		targets = append(targets, target{l: l, req: req, pid: pid, a: a})
		if a == nil {
			roots[l.key()] = pid
		}
	}
	reading := g.sampler.sample(roots)
	// Asking this host how it stops a process starts one and waits for it.
	facts := g.hostFacts(asksTheHost(targets))
	now := g.sched.now()

	rates := map[*Lease]float64{}
	for _, t := range targets {
		var rate, seconds float64
		if t.a != nil {
			cpus, ok := t.a.Usage()
			g.mu.Lock()
			last, had := g.lastAt[t.l]
			g.lastAt[t.l] = now
			g.mu.Unlock()
			if !ok {
				continue
			}
			if !had {
				last = t.l.started
			}
			rate, seconds = cpus, cpus*now.Sub(last).Seconds()
		} else {
			r, ok := reading.Jobs[t.l.key()]
			if !reading.OK || !ok {
				continue
			}
			rate, seconds = r, r*reading.Window.Seconds()
		}
		rates[t.l] = rate
		t.l.mu.Lock()
		t.l.cpus = rate
		t.l.measured = true
		if seconds > 0 {
			t.l.cpuSeconds += seconds
		}
		t.l.mu.Unlock()
	}

	budget := g.sched.Budget()
	revoke := map[*Lease]string{}
	g.mu.Lock()
	g.sample = reading
	g.rates = rates
	for _, t := range targets {
		rate, ok := rates[t.l]
		if !ok {
			continue
		}
		if reason := g.enforceLocked(t, rate, now); reason != "" {
			revoke[t.l] = reason
		}
	}
	measured := 0.0
	for _, r := range rates {
		measured += r
	}
	if measured > budget {
		g.throttleLocked(targets, rates, reading, facts)
	} else {
		// Back within budget: give every lease back its normal scheduling.
		for l := range g.active {
			g.releaseLocked(l, facts.CPUs)
		}
	}
	g.mu.Unlock()

	for l, reason := range revoke {
		g.sched.cfg.Logf("go-jobserver: revoking %s: %s", l.key(), reason)
		l.Revoke(reason)
	}
}

// enforceLocked applies a lease's hard limits and returns why the lease must
// end, or "" when it may go on.
func (g *governor) enforceLocked(t target, rate float64, now time.Time) string {
	lim := t.req.Limits
	t.l.mu.Lock()
	used := t.l.cpuSeconds
	t.l.mu.Unlock()
	if lim.MaxCPUSeconds > 0 && used > lim.MaxCPUSeconds {
		return fmt.Sprintf("used %s CPU-seconds, over its limit of %s", fmtCPUs(used), fmtCPUs(lim.MaxCPUSeconds))
	}
	if lim.MaxCPUs <= 0 || rate <= lim.MaxCPUs {
		t.l.mu.Lock()
		t.l.overSince = time.Time{}
		t.l.mu.Unlock()
		return ""
	}
	if _, capped := g.caps[t.l]; capped {
		return ""
	}
	if t.a != nil {
		out, detail := t.a.Cap(lim.MaxCPUs)
		c := Control{Job: t.l.key(), Mechanism: MechCap, Outcome: out, Detail: detail,
			Active: out == OutcomeApplied, At: now}
		g.appendHistoryLocked(c)
		if c.Active {
			g.caps[t.l] = c
			return ""
		}
	}
	// Nothing bounds this work, so it is given Grace to come back under the limit.
	t.l.mu.Lock()
	defer t.l.mu.Unlock()
	if t.l.overSince.IsZero() {
		t.l.overSince = now
	}
	if over := now.Sub(t.l.overSince); over >= lim.grace() {
		return fmt.Sprintf("used %s CPUs for %s, over its limit of %s", fmtCPUs(rate), over.Round(time.Second), fmtCPUs(lim.MaxCPUs))
	}
	return ""
}

// throttleLocked applies the enabled mechanisms to the leases behind an
// oversubscription: the ones above their admitted cost, or all of them when
// none is.
func (g *governor) throttleLocked(targets []target, rates map[*Lease]float64, reading Reading, facts hostFacts) {
	var offenders []target
	for _, t := range targets {
		if rate, ok := rates[t.l]; ok && rate > t.l.cost+offenderSlack {
			offenders = append(offenders, t)
		}
	}
	if len(offenders) == 0 {
		offenders = targets
	}
	var index *procIndex
	for _, t := range offenders {
		for _, m := range t.req.Policy.Enabled() {
			if t.a != nil {
				if g.isActiveLocked(t.l, m, 0) {
					continue
				}
				out, detail := t.a.Apply(m, t.req.Policy)
				g.recordLocked(t.l, Control{Job: t.l.key(), Mechanism: m, Outcome: out, Detail: detail,
					Active: out == OutcomeApplied, At: time.Now()})
				continue
			}
			if !reading.OK {
				continue
			}
			if index == nil {
				index = newProcIndex(reading.Table)
			}
			settings := t.req.Policy.settings(m)
			for _, p := range settings.selected(index.tree(t.pid)) {
				if g.isActiveLocked(t.l, m, p.PID) {
					continue
				}
				res := applyMechanism(m, p.PID, t.req.Policy, facts)
				// Only a mechanism that took effect is in effect. Any other answer left the process as it was.
				g.recordLocked(t.l, Control{
					Job: t.l.key(), Mechanism: m, PID: p.PID, Name: p.Name,
					Outcome: res.outcome, Detail: res.detail,
					Prior: res.prior, HadPrior: res.hadPrior,
					Active: res.outcome == OutcomeApplied, At: time.Now(),
				})
			}
		}
	}
}

// asksTheHost reports whether a process-tree lease enables a mechanism whose
// answer this host has to be asked for.
func asksTheHost(targets []target) bool {
	for _, t := range targets {
		if t.a == nil && (t.req.Policy.Freeze.Enabled || t.req.Policy.Priority.Enabled) {
			return true
		}
	}
	return false
}

// hostFacts reports what this host can do. The answers it cannot read off the
// platform are asked for once, on a process of this daemon's own.
func (g *governor) hostFacts(want bool) hostFacts {
	g.mu.Lock()
	answer := g.probe
	cpus := g.sample.CPUs
	g.mu.Unlock()
	if want && !answer.known {
		answer = probeHost()
		g.mu.Lock()
		g.probe = answer
		g.mu.Unlock()
	}
	if cpus <= 0 {
		cpus = runtime.NumCPU()
	}
	return hostFacts{
		CPUs:         cpus,
		CanFreeze:    !answer.known || answer.canFreeze,
		FreezeDetail: answer.freezeWhy,
		NiceSpelling: answer.spelling,
		NiceDetail:   answer.niceWhy,
	}
}

// isActiveLocked reports whether a mechanism is already in effect on a
// process, so a sample that is still over budget does not reapply it.
func (g *governor) isActiveLocked(l *Lease, m Mechanism, pid int) bool {
	for _, c := range g.active[l] {
		if c.Mechanism == m && c.PID == pid {
			return true
		}
	}
	return false
}

// recordLocked files one control, remembering it as in effect when applied.
func (g *governor) recordLocked(l *Lease, c Control) {
	if c.Active {
		g.active[l] = append(g.active[l], c)
	}
	g.appendHistoryLocked(c)
}

func (g *governor) appendHistoryLocked(c Control) {
	g.history = append(g.history, c)
	if len(g.history) > historyLimit {
		g.history = append([]Control(nil), g.history[len(g.history)-historyLimit:]...)
	}
}

// releaseLocked reverts every budget control in effect on one lease.
func (g *governor) releaseLocked(l *Lease, cpus int) {
	_, _, a := l.snapshot()
	for _, c := range g.active[l] {
		var res controlResult
		if a != nil {
			out, detail := a.Revert(c.Mechanism)
			res = appliedResult(out, detail)
		} else {
			res = revertMechanism(c, cpus)
		}
		g.appendHistoryLocked(Control{
			Job: c.Job, Mechanism: c.Mechanism, PID: c.PID, Name: c.Name,
			Outcome: res.outcome, Detail: res.detail, At: time.Now(),
		})
	}
	delete(g.active, l)
}

// finish reverts every control in effect on a lease and forgets it.
func (g *governor) finish(l *Lease) {
	_, _, a := l.snapshot()
	g.mu.Lock()
	defer g.mu.Unlock()
	cpus := g.sample.CPUs
	if cpus <= 0 {
		cpus = runtime.NumCPU()
	}
	g.releaseLocked(l, cpus)
	if c, ok := g.caps[l]; ok && a != nil {
		out, detail := a.Cap(0)
		g.appendHistoryLocked(Control{Job: c.Job, Mechanism: MechCap, Outcome: out, Detail: detail, At: time.Now()})
	}
	delete(g.caps, l)
	delete(g.rates, l)
	delete(g.lastAt, l)
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

// report returns the latest measurement as a status object.
func (g *governor) report() CPUReport {
	budget := g.sched.Budget()
	g.mu.Lock()
	defer g.mu.Unlock()
	out := CPUReport{
		At:       g.sample.At,
		CPUs:     g.sample.CPUs,
		Budget:   budget,
		HostBusy: g.sample.HostBusy,
		Jobs:     map[string]float64{},
		Note:     g.sample.Note,
	}
	if out.CPUs == 0 {
		out.CPUs = runtime.NumCPU()
	}
	for l, rate := range g.rates {
		out.Jobs[l.key()] = rate
		out.Measured += rate
	}
	out.OverBudget = len(g.rates) > 0 && out.Measured > budget
	return out
}

// controls lists the controls in effect right now: the budget controls and
// the CPU caps. The history log carries every control ever applied, so it is
// the live set that answers this, not the log.
func (g *governor) controls() []Control {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []Control
	for _, cs := range g.active {
		out = append(out, cs...)
	}
	for _, c := range g.caps {
		out = append(out, c)
	}
	sort.Slice(out, func(i, k int) bool {
		if out[i].Job != out[k].Job {
			return out[i].Job < out[k].Job
		}
		if out[i].Mechanism != out[k].Mechanism {
			return out[i].Mechanism < out[k].Mechanism
		}
		return out[i].PID < out[k].PID
	})
	return out
}

// events lists the recent control history, newest first.
func (g *governor) events() []Control {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Control, 0, len(g.history))
	for i := len(g.history) - 1; i >= 0; i-- {
		out = append(out, g.history[i])
	}
	return out
}
