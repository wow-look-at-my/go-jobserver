package jobserver

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/wow-look-at-my/simple-stats-api/cpusample"
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
	// HostCPUs is how many CPUs the host's own counters cover. Zero means they were not read.
	HostCPUs int `json:"host_cpus"`
	// HostRunnable is how many tasks wanted a CPU at the last sample.
	HostRunnable int `json:"host_runnable"`
	// HostError says why the host's own CPU counters cannot be read.
	HostError string `json:"host_error,omitempty"`
	// OverBudget reports whether Measured is above Budget.
	OverBudget bool `json:"over_budget"`
	// Unmeasured is how many running jobs had no CPU measurement at the last sample. Until a job is measured, the governor assumes it uses its cost.
	Unmeasured int `json:"unmeasured"`
	// Jobs is each running job's CPU use, by ID.
	Jobs map[string]float64 `json:"jobs,omitempty"`
	// Note explains a report that carries no measurement, or a host read that failed.
	Note string `json:"note,omitempty"`
}

// offenderSlack is how far above its cost a lease may run before it counts as the cause of an oversubscription.
const offenderSlack = 0.05

// MechCap is the control a Limits.MaxCPUs bound applies to an actuator.
const MechCap Mechanism = "cap"

// A governor samples what the running jobs use. It enforces each lease's limits.
//
// Lock order: tickMu, then a lease's ctl, then mu, then a lease's mu. mu
// guards the governor's own state, and no control call runs with it held.
type governor struct {
	sched *Scheduler
	// tickMu runs one tick at a time.
	tickMu  sync.Mutex
	mu      sync.Mutex
	sampler *cpusample.Sampler
	sample  Reading
	// rates is what each lease used at the latest sample.
	rates map[*Lease]float64
	// unmeasured is how many running jobs had no CPU measurement at the latest sample.
	unmeasured int
	// lastAt is when each actuator lease was read last.
	lastAt map[*Lease]time.Time
	// active holds the budget controls in effect, by lease.
	active map[*Lease][]Control
	// caps holds the MaxCPUs bound in effect, by lease.
	caps map[*Lease]Control
	// failed holds when each control that did not take effect was last tried, by lease.
	failed map[*Lease]map[controlKey]time.Time
	// hostErr is why the host's own counters cannot be read. It is empty while they can.
	hostErr string
	// unreadableSince is when the host's counters first could not be read, and errorLoggedAt is when the error about it was last logged.
	unreadableSince, errorLoggedAt time.Time
	// lowSince is when the load first fell to the budget. It is zero while the load is above the budget.
	lowSince time.Time
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
		sampler: newSampler(s.cfg.StatsURL),
		rates:   map[*Lease]float64{},
		lastAt:  map[*Lease]time.Time{},
		active:  map[*Lease][]Control{},
		caps:    map[*Lease]Control{},
		failed:  map[*Lease]map[controlKey]time.Time{},
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

// A target is one running job the governor can see.
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

// tick takes one sample, enforces limits, and brings the budget controls in
// line. It decides with mu held, and makes the control calls after.
func (g *governor) tick() {
	g.tickMu.Lock()
	defer g.tickMu.Unlock()
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
	reading := g.sampler.Sample(roots)
	// Asking this host how it stops a process starts one and waits for it.
	facts := g.hostFacts(asksTheHost(targets))
	now := g.sched.now()
	budget := g.sched.Budget()
	rates := g.measure(targets, reading, now)

	g.mu.Lock()
	live := g.recordSampleLocked(targets, rates, reading, now)
	errLine, okLine := g.watchHostLoadLocked(reading, now)
	plan, limited, revoke := g.planLimitsLocked(live, now)
	plan = append(plan, g.planBudgetLocked(live, reading, budget, now)...)
	g.mu.Unlock()

	if errLine != "" {
		g.sched.cfg.ErrorLogf("%s", errLine)
	}
	if okLine != "" {
		g.sched.cfg.Logf("%s", okLine)
	}
	g.sched.sampled(hostLoad{cpus: reading.HostCPUs, runnable: reading.HostRunnable})
	for _, a := range plan {
		g.perform(a, facts, now)
	}

	g.mu.Lock()
	g.graceLocked(limited, now, revoke)
	g.mu.Unlock()
	for l, reason := range revoke {
		g.sched.cfg.Logf("go-jobserver: revoking %s: %s", l.key(), reason)
		l.Revoke(reason)
	}
}

// measure reads what each target used since the last sample, and adds it to
// the lease. A target with no reading is left out of the rates.
func (g *governor) measure(targets []target, reading Reading, now time.Time) map[*Lease]float64 {
	g.mu.Lock()
	last := make(map[*Lease]time.Time, len(targets))
	for _, t := range targets {
		if at, ok := g.lastAt[t.l]; ok {
			last[t.l] = at
		}
	}
	g.mu.Unlock()

	rates := map[*Lease]float64{}
	for _, t := range targets {
		var rate float64
		var span time.Duration
		if t.a != nil {
			cpus, ok := t.a.Usage()
			if !ok {
				continue
			}
			from, had := last[t.l]
			if !had {
				from = t.l.started
			}
			rate, span = cpus, now.Sub(from)
		} else {
			r, ok := reading.Jobs[t.l.key()]
			if !reading.OK || !ok {
				continue
			}
			rate, span = r, reading.Window
		}
		rates[t.l] = rate
		t.l.mu.Lock()
		t.l.cpus = rate
		t.l.measured = true
		if span > 0 {
			if seconds := rate * span.Seconds(); seconds > 0 {
				t.l.cpuSeconds += seconds
			}
			t.l.measuredFor += span
		}
		t.l.mu.Unlock()
	}
	return rates
}

// recordSampleLocked files a sample, and returns the targets whose lease has
// not ended.
func (g *governor) recordSampleLocked(targets []target, rates map[*Lease]float64, reading Reading, now time.Time) []target {
	g.sample = reading
	g.rates = map[*Lease]float64{}
	g.unmeasured = 0
	var live []target
	for _, t := range targets {
		if t.l.ended {
			continue
		}
		live = append(live, t)
		if t.a != nil {
			g.lastAt[t.l] = now
		}
		rate, ok := rates[t.l]
		if !ok {
			g.unmeasured++
			continue
		}
		g.rates[t.l] = rate
	}
	return live
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

// report returns the latest measurement as a status object.
func (g *governor) report() CPUReport {
	budget := g.sched.Budget()
	g.mu.Lock()
	defer g.mu.Unlock()
	out := CPUReport{
		At:           g.sample.At,
		CPUs:         g.sample.CPUs,
		Budget:       budget,
		HostBusy:     g.sample.HostBusy,
		HostCPUs:     g.sample.HostCPUs,
		HostRunnable: g.sample.HostRunnable,
		HostError:    g.hostErr,
		Unmeasured:   g.unmeasured,
		Jobs:         map[string]float64{},
		Note:         g.sample.Note,
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
