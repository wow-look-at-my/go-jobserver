package jobserver

import (
	"sync"
	"time"
)

// CPUReport is what the daemon last measured, and what it did about it.
type CPUReport struct {
	At   time.Time `json:"at,omitempty"`
	CPUs int       `json:"cpus"`
	// Budget is how many CPUs the running jobs may use together.
	Budget float64 `json:"budget"`
	// Measured is how many CPUs the running jobs are using.
	Measured float64 `json:"measured"`
	// HostBusy is how many CPUs were busy across the whole host.
	HostBusy float64 `json:"host_busy"`
	// OverBudget reports whether Measured is above Budget.
	OverBudget bool `json:"over_budget"`
	// Jobs is each running job's CPU use.
	Jobs map[string]float64 `json:"jobs,omitempty"`
	// Note explains a report that carries no measurement.
	Note string `json:"note,omitempty"`
}

// A governor samples CPU use and applies the controls a running job's policy
// enables whenever the running jobs together exceed the budget.
type governor struct {
	srv     *Server
	mu      sync.Mutex
	sampler *sampler
	sample  Reading
	// used accumulates CPU seconds per job across samples, so a finished job can record what its process tree consumed.
	used map[string]float64
	// active holds the controls in effect, by job.
	active map[string][]Control
	// history is every control this daemon has applied or reverted, newest last.
	history []Control
}

// historyLimit bounds the control log the status surface carries.
const historyLimit = 64

func newGovernor(s *Server) *governor {
	return &governor{
		srv:     s,
		sampler: newSampler(),
		used:    map[string]float64{},
		active:  map[string][]Control{},
	}
}

// loop samples on a cadence until the server closes.
func (g *governor) loop() {
	defer g.srv.wg.Done()
	t := time.NewTicker(g.srv.cfg.SampleInterval)
	defer t.Stop()
	for {
		select {
		case <-g.srv.quit:
			return
		case <-t.C:
			g.tick()
		}
	}
}

// tick takes one sample and brings the controls in line with it.
func (g *governor) tick() {
	roots := g.srv.runningRoots()
	reading := g.sampler.sample(roots)

	g.mu.Lock()
	defer g.mu.Unlock()
	g.sample = reading
	if !reading.OK {
		return
	}
	for id, rate := range reading.Jobs {
		if rate > 0 {
			g.used[id] += rate * reading.Window.Seconds()
		}
	}
	measured := 0.0
	for _, rate := range reading.Jobs {
		measured += rate
	}
	if budget := g.srv.budget(); measured > budget {
		g.throttle(reading, roots)
		return
	}
	// Back within budget: give every process back its normal scheduling.
	for id := range g.active {
		g.releaseLocked(id, reading.CPUs)
	}
}

// throttle applies every mechanism a running job enables to the processes that
// job's selectors pick out.
func (g *governor) throttle(reading Reading, roots map[string]int) {
	index := newProcIndex(reading.Table)
	for id, root := range roots {
		j, ok := g.srv.store.Job(id)
		if !ok {
			continue
		}
		for _, m := range j.Policy.Enabled() {
			settings := j.Policy.settings(m)
			for _, p := range settings.selected(index.tree(root)) {
				if g.isActiveLocked(id, m, p.PID) {
					continue
				}
				out, detail := applyMechanism(m, p.PID, j.Policy, reading.CPUs)
				g.recordLocked(Control{
					Job: id, Mechanism: m, PID: p.PID, Name: p.Name,
					Outcome: out, Detail: detail, Active: true, At: time.Now(),
				})
			}
		}
	}
}

// isActiveLocked reports whether a mechanism is already in effect on a
// process, so a sample that is still over budget does not reapply it.
func (g *governor) isActiveLocked(id string, m Mechanism, pid int) bool {
	for _, c := range g.active[id] {
		if c.Mechanism == m && c.PID == pid {
			return true
		}
	}
	return false
}

// recordLocked files one control, remembering it as in effect when applied.
func (g *governor) recordLocked(c Control) {
	if c.Active {
		g.active[c.Job] = append(g.active[c.Job], c)
	}
	g.history = append(g.history, c)
	if len(g.history) > historyLimit {
		g.history = append([]Control(nil), g.history[len(g.history)-historyLimit:]...)
	}
}

// releaseLocked reverts every control in effect on one job's processes.
func (g *governor) releaseLocked(id string, cpus int) {
	for _, c := range g.active[id] {
		out, detail := revertMechanism(c.Mechanism, c.PID, cpus)
		g.history = append(g.history, Control{
			Job: id, Mechanism: c.Mechanism, PID: c.PID, Name: c.Name,
			Outcome: out, Detail: detail, At: time.Now(),
		})
	}
	delete(g.active, id)
}

// finish records a job's measured CPU, reverts its controls, and forgets it.
func (g *governor) finish(id string) {
	cpus := g.srv.sampleCPUs()
	g.mu.Lock()
	used, had := g.used[id]
	g.releaseLocked(id, cpus)
	delete(g.used, id)
	g.mu.Unlock()
	if had {
		if err := g.srv.store.FinishCPU(id, used); err != nil {
			g.srv.cfg.Logf("go-jobserver: cpu %s: %v", id, err)
		}
	}
}

// releaseAll reverts every control this daemon has applied.
func (g *governor) releaseAll() {
	cpus := g.srv.sampleCPUs()
	g.mu.Lock()
	defer g.mu.Unlock()
	for id := range g.active {
		g.releaseLocked(id, cpus)
	}
}

// report returns the latest measurement as a status object.
func (g *governor) report() CPUReport {
	budget := g.srv.budget()
	g.mu.Lock()
	defer g.mu.Unlock()
	out := CPUReport{
		At:       g.sample.At,
		CPUs:     g.sample.CPUs,
		Budget:   budget,
		Measured: 0,
		HostBusy: g.sample.HostBusy,
		Jobs:     map[string]float64{},
		Note:     g.sample.Note,
	}
	if out.CPUs == 0 {
		out.CPUs = g.srv.cfg.cpuCount()
	}
	for id, rate := range g.sample.Jobs {
		out.Jobs[id] = rate
		out.Measured += rate
	}
	out.OverBudget = g.sample.OK && out.Measured > budget
	return out
}

// controls lists the controls in effect right now.
func (g *governor) controls() []Control {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Control, 0, len(g.history))
	for _, c := range g.history {
		if c.Active {
			out = append(out, c)
		}
	}
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
