package jobserver

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A Proc is one process as the host reported it.
type Proc struct {
	PID  int
	PPID int
	PGID int
	Name string
	// CPU is the process's accumulated user plus system time.
	CPU time.Duration
}

// A Reading is one sample of what the daemon's jobs and the host are using.
type Reading struct {
	At time.Time
	// CPUs is how many logical CPUs the host has.
	CPUs int
	// HostBusy is how many CPUs were busy across the whole host, counted from process CPU time.
	HostBusy float64
	// Window is how long the rates were measured over.
	Window time.Duration
	// Jobs maps a job id to how many CPUs its process tree used.
	Jobs map[string]float64
	// Table is the snapshot the rates came from.
	Table []Proc
	// OK reports whether rates could be computed.
	OK bool
	// Note explains a sample that carries no rates.
	Note string
}

// clockTick is the resolution of the CPU times /proc reports. Linux counts in hundredths of a second.
const clockTick = 10 * time.Millisecond

// A procSource lists every process on the host in one shot.
type procSource interface {
	procs() ([]Proc, error)
}

// procfsSource reads /proc. It is what a Linux host offers.
type procfsSource struct{ root string }

func (s procfsSource) procs() ([]Proc, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	out := make([]Proc, 0, len(entries))
	for _, e := range entries {
		if !isDigits(e.Name()) {
			continue
		}
		body, err := os.ReadFile(s.root + "/" + e.Name() + "/stat")
		if err != nil {
			// The process exited between the listing and the read.
			continue
		}
		if p, ok := parseProcStat(string(body)); ok {
			out = append(out, p)
		}
	}
	return out, nil
}

// psSource asks ps for the process table. It is what a host without /proc offers.
type psSource struct{}

func (psSource) procs() ([]Proc, error) {
	out, err := exec.Command("ps", "-Ao", "pid=,ppid=,pgid=,time=,comm=").Output()
	if err != nil {
		return nil, fmt.Errorf("ps: %w", err)
	}
	return parsePS(string(out))
}

// newProcSource picks the process source for the host this binary is running
// on, which is a runtime question: one binary serves Linux, macOS and Windows.
func newProcSource() procSource {
	if _, err := os.Stat("/proc/self/stat"); err == nil {
		return procfsSource{root: "/proc"}
	}
	return psSource{}
}

// parseProcStat parses one /proc/<pid>/stat line. The command name is
// parenthesized and may itself contain spaces and parentheses, so the fields
// after the last ')' count from the state field.
func parseProcStat(text string) (Proc, bool) {
	open := strings.IndexByte(text, '(')
	closeAt := strings.LastIndexByte(text, ')')
	if open < 0 || closeAt < open {
		return Proc{}, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(text[:open]))
	if err != nil {
		return Proc{}, false
	}
	fields := strings.Fields(text[closeAt+1:])
	if len(fields) < 13 {
		return Proc{}, false
	}
	utime, ok1 := atoiField(fields[11])
	stime, ok2 := atoiField(fields[12])
	if !ok1 || !ok2 {
		return Proc{}, false
	}
	return Proc{
		PID:  pid,
		Name: text[open+1 : closeAt],
		PPID: atoiOr(fields[1]),
		PGID: atoiOr(fields[2]),
		CPU:  time.Duration(utime+stime) * clockTick,
	}, true
}

// parsePS parses the "pid ppid pgid time comm" table ps prints.
func parsePS(text string) ([]Proc, error) {
	var out []Proc
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		cpu, err := parseCPUTime(fields[3])
		if err != nil {
			continue
		}
		out = append(out, Proc{
			PID:  atoiOr(fields[0]),
			PPID: atoiOr(fields[1]),
			PGID: atoiOr(fields[2]),
			Name: strings.Join(fields[4:], " "),
			CPU:  cpu,
		})
	}
	return out, nil
}

// parseCPUTime reads the elapsed CPU time ps prints: [[D-]HH:]MM:SS[.ff].
func parseCPUTime(text string) (time.Duration, error) {
	days := 0
	if dash := strings.IndexByte(text, '-'); dash > 0 {
		d, err := strconv.Atoi(text[:dash])
		if err != nil {
			return 0, err
		}
		days = d
		text = text[dash+1:]
	}
	parts := strings.Split(text, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("jobserver: %q is not a CPU time", text)
	}
	secs, err := strconv.ParseFloat(parts[len(parts)-1], 64)
	if err != nil {
		return 0, err
	}
	higher := 0
	for i, part := range parts[:len(parts)-1] {
		v, err := strconv.Atoi(part)
		if err != nil {
			return 0, err
		}
		// The field before the seconds is minutes; anything further left is a whole day's worth of minutes.
		mult := 1
		if i == 0 && len(parts) == 3 {
			mult = 60
		}
		higher += v * mult
	}
	total := time.Duration(days)*24*time.Hour +
		time.Duration(higher)*time.Minute +
		time.Duration(secs*float64(time.Second))
	return total, nil
}

func atoiField(text string) (int, bool) {
	v, err := strconv.Atoi(text)
	if err != nil {
		return 0, false
	}
	return v, true
}

func atoiOr(text string) int {
	v, _ := atoiField(text)
	return v
}

func isDigits(text string) bool {
	if text == "" {
		return false
	}
	for _, r := range text {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// A procIndex makes process-tree lookups cheap within one snapshot.
type procIndex struct {
	children map[int][]int
	group    map[int][]Proc
	byPID    map[int]Proc
}

func newProcIndex(procs []Proc) *procIndex {
	x := &procIndex{
		children: make(map[int][]int, len(procs)),
		group:    make(map[int][]Proc, len(procs)),
		byPID:    make(map[int]Proc, len(procs)),
	}
	for _, p := range procs {
		x.byPID[p.PID] = p
		x.children[p.PPID] = append(x.children[p.PPID], p.PID)
		x.group[p.PGID] = append(x.group[p.PGID], p)
	}
	return x
}

// tree returns the processes belonging to the tree rooted at root: the root
// itself, every descendant of it, and every process in its process group.
// Descendants cover a child that changed its own group; the group covers a
// descendant that was reparented away.
func (x *procIndex) tree(root int) []Proc {
	seen := map[int]bool{root: true}
	out := make([]Proc, 0, 4)
	if p, ok := x.byPID[root]; ok {
		out = append(out, p)
	}
	queue := []int{root}
	for len(queue) > 0 {
		pid := queue[0]
		queue = queue[1:]
		for _, child := range x.children[pid] {
			if seen[child] {
				continue
			}
			seen[child] = true
			if p, ok := x.byPID[child]; ok {
				out = append(out, p)
			}
			queue = append(queue, child)
		}
	}
	for _, p := range x.group[root] {
		if seen[p.PID] {
			continue
		}
		seen[p.PID] = true
		out = append(out, p)
	}
	return out
}

// A sampler turns process snapshots into CPU rates.
type sampler struct {
	src  procSource
	now  func() time.Time
	mu   sync.Mutex
	at   time.Time
	prev map[int]time.Duration
	base bool
}

func newSampler() *sampler { return &sampler{src: newProcSource(), now: time.Now} }

// nowAt reads the clock, falling back to the real one.
func (s *sampler) nowAt() time.Time {
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

// ownPID is this process's id, which the daemon samples alongside everything else.
var ownPID = os.Getpid()

// sample returns the CPU used since the sample, split by job. roots maps a
// job id to the process id at the head of that job's process tree.
func (s *sampler) sample(roots map[string]int) Reading {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowAt()
	r := Reading{At: now, CPUs: runtime.NumCPU(), Jobs: map[string]float64{}}
	procs, err := s.src.procs()
	if err != nil {
		s.at, s.base, s.prev = now, false, nil
		r.Note = err.Error()
		return r
	}
	r.Table = procs
	window := now.Sub(s.at)
	r.Window = window
	if !s.base || window <= 0 {
		s.at, s.base = now, true
		s.prev = cpuByPID(procs)
		r.Note = "measuring from the next sample"
		return r
	}
	delta := make(map[int]time.Duration, len(procs))
	var host time.Duration
	for _, p := range procs {
		// A process that appeared since the last sample has no rate yet, and a
		// recycled pid never goes backwards. A negative step is left out.
		if was, had := s.prev[p.PID]; had && p.CPU >= was {
			d := p.CPU - was
			delta[p.PID] = d
			host += d
		}
	}
	seconds := window.Seconds()
	r.OK = true
	r.HostBusy = float64(host) / float64(time.Second) / seconds
	index := newProcIndex(procs)
	for id, root := range roots {
		var used time.Duration
		for _, p := range index.tree(root) {
			used += delta[p.PID]
		}
		r.Jobs[id] = float64(used) / float64(time.Second) / seconds
	}
	s.at = now
	s.prev = cpuByPID(procs)
	return r
}

// cpuByPID indexes a snapshot's accumulated CPU times.
func cpuByPID(procs []Proc) map[int]time.Duration {
	out := make(map[int]time.Duration, len(procs))
	for _, p := range procs {
		out[p.PID] = p.CPU
	}
	return out
}
