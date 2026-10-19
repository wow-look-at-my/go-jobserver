package jobserver

import (
	"math"
	"sync"
	"time"

	"github.com/wow-look-at-my/simple-stats-api/cpusample"
)

// scriptedHost serves host counters that move by a set busy share at each read.
type scriptedHost struct {
	mu      sync.Mutex
	cpus    int
	share   float64
	running int
	err     error
	busy    uint64
	total   uint64
}

func (h *scriptedHost) HostStat() (cpusample.HostStat, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return cpusample.HostStat{}, h.err
	}
	const step = 1000
	h.total += step
	h.busy += uint64(math.Round(h.share * step))
	return cpusample.HostStat{Busy: h.busy, Total: h.total, CPUs: h.cpus, Running: h.running + 1}, nil
}

// set sets the busy share of the host's CPUs from the next read on.
func (h *scriptedHost) set(share float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.share = share
}

// setRunning sets how many tasks want a CPU, not counting the reader.
func (h *scriptedHost) setRunning(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.running = n
}

// hostOf hands a scripted host to a sampler.
func hostOf(h *scriptedHost) cpusample.HostReader {
	return cpusample.NewCounterReader(h)
}

// countingSource serves one process table and counts the scans.
type countingSource struct {
	mu    sync.Mutex
	table []Proc
	scans int
}

func (c *countingSource) Procs() ([]Proc, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.scans++
	return append([]Proc(nil), c.table...), nil
}

func (c *countingSource) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scans
}

// burn adds CPU time to one process of the table.
func (c *countingSource) burn(pid int, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.table {
		if c.table[i].PID == pid {
			c.table[i].CPU += d
		}
	}
}
