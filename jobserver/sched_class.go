package jobserver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ClassStats is what the scheduler measured for one class of work.
type ClassStats struct {
	Class string `json:"class"`
	// Cost is the CPUs a run of the class is expected to use: a moving average of measured runs.
	Cost float64 `json:"cost"`
	// Peak is the most CPUs one measured run used on average.
	Peak float64 `json:"peak"`
	// Runs is how many measured runs the history holds.
	Runs int `json:"runs"`
	// Revoked is how many of them the scheduler ended for exceeding a limit.
	Revoked int `json:"revoked"`
	// CPUSeconds is the CPU time every measured run used, together.
	CPUSeconds float64   `json:"cpu_seconds"`
	Last       time.Time `json:"last"`
}

// classWeight is how much one measured run moves a class's cost.
const classWeight = 0.3

// A classBook keeps the cost history per class, in a file when it has one.
type classBook struct {
	path string
	logf func(string, ...any)

	mu      sync.Mutex
	saveMu  sync.Mutex
	classes map[string]*ClassStats
}

func openClassBook(path string, logf func(string, ...any)) *classBook {
	b := &classBook{path: path, logf: logf, classes: map[string]*ClassStats{}}
	if path == "" {
		return b
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			logf("go-jobserver: class history %s: %v", path, err)
		}
		return b
	}
	var list []ClassStats
	if err := json.Unmarshal(data, &list); err != nil {
		logf("go-jobserver: class history %s is unreadable, starting empty: %v", path, err)
		return b
	}
	for i := range list {
		c := list[i]
		if c.Class != "" {
			b.classes[c.Class] = &c
		}
	}
	return b
}

// cost returns a class's expected cost, when the class has history.
func (b *classBook) cost(class string) (float64, bool) {
	if class == "" {
		return 0, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.classes[class]
	if !ok || c.Runs == 0 {
		return 0, false
	}
	return c.Cost, true
}

// observe records one measured run of a class and saves the history. span is
// how long the run's measurements cover.
func (b *classBook) observe(class string, cpuSeconds float64, span time.Duration, revoked bool) {
	if class == "" || span <= 0 {
		return
	}
	avg := cpuSeconds / span.Seconds()
	b.mu.Lock()
	c, ok := b.classes[class]
	if !ok {
		c = &ClassStats{Class: class, Cost: avg}
		b.classes[class] = c
	} else {
		c.Cost += classWeight * (avg - c.Cost)
	}
	c.Runs++
	c.CPUSeconds += cpuSeconds
	if avg > c.Peak {
		c.Peak = avg
	}
	if revoked {
		c.Revoked++
	}
	c.Last = time.Now().UTC()
	b.mu.Unlock()
	b.save()
}

func (b *classBook) list() []ClassStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listLocked()
}

func (b *classBook) listLocked() []ClassStats {
	out := make([]ClassStats, 0, len(b.classes))
	for _, c := range b.classes {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Class < out[k].Class })
	return out
}

// save writes the history through a temporary file, so a crash leaves the
// file or the new one. Saves are serialized and each writes the newest
// history, so an older one never lands last.
func (b *classBook) save() {
	if b.path == "" {
		return
	}
	b.saveMu.Lock()
	defer b.saveMu.Unlock()
	data, err := json.Marshal(b.list())
	if err != nil {
		b.logf("go-jobserver: class history: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(b.path), 0o755); err != nil {
		b.logf("go-jobserver: class history: %v", err)
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(b.path), ".classes-*")
	if err != nil {
		b.logf("go-jobserver: class history: %v", err)
		return
	}
	_, werr := tmp.Write(data)
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		os.Remove(tmp.Name())
		b.logf("go-jobserver: class history: write %v, close %v", werr, cerr)
		return
	}
	if err := os.Rename(tmp.Name(), b.path); err != nil {
		os.Remove(tmp.Name())
		b.logf("go-jobserver: class history: %v", err)
	}
}
