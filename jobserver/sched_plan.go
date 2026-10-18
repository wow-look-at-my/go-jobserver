package jobserver

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// BatchGrace is how long a batch stays open after its last member ends, so members that arrive later still go first.
const BatchGrace = 10 * time.Minute

type batchState struct {
	running int
	lastEnd time.Time
}

// planLocked grants queued leases in admission order while they fit, then
// tells each lease still queued where it stands. Caller holds s.mu.
//
// A lease fits when its group has a free slot, and, unless it is GroupOnly,
// when the count cap has room. Its cost fits the CPU budget. The CPU budget
// never holds back the only lease: work larger than the whole budget still
// runs on an otherwise idle scheduler.
func (s *Scheduler) planLocked() {
	if len(s.waiting) == 0 {
		return
	}
	now := s.now()
	budget := s.budgetLocked()
	running, reserved := 0, 0.0
	inGroup := map[string]int{}
	for _, h := range s.holding {
		if h.req.Group != "" {
			inGroup[h.req.Group]++
		}
		if !h.req.GroupOnly {
			running++
			reserved += h.cost
		}
	}
	type wait struct {
		l  *Lease
		st QueueState
	}
	var queued []wait
	for _, l := range s.orderLocked(now) {
		if g := l.req.Group; g != "" {
			if limit := s.groups[g]; inGroup[g] >= limit {
				queued = append(queued, wait{l, QueueState{Kind: WaitGroup, Key: g,
					Reason: fmt.Sprintf("group %s: %d of %d running", g, inGroup[g], limit)}})
				continue
			}
		}
		if !l.req.GroupOnly {
			if s.maxRuns > 0 && running >= s.maxRuns {
				queued = append(queued, wait{l, QueueState{Kind: WaitCap, Key: WaitCap,
					Reason: fmt.Sprintf("run cap: %d of %d running", running, s.maxRuns)}})
				continue
			}
			if reserved > 0 && reserved+l.cost > budget {
				queued = append(queued, wait{l, QueueState{Kind: WaitCPU, Key: WaitCPU,
					Reason: fmt.Sprintf("cpu budget: %s of %s CPUs reserved, this run needs %s",
						fmtCPUs(reserved), fmtCPUs(budget), fmtCPUs(l.cost))}})
				continue
			}
			running++
			reserved += l.cost
		}
		if l.req.Group != "" {
			inGroup[l.req.Group]++
		}
		l.started = now
		s.holding = append(s.holding, l)
		s.admitBatch(l.req.Batch, now)
		close(l.ready)
	}

	s.waiting = s.waiting[:0]
	positions := map[string]int{}
	for _, w := range queued {
		s.waiting = append(s.waiting, w.l)
		positions[w.st.Key]++
		w.st.Position = positions[w.st.Key]
		w.st.Holders = s.holderIDsLocked(w.st)
		if w.l.told && w.l.state.equal(w.st) {
			continue
		}
		w.l.told, w.l.state = true, w.st
		if w.l.notify != nil {
			w.l.notify(w.st)
		}
	}
}

// holderIDsLocked lists the holders a queued lease waits behind: its group's
// for a group wait, every counted holder's otherwise.
func (s *Scheduler) holderIDsLocked(st QueueState) []string {
	var out []string
	for _, h := range s.holding {
		if h.req.ID == "" {
			continue
		}
		if st.Kind == WaitGroup {
			if h.req.Group == st.Key {
				out = append(out, h.req.ID)
			}
			continue
		}
		if !h.req.GroupOnly {
			out = append(out, h.req.ID)
		}
	}
	return out
}

// orderLocked returns the queued leases in admission order: by Key, then a
// lease whose batch has started before one whose batch has not, then arrival.
// A pick that starts a new batch pulls the rest of that batch in right behind
// it.
func (s *Scheduler) orderLocked(now time.Time) []*Lease {
	type ranked struct {
		lease *Lease
		key   []int64
	}
	width := 0
	for _, l := range s.waiting {
		width = max(width, len(l.req.Key))
	}
	all := make([]ranked, 0, len(s.waiting))
	for _, l := range s.waiting {
		// The batch row follows the longest key, so every lease compares it at one position.
		key := make([]int64, width+1)
		copy(key, l.req.Key)
		if !s.batchOpen(l.req.Batch, now) {
			key[width] = 1
		}
		all = append(all, ranked{lease: l, key: key})
	}
	slices.SortStableFunc(all, func(a, b ranked) int {
		if c := slices.Compare(a.key, b.key); c != 0 {
			return c
		}
		return cmpSeq(a.lease, b.lease)
	})
	out := make([]*Lease, 0, len(all))
	for start := 0; start < len(all); {
		end := start + 1
		for end < len(all) && slices.Equal(all[end].key, all[start].key) {
			end++
		}
		level := make([]*Lease, 0, end-start)
		for _, r := range all[start:end] {
			level = append(level, r.lease)
		}
		out = append(out, groupBatches(level)...)
		start = end
	}
	return out
}

func cmpSeq(a, b *Lease) int {
	switch {
	case a.seq < b.seq:
		return -1
	case a.seq > b.seq:
		return 1
	}
	return 0
}

// groupBatches keeps ls in arrival order, except that each batch's members
// follow its first member, so a new batch starts as one unit.
func groupBatches(ls []*Lease) []*Lease {
	taken := set.New[*Lease]()
	out := make([]*Lease, 0, len(ls))
	for _, l := range ls {
		if taken.Contains(l) {
			continue
		}
		taken.Add(l)
		out = append(out, l)
		if l.req.Batch == "" {
			continue
		}
		for _, m := range ls {
			if !taken.Contains(m) && m.req.Batch == l.req.Batch {
				taken.Add(m)
				out = append(out, m)
			}
		}
	}
	return out
}

// BatchOpen reports whether a batch has started: a member holds a slot now,
// or one released within BatchGrace.
func (s *Scheduler) BatchOpen(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batchOpen(key, s.now())
}

func (s *Scheduler) batchOpen(key string, now time.Time) bool {
	if key == "" {
		return false
	}
	b := s.batches[key]
	return b != nil && (b.running > 0 || now.Sub(b.lastEnd) < BatchGrace)
}

func (s *Scheduler) admitBatch(key string, now time.Time) {
	if key == "" {
		return
	}
	b := s.batches[key]
	if b == nil || (b.running == 0 && now.Sub(b.lastEnd) >= BatchGrace) {
		b = &batchState{}
		s.batches[key] = b
	}
	b.running++
}

func (s *Scheduler) endBatch(key string, now time.Time) {
	if b := s.batches[key]; b != nil {
		b.running--
		b.lastEnd = now
	}
	for k, b := range s.batches {
		if b.running <= 0 && now.Sub(b.lastEnd) >= BatchGrace {
			delete(s.batches, k)
		}
	}
}

// fmtCPUs renders a CPU count with no more precision than it carries.
func fmtCPUs(v float64) string {
	return strconv.FormatFloat(math.Round(v*100)/100, 'f', -1, 64)
}
