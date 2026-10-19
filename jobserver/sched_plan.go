package jobserver

import (
	"fmt"
	"math"
	"slices"
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
// when the count cap has room. Its cost fits the CPU budget plus a tenth, or
// the CPUs are idle (see idleFits). The CPU budget never holds back the only
// lease: work larger than the whole budget still runs on an otherwise idle
// scheduler.
func (s *Scheduler) planLocked() {
	if len(s.waiting) == 0 {
		return
	}
	now := s.now()
	budget := s.budgetLocked()
	running, reserved := 0, 0.0
	inUse, unmeasuredCost := s.useLocked()
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
			if reserved > 0 && reserved+l.cost > budget*overbook {
				if !s.idleFits(l.cost, reserved, inUse, unmeasuredCost, budget) {
					queued = append(queued, wait{l, QueueState{Kind: WaitCPU, Key: WaitCPU,
						Reason: fmt.Sprintf("cpu budget: %s of %s CPUs reserved, this run needs %s",
							fmtCPUs(reserved), fmtCPUs(budget), fmtCPUs(l.cost))}})
					continue
				}
				s.filled = s.used.n
			}
			running++
			reserved += l.cost
			inUse += l.cost
			unmeasuredCost += l.cost
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

// useLocked adds up the CPUs the running jobs use now. A job that is not
// measured yet counts at its cost, and unmeasured is the part of the sum that
// comes from costs.
func (s *Scheduler) useLocked() (inUse, unmeasured float64) {
	for _, h := range s.holding {
		if h.req.GroupOnly {
			continue
		}
		if rate, ok := s.used.rates[h]; ok {
			inUse += rate
			continue
		}
		inUse += h.cost
		unmeasured += h.cost
	}
	return inUse, unmeasured
}

// idleFits reports whether a job whose cost does not fit the booked CPUs may
// start because CPUs are idle. The CPUs must have stayed idle for idleSamples
// samples, and the last such start must be as old. The host and the budget
// must both have room for the job, which is its cost or idleRoom, whichever
// is less.
func (s *Scheduler) idleFits(cost, reserved, inUse, unmeasured, budget float64) bool {
	u := s.used
	if u.hostCPUs == 0 || s.idleRun < idleSamples || u.n-s.filled < idleSamples {
		return false
	}
	room := min(cost, idleRoom)
	return reserved+cost <= budget*fillLimit &&
		inUse+room <= budget &&
		u.hostBusy+unmeasured+room <= float64(u.hostCPUs)
}

// sampled takes what the CPUs did at a sample, counts the samples in a row at
// which they sat idle, and plans again.
func (s *Scheduler) sampled(u usage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.used = u
	inUse, unmeasured := s.useLocked()
	if u.hostCPUs > 0 && min(float64(u.hostCPUs)-u.hostBusy-unmeasured, s.budgetLocked()-inUse) >= idleRoom {
		s.idleRun++
	} else {
		s.idleRun = 0
	}
	s.planLocked()
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

// orderLocked returns the queued leases in admission order: the priority keys
// compared top to bottom, lower first, then arrival. A pick that starts a new
// batch pulls the rest of that batch in right behind it.
func (s *Scheduler) orderLocked(now time.Time) []*Lease {
	type ranked struct {
		lease *Lease
		key   []float64
	}
	all := make([]ranked, 0, len(s.waiting))
	for _, l := range s.waiting {
		l.rank[s.priority.batch] = 1
		if s.batchOpen(l.req.Batch, now) {
			l.rank[s.priority.batch] = 0
		}
		all = append(all, ranked{lease: l, key: l.rank})
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
