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
// when the count cap has room. Its cost fits bookable. The host must also
// have room: the tasks that want a CPU. That host is with the jobs not
// measured yet and this counted at their cost, stay within bookable for the
// host's CPUs. Neither CPU limit holds back the only lease, so work larger
// than the whole budget still runs on an otherwise idle scheduler.
func (s *Scheduler) planLocked() {
	if len(s.waiting) == 0 {
		return
	}
	now := s.now()
	p := s.planStateLocked()
	type wait struct {
		l  *Lease
		st QueueState
	}
	var queued []wait
	for _, l := range s.orderLocked(now) {
		if st, ok := s.fitLocked(p, l.req, l.cost); !ok {
			queued = append(queued, wait{l, st})
			continue
		}
		p.take(l.req, l.cost)
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

// planState is what the holders take, counted the way one plan pass counts it.
type planState struct {
	budget     float64
	running    int
	reserved   float64
	unmeasured float64
	inGroup    map[string]int
}

func (s *Scheduler) planStateLocked() *planState {
	p := &planState{budget: s.budgetLocked(), inGroup: map[string]int{}}
	for _, h := range s.holding {
		if h.req.Group != "" {
			p.inGroup[h.req.Group]++
		}
		if !h.req.GroupOnly {
			p.running++
			p.reserved += h.cost
			if !h.Measured() {
				p.unmeasured += h.cost
			}
		}
	}
	return p
}

// fitLocked says whether a lease for req at cost starts against p, and where
// it waits when it does not. It is the place the admission gates live.
func (s *Scheduler) fitLocked(p *planState, req LeaseRequest, cost float64) (QueueState, bool) {
	if g := req.Group; g != "" {
		if limit := s.groups[g]; p.inGroup[g] >= limit {
			return QueueState{Kind: WaitGroup, Key: g,
				Reason: fmt.Sprintf("group %s: %d of %d running", g, p.inGroup[g], limit)}, false
		}
	}
	if req.GroupOnly {
		return QueueState{}, true
	}
	if s.maxRuns > 0 && p.running >= s.maxRuns {
		return QueueState{Kind: WaitCap, Key: WaitCap,
			Reason: fmt.Sprintf("run cap: %d of %d running", p.running, s.maxRuns)}, false
	}
	if p.reserved > 0 && p.reserved+cost > bookable(p.budget) {
		return QueueState{Kind: WaitCPU, Key: WaitCPU,
			Reason: fmt.Sprintf("cpu budget: %s of %s CPUs reserved, this run needs %s",
				fmtCPUs(p.reserved), fmtCPUs(p.budget), fmtCPUs(cost))}, false
	}
	if p.running > 0 && s.host.cpus > 0 && float64(s.host.runnable)+p.unmeasured+cost > bookable(float64(s.host.cpus)) {
		return QueueState{Kind: WaitCPU, Key: WaitCPU,
			Reason: fmt.Sprintf("host load: %d tasks want a CPU on %d CPUs, this run needs %s",
				s.host.runnable, s.host.cpus, fmtCPUs(cost))}, false
	}
	return QueueState{}, true
}

// take counts a granted lease in p.
func (p *planState) take(req LeaseRequest, cost float64) {
	if !req.GroupOnly {
		p.running++
		p.reserved += cost
		p.unmeasured += cost
	}
	if req.Group != "" {
		p.inGroup[req.Group]++
	}
}

// An Admission is what would happen to a lease asked.
type Admission struct {
	// Cost is the CPUs the lease would be admitted at: declared, then measured for its class, then the default.
	Cost float64 `json:"cost"`
	// Starts is true when the lease would take a slot at once.
	Starts bool `json:"starts"`
	// Wait is where it would queue when it does not start. Position and Holders are not set.
	Wait *QueueState `json:"wait,omitempty"`
}

// Admits says whether a lease for req would start now, through the same gates
// a plan pass applies. It queues nothing and changes nothing. A lease queued
// ahead that cannot start holds nothing back, because the pass is first fit.
func (s *Scheduler) Admits(req LeaseRequest) (Admission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Group != "" {
		if _, ok := s.groups[req.Group]; !ok {
			return Admission{}, fmt.Errorf("%w: %q", ErrUnknownGroup, req.Group)
		}
	}
	a := Admission{Cost: s.costOf(req)}
	st, ok := s.fitLocked(s.planStateLocked(), req, a.Cost)
	a.Starts = ok
	if !ok {
		a.Wait = &st
	}
	return a, nil
}

// sampled takes the host's load at a sample and plans again, so a queued job
// starts as soon as the host has room.
func (s *Scheduler) sampled(h hostLoad) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.host = h
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
