// BatchGrace is how long a batch stays open after its last member ends, so members that arrive later still go first.

package jobserver

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
	"github.com/wow-look-at-my/go-jobserver/priority"
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
// than the whole budget still runs on an otherwise idle scheduler. The host
// gate reads the median of the last samples, and it skips a lease that costs
// freeCost or less.
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
		if limit, _ := s.groupLimitLocked(g); p.inGroup[g] >= limit {
			return QueueState{Kind: WaitGroup, Key: g,
				Reason: fmt.Sprintf("group %s: %d of %d running", g, p.inGroup[g], limit)}, false
		}
	}
	if req.GroupOnly {
		return QueueState{}, true
	}
	if limit := s.capLocked(); limit > 0 && p.running >= limit {
		return QueueState{Kind: WaitCap, Key: WaitCap,
			Reason: fmt.Sprintf("run cap: %d of %d running", p.running, limit)}, false
	}
	if p.reserved > 0 && p.reserved+cost > bookable(p.budget) {
		return QueueState{Kind: WaitCPU, Key: WaitCPU,
			Reason: fmt.Sprintf("cpu budget: %s of %s CPUs reserved, this run needs %s",
				fmtCPUs(p.reserved), fmtCPUs(p.budget), fmtCPUs(cost))}, false
	}
	if cost <= freeCost {
		return QueueState{}, true
	}
	if runnable := s.host.runnable(); p.running > 0 && s.host.cpus > 0 && float64(runnable)+p.unmeasured+cost > bookable(float64(s.host.cpus)) {
		return QueueState{Kind: WaitCPU, Key: WaitCPU,
			Reason: fmt.Sprintf("host load: %d tasks want a CPU on %d CPUs, this run needs %s",
				runnable, s.host.cpus, fmtCPUs(cost))}, false
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
func (s *Scheduler) sampled(cpus, runnable int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.host.add(cpus, runnable)
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

// orderLocked returns the queued leases in admission order. Each key list
// sorts its own leases by its Compare, then arrival. A pick that starts a new
// batch pulls the rest of that batch in right behind it. The lists then merge:
// the head with the lower rank goes first, then the earlier arrival.
func (s *Scheduler) orderLocked(now time.Time) []*Lease {
	byKeys := map[*priority.Keys][]*Lease{}
	var lists []*priority.Keys
	for _, l := range s.waiting {
		if k := l.order.keys.Lookup(KeyOpenBatch); k != nil {
			v := 1
			if s.batchOpen(l.req.Batch, now) {
				v = 0
			}
			l.order.prio.Set(k, v)
		}
		if _, seen := byKeys[l.order.keys]; !seen {
			lists = append(lists, l.order.keys)
		}
		byKeys[l.order.keys] = append(byKeys[l.order.keys], l)
	}
	sorted := make([][]*Lease, 0, len(lists))
	for _, keys := range lists {
		sorted = append(sorted, orderOneList(keys, byKeys[keys]))
	}
	out := make([]*Lease, 0, len(s.waiting))
	for {
		best := -1
		for i, ls := range sorted {
			if len(ls) == 0 {
				continue
			}
			if best < 0 || cmpHead(ls[0], sorted[best][0]) < 0 {
				best = i
			}
		}
		if best < 0 {
			return out
		}
		out = append(out, sorted[best][0])
		sorted[best] = sorted[best][1:]
	}
}

// orderOneList sorts the leases of one key list by priority, then arrival, with each new batch kept together.
func orderOneList(keys *priority.Keys, ls []*Lease) []*Lease {
	slices.SortStableFunc(ls, func(a, b *Lease) int {
		if c := keys.Compare(a.order.prio, b.order.prio); c != 0 {
			return c
		}
		return cmpSeq(a, b)
	})
	out := make([]*Lease, 0, len(ls))
	for start := 0; start < len(ls); {
		end := start + 1
		for end < len(ls) && keys.Compare(ls[end].order.prio, ls[start].order.prio) == 0 {
			end++
		}
		out = append(out, groupBatches(ls[start:end])...)
		start = end
	}
	return out
}

// cmpHead orders the heads of key lists: lower rank first, then arrival.
func cmpHead(a, b *Lease) int {
	if a.order.rank != b.order.rank {
		if a.order.rank < b.order.rank {
			return -1
		}
		return 1
	}
	return cmpSeq(a, b)
}

// cmpSeq orders leases by arrival: the task's when both carry one, else the lease's.
func cmpSeq(a, b *Lease) int {
	if a.order.arrival != 0 && b.order.arrival != 0 && a.order.arrival != b.order.arrival {
		if a.order.arrival < b.order.arrival {
			return -1
		}
		return 1
	}
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
