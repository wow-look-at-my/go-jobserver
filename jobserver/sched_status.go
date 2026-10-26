// LeaseInfo is one lease as the status surface shows it.

package jobserver

import (
	"sort"
	"time"
)

// LeaseInfo is one lease as the status surface shows it.
type LeaseInfo struct {
	ID    string `json:"id"`
	Class string `json:"class,omitempty"`
	Group string `json:"group,omitempty"`
	// Priority is the lease's value for every priority key, defaults included.
	Priority map[string]float64 `json:"priority"`
	Batch    string             `json:"batch,omitempty"`
	// GroupOnly marks a lease that takes only its group slot.
	GroupOnly bool      `json:"group_only,omitempty"`
	Since     time.Time `json:"since"`
	// Cost is the CPUs the lease is admitted, or would be admitted, at.
	Cost float64 `json:"cost"`
	// CPUs is what the governor last measured the lease using.
	CPUs float64 `json:"cpus,omitempty"`
	// CPUSeconds is what the governor measured the lease using in all.
	CPUSeconds float64 `json:"cpu_seconds,omitempty"`
	// Revoked says why the scheduler ended the lease's work, once it has.
	Revoked string `json:"revoked,omitempty"`
	// Wait is where a queued lease stands. It is empty for a holder.
	Wait *QueueState `json:"wait,omitempty"`
}

// GroupInfo is one named group's use.
type GroupInfo struct {
	Name string `json:"name"`
	// Limit is the effective limit that admission uses.
	Limit int `json:"limit"`
	// Declared is the limit SetGroups or the config gave.
	Declared int `json:"declared"`
	// Override is the operator's limit. It is nil when none is set.
	Override *int `json:"override,omitempty"`
	Active   int  `json:"active"`
	Waiting  int  `json:"waiting"`
}

// BatchInfo is one open batch.
type BatchInfo struct {
	Key string `json:"key"`
	// Holding and Waiting are the IDs of the batch's leases that hold a slot and that wait.
	Holding []string `json:"holding"`
	Waiting []string `json:"waiting"`
	// Closes is when the batch closes. It is nil while a member holds a slot.
	Closes *time.Time `json:"closes,omitempty"`
}

// SchedulerStatus is the scheduler's live state.
type SchedulerStatus struct {
	// MaxConcurrent is the effective count cap that admission uses. Zero means no cap.
	MaxConcurrent int `json:"max_concurrent"`
	// MaxConcurrentDeclared is the count cap the config or SetMaxConcurrent gave.
	MaxConcurrentDeclared int `json:"max_concurrent_declared"`
	// MaxConcurrentOverride is the operator's count cap. It is nil when none is set.
	MaxConcurrentOverride *int    `json:"max_concurrent_override,omitempty"`
	Budget                float64 `json:"budget"`
	// Running is how many counted leases hold a slot.
	Running int `json:"running"`
	// Reserved is the CPUs the counted holders were admitted at, together.
	Reserved float64     `json:"reserved"`
	Holding  []LeaseInfo `json:"holding"`
	Waiting  []LeaseInfo `json:"waiting"`
	Groups   []GroupInfo `json:"groups"`
	// Batches are the open batches, sorted by key.
	Batches []BatchInfo `json:"batches"`
	// PriorityKeys is the admission order, compared top to bottom.
	PriorityKeys []PriorityKey `json:"priority_keys"`
}

// Status reports what holds a slot, what waits and why, and each group's use.
// Holders are in admission order and waiters in planned order.
func (s *Scheduler) Status() SchedulerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	st := SchedulerStatus{MaxConcurrent: s.capLocked(), MaxConcurrentDeclared: s.maxRuns, Budget: s.budgetLocked(),
		Holding: []LeaseInfo{}, Waiting: []LeaseInfo{}, Groups: []GroupInfo{}, Batches: []BatchInfo{},
		PriorityKeys: s.PriorityKeys()}
	if s.capOverridden {
		st.MaxConcurrentOverride = new(s.capOverride)
	}
	groups := map[string]*GroupInfo{}
	for name, declared := range s.groups {
		limit, _ := s.groupLimitLocked(name)
		g := &GroupInfo{Name: name, Limit: limit, Declared: declared}
		if ov, ok := s.groupOverrides[name]; ok {
			g.Override = new(ov)
		}
		groups[name] = g
	}
	batches := map[string]*BatchInfo{}
	for key, sb := range s.batches {
		if !s.batchOpen(key, now) {
			continue
		}
		b := &BatchInfo{Key: key, Holding: []string{}, Waiting: []string{}}
		if sb.running <= 0 {
			b.Closes = new(sb.lastEnd.Add(BatchGrace))
		}
		batches[key] = b
	}
	for _, l := range s.holding {
		st.Holding = append(st.Holding, l.info(nil))
		if b := batches[l.req.Batch]; b != nil && l.req.ID != "" {
			b.Holding = append(b.Holding, l.req.ID)
		}
		if !l.req.GroupOnly {
			st.Running++
			st.Reserved += l.cost
		}
		if g := groups[l.req.Group]; g != nil {
			g.Active++
		}
	}
	for _, l := range s.waiting {
		w := l.state
		st.Waiting = append(st.Waiting, l.info(&w))
		if g := groups[l.req.Group]; g != nil {
			g.Waiting++
		}
		if b := batches[l.req.Batch]; b != nil && l.req.ID != "" {
			b.Waiting = append(b.Waiting, l.req.ID)
		}
	}
	for _, g := range groups {
		st.Groups = append(st.Groups, *g)
	}
	for _, b := range batches {
		st.Batches = append(st.Batches, *b)
	}
	sort.Slice(st.Batches, func(i, k int) bool { return st.Batches[i].Key < st.Batches[k].Key })
	sort.Slice(st.Groups, func(i, k int) bool { return st.Groups[i].Name < st.Groups[k].Name })
	return st
}

// priorityValues names each value of a lease's priority by its key.
func priorityValues(o leaseOrder) map[string]float64 {
	out := map[string]float64{}
	for name, v := range o.keys.Values(o.prio) {
		out[name] = float64(v)
	}
	return out
}

// info renders a lease for the status surface. Caller holds the scheduler's lock.
func (l *Lease) info(wait *QueueState) LeaseInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	since := l.since
	if wait == nil {
		since = l.started
	}
	return LeaseInfo{
		ID: l.req.ID, Class: l.req.Class, Group: l.req.Group, Priority: priorityValues(l.order),
		Batch: l.req.Batch, GroupOnly: l.req.GroupOnly, Since: since, Cost: l.cost, CPUs: l.cpus,
		CPUSeconds: l.cpuSeconds, Revoked: l.reason, Wait: wait,
	}
}
