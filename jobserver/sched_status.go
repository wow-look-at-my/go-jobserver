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

// BatchInfo is one batch: its members, and whether the open-batch key moves them forward.
type BatchInfo struct {
	Key string `json:"key"`
	// Open is true while a member holds a slot, or for BatchGrace after the last one ended.
	Open    bool      `json:"open"`
	Running int       `json:"running"`
	LastEnd time.Time `json:"last_end,omitzero"`
	// Holding and Waiting are the members' lease IDs.
	Holding []string `json:"holding"`
	Waiting []string `json:"waiting"`
}

// GroupInfo is one named group's use.
type GroupInfo struct {
	Name  string `json:"name"`
	Limit int    `json:"limit"`
	// Declared is the limit the host declared. Limit differs from it while Overridden.
	Declared   int  `json:"declared"`
	Overridden bool `json:"overridden,omitempty"`
	Active  int    `json:"active"`
	Waiting int    `json:"waiting"`
}

// SchedulerStatus is the scheduler's live state.
type SchedulerStatus struct {
	MaxConcurrent int     `json:"max_concurrent"`
	Budget        float64 `json:"budget"`
	// Running is how many counted leases hold a slot.
	Running int `json:"running"`
	// Reserved is the CPUs the counted holders were admitted at, together.
	Reserved float64     `json:"reserved"`
	Holding  []LeaseInfo `json:"holding"`
	Waiting  []LeaseInfo `json:"waiting"`
	// Monitors are the monitor leases. They hold no slot, so Holding and Running leave them out.
	Monitors []LeaseInfo `json:"monitors"`
	// Batches are the batches the scheduler keeps open or has members of, by key.
	Batches []BatchInfo `json:"batches"`
	// Cap is the count cap's declared and override layers. MaxConcurrent is its effective value.
	Cap LimitInfo `json:"cap"`
	Groups   []GroupInfo `json:"groups"`
	// PriorityKeys is the admission order, compared top to bottom.
	PriorityKeys []PriorityKey `json:"priority_keys"`
}

// Status reports what holds a slot, what waits and why, and each group's use.
// Holders are in admission order and waiters in planned order.
func (s *Scheduler) Status() SchedulerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := SchedulerStatus{MaxConcurrent: s.maxRuns, Budget: s.budgetLocked(),
		Holding: []LeaseInfo{}, Waiting: []LeaseInfo{}, Monitors: []LeaseInfo{}, Batches: []BatchInfo{}, PriorityKeys: s.PriorityKeys()}
	groups := map[string]*GroupInfo{}
	for name, limit := range s.groups {
		_, over := s.limits.groupOverride[name]
		groups[name] = &GroupInfo{Name: name, Limit: limit, Declared: s.limits.groupDeclared[name], Overridden: over}
	}
	st.Cap = s.capInfoLocked()
	for _, l := range s.holding {
		if l.req.Monitor {
			st.Monitors = append(st.Monitors, l.info(nil))
			continue
		}
		st.Holding = append(st.Holding, l.info(nil))
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
	}
	for _, g := range groups {
		st.Groups = append(st.Groups, *g)
	}
	st.Batches = s.batchInfoLocked()
	sort.Slice(st.Groups, func(i, k int) bool { return st.Groups[i].Name < st.Groups[k].Name })
	return st
}

// batchInfoLocked lists every batch the scheduler tracks or a live lease names, sorted by key. Caller holds s.mu.
func (s *Scheduler) batchInfoLocked() []BatchInfo {
	now := s.now()
	byKey := map[string]*BatchInfo{}
	get := func(key string) *BatchInfo {
		b := byKey[key]
		if b == nil {
			b = &BatchInfo{Key: key, Open: s.batchOpen(key, now), Holding: []string{}, Waiting: []string{}}
			if st := s.batches[key]; st != nil {
				b.Running, b.LastEnd = st.running, st.lastEnd
			}
			byKey[key] = b
		}
		return b
	}
	for key := range s.batches {
		get(key)
	}
	for _, l := range s.holding {
		if l.req.Batch != "" {
			b := get(l.req.Batch)
			b.Holding = append(b.Holding, l.req.ID)
		}
	}
	for _, l := range s.waiting {
		if l.req.Batch != "" {
			b := get(l.req.Batch)
			b.Waiting = append(b.Waiting, l.req.ID)
		}
	}
	out := make([]BatchInfo, 0, len(byKey))
	for _, b := range byKey {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Key < out[k].Key })
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
		ID: l.req.ID, Class: l.req.Class, Group: l.req.Group, Priority: l.sched.priority.values(l.rank),
		Batch: l.req.Batch, GroupOnly: l.req.GroupOnly, Since: since, Cost: l.cost, CPUs: l.cpus,
		CPUSeconds: l.cpuSeconds, Revoked: l.reason, Wait: wait,
	}
}
