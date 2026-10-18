package jobserver

import (
	"sort"
	"time"
)

// LeaseInfo is one lease as the status surface shows it.
type LeaseInfo struct {
	ID    string  `json:"id"`
	Class string  `json:"class,omitempty"`
	Group string  `json:"group,omitempty"`
	Key   []int64 `json:"key,omitempty"`
	Batch string  `json:"batch,omitempty"`
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
	Name    string `json:"name"`
	Limit   int    `json:"limit"`
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
	Groups   []GroupInfo `json:"groups"`
}

// Status reports what holds a slot, what waits and why, and each group's use.
// Holders are in admission order and waiters in planned order.
func (s *Scheduler) Status() SchedulerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := SchedulerStatus{MaxConcurrent: s.maxRuns, Budget: s.budgetLocked(),
		Holding: []LeaseInfo{}, Waiting: []LeaseInfo{}}
	groups := map[string]*GroupInfo{}
	for name, limit := range s.groups {
		groups[name] = &GroupInfo{Name: name, Limit: limit}
	}
	for _, l := range s.holding {
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
	sort.Slice(st.Groups, func(i, k int) bool { return st.Groups[i].Name < st.Groups[k].Name })
	return st
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
		ID: l.req.ID, Class: l.req.Class, Group: l.req.Group, Key: l.req.Key,
		Batch: l.req.Batch, GroupOnly: l.req.GroupOnly, Since: since, Cost: l.cost, CPUs: l.cpus,
		CPUSeconds: l.cpuSeconds, Revoked: l.reason, Wait: wait,
	}
}
