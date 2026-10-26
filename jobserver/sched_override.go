// The scheduler's limits in layers: what the host declares, and what an operator overrides. The override wins
// while it is set, and clearing it brings the declared value back.

package jobserver

import "maps"

// limitLayer is the declared cap and groups, and the overrides on top.
type limitLayer struct {
	capDeclared   int
	capOverride   int
	capOverridden bool
	groupDeclared map[string]int
	groupOverride map[string]int
}

func (l limitLayer) effectiveCap() int {
	if l.capOverridden {
		return l.capOverride
	}
	return l.capDeclared
}

// effectiveGroups is the declared groups with each override applied. An override for a group nobody declares does nothing.
func (l limitLayer) effectiveGroups() map[string]int {
	out := maps.Clone(l.groupDeclared)
	if out == nil {
		out = map[string]int{}
	}
	for name, limit := range l.groupOverride {
		if _, ok := out[name]; ok {
			out[name] = limit
		}
	}
	return out
}

func (s *Scheduler) applyCapLocked() {
	s.maxRuns = s.limits.effectiveCap()
	s.planLocked()
}

// OverrideMaxConcurrent sets the count cap over the declared one until ClearMaxConcurrentOverride. Zero means no cap.
func (s *Scheduler) OverrideMaxConcurrent(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits.capOverride, s.limits.capOverridden = max(n, 0), true
	s.applyCapLocked()
}

// ClearMaxConcurrentOverride brings the declared count cap back.
func (s *Scheduler) ClearMaxConcurrentOverride() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits.capOverride, s.limits.capOverridden = 0, false
	s.applyCapLocked()
}

// SetGroupOverrides replaces every group override. A limit below one is one. An empty map clears them all.
func (s *Scheduler) SetGroupOverrides(limits map[string]int) {
	next := make(map[string]int, len(limits))
	for name, limit := range limits {
		next[name] = max(limit, 1)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits.groupOverride = next
	s.applyGroupsLocked()
}

// LimitInfo is the count cap as both layers make it.
type LimitInfo struct {
	Declared   int  `json:"declared"`
	Effective  int  `json:"effective"`
	Overridden bool `json:"overridden"`
}

// capInfoLocked reports the cap's layers. Caller holds s.mu.
func (s *Scheduler) capInfoLocked() LimitInfo {
	return LimitInfo{Declared: s.limits.capDeclared, Effective: s.maxRuns, Overridden: s.limits.capOverridden}
}
