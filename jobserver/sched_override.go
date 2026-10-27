// The operator overrides of the count cap and the group limits.

package jobserver

import "fmt"

// OverrideMaxConcurrent sets the count cap over the declared value. Zero means no cap. A negative value is ErrInvalidLimit.
func (s *Scheduler) OverrideMaxConcurrent(n int) error {
	if n < 0 {
		return fmt.Errorf("%w: count cap %d", ErrInvalidLimit, n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capOverride, s.capOverridden = n, true
	s.planLocked()
	return nil
}

// ClearMaxConcurrentOverride removes the cap override, so the declared value applies again.
func (s *Scheduler) ClearMaxConcurrentOverride() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capOverride, s.capOverridden = 0, false
	s.planLocked()
}

// OverrideGroup sets a group's limit over its declared limit. The scheduler keeps
// an override for a group that is not declared, and applies it when SetGroups
// declares the group. A limit below one is ErrInvalidLimit.
func (s *Scheduler) OverrideGroup(name string, limit int) error {
	if limit < 1 {
		return fmt.Errorf("%w: group %q limit %d", ErrInvalidLimit, name, limit)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupOverrides[name] = limit
	s.planLocked()
	return nil
}

// ClearGroupOverride removes a group's override, so its declared limit applies again.
func (s *Scheduler) ClearGroupOverride(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.groupOverrides, name)
	s.planLocked()
}

// groupLimitLocked is a declared group's effective limit. Caller holds s.mu.
func (s *Scheduler) groupLimitLocked(name string) (int, bool) {
	limit, ok := s.groups[name]
	if !ok {
		return 0, false
	}
	if ov, set := s.groupOverrides[name]; set {
		limit = ov
	}
	return limit, true
}
