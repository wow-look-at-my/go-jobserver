// The scheduler's read of the store: it reads the store's own tasks under the lock, and copies none of them.

package jobserver

import "github.com/wow-look-at-my/go-containers/set"

// schedule decides which tasks to block and which are ready, from the tasks that have not ended and their direct
// dependencies. It reads the store's own records under s.mu and copies no task. deps holds the ID, state and
// identity of each dependency of a ready task, for identityOf after the lock is released.
func (s *Store) schedule() (blocked, ready []string, deps map[string]*Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := set.New[string]()
	var jobs []*Job
	add := func(id string) {
		if seen.Contains(id) {
			return
		}
		j, ok := s.jobs[id]
		if !ok {
			return
		}
		seen.Add(id)
		jobs = append(jobs, j)
	}
	for _, ids := range s.idx.live {
		for id := range each(ids) {
			add(id)
			for _, d := range s.jobs[id].Deps {
				add(d)
			}
		}
	}
	blocked, ready = Blocked(jobs), Ready(jobs)
	deps = map[string]*Job{}
	for _, id := range ready {
		for _, d := range s.jobs[id].Deps {
			if dep, ok := s.jobs[d]; ok {
				deps[d] = &Job{ID: dep.ID, State: dep.State, Identity: dep.Identity}
			}
		}
	}
	return blocked, ready, deps
}
