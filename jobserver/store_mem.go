// What memory holds of the tasks: the open ones, the ended ones a caller still needs, and a weak map of the rest.

package jobserver

import (
	"runtime"
	"slices"
	"weak"
)

// held returns the record of a task this process holds, strongly or weakly, or nil. Caller holds s.mu.
func (s *Store) held(id string) *Job {
	if j := s.jobs[id]; j != nil {
		return j
	}
	s.weakMu.Lock()
	defer s.weakMu.Unlock()
	if p, ok := s.weak[id]; ok {
		return p.Value()
	}
	return nil
}

// register makes j the record of its task in this process, unless another reader registered one first. It returns
// the record that holds the place. Caller holds s.mu.
func (s *Store) register(j *Job) *Job {
	s.weakMu.Lock()
	defer s.weakMu.Unlock()
	if p, ok := s.weak[j.ID]; ok {
		if have := p.Value(); have != nil {
			return have
		}
	}
	s.weakLocked(j)
	return j
}

// weakLocked puts j in the weak map, and removes the entry once the collector frees j. Caller holds s.weakMu.
func (s *Store) weakLocked(j *Job) {
	if p, ok := s.weak[j.ID]; ok && p.Value() == j {
		return
	}
	s.weak[j.ID] = weak.Make(j)
	runtime.AddCleanup(j, s.forgetWeak, j.ID)
}

// forgetWeak removes the weak entry of a freed record. A newer record of the same task keeps its entry.
func (s *Store) forgetWeak(id string) {
	s.weakMu.Lock()
	defer s.weakMu.Unlock()
	if p, ok := s.weak[id]; ok && p.Value() == nil {
		delete(s.weak, id)
	}
}

// dropWeak removes the weak entry of a task the store deleted. A holder of the record still reads it.
func (s *Store) dropWeak(id string) {
	s.weakMu.Lock()
	defer s.weakMu.Unlock()
	delete(s.weak, id)
}

// lookup returns the record of a task: from memory, else from the database. It is nil for a task the store does not
// hold. Caller holds s.mu.
func (s *Store) lookup(id string) (*Job, error) {
	if j := s.held(id); j != nil {
		return j, nil
	}
	jobs, err := s.loadWhere("t.id = ?", "t.n", id)
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	return jobs[0], nil
}

// keep reports whether memory must hold a record strongly: it has not ended.
// A task that has not ended depends on it, or a caller waits on it.
func (s *Store) keep(j *Job) bool {
	return !j.State.Terminal() || s.depRefs[j.ID] > 0 || s.waiters[j.ID] > 0
}

// pin makes memory hold a task strongly. Caller holds s.mu for writing.
func (s *Store) pin(id string) error {
	if s.jobs[id] != nil {
		return nil
	}
	j, err := s.lookup(id)
	if err != nil {
		return err
	}
	if j != nil {
		s.jobs[id] = j
	}
	return nil
}

// unpin lets memory drop a task's strong pointer when nothing needs it. The weak map keeps it for its holders.
// Caller holds s.mu for writing.
func (s *Store) unpin(id string) {
	j := s.jobs[id]
	if j == nil || s.keep(j) {
		return
	}
	delete(s.jobs, id)
	s.weakMu.Lock()
	s.weakLocked(j)
	s.weakMu.Unlock()
}

// retain updates what j holds in memory after a change: the dependencies it counts while it has not ended, and its
// own place. Caller holds s.mu for writing.
func (s *Store) retain(j *Job) error {
	var want []string
	if !j.State.Terminal() {
		want = j.Deps
		s.jobs[j.ID] = j
	}
	old := s.refs[j.ID]
	var err error
	if !slices.Equal(old, want) {
		for _, d := range want {
			s.depRefs[d]++
			if perr := s.pin(d); err == nil {
				err = perr
			}
		}
		for _, d := range old {
			if s.depRefs[d]--; s.depRefs[d] <= 0 {
				delete(s.depRefs, d)
			}
			s.unpin(d)
		}
		if want == nil {
			delete(s.refs, j.ID)
		} else {
			s.refs[j.ID] = want
		}
	}
	s.unpin(j.ID)
	return err
}

// Read runs fn with the store's read lock held.
func (s *Store) Read(fn func()) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn()
}
