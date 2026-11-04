// The store's reads: each returns the record of a task, or reads what it
// needs under the read lock.

package jobserver

// readErr reports a failed read of the database. The caller then returns what memory holds. Caller holds s.mu.
func (s *Store) readErr(what string, err error) {
	s.logf("go-jobserver: ERROR: read %s from the database: %v. The result holds only the tasks that memory holds.", what, err)
}

// jobsLocked returns every task the store holds, in creation order. Caller holds s.mu.
func (s *Store) jobsLocked() []*Job {
	out, err := s.loadWhere("1", "t.n")
	if err != nil {
		s.readErr("the tasks", err)
		return SortedJobs(s.memJobs())
	}
	return out
}

// memJobs returns the records memory keeps. Caller holds s.mu.
func (s *Store) memJobs() []*Job {
	out := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	return out
}

// Jobs returns the record of every task, in creation order. It reads the ended tasks from the database.
func (s *Store) Jobs() []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.jobsLocked()
}

// Order returns the task IDs in creation order.
func (s *Store) Order() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return nil
	}
	rows, err := s.db.Query(`SELECT id FROM tasks ORDER BY n`)
	if err != nil {
		s.readErr("the task IDs", err)
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			s.readErr("the task IDs", err)
			return out
		}
		out = append(out, id)
	}
	return out
}

// Job returns the record of one task.
func (s *Store) Job(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, err := s.lookup(CanonicalID(id))
	if err != nil {
		s.readErr("task "+id, err)
	}
	return j, j != nil
}

// view runs fn on the record of one task under the read lock. It reports false for a task the store does not hold.
func (s *Store) view(id string, fn func(j *Job)) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, err := s.lookup(CanonicalID(id))
	if err != nil {
		s.readErr("task "+id, err)
	}
	if j == nil {
		return false
	}
	fn(j)
	return true
}

// stateOf returns the state of one task.
func (s *Store) stateOf(id string) (State, bool) {
	var st State
	ok := s.view(id, func(j *Job) { st = j.State })
	return st, ok
}

// stateCounts counts the tasks of each state with one query, and reads no task.
func (s *Store) stateCounts() map[State]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[State]int{}
	if s.db == nil {
		return out
	}
	rows, err := s.db.Query(`SELECT state, COUNT(*) FROM tasks GROUP BY state`)
	if err != nil {
		s.readErr("the count of each state", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			s.readErr("the count of each state", err)
			return out
		}
		out[State(st)] = n
	}
	return out
}
