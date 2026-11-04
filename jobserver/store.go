// The store holds the tasks: the open ones in memory, every one in the database.

package jobserver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	"github.com/wow-look-at-my/go-containers/set"
)

// logsDirName is the directory of the task logs under a server directory.
const logsDirName = "logs"

// Store is the durable half of the server: the task table and the log files. The database holds every task. Memory
// holds each task that has not ended, and each ended task that an open task depends on or that a caller waits on.
// There is one record per task in this process. Every accessor returns that record, and the store changes it in place
// under s.mu. Read its fields inside Read, except the fields Job names as fixed.
type Store struct {
	dir string
	db  *sql.DB

	mu sync.RWMutex
	// jobs holds the records memory keeps: see keep.
	jobs map[string]*Job
	// refs is the dependency list each open task counts in depRefs. depRefs counts the open tasks that depend on a task.
	refs    map[string][]string
	depRefs map[string]int
	// weak holds each record that memory let go, while a caller still holds it. weakMu guards it.
	weakMu sync.Mutex
	weak   map[string]weak.Pointer[Job]
	paused bool
	logs   map[string]*LogWriter
	closed bool
	// watch is closed and replaced on every change.
	watch chan struct{}
	// idx holds the open tasks of each inbox.
	idx inboxIndex
	// waiters counts the callers that block on each task, so a prune keeps it.
	waiters map[string]int
	// seq counts the changes. It goes on from the highest seq in the database. epoch names this open.
	seq   uint64
	epoch string
	// prunes counts the prunes that dropped a task.
	prunes uint64
	// visits counts the tasks an index read looked at.
	visits atomic.Int64
	// stepCap and waitCap bound the steps and the waits of one task.
	stepCap, waitCap int
	// reasonAt is when QueueWait last wrote the reason of each task's open queue wait.
	reasonAt map[string]time.Time
	// now is the store's clock. A test replaces it.
	now func() time.Time
	// logf receives the errors of reads that have no error result.
	logf func(format string, args ...any)
}

// OpenStore opens (or creates) the server directory and its database. A journal file of an older release moves into
// the database once.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, logsDirName), 0o755); err != nil {
		return nil, err
	}
	db, err := openDB(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:      dir,
		db:       db,
		jobs:     map[string]*Job{},
		refs:     map[string][]string{},
		depRefs:  map[string]int{},
		weak:     map[string]weak.Pointer[Job]{},
		logs:     map[string]*LogWriter{},
		watch:    make(chan struct{}),
		idx:      newInboxIndex(),
		waiters:  map[string]int{},
		stepCap:  maxTaskSteps,
		waitCap:  maxTaskWaits,
		epoch:    NewID(func(string) bool { return false }),
		reasonAt: map[string]time.Time{},
		now:      time.Now,
		logf:     log.Printf,
	}
	// A compaction of an older release that did not reach its rename leaves this file. The journal holds everything.
	if err := os.Remove(filepath.Join(dir, journalName+".compact")); err != nil && !errors.Is(err, os.ErrNotExist) {
		db.Close()
		return nil, err
	}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("jobserver: move the journal into the database: %w", err)
	}
	if err := s.load(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// SetLogf sets where the store reports the errors of reads that have no error result. The default is the log package.
func (s *Store) SetLogf(logf func(format string, args ...any)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logf = logf
}

// load reads the open tasks and the paused flag, and settles each open task's log.
func (s *Store) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var paused string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'paused'`).Scan(&paused)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	s.paused = paused == "true"
	var seq sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(seq) FROM tasks`).Scan(&seq); err != nil {
		return err
	}
	s.seq = uint64(seq.Int64)
	open, err := s.loadWhere("t.ended IS NULL", "t.n")
	if err != nil {
		return err
	}
	for _, j := range open {
		s.jobs[j.ID] = j
		s.idx.put(j)
	}
	for _, j := range open {
		if err := s.retain(j); err != nil {
			return err
		}
	}
	return s.recoverLogs(open)
}

// Dir is the server directory.
func (s *Store) Dir() string { return s.dir }

// LogPath is the file holding one job's output.
func (s *Store) LogPath(id string) string { return filepath.Join(s.dir, logsDirName, id+".log") }

// recoverLogs settles each open task's log against what the database recorded. It fails a task of the default inbox
// that was running when the process died. Caller holds s.mu for writing.
func (s *Store) recoverLogs(open []*Job) error {
	for _, j := range open {
		info, err := os.Stat(s.LogPath(j.ID))
		switch {
		case err == nil && j.LogBytes > 0 && info.Size() > j.LogBytes:
			// A crash left bytes the database never recorded.
			if err := os.Truncate(s.LogPath(j.ID), j.LogBytes); err != nil {
				return err
			}
		case err == nil:
			j.LogBytes = info.Size()
		case errors.Is(err, os.ErrNotExist):
		default:
			return err
		}
		// A named inbox decides about its running tasks when it opens again.
		if j.State == StateRunning && j.Inbox == "" {
			j.State = StateFailed
			j.Error = "interrupted by daemon restart"
			j.Finished = time.Now()
			d := closeOpen(j, j.Finished)
			if err := s.save(j, &d); err != nil {
				return err
			}
		}
	}
	return nil
}

// save records a change of j: the change count, the index, the database row, the cache and what memory keeps.
// d is the detail change the row does not hold, or nil. Caller holds s.mu for writing.
func (s *Store) save(j *Job, d *detailRec) error {
	s.seq++
	j.Seq = s.seq
	s.idx.put(j)
	err := s.inTx(func(tx *sql.Tx) error {
		if err := updateTask(tx, j); err != nil {
			return err
		}
		if err := writeDetail(tx, j.ID, d); err != nil {
			return err
		}
		return remember(tx, j)
	})
	if rerr := s.retain(j); err == nil {
		err = rerr
	}
	s.bump()
	return err
}

// rec returns the record of a task, or ErrNotFound. Caller holds s.mu.
func (s *Store) rec(id string) (*Job, error) {
	id = CanonicalID(id)
	j, err := s.lookup(id)
	if err != nil {
		return nil, err
	}
	if j == nil {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return j, nil
}

// Close stops the store and closes every open log file and the database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	for id, w := range s.logs {
		if werr := w.Close(); err == nil {
			err = werr
		}
		delete(s.logs, id)
	}
	if s.db != nil {
		if derr := s.db.Close(); err == nil {
			err = derr
		}
		s.db = nil
	}
	return err
}

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

// Paused reports whether scheduling is paused.
func (s *Store) Paused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paused
}

// SetPaused records the paused flag.
func (s *Store) SetPaused(paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paused == paused {
		return nil
	}
	s.paused = paused
	err := s.inTx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES ('paused', ?)`, strconv.FormatBool(paused))
		return err
	})
	s.bump()
	return err
}

// Create adds a job. An empty Spec.ID gets a generated one, and the returned job is the stored record.
func (s *Store) Create(spec *Spec) (*Job, error) { return s.create(spec, nil) }

// create adds a job, with the task fields meta carries. A task in a named inbox needs no command.
func (s *Store) create(spec *Spec, meta *taskMeta) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil, errors.New("jobserver: store is closed")
	}
	if len(spec.Command) == 0 && (meta == nil || meta.Inbox == "") {
		return nil, ErrNoCommand
	}
	id := CanonicalID(spec.ID)
	if id == "" {
		id = NewID(func(candidate string) bool {
			j, _ := s.lookup(candidate)
			return j != nil
		})
	} else if j, err := s.lookup(id); err != nil {
		return nil, err
	} else if j != nil {
		return nil, fmt.Errorf("%w: %s", ErrDuplicate, id)
	}
	deps := ValidDeps(spec.Deps)
	for _, d := range deps {
		if dep, err := s.lookup(d); err != nil {
			return nil, err
		} else if dep == nil {
			return nil, fmt.Errorf("%w: %s", ErrMissingDep, d)
		}
	}
	if err := s.checkCycle(id, deps); err != nil {
		return nil, err
	}
	state := StateActive
	if spec.Draft {
		state = StateDraft
	}
	j := &Job{
		ID:      id,
		Name:    spec.Name,
		Command: append([]string(nil), spec.Command...),
		Deps:    deps,
		Inputs:  append([]string(nil), spec.Inputs...),
		Outputs: append([]string(nil), spec.Outputs...),
		Env:     append([]string(nil), spec.Env...),
		Dir:     spec.Dir,
		Key:     spec.Key,
		Force:   spec.Force,
		State:   state,
		Created: time.Now(),
		Policy:  spec.Policy.normalize(),
	}
	if meta != nil {
		if len(meta.Spec) > 0 && !json.Valid(meta.Spec) {
			return nil, fmt.Errorf("jobserver: task %s: the spec is not valid JSON", id)
		}
		meta.applyTo(j)
	}
	j.Monitor = monitorOf(j)
	s.seq++
	j.Seq = s.seq
	if err := s.inTx(func(tx *sql.Tx) error { return insertTask(tx, j) }); err != nil {
		return nil, err
	}
	s.idx.put(j)
	err := s.retain(j)
	s.bump()
	if err != nil {
		return nil, err
	}
	return j, nil
}

// checkCycle rejects a dependency set that would close a loop through deps. Caller holds s.mu.
func (s *Store) checkCycle(id string, deps []string) error {
	// Walk from each new dependency; reaching id means a loop.
	seen := set.New[string]()
	var walk func(cur string) (bool, error)
	walk = func(cur string) (bool, error) {
		if cur == id {
			return true, nil
		}
		if seen.Contains(cur) {
			return false, nil
		}
		seen.Add(cur)
		j, err := s.lookup(cur)
		if err != nil || j == nil {
			return false, err
		}
		for _, d := range j.Deps {
			if loop, err := walk(d); loop || err != nil {
				return loop, err
			}
		}
		return false, nil
	}
	for _, d := range deps {
		loop, err := walk(d)
		if err != nil {
			return err
		}
		if loop {
			return fmt.Errorf("%w: %s -> ... -> %s", ErrCycle, id, d)
		}
	}
	return nil
}

// AddDep adds one dependency to a job that has not finished.
func (s *Store) AddDep(id, dep string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	if j.State.Terminal() {
		return fmt.Errorf("%w: %s is %s", ErrBadState, j.ID, j.State)
	}
	dep = CanonicalID(dep)
	if dep == j.ID {
		return fmt.Errorf("%w: %s cannot depend on itself", ErrCycle, j.ID)
	}
	if d, err := s.lookup(dep); err != nil {
		return err
	} else if d == nil {
		return fmt.Errorf("%w: %s", ErrMissingDep, dep)
	}
	for _, d := range j.Deps {
		if d == dep {
			return nil
		}
	}
	deps := ValidDeps(append(append([]string(nil), j.Deps...), dep))
	if err := s.checkCycle(j.ID, deps); err != nil {
		return err
	}
	j.Deps = deps
	return s.save(j, nil)
}

// SetDeps replaces a job's dependency list, rejecting cycles.
func (s *Store) SetDeps(id string, deps []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	if j.State.Terminal() {
		return fmt.Errorf("%w: %s is %s", ErrBadState, j.ID, j.State)
	}
	next := ValidDeps(deps)
	for _, d := range next {
		if d == j.ID {
			return fmt.Errorf("%w: %s cannot depend on itself", ErrCycle, j.ID)
		}
		if dep, err := s.lookup(d); err != nil {
			return err
		} else if dep == nil {
			return fmt.Errorf("%w: %s", ErrMissingDep, d)
		}
	}
	if err := s.checkCycle(j.ID, next); err != nil {
		return err
	}
	j.Deps = next
	return s.save(j, nil)
}

// setState records a new lifecycle state for a job. Caller holds s.mu for writing.
func (s *Store) setState(id string, st State, msg string) error {
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	if j.State == st {
		return nil
	}
	j.State = st
	j.Error = msg
	return s.save(j, nil)
}

// Activate moves a draft job to active.
func (s *Store) Activate(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	if j.State == StateActive {
		return nil
	}
	if j.State != StateDraft && j.State != StateBlocked {
		return fmt.Errorf("%w: %s is %s", ErrBadState, j.ID, j.State)
	}
	return s.setState(j.ID, StateActive, "")
}

// Start records the beginning of an attempt.
func (s *Store) Start(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	return s.startLocked(j)
}

// startLocked marks a job as running and records the start. Caller holds s.mu for writing.
func (s *Store) startLocked(j *Job) error {
	j.State = StateRunning
	j.Started = time.Now()
	j.Attempts++
	j.Finished = time.Time{}
	j.ExitCode = 0
	j.Error = ""
	return s.save(j, nil)
}

// SetIdentity records the dedup identity and the job whose work was reused.
func (s *Store) SetIdentity(id, identity, cacheOf string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	j.Identity = identity
	j.CacheOf = cacheOf
	return s.save(j, nil)
}

// SetPolicy replaces a job's CPU policy.
func (s *Store) SetPolicy(id string, policy JobPolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	policy = policy.normalize()
	if policy.equal(j.Policy) {
		return nil
	}
	j.Policy = policy
	return s.save(j, nil)
}

// FinishCPU records how much CPU a job's process tree used across its run.
func (s *Store) FinishCPU(id string, seconds float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	j.CPUSeconds = seconds
	return s.save(j, nil)
}

// Finish records the end of an attempt and closes the job's log file.
func (s *Store) Finish(id string, st State, exitCode int, msg string, artifacts []Artifact, logBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	return s.finishLocked(j, st, exitCode, msg, artifacts, logBytes)
}

// finishLocked ends a task and closes its log file. Caller holds s.mu for writing.
func (s *Store) finishLocked(j *Job, st State, exitCode int, msg string, artifacts []Artifact, logBytes int64) error {
	j.State = st
	j.ExitCode = exitCode
	j.Finished = time.Now()
	j.Error = msg
	j.Artifacts = artifacts
	j.LogBytes = logBytes
	j.MonitorStatus = nil
	delete(s.reasonAt, j.ID)
	d := closeOpen(j, j.Finished)
	err := s.save(j, &d)
	if cerr := s.closeLog(j.ID); err == nil {
		err = cerr
	}
	return err
}

// finishIfLive ends a task that has not ended, with the log size it recorded. It reports whether it ended the task.
func (s *Store) finishIfLive(id string, st State, msg string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil || j.State.Terminal() {
		return false, err
	}
	return true, s.finishLocked(j, st, -1, msg, nil, j.LogBytes)
}

// closeLog closes a job's cached log file and forgets it. The caller holds the
// lock.
func (s *Store) closeLog(id string) error {
	w, ok := s.logs[id]
	if !ok {
		return nil
	}
	delete(s.logs, id)
	return w.Close()
}

// LogWriter returns the append handle for a job's output file, creating it on
// first use. Finish closes the handle, so a later call opens a new one.
func (s *Store) LogWriter(id string) (*LogWriter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id = CanonicalID(id)
	if w, ok := s.logs[id]; ok {
		return w, nil
	}
	f, err := os.OpenFile(s.LogPath(id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	// A reopened file already holds output, and N counts it.
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &LogWriter{f: f, n: info.Size()}
	s.logs[id] = w
	return w, nil
}

// An IdentityIndex maps a dedup identity to a job that finished with it.
type IdentityIndex map[string]string

// IdentityIndex builds the lookup of cache hits: identity to the most recent job that produced it and succeeded.
// It reads the whole cache. The scheduler reads one entry at a time instead.
func (s *Store) IdentityIndex() IdentityIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx := IdentityIndex{}
	if s.db == nil {
		return idx
	}
	rows, err := s.db.Query(`SELECT identity, id FROM cache`)
	if err != nil {
		s.readErr("the dedup cache", err)
		return idx
	}
	defer rows.Close()
	for rows.Next() {
		var identity, id string
		if err := rows.Scan(&identity, &id); err != nil {
			s.readErr("the dedup cache", err)
			return idx
		}
		idx[identity] = id
	}
	return idx
}

// SortedJobs orders jobs by creation time, breaking ties on ID.
func SortedJobs(jobs []*Job) []*Job {
	out := append([]*Job(nil), jobs...)
	sort.SliceStable(out, func(i, k int) bool {
		if out[i].Created.Equal(out[k].Created) {
			return out[i].ID < out[k].ID
		}
		return out[i].Created.Before(out[k].Created)
	})
	return out
}
