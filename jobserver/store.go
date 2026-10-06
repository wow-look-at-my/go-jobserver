package jobserver

import (
	"errors"
	"fmt"
	"github.com/wow-look-at-my/go-containers/set"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// The directory layout under a server directory:.
const (
	journalName = "journal.log"
	logsDirName = "logs"
)

// Store is the durable half of the server: the job table, the journal that
// rebuilds it, and the log files.
type Store struct {
	dir     string
	journal *os.File

	mu     sync.Mutex
	jobs   map[string]*Job
	order  []string
	paused bool
	logs   map[string]*LogWriter
	closed bool
}

// OpenStore opens (or creates) the server directory and replays its journal.
// A journal whose last frame is half-written is cut back to the last good
// frame. A crash during an append costs that one record and nothing else.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, logsDirName), 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		dir:  dir,
		jobs: make(map[string]*Job),
		logs: make(map[string]*LogWriter),
	}
	if err := s.replay(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.journalPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.journal = f
	if err := s.recoverLogs(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) journalPath() string { return filepath.Join(s.dir, journalName) }

// Dir is the server directory.
func (s *Store) Dir() string { return s.dir }

// LogPath is the file holding one job's output.
func (s *Store) LogPath(id string) string { return filepath.Join(s.dir, logsDirName, id+".log") }

// replay reads the journal into the job table, cutting a torn tail.
func (s *Store) replay() error {
	path := s.journalPath()
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	var (
		offset int64
		good   int64
	)
	for {
		payload, err := readFrame(f)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A torn or corrupt frame ends the replay. Everything after it is unreadable, so the file is cut here.
			break
		}
		rec, err := decodeRecord(payload)
		if err != nil {
			break
		}
		s.apply(rec)
		good += int64(frameHeaderSize + len(payload))
		offset = good
	}
	if err := f.Close(); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > offset {
		if err := os.Truncate(path, offset); err != nil {
			return err
		}
	}
	return nil
}

// recoverLogs settles each job's log against what the journal recorded, and
// fails a job that was running when the process died.
func (s *Store) recoverLogs() error {
	for _, id := range s.order {
		j := s.jobs[id]
		info, err := os.Stat(s.LogPath(id))
		switch {
		case err == nil && j.LogBytes > 0 && info.Size() > j.LogBytes:
			// A crash left bytes the journal never recorded.
			if err := os.Truncate(s.LogPath(id), j.LogBytes); err != nil {
				return err
			}
		case err == nil:
			j.LogBytes = info.Size()
		case errors.Is(err, os.ErrNotExist):
			// The journal is the record of how much output a finished job
			// wrote; a missing file is not evidence that it wrote none.
		default:
			return err
		}
		if j.State == StateRunning {
			j.State = StateFailed
			j.Error = "interrupted by daemon restart"
			j.Finished = time.Now()
			if err := s.append(record{kind: recFinish, id: id, state: StateFailed, exitCode: -1, finished: j.Finished.UnixNano(), errMsg: j.Error, logBytes: j.LogBytes}); err != nil {
				return err
			}
			if err := s.sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

// apply folds one journal record into the in-memory table.
func (s *Store) apply(r record) {
	switch r.kind {
	case recCreate:
		j := &Job{
			ID:      r.id,
			Name:    r.name,
			Command: r.command,
			Deps:    r.deps,
			Inputs:  r.inputs,
			Outputs: r.outputs,
			Env:     r.env,
			Dir:     r.dir,
			Key:     r.key,
			Force:   r.force,
			State:   r.state,
			Created: time.Unix(0, r.created),
		}
		if j.State == "" {
			j.State = StateActive
		}
		if _, ok := s.jobs[j.ID]; !ok {
			s.order = append(s.order, j.ID)
		}
		s.jobs[j.ID] = j
	case recStart:
		if j, ok := s.jobs[r.id]; ok {
			j.State = StateRunning
			j.Attempts = r.attempt
			j.Started = time.Unix(0, r.started)
		}
	case recFinish:
		if j, ok := s.jobs[r.id]; ok {
			j.State = r.state
			j.ExitCode = r.exitCode
			j.Finished = time.Unix(0, r.finished)
			j.Error = r.errMsg
			j.LogBytes = r.logBytes
			j.Artifacts = r.artifacts
		}
	case recIdentity:
		if j, ok := s.jobs[r.id]; ok {
			j.Identity = r.identity
			j.CacheOf = r.cacheOf
		}
	case recState:
		if j, ok := s.jobs[r.id]; ok {
			j.State = r.state
			j.Error = r.errMsg
		}
	case recControl:
		s.paused = r.paused
	case recDeps:
		if j, ok := s.jobs[r.id]; ok {
			j.Deps = r.replaceDeps
		}
	}
}

// append writes one record and returns after the bytes are in the file.
func (s *Store) append(r record) error {
	if s.journal == nil {
		return errors.New("jobserver: store is closed")
	}
	_, err := s.journal.Write(frame(r.encode()))
	return err
}

// sync pushes every appended byte to stable storage.
func (s *Store) sync() error {
	if s.journal == nil {
		return nil
	}
	return s.journal.Sync()
}

// commit appends a record and syncs it.
func (s *Store) commit(r record) error {
	if err := s.append(r); err != nil {
		return err
	}
	return s.sync()
}

// Close stops the store and closes every open log file.
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
	if s.journal != nil {
		if jerr := s.journal.Close(); err == nil {
			err = jerr
		}
		s.journal = nil
	}
	return err
}

// Jobs returns a copy of every job, in creation order.
func (s *Store) Jobs() []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Job, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.jobs[id].clone())
	}
	return out
}

// Order returns the job IDs in creation order.
func (s *Store) Order() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

// Job returns a copy of one job.
func (s *Store) Job(id string) (*Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return nil, false
	}
	return j.clone(), true
}

// Paused reports whether scheduling is paused.
func (s *Store) Paused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	return s.commit(record{kind: recControl, paused: paused, created: time.Now().UnixNano()})
}

// Create adds a job. An empty Spec.ID gets a generated one, and the returned
// job is the stored copy.
func (s *Store) Create(spec *Spec) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(spec.Command) == 0 {
		return nil, ErrNoCommand
	}
	id := CanonicalID(spec.ID)
	if id == "" {
		id = NewID(func(candidate string) bool {
			_, taken := s.jobs[candidate]
			return taken
		})
	} else if _, taken := s.jobs[id]; taken {
		return nil, fmt.Errorf("%w: %s", ErrDuplicate, id)
	}
	deps := ValidDeps(spec.Deps)
	for _, d := range deps {
		if _, ok := s.jobs[d]; !ok {
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
	}
	if err := s.commit(record{
		kind: recCreate, id: j.ID, name: j.Name, command: j.Command, deps: j.Deps,
		inputs: j.Inputs, outputs: j.Outputs, env: j.Env, dir: j.Dir, key: j.Key,
		force: j.Force, created: j.Created.UnixNano(), state: j.State,
	}); err != nil {
		return nil, err
	}
	s.jobs[j.ID] = j
	s.order = append(s.order, j.ID)
	return j.clone(), nil
}

// checkCycle rejects a dependency set that would close a loop through deps.
// The caller holds the lock.
func (s *Store) checkCycle(id string, deps []string) error {
	// Walk from each new dependency; reaching id means a loop.
	seen := set.New[string]()
	var walk func(cur string) bool
	walk = func(cur string) bool {
		if cur == id {
			return true
		}
		if seen.Contains(cur) {
			return false
		}
		seen.Add(cur)
		j, ok := s.jobs[cur]
		if !ok {
			return false
		}
		for _, d := range j.Deps {
			if walk(d) {
				return true
			}
		}
		return false
	}
	for _, d := range deps {
		if walk(d) {
			return fmt.Errorf("%w: %s -> ... -> %s", ErrCycle, id, d)
		}
	}
	return nil
}

// AddDep adds one dependency to a job that has not finished.
func (s *Store) AddDep(id, dep string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if j.State.Terminal() {
		return fmt.Errorf("%w: %s is %s", ErrBadState, j.ID, j.State)
	}
	dep = CanonicalID(dep)
	if dep == j.ID {
		return fmt.Errorf("%w: %s cannot depend on itself", ErrCycle, j.ID)
	}
	if _, ok := s.jobs[dep]; !ok {
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
	return s.commit(record{kind: recDeps, id: j.ID, replaceDeps: deps})
}

// SetDeps replaces a job's dependency list, rejecting cycles.
func (s *Store) SetDeps(id string, deps []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if j.State.Terminal() {
		return fmt.Errorf("%w: %s is %s", ErrBadState, j.ID, j.State)
	}
	next := ValidDeps(deps)
	for _, d := range next {
		if d == j.ID {
			return fmt.Errorf("%w: %s cannot depend on itself", ErrCycle, j.ID)
		}
		if _, ok := s.jobs[d]; !ok {
			return fmt.Errorf("%w: %s", ErrMissingDep, d)
		}
	}
	if err := s.checkCycle(j.ID, next); err != nil {
		return err
	}
	j.Deps = next
	return s.commit(record{kind: recDeps, id: j.ID, replaceDeps: next})
}

// setState records a new lifecycle state for a job.
func (s *Store) setState(id string, st State, msg string) error {
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if j.State == st {
		return nil
	}
	j.State = st
	j.Error = msg
	return s.commit(record{kind: recState, id: j.ID, state: st, errMsg: msg})
}

// Activate moves a draft job to active.
func (s *Store) Activate(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
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
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	j.State = StateRunning
	j.Started = time.Now()
	j.Attempts++
	j.Finished = time.Time{}
	j.ExitCode = 0
	j.Error = ""
	return s.commit(record{kind: recStart, id: j.ID, attempt: j.Attempts, started: j.Started.UnixNano()})
}

// SetIdentity records the dedup identity and the job whose work was reused.
func (s *Store) SetIdentity(id, identity, cacheOf string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	j.Identity = identity
	j.CacheOf = cacheOf
	return s.commit(record{kind: recIdentity, id: j.ID, identity: identity, cacheOf: cacheOf})
}

// Finish records the end of an attempt.
func (s *Store) Finish(id string, st State, exitCode int, msg string, artifacts []Artifact, logBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	j.State = st
	j.ExitCode = exitCode
	j.Finished = time.Now()
	j.Error = msg
	j.Artifacts = artifacts
	j.LogBytes = logBytes
	return s.commit(record{
		kind: recFinish, id: j.ID, state: st, exitCode: exitCode,
		finished: j.Finished.UnixNano(), errMsg: msg, logBytes: logBytes, artifacts: artifacts,
	})
}

// LogWriter returns the append handle for a job's output file, creating it on
// first use.
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
	w := &LogWriter{f: f}
	s.logs[id] = w
	return w, nil
}

func (s *Store) LogBytes(id string) int64 {
	info, err := os.Stat(s.LogPath(CanonicalID(id)))
	if err != nil {
		return 0
	}
	return info.Size()
}

// ReadLog returns up to max bytes of a job's output starting at offset.
func (s *Store) ReadLog(id string, offset int64, max int) ([]byte, error) {
	f, err := os.Open(s.LogPath(CanonicalID(id)))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	if max <= 0 {
		max = 64 << 10
	}
	return io.ReadAll(io.LimitReader(f, int64(max)))
}

// RemoveLog drops a job's output file. It is used when a job is removed.
func (s *Store) RemoveLog(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id = CanonicalID(id)
	if w, ok := s.logs[id]; ok {
		w.Close()
		delete(s.logs, id)
	}
	err := os.Remove(s.LogPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// An IdentityIndex maps a dedup identity to a job that finished with it.
type IdentityIndex map[string]string

// IdentityIndex builds the lookup the scheduler uses for cache hits: identity
// to the most recent job that produced it and succeeded.
func (s *Store) IdentityIndex() IdentityIndex {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := IdentityIndex{}
	for _, id := range s.order {
		j := s.jobs[id]
		if j.Identity == "" || !j.State.succeeded() {
			continue
		}
		idx[j.Identity] = j.ID
	}
	return idx
}

// clone returns a deep copy, so callers never race with the store.
func (j *Job) clone() *Job {
	c := *j
	c.Command = append([]string(nil), j.Command...)
	c.Deps = append([]string(nil), j.Deps...)
	c.Inputs = append([]string(nil), j.Inputs...)
	c.Outputs = append([]string(nil), j.Outputs...)
	c.Env = append([]string(nil), j.Env...)
	c.Artifacts = append([]Artifact(nil), j.Artifacts...)
	return &c
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

// A LogWriter appends one job's output.
type LogWriter struct {
	f *os.File
	n int64
}

// Write appends output.
func (w *LogWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.n += int64(n)
	return n, err
}

// N is how many bytes this writer has appended.
func (w *LogWriter) N() int64 { return w.n }

// Sync flushes the file to stable storage.
func (w *LogWriter) Sync() error { return w.f.Sync() }

// Close syncs and closes the file.
func (w *LogWriter) Close() error {
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}
