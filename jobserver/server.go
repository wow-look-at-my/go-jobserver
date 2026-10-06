package jobserver

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

// Config is how a server is built. The zero value is usable: DefaultConfig
// fills in a directory under the user's home and turns on every transport.
type Config struct {
	// Dir holds the journal, the logs and the socket.
	Dir string
	// MaxConcurrent bounds how many jobs run at once. Zero means one per CPU.
	MaxConcurrent int
	// SpoolDir is watched for job files. Empty disables the watch.
	SpoolDir string
	// HTTPAddr is the dashboard and JSON API address. Empty disables it.
	HTTPAddr string
	// UnixSocket is the file socket path. Empty disables it.
	UnixSocket string
	// IPCName is the go-ipc service name. Empty disables it.
	IPCName string
	// IPC enables the go-ipc service over the name derived from Dir.
	IPC bool
	// SpoolInterval is how often the spool directory is scanned.
	SpoolInterval time.Duration
	// SyncInterval is how often running job logs are flushed to disk.
	SyncInterval time.Duration
	// Runner runs the jobs. The zero value runs them as child processes.
	Runner Runner
	// Logf receives the server's own status lines. Nil discards them.
	Logf func(format string, args ...any)
}

// DefaultDir is the server directory used when none is given.
func DefaultDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "go-jobserver")
	}
	return filepath.Join(home, ".local", "state", "go-jobserver")
}

// DefaultConfig returns the daemon's out-of-the-box settings for a directory.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:           dir,
		MaxConcurrent: runtime.NumCPU(),
		SpoolDir:      filepath.Join(dir, "spool"),
		HTTPAddr:      "127.0.0.1:8059",
		UnixSocket:    filepath.Join(dir, "go-jobserver.sock"),
		IPC:           true,
		SpoolInterval: 250 * time.Millisecond,
		SyncInterval:  time.Second,
	}
}

// Server is the daemon: it owns the store, decides what runs next, runs it,
// and answers the transports.
type Server struct {
	cfg    Config
	store  *Store
	runner Runner

	mu      sync.Mutex
	running map[string]context.CancelFunc
	closed  bool

	wake   chan struct{}
	quit   chan struct{}
	wg     sync.WaitGroup
	start  time.Time
	closes []io.Closer
}

// New opens the server's directory and replays its journal.
func New(cfg Config) (*Server, error) {
	if cfg.Dir == "" {
		cfg.Dir = DefaultDir()
	}
	if cfg.MaxConcurrent < 0 {
		cfg.MaxConcurrent = 0
	}
	if cfg.IPC && cfg.IPCName == "" {
		cfg.IPCName = "go-jobserver-" + nameSuffix(cfg.Dir)
	}
	if cfg.Runner == nil {
		cfg.Runner = ExecRunner{}
	}
	if cfg.SpoolInterval <= 0 {
		cfg.SpoolInterval = 250 * time.Millisecond
	}
	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = time.Second
	}
	st, err := OpenStore(cfg.Dir)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:     cfg,
		store:   st,
		runner:  cfg.Runner,
		running: make(map[string]context.CancelFunc),
		wake:    make(chan struct{}, 1),
		quit:    make(chan struct{}),
		start:   time.Now(),
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return s, nil
}

// Config returns the settings the server was built with.
func (s *Server) Config() Config { return s.cfg }

// Store returns the server's store.
func (s *Server) Store() *Store { return s.store }

// Start runs the scheduler and brings up every configured transport. It
// returns once each transport is listening.
func (s *Server) Start() error {
	s.wg.Add(1)
	go s.loop()
	s.wg.Add(1)
	go s.syncLoop()
	if s.cfg.SpoolDir != "" {
		s.wg.Add(1)
		go s.watchSpool()
	}
	if err := s.startUnix(); err != nil {
		s.Close()
		return err
	}
	if err := s.startHTTP(); err != nil {
		s.Close()
		return err
	}
	if err := s.startIPC(); err != nil {
		s.Close()
		return err
	}
	s.cfg.Logf("go-jobserver: dir %s", s.cfg.Dir)
	s.Wake()
	return nil
}

// Close stops the transports, interrupts running jobs, and closes the store.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	closes := s.closes
	s.closes = nil
	cancels := make([]context.CancelFunc, 0, len(s.running))
	for _, c := range s.running {
		cancels = append(cancels, c)
	}
	s.mu.Unlock()

	for _, c := range cancels {
		c()
	}
	close(s.quit)
	s.wg.Wait()
	var err error
	for _, c := range closes {
		if cerr := c.Close(); cerr == nil {
			continue
		} else if err == nil {
			err = cerr
		}
	}
	if serr := s.store.Close(); err == nil {
		err = serr
	}
	return err
}

// addCloser registers something Close should shut down.
func (s *Server) addCloser(c io.Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes = append(s.closes, c)
}

// Wake asks the scheduler to look at the job table again.
func (s *Server) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// loop is the scheduler: every wake, it blocks what cannot run and starts
// what can.
func (s *Server) loop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.quit:
			return
		case <-s.wake:
		}
		s.tick()
	}
}

// syncLoop flushes running job logs to stable storage on a cadence.
func (s *Server) syncLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-t.C:
			s.mu.Lock()
			ids := make([]string, 0, len(s.running))
			for id := range s.running {
				ids = append(ids, id)
			}
			s.mu.Unlock()
			for _, id := range ids {
				if w, err := s.store.LogWriter(id); err == nil {
					w.Sync()
				}
			}
		}
	}
}

// tick is one scheduling decision.
func (s *Server) tick() {
	if s.store.Paused() {
		return
	}
	jobs := s.store.Jobs()
	for _, id := range Blocked(jobs) {
		j, ok := s.store.Job(id)
		if !ok || j.State != StateActive {
			continue
		}
		if err := s.store.setState(id, StateBlocked, "a dependency failed"); err != nil {
			s.cfg.Logf("go-jobserver: block %s: %v", id, err)
		}
	}
	jobs = s.store.Jobs()
	byID := Index(jobs)
	for _, id := range Ready(jobs) {
		if !s.capacity() {
			break
		}
		j, ok := s.store.Job(id)
		if !ok {
			continue
		}
		identity, err := s.identityOf(j, byID)
		if err != nil {
			s.cfg.Logf("go-jobserver: identity %s: %v", id, err)
		}
		if identity != "" {
			if err := s.store.SetIdentity(id, identity, ""); err != nil {
				s.cfg.Logf("go-jobserver: identity %s: %v", id, err)
			}
			if !j.Force {
				if src := CacheHit(j, s.store.IdentityIndex(), byID); src != "" {
					s.reuse(j, src, identity)
					continue
				}
			}
		}
		s.dispatch(j.ID)
	}
}

// capacity reports whether another job may start.
func (s *Server) capacity() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.MaxConcurrent <= 0 {
		return true
	}
	return len(s.running) < s.cfg.MaxConcurrent
}

// identityOf computes a job's dedup identity, hashing its declared inputs.
func (s *Server) identityOf(j *Job, byID map[string]*Job) (string, error) {
	resolved := *j
	resolved.Inputs = hashInputs(j.Dir, j.Inputs)
	return JobIdentity(&resolved, byID), nil
}

// reuse records that a job's work was already done by another.
func (s *Server) reuse(j *Job, srcID, identity string) {
	src, ok := s.store.Job(srcID)
	if !ok {
		s.dispatch(j.ID)
		return
	}
	if err := s.store.SetIdentity(j.ID, identity, srcID); err != nil {
		s.cfg.Logf("go-jobserver: cache %s: %v", j.ID, err)
	}
	if err := s.store.Finish(j.ID, StateCached, 0, "reused "+srcID, src.Artifacts, 0); err != nil {
		s.cfg.Logf("go-jobserver: cache %s: %v", j.ID, err)
	}
	s.Wake()
}

// dispatch starts one job's run in the background.
func (s *Server) dispatch(id string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.running[id] = cancel
	s.mu.Unlock()

	j, ok := s.store.Job(id)
	if !ok {
		s.finishRun(id)
		cancel()
		return
	}
	if err := s.store.Start(id); err != nil {
		s.cfg.Logf("go-jobserver: start %s: %v", id, err)
		s.finishRun(id)
		cancel()
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			s.finishRun(id)
			cancel()
			s.Wake()
		}()
		s.runOnce(ctx, j)
	}()
}

// finishRun forgets a job's cancellation handle.
func (s *Server) finishRun(id string) {
	s.mu.Lock()
	delete(s.running, id)
	s.mu.Unlock()
}

// runOnce runs a job and records everything that happened.
func (s *Server) runOnce(ctx context.Context, j *Job) {
	w, err := s.store.LogWriter(j.ID)
	if err != nil {
		s.finish(j.ID, StateFailed, -1, err.Error(), nil, 0)
		return
	}
	code, runErr := s.runner.Run(ctx, j, w)
	if err := w.Sync(); err != nil {
		s.cfg.Logf("go-jobserver: sync log %s: %v", j.ID, err)
	}
	n := w.N()
	switch {
	case ctx.Err() != nil:
		s.finish(j.ID, StateCancelled, -1, "interrupted", nil, n)
	case runErr != nil:
		s.finish(j.ID, StateFailed, -1, runErr.Error(), nil, n)
	case code != 0:
		s.finish(j.ID, StateFailed, code, fmt.Sprintf("exit status %d", code), nil, n)
	default:
		artifacts, aerr := hashArtifacts(j)
		if aerr != nil {
			s.finish(j.ID, StateFailed, -1, aerr.Error(), nil, n)
			return
		}
		s.finish(j.ID, StateCompleted, 0, "", artifacts, n)
	}
}

// finish records a run's outcome.
func (s *Server) finish(id string, st State, code int, msg string, artifacts []Artifact, logBytes int64) {
	if err := s.store.Finish(id, st, code, msg, artifacts, logBytes); err != nil {
		s.cfg.Logf("go-jobserver: finish %s: %v", id, err)
	}
}

// Enqueue adds a job. When the spec carries a key that an unfinished job
// already holds, that job is returned instead and nothing is added.
func (s *Server) Enqueue(spec Spec) (*Job, bool, error) {
	if spec.Key != "" && !spec.Force {
		for _, j := range s.store.Jobs() {
			if j.Key == spec.Key && !j.State.Terminal() {
				return j, true, nil
			}
		}
	}
	j, err := s.store.Create(&spec)
	if err != nil {
		return nil, false, err
	}
	s.Wake()
	return j, false, nil
}

// Activate moves a draft job to active and wakes the scheduler.
func (s *Server) Activate(id string) (*Job, error) {
	if err := s.store.Activate(id); err != nil {
		return nil, err
	}
	s.Wake()
	return mustJob(s.store, id)
}

// AddDep adds a dependency to a job.
func (s *Server) AddDep(id, dep string) (*Job, error) {
	if err := s.store.AddDep(id, dep); err != nil {
		return nil, err
	}
	s.Wake()
	return mustJob(s.store, id)
}

// SetDeps replaces a job's dependencies.
func (s *Server) SetDeps(id string, deps []string) (*Job, error) {
	if err := s.store.SetDeps(id, deps); err != nil {
		return nil, err
	}
	s.Wake()
	return mustJob(s.store, id)
}

// Pause stops scheduling new jobs. Running jobs finish.
func (s *Server) Pause() error { return s.store.SetPaused(true) }

// Resume allows scheduling again.
func (s *Server) Resume() error {
	if err := s.store.SetPaused(false); err != nil {
		return err
	}
	s.Wake()
	return nil
}

// Paused reports whether scheduling is paused.
func (s *Server) Paused() bool { return s.store.Paused() }

// Interrupt cancels one running job.
func (s *Server) Interrupt(id string) error {
	id = CanonicalID(id)
	s.mu.Lock()
	cancel, ok := s.running[id]
	s.mu.Unlock()
	if !ok {
		j, found := s.store.Job(id)
		if !found {
			return fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		if j.State.Terminal() {
			return fmt.Errorf("%w: %s is %s", ErrBadState, id, j.State)
		}
		// Not running yet: cancel it outright so the scheduler skips it.
		return s.store.Finish(id, StateCancelled, -1, "interrupted", nil, j.LogBytes)
	}
	cancel()
	return nil
}

// InterruptAll cancels every running job.
func (s *Server) InterruptAll() int {
	s.mu.Lock()
	ids := make([]string, 0, len(s.running))
	for id := range s.running {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	sort.Strings(ids)
	for _, id := range ids {
		if err := s.Interrupt(id); err != nil {
			s.cfg.Logf("go-jobserver: interrupt %s: %v", id, err)
		}
	}
	return len(ids)
}

// List returns every job in creation order.
func (s *Server) List() []*Job { return s.store.Jobs() }

// Get returns one job.
func (s *Server) Get(id string) (*Job, error) { return mustJob(s.store, id) }

// Logs returns up to max bytes of a job's output starting at offset.
func (s *Server) Logs(id string, offset int64, max int) ([]byte, error) {
	if _, err := mustJob(s.store, id); err != nil {
		return nil, err
	}
	return s.store.ReadLog(id, offset, max)
}

// Stats summarizes the job table.
type Stats struct {
	Dir      string         `json:"dir"`
	Paused   bool           `json:"paused"`
	Uptime   string         `json:"uptime"`
	Running  int            `json:"running"`
	Total    int            `json:"total"`
	ByState  map[string]int `json:"by_state"`
	PID      int            `json:"pid"`
	Started  time.Time      `json:"started"`
	Revision string         `json:"version"`
}

// Stats reports the server's current shape.
func (s *Server) Stats() Stats {
	jobs := s.store.Jobs()
	st := Stats{
		Dir:      s.cfg.Dir,
		Paused:   s.store.Paused(),
		Uptime:   time.Since(s.start).Round(time.Second).String(),
		Total:    len(jobs),
		ByState:  map[string]int{},
		PID:      os.Getpid(),
		Started:  s.start,
		Revision: Revision,
	}
	for _, j := range jobs {
		st.ByState[string(j.State)]++
	}
	s.mu.Lock()
	st.Running = len(s.running)
	s.mu.Unlock()
	return st
}

// Wait blocks until the server is closed.
func (s *Server) Wait() { <-s.quit }

func mustJob(st *Store, id string) (*Job, error) {
	j, ok := st.Job(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return j, nil
}

// nameSuffix derives a stable, filesystem-safe suffix from a directory path.
func nameSuffix(dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		abs = dir
	}
	sum := computeIdentity(identityInput{Key: abs})
	return sum[len("sha256:"):][:12]
}

// Revision is the build revision, replaced by the toolchain's linker flag.
var Revision = "dev"
