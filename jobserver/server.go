// Config is how a server is built.

package jobserver

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
	// HTTPAddr is the dashboard and JSON API address. Empty disables it.
	HTTPAddr string
	// UnixSocket is the file socket path. Empty disables it.
	UnixSocket string
	// IPCName is the go-ipc service name. Empty disables it.
	IPCName string
	// IPC enables the go-ipc service over the name derived from Dir.
	IPC bool
	// SyncInterval is how often running job logs are flushed to disk.
	SyncInterval time.Duration
	// CPUBudget is how many CPUs the running jobs may use together. Zero means one per CPU.
	CPUBudget float64
	// DefaultCost is the CPU cost assumed for a job that declares none and has never run. Zero means one.
	DefaultCost float64
	// SampleInterval is how often CPU use is sampled. Zero means one second.
	SampleInterval time.Duration
	// DisableCPU turns off CPU sampling, so the daemon neither measures nor throttles anything.
	DisableCPU bool
	// StatsURL is a simple-stats-api to read the host load from. Empty reads this host.
	StatsURL string
	// Runner runs the jobs. The zero value runs them as child processes.
	Runner Runner
	// Scheduler is a scheduler the host already admits other work through.
	Scheduler *Scheduler
	// Monitors runs the tasks of the monitor inbox. Nil means the server knows no monitor kind and opens no monitor inbox.
	Monitors *MonitorEnv
	// MonitorPolicy and MonitorBudget go to the scheduler's fields of the same names.
	MonitorPolicy JobPolicy
	MonitorBudget float64
	// Retention is how long the server keeps an ended task, its result and its log. Zero means DefaultRetention.
	Retention time.Duration
	// RetainCount is how many ended tasks the server keeps per inbox. Zero means DefaultRetainCount.
	RetainCount int
	// PruneInterval is how often the server drops ended tasks. Zero means DefaultPruneInterval.
	PruneInterval time.Duration
	// CacheRetention and CacheCount bound the dedup cache, apart from the tasks. Zero, the default, is no bound.
	CacheRetention time.Duration
	CacheCount     int
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
		Dir:            dir,
		MaxConcurrent:  runtime.NumCPU(),
		HTTPAddr:       "127.0.0.1:8059",
		UnixSocket:     filepath.Join(dir, "go-jobserver.sock"),
		IPC:            true,
		SyncInterval:   time.Second,
		CPUBudget:      float64(runtime.NumCPU()),
		DefaultCost:    1,
		SampleInterval: time.Second,
	}
}

// Server is the daemon: it owns the store, decides what runs next, runs it,
// and answers the transports.
type Server struct {
	cfg    Config
	store  *Store
	runner Runner
	sched  *Scheduler
	// sharedSched is true when Config.Scheduler gave the scheduler. Its owner closes it, not Close.
	sharedSched bool
	// gov is the scheduler's governor.
	gov *governor

	mu      sync.Mutex
	running map[string]*runHandle
	closed  bool

	wake   chan struct{}
	quit   chan struct{}
	wg     sync.WaitGroup
	start  time.Time
	closes []io.Closer
	// inboxes are the named inboxes open in this process, under mu.
	inboxes map[string]*inboxCore
	def     *Inbox[*CommandSpec, CommandResult]
	// defCore admits the default inbox's tasks. The executor is its consumer.
	defCore *inboxCore
	// stopped refuses new tasks once Stop runs.
	stopped bool
	// errs holds the error each failed task ended with in this process.
	errs map[string]error
	// watching holds the last status of each running monitor, by task ID, under mu.
	watching map[string]MonitorStatus
	// watchMu makes the dedup check and the add of Watch one step.
	watchMu sync.Mutex
	// every holds the schedules that add tasks on an interval.
	every *scheduleSet
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
	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = time.Second
	}
	if cfg.DefaultCost < 0 {
		cfg.DefaultCost = 0
	}
	if cfg.Retention < 0 || cfg.RetainCount < 0 || cfg.PruneInterval < 0 || cfg.CacheRetention < 0 || cfg.CacheCount < 0 {
		return nil, fmt.Errorf("jobserver: Retention, RetainCount, PruneInterval, CacheRetention and CacheCount must not be negative")
	}
	if cfg.Retention == 0 {
		cfg.Retention = DefaultRetention
	}
	if cfg.RetainCount == 0 {
		cfg.RetainCount = DefaultRetainCount
	}
	if cfg.PruneInterval == 0 {
		cfg.PruneInterval = DefaultPruneInterval
	}
	st, err := OpenStore(cfg.Dir)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:     cfg,
		store:   st,
		runner:  cfg.Runner,
		running: make(map[string]*runHandle),
		wake:    make(chan struct{}, 1),
		quit:    make(chan struct{}),
		start:   time.Now(),
		inboxes: make(map[string]*inboxCore),
		errs:    make(map[string]error),

		watching: make(map[string]MonitorStatus),
		every:    newScheduleSet(cfg.Logf),
	}
	s.sched, s.sharedSched = cfg.Scheduler, cfg.Scheduler != nil
	if s.sched == nil {
		s.sched = NewScheduler(SchedulerConfig{
			MaxConcurrent:  cfg.MaxConcurrent,
			CPUBudget:      cfg.CPUBudget,
			DefaultCost:    cfg.DefaultCost,
			SampleInterval: cfg.SampleInterval,
			DisableCPU:     cfg.DisableCPU,
			StatsURL:       cfg.StatsURL,
			HistoryPath:    filepath.Join(cfg.Dir, classHistoryName),
			Logf:           cfg.Logf,
			MonitorPolicy:  cfg.MonitorPolicy,
			MonitorBudget:  cfg.MonitorBudget,
		})
	}
	s.gov = s.sched.gov
	s.defCore = newInboxCore(s, DefaultInboxName, defaultKeys, 0, nil)
	s.defCore.isDefault = true
	s.def = &Inbox[*CommandSpec, CommandResult]{b: localDefault{s}, name: DefaultInboxName, keys: defaultKeys}
	if err := s.openMonitorInbox(); err != nil {
		return nil, err
	}
	return s, nil
}

// classHistoryName is the file the scheduler keeps its per-class cost history in.
const classHistoryName = "classes.json"

// cpuCount is how many logical CPUs the host has.
func (c Config) cpuCount() int { return runtime.NumCPU() }

// Config returns the settings the server was built with.
func (s *Server) Config() Config { return s.cfg }

// Store returns the server's store.
func (s *Server) Store() *Store { return s.store }

// Scheduler returns the scheduler that admits the server's jobs.
func (s *Server) Scheduler() *Scheduler { return s.sched }

// Start runs the scheduler and brings up every configured transport. It
// returns once each transport is listening.
func (s *Server) Start() error {
	s.wg.Add(1)
	go s.loop()
	s.wg.Add(1)
	go s.syncLoop()
	s.wg.Add(1)
	go s.pruneLoop()
	s.wg.Add(1)
	go s.scheduleLoop()
	s.sched.Start()
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

// A runHandle is one job's run in flight: the way to stop it, and the moment
// it has stopped for good.
type runHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
	// pid is the running job's process, once the runner has started it.
	pid int
	// lease is the job's admission. The scheduler revokes it to end the run.
	lease *Lease
	// revoked says why the scheduler ended the run.
	revoked string
}

// An interruptTimeout bounds how long Interrupt waits for a run to stop.
const interruptTimeout = 30 * time.Second

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
	for _, h := range s.running {
		cancels = append(cancels, h.cancel)
	}
	s.mu.Unlock()

	for _, c := range cancels {
		c()
	}
	s.closeInboxes()
	close(s.quit)
	s.wg.Wait()
	if !s.sharedSched {
		s.sched.Close()
	}
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
	// The ticker re-runs the decision.
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-s.wake:
		case <-t.C:
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
			s.syncLogs()
		}
	}
}

// syncLogs flushes the log of every running job to stable storage.
func (s *Server) syncLogs() {
	s.mu.Lock()
	ids := make([]string, 0, len(s.running))
	for id := range s.running {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		if err := s.store.SyncLog(id); err != nil {
			s.cfg.Logf("go-jobserver: sync log %s: %v", id, err)
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
		if j.Trigger != "" {
			s.finish(id, StateCancelled, -1, msgNeverFires, nil, j.LogBytes)
			continue
		}
		if err := s.store.setState(id, StateBlocked, "a dependency failed"); err != nil {
			s.cfg.Logf("go-jobserver: block %s: %v", id, err)
		}
	}
	jobs = s.store.Jobs()
	byID := Index(jobs)
	for _, id := range Ready(jobs) {
		j, ok := s.store.Job(id)
		if !ok {
			continue
		}
		if j.Inbox != "" {
			s.offerInbox(j)
			continue
		}
		if s.defCore.pending(j.ID) {
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
				if src := CacheHit(identity, s.store.IdentityIndex(), j.ID); src != "" && s.reuse(j, src, identity) {
					continue
				}
			}
		}
		s.defCore.offer(j)
	}
	s.signalTakers()
}

// leaseFor is what the scheduler admits a job as. The job's own cost comes
// first: what it declared, then what its last attempt measured.
func (s *Server) leaseFor(j *Job) LeaseRequest {
	policy := j.Policy.clone()
	policy.Cost = j.ExpectedCost(0)
	return LeaseRequest{ID: j.ID, Class: j.Name, Policy: policy}
}

// identityOf computes a job's dedup identity, hashing its declared inputs.
func (s *Server) identityOf(j *Job, byID map[string]*Job) (string, error) {
	resolved := *j
	resolved.Inputs = hashInputs(j.Dir, j.Inputs)
	return JobIdentity(&resolved, byID), nil
}

// reuse records that a job's work was already done by another, from the dedup cache. The source task can be pruned.
// It reports false when the cache no longer names that task, so the job runs after all.
func (s *Server) reuse(j *Job, srcID, identity string) bool {
	src, ok := s.store.cacheEntry(identity)
	if !ok || src.ID != srcID {
		return false
	}
	if err := s.store.SetIdentity(j.ID, identity, srcID); err != nil {
		s.cfg.Logf("go-jobserver: cache %s: %v", j.ID, err)
	}
	if err := s.store.Finish(j.ID, StateCached, 0, "reused "+srcID, src.Artifacts, 0); err != nil {
		s.cfg.Logf("go-jobserver: cache %s: %v", j.ID, err)
	}
	s.Wake()
	return true
}

// dispatch starts one job's run in the background under the lease the
// scheduler granted it.
func (s *Server) dispatch(id string, l *Lease) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		l.Release()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &runHandle{cancel: cancel, done: make(chan struct{}), lease: l}
	s.running[id] = h
	s.mu.Unlock()

	j, ok := s.store.Job(id)
	if !ok {
		s.finishRun(id)
		l.Release()
		cancel()
		close(h.done)
		return
	}
	if err := s.store.Start(id); err != nil {
		s.cfg.Logf("go-jobserver: start %s: %v", id, err)
		s.finishRun(id)
		l.Release()
		cancel()
		close(h.done)
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(h.done)
		defer func() {
			// runOnce settles the lease before it records the outcome. This covers a run that never got that far.
			l.Release()
			s.finishRun(id)
			cancel()
			s.Wake()
		}()
		// A revoked lease ends the run, and the job records why.
		go func() {
			select {
			case <-l.Revoked():
				s.mu.Lock()
				h.revoked = l.RevokeReason()
				s.mu.Unlock()
				cancel()
			case <-ctx.Done():
			}
		}()
		s.runOnce(ctx, j, h, func(pid int) {
			s.mu.Lock()
			h.pid = pid
			s.mu.Unlock()
			l.AttachProcess(pid)
		})
	}()
}

// settle records what a job's process tree used and gives its slot back.
// Release also gives the job's processes their normal scheduling back.
func (s *Server) settle(id string, l *Lease) {
	measured, used := l.Measured(), l.CPUSeconds()
	l.Release()
	if !measured {
		return
	}
	if err := s.store.FinishCPU(id, used); err != nil {
		s.cfg.Logf("go-jobserver: cpu %s: %v", id, err)
	}
}

// finishRun forgets a job's cancellation handle.
func (s *Server) finishRun(id string) {
	s.mu.Lock()
	delete(s.running, id)
	s.mu.Unlock()
}

// runOnce runs a job and records everything that happened.
func (s *Server) runOnce(ctx context.Context, j *Job, h *runHandle, started func(pid int)) {
	w, err := s.store.LogWriter(j.ID)
	if err != nil {
		s.finish(j.ID, StateFailed, -1, err.Error(), nil, 0)
		return
	}
	parent := ctx
	if j.Timeout > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, j.Timeout)
		defer stop()
	}
	code, runErr := s.runner.Run(ctx, j, w, started)
	if err := w.Sync(); err != nil {
		s.cfg.Logf("go-jobserver: sync log %s: %v", j.ID, err)
	}
	n := w.N()
	// Record what the job's tree consumed, and give its processes their normal scheduling back, before the outcome is recorded.
	s.settle(j.ID, h.lease)
	s.mu.Lock()
	revoked := h.revoked
	s.mu.Unlock()
	switch {
	case revoked != "":
		s.finish(j.ID, StateFailed, -1, "revoked by the scheduler: "+revoked, nil, n)
	case parent.Err() == nil && ctx.Err() != nil:
		s.finish(j.ID, StateFailed, -1, msgTimedOut, nil, n)
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

// Deprecated: use DefaultInbox().AddTask. Enqueue adds a job to the default inbox.
func (s *Server) Enqueue(spec Spec) (*Job, bool, error) { return s.enqueue(spec, nil) }

// enqueue adds a job to the default inbox with the task fields meta carries.
func (s *Server) enqueue(spec Spec, meta *taskMeta) (*Job, bool, error) {
	if s.isStopped() {
		return nil, false, ErrStopped
	}
	if spec.Key != "" && !spec.Force {
		for _, j := range s.store.liveIn(DefaultInboxName) {
			if j.Key == spec.Key {
				return j, true, nil
			}
		}
	}
	j, err := s.store.create(&spec, meta)
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

// List returns every job in creation order.
func (s *Server) List() []*Job { return s.withStatus(s.store.Jobs()) }

// Get returns one job.
func (s *Server) Get(id string) (*Job, error) {
	j, err := mustJob(s.store, id)
	if err != nil {
		return nil, err
	}
	return s.withStatus([]*Job{j})[0], nil
}

// Logs returns up to max bytes of a job's output starting at offset.
func (s *Server) Logs(id string, offset int64, max int) ([]byte, error) {
	if _, err := mustJob(s.store, id); err != nil {
		return nil, err
	}
	return s.store.ReadLog(id, offset, max)
}

// SchedulerStatus reports what holds a slot, what waits and why.
func (s *Server) SchedulerStatus() SchedulerStatus { return s.sched.Status() }

// Readings returns the daemon's latest CPU measurement.
func (s *Server) Readings() CPUReport { return s.gov.report() }

// Controls returns the process controls in effect right now.
func (s *Server) Controls() []Control { return s.gov.controls() }

// Events returns the recent control history, newest first.
func (s *Server) Events() []Control { return s.gov.events() }

// SetPolicy replaces a job's CPU policy.
func (s *Server) SetPolicy(id string, policy JobPolicy) (*Job, error) {
	if err := s.store.SetPolicy(id, policy); err != nil {
		return nil, err
	}
	// A running job's governor reads its lease, so the new policy has to reach that too.
	s.sched.SetPolicy(CanonicalID(id), policy)
	s.Wake()
	return mustJob(s.store, id)
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
