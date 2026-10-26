// The Scheduler decides what runs.

package jobserver

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"time"
)

// The Scheduler decides what runs. It holds the count cap, the named groups,
// the CPU budget and the per-class cost history. Its governor measures what
// runs and acts on work that misbehaves. The daemon's own jobs go through it,
// and so does work that another program runs itself.

var (
	// ErrUnknownGroup is returned for a lease that names a group the scheduler does not have.
	ErrUnknownGroup = errors.New("jobserver: unknown group")
	// ErrGroupRemoved is returned to a queued lease whose group was removed.
	ErrGroupRemoved = errors.New("jobserver: group was removed while queued")
	// ErrSchedulerClosed is returned once the scheduler is closed.
	ErrSchedulerClosed = errors.New("jobserver: scheduler is closed")
	// ErrDuplicateLease is returned for a lease ID that a live lease already holds.
	ErrDuplicateLease = errors.New("jobserver: duplicate lease id")
)

// SchedulerConfig is how a Scheduler is built.
type SchedulerConfig struct {
	// MaxConcurrent bounds how many counted leases hold a slot at once. Zero means no bound. It is the declared value.
	MaxConcurrent int
	// Groups are the declared named groups and their limits, as SetGroups takes them.
	Groups map[string]int
	// CPUBudget is how many CPUs the running jobs may use together. Zero means one per CPU.
	CPUBudget float64
	// DefaultCost is the CPU cost of a lease that declares none and whose class has no history.
	DefaultCost float64
	// SampleInterval is how often the governor samples CPU use. Zero means one second.
	SampleInterval time.Duration
	// StatsURL is a simple-stats-api to read the host load from. Empty reads this host's /proc/stat. Process trees are always read here.
	StatsURL string
	// DisableCPU turns the governor off: nothing is measured, throttled or limited.
	DisableCPU bool
	// HistoryPath is the file the per-class cost history is kept in. Empty keeps it in memory.
	HistoryPath string
	// Logf receives the scheduler's own status lines. Nil discards them.
	Logf func(format string, args ...any)
	// PriorityKeys is the admission order, compared top to bottom. NewScheduler panics on an invalid list.
	PriorityKeys []PriorityKey
	// RevertAfter is how long the load must stay at or below the budget before the governor reverts its controls.
	RevertAfter time.Duration
	// RetryInterval is the wait before a failed control is tried again. Zero means DefaultRetryInterval. A negative value means no wait.
	RetryInterval time.Duration
	// ErrorLogf receives the lines that name a configuration error the operator has to fix. Nil sends them to Logf.
	ErrorLogf func(format string, args ...any)
}

// The defaults for the governor's SchedulerConfig fields.
const (
	// DefaultRevertAfter is how long the load must stay at or below the budget before the controls come off.
	DefaultRevertAfter = 5 * time.Second
	// DefaultRetryInterval is how long a control that did not take effect waits before the next try.
	DefaultRetryInterval = 10 * time.Second
)

// bookable is how many CPUs of cost the scheduler books for a budget. The
// budget plus CPUs, or plus the whole budget when that is less.
func bookable(budget float64) float64 {
	return budget + min(budget, 2)
}

// Scheduler starts leases and governs the ones that run.
type Scheduler struct {
	cfg SchedulerConfig

	mu      sync.Mutex
	closed  bool
	seq     uint64
	waiting []*Lease
	holding []*Lease
	// groups holds the declared limits and groupOverrides the operator's. An override with no declared group has no effect.
	groups         map[string]int
	groupOverrides map[string]int
	// maxRuns is the declared cap. capOverride replaces it while capOverridden is set.
	maxRuns       int
	capOverride   int
	capOverridden bool
	budget        float64
	batches       map[string]*batchState
	now           func() time.Time
	// host is the host's load over the last samples.
	host hostLoad

	classes  *classBook
	gov      *governor
	priority priorityTable

	quit      chan struct{}
	wg        sync.WaitGroup
	startOnce sync.Once
	closeOnce sync.Once
}

// NewScheduler builds a scheduler. Start runs its governor.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	if cfg.MaxConcurrent < 0 {
		cfg.MaxConcurrent = 0
	}
	if cfg.DefaultCost < 0 {
		cfg.DefaultCost = 0
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.ErrorLogf == nil {
		cfg.ErrorLogf = cfg.Logf
	}
	if cfg.RevertAfter == 0 {
		cfg.RevertAfter = DefaultRevertAfter
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = DefaultRetryInterval
	}
	priority, err := newPriorityTable(cfg.PriorityKeys)
	if err != nil {
		panic(err)
	}
	s := &Scheduler{
		priority:       priority,
		cfg:            cfg,
		groups:         clampGroups(cfg.Groups),
		groupOverrides: map[string]int{},
		maxRuns:        cfg.MaxConcurrent,
		budget:         cfg.CPUBudget,
		batches:        map[string]*batchState{},
		now:            time.Now,
		quit:           make(chan struct{}),
	}
	s.classes = openClassBook(cfg.HistoryPath, cfg.Logf)
	s.gov = newGovernor(s)
	return s
}

// Start runs the governor. It does nothing when CPU sampling is off.
func (s *Scheduler) Start() {
	s.startOnce.Do(func() {
		if s.cfg.DisableCPU {
			return
		}
		s.wg.Add(1)
		go s.gov.loop()
	})
}

// Close fails every queued lease, stops the governor and gives every
// throttled process its normal scheduling back. Holders keep their slots
// until they release them.
func (s *Scheduler) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		for _, l := range s.waiting {
			l.fail(ErrSchedulerClosed)
		}
		s.waiting = nil
		s.mu.Unlock()
		close(s.quit)
		s.wg.Wait()
		s.gov.releaseAll()
	})
}

// Budget is how many CPUs the running jobs may use together.
func (s *Scheduler) Budget() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.budgetLocked()
}

func (s *Scheduler) budgetLocked() float64 {
	if s.budget > 0 {
		return s.budget
	}
	return float64(runtime.NumCPU())
}

// SetCPUBudget moves the CPU budget. Zero means one per CPU. Queued leases
// are planned again at once.
func (s *Scheduler) SetCPUBudget(cpus float64) {
	if cpus < 0 {
		cpus = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.budget = cpus
	s.planLocked()
}

// ErrInvalidLimit is returned for an override below the smallest limit it may take.
var ErrInvalidLimit = errors.New("jobserver: invalid limit")

// MaxConcurrent is the effective count cap: the override when one is set, else the declared value. Zero means no cap.
func (s *Scheduler) MaxConcurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.capLocked()
}

// capLocked is the effective count cap. Caller holds s.mu.
func (s *Scheduler) capLocked() int {
	if s.capOverridden {
		return s.capOverride
	}
	return s.maxRuns
}

// SetMaxConcurrent moves the declared count cap. Zero means no cap. An override stays in effect over it.
func (s *Scheduler) SetMaxConcurrent(n int) {
	if n < 0 {
		n = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxRuns = n
	s.planLocked()
}

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

// clampGroups copies limits and raises a limit below one to one.
func clampGroups(limits map[string]int) map[string]int {
	next := make(map[string]int, len(limits))
	for name, limit := range limits {
		if limit < 1 {
			limit = 1
		}
		next[name] = limit
	}
	return next
}

// SetGroups replaces the declared groups and their limits. A limit below one is
// one. An override keeps its effect over the new declared limit. A queued
// lease whose group is gone fails with ErrGroupRemoved. A holder of a removed
// group keeps its slot, and counts again if the group returns.
func (s *Scheduler) SetGroups(limits map[string]int) {
	next := clampGroups(limits)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups = next
	kept := s.waiting[:0]
	for _, l := range s.waiting {
		if l.req.Group != "" {
			if _, ok := next[l.req.Group]; !ok {
				l.fail(fmt.Errorf("%w: %q", ErrGroupRemoved, l.req.Group))
				continue
			}
		}
		kept = append(kept, l)
	}
	s.waiting = kept
	s.planLocked()
}

// HasGroup reports whether a group is declared, and its effective limit.
func (s *Scheduler) HasGroup(name string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groupLimitLocked(name)
}

// Acquire queues a lease and blocks until the scheduler grants it, ctx ends,
// or the lease cannot be granted at all. onQueue follows the queue contract:
// its first call means the lease has to wait, and later calls mean its place
// or reason changed. Calls are serialized under the scheduler's lock, so keep
// them fast and never call back into the scheduler.
func (s *Scheduler) Acquire(ctx context.Context, req LeaseRequest, onQueue func(QueueState)) (*Lease, error) {
	l, err := s.enqueue(req, onQueue)
	if err != nil {
		return nil, err
	}
	// A lease the first plan granted is kept even when ctx is already done.
	select {
	case <-l.ready:
		if l.err != nil {
			return nil, l.err
		}
		return l, nil
	default:
	}
	select {
	case <-l.ready:
		if l.err != nil {
			return nil, l.err
		}
		return l, nil
	case <-ctx.Done():
		s.mu.Lock()
		select {
		case <-l.ready:
			s.mu.Unlock()
			if l.err == nil {
				// Granted in the same instant the wait ended: give the slot straight back.
				l.Release()
			}
		default:
			s.waiting = slices.DeleteFunc(s.waiting, func(c *Lease) bool { return c == l })
			s.planLocked()
			s.mu.Unlock()
		}
		return nil, ctx.Err()
	}
}

// TryAcquire grants a lease only if the plan grants it now. It never waits, so
// a lease that has to queue is withdrawn and ok is false.
func (s *Scheduler) TryAcquire(req LeaseRequest) (l *Lease, ok bool, err error) {
	l, err = s.enqueue(req, nil)
	if err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-l.ready:
		if l.err != nil {
			return nil, false, l.err
		}
		return l, true, nil
	default:
	}
	s.waiting = slices.DeleteFunc(s.waiting, func(c *Lease) bool { return c == l })
	return nil, false, nil
}

// enqueue adds a lease to the queue and plans once.
func (s *Scheduler) enqueue(req LeaseRequest, onQueue func(QueueState)) (*Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrSchedulerClosed
	}
	if req.Group != "" {
		if _, ok := s.groups[req.Group]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownGroup, req.Group)
		}
	}
	if req.ID != "" {
		for _, l := range s.holding {
			if l.req.ID == req.ID {
				return nil, fmt.Errorf("%w: %s", ErrDuplicateLease, req.ID)
			}
		}
		for _, l := range s.waiting {
			if l.req.ID == req.ID {
				return nil, fmt.Errorf("%w: %s", ErrDuplicateLease, req.ID)
			}
		}
	}
	rank, err := s.priority.rank(req.Priority)
	if err != nil {
		return nil, err
	}
	s.seq++
	l := newLease(s, req, s.seq, s.now().UTC(), onQueue)
	l.rank = rank
	l.cost = s.costOf(req)
	s.waiting = append(s.waiting, l)
	s.planLocked()
	return l, nil
}

// costOf is what a lease is expected to use. What it declared, then what its
// class measured, then what each fallback class measured, then the default.
func (s *Scheduler) costOf(req LeaseRequest) float64 {
	if req.Policy.Cost > 0 {
		return req.Policy.Cost
	}
	for _, class := range req.classes() {
		if c, ok := s.classes.cost(class); ok {
			return c
		}
	}
	return s.cfg.DefaultCost
}

// classes is the lease's class, then its fallbacks.
func (r LeaseRequest) classes() []string {
	return append([]string{r.Class}, r.Fallback...)
}

// release removes a holder and plans again.
func (s *Scheduler) release(l *Lease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i := slices.Index(s.holding, l); i >= 0 {
		s.holding = slices.Delete(s.holding, i, i+1)
		s.endBatch(l.req.Batch, s.now())
	}
	s.planLocked()
}

// holders returns the leases holding a slot now.
func (s *Scheduler) holders() []*Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*Lease(nil), s.holding...)
}

// SetPolicy replaces the policy of a live lease, so the governor applies the
// new one from its next sample. It reports whether the lease was found.
func (s *Scheduler) SetPolicy(id string, policy JobPolicy) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range append(append([]*Lease(nil), s.holding...), s.waiting...) {
		if l.req.ID == id {
			l.mu.Lock()
			l.req.Policy = policy.clone()
			l.mu.Unlock()
			return true
		}
	}
	return false
}

// Readings returns the latest CPU measurement.
func (s *Scheduler) Readings() CPUReport { return s.gov.report() }

// Controls returns the controls in effect right now.
func (s *Scheduler) Controls() []Control { return s.gov.controls() }

// Events returns the recent control history, newest first.
func (s *Scheduler) Events() []Control { return s.gov.events() }

// Classes returns the cost history of every class the scheduler has measured.
func (s *Scheduler) Classes() []ClassStats { return s.classes.list() }
