package jobserver

import (
	"sync"
	"time"
)

// LeaseRequest describes work the scheduler admits. The daemon's own jobs are
// leases, and so is work another program runs itself and hands to the
// scheduler for admission and governance.
type LeaseRequest struct {
	// ID names the lease. It must be unique among live leases. Empty is allowed.
	ID string `json:"id"`
	// Class is the kind of work. The scheduler keeps a cost history per class.
	Class string `json:"class,omitempty"`
	// Group is a named group whose limit the lease counts against. Empty means none.
	Group string `json:"group,omitempty"`
	// Priority holds the lease's value per priority key, by name. A key it leaves out takes the key's default.
	Priority map[string]float64 `json:"priority,omitempty"`
	// Batch ties leases together. While one member holds a slot, the open-batch key moves the rest forward.
	Batch string `json:"batch,omitempty"`
	// GroupOnly takes only the group slot. The count cap and the CPU budget never hold it back.
	GroupOnly bool `json:"group_only,omitempty"`
	// Policy is the lease's CPU cost and the controls the governor may apply while the budget is exceeded.
	Policy JobPolicy `json:"policy"`
	// Limits are hard bounds on what the lease may use.
	Limits Limits `json:"limits,omitzero"`
}

// Limits bound what one lease may use, whatever the rest of the host does.
type Limits struct {
	// MaxCPUs is how many CPUs the lease may use. A target that can be capped is capped there.
	MaxCPUs float64 `json:"max_cpus,omitempty"`
	// MaxCPUSeconds is how much CPU time the lease may use in all. The lease is revoked past it.
	MaxCPUSeconds float64 `json:"max_cpu_seconds,omitempty"`
	// Grace is how long an uncappable lease may stay above MaxCPUs. Zero means DefaultGrace.
	Grace time.Duration `json:"grace,omitempty"`
}

// DefaultGrace is how long an uncappable lease may stay above its CPU limit.
const DefaultGrace = 30 * time.Second

func (l Limits) grace() time.Duration {
	if l.Grace > 0 {
		return l.Grace
	}
	return DefaultGrace
}

// Wait kinds name the constraint a queued lease waits on.
const (
	// WaitCap is the count cap.
	WaitCap = "cap"
	// WaitGroup is the lease's named group.
	WaitGroup = "group"
	// WaitCPU is the CPU budget.
	WaitCPU = "cpu"
)

// QueueState is where a queued lease stands.
type QueueState struct {
	// Kind is the constraint the lease waits on: WaitCap, WaitGroup or WaitCPU.
	Kind string `json:"kind"`
	// Key is the group name for WaitGroup, and the kind otherwise.
	Key string `json:"key"`
	// Holders are the IDs of the leases holding what this waits for.
	Holders []string `json:"holders,omitempty"`
	// Position is the lease's place among the leases waiting on the same thing, from one.
	Position int `json:"position"`
	// Reason says why the lease waits, in words.
	Reason string `json:"reason"`
}

func (q QueueState) equal(o QueueState) bool {
	if q.Kind != o.Kind || q.Key != o.Key || q.Position != o.Position || q.Reason != o.Reason || len(q.Holders) != len(o.Holders) {
		return false
	}
	for i := range q.Holders {
		if q.Holders[i] != o.Holders[i] {
			return false
		}
	}
	return true
}

// An Actuator measures and controls work the governor cannot see as a process
// tree, such as a container on another daemon.
type Actuator interface {
	// Usage reports how many CPUs the work used since the call. ok is false when there is no reading yet.
	Usage() (cpus float64, ok bool)
	// Apply turns on one mechanism of the policy.
	Apply(m Mechanism, policy JobPolicy) (Outcome, string)
	// Revert turns one mechanism off again.
	Revert(m Mechanism) (Outcome, string)
	// Cap bounds the work at a number of CPUs. Zero removes the bound.
	Cap(cpus float64) (Outcome, string)
}

// A Lease is one admission. The holder runs the work, tells the scheduler
// what to watch, and releases the lease when the work ends.
type Lease struct {
	sched  *Scheduler
	seq    uint64
	since  time.Time
	notify func(QueueState)
	ready  chan struct{}
	err    error

	// Planner state, under the scheduler's lock. rank holds one value per priority key, in key order.
	rank    []float64
	cost    float64
	started time.Time
	state   QueueState
	told    bool

	mu         sync.Mutex
	req        LeaseRequest
	pid        int
	actuator   Actuator
	revoked    chan struct{}
	reason     string
	released   bool
	cpuSeconds float64
	// measuredFor is how long the measurements behind cpuSeconds cover.
	measuredFor time.Duration
	cpus        float64
	measured    bool
	overSince   time.Time
	releaseOnce sync.Once

	// ctl serializes the governor's control calls on this lease.
	ctl sync.Mutex
	// ended is set with ctl and the governor's mu both held, so either lock is enough to read it.
	ended bool
}

func newLease(s *Scheduler, req LeaseRequest, seq uint64, since time.Time, notify func(QueueState)) *Lease {
	req.Policy = req.Policy.clone()
	return &Lease{
		sched: s, seq: seq, since: since, notify: notify,
		ready: make(chan struct{}), revoked: make(chan struct{}), req: req,
	}
}

// fail ends a queued lease with an error. Caller holds the scheduler's lock.
func (l *Lease) fail(err error) {
	l.err = err
	close(l.ready)
}

// ID returns the lease's ID.
func (l *Lease) ID() string { return l.req.ID }

// Cost is the CPU cost the lease was admitted at.
func (l *Lease) Cost() float64 { return l.cost }

// AttachProcess names the process at the head of the work's process tree. The
// governor measures and controls that tree.
func (l *Lease) AttachProcess(pid int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pid = pid
}

// AttachActuator hands the governor what it measures and controls the work
// through, in place of a process tree.
func (l *Lease) AttachActuator(a Actuator) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.actuator = a
}

// Revoked is closed when the scheduler decides the work must end.
func (l *Lease) Revoked() <-chan struct{} { return l.revoked }

// RevokeReason says why the lease was revoked. It is empty until then.
func (l *Lease) RevokeReason() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reason
}

// Revoke ends the lease's work for a reason. The first reason wins.
func (l *Lease) Revoke(reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reason != "" || l.released {
		return
	}
	l.reason = reason
	close(l.revoked)
}

// CPUSeconds is how much CPU time the governor measured the work using.
func (l *Lease) CPUSeconds() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cpuSeconds
}

// Measured reports whether the governor took any reading of the work.
func (l *Lease) Measured() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.measured
}

// Release gives the slot back. It reverts every control in effect on the work
// and records what the work used in its class's history. Only the first call
// does anything.
func (l *Lease) Release() {
	l.releaseOnce.Do(func() {
		l.sched.gov.finish(l)
		l.mu.Lock()
		l.released = true
		used, span, measured := l.cpuSeconds, l.measuredFor, l.measured
		class, revoked := l.req.Class, l.reason != ""
		l.mu.Unlock()
		if measured {
			// The time the lease held its slot unmeasured would read as idle time.
			l.sched.classes.observe(class, used, span, revoked)
		}
		l.sched.release(l)
	})
}

// snapshot reads what the governor needs from a lease.
func (l *Lease) snapshot() (req LeaseRequest, pid int, a Actuator) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.req, l.pid, l.actuator
}
