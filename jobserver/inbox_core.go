// inboxCore holds the live state of one named inbox: its leases, its consumer and its checked-out tasks.

package jobserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wow-look-at-my/go-jobserver/priority"
)

// runFunc runs one task spec and returns its result, as JSON.
type runFunc func(ctx context.Context, spec json.RawMessage) (json.RawMessage, error)

// inboxCore holds the live state of one named inbox. The store holds its tasks.
type inboxCore struct {
	s    *Server
	name string
	keys *priority.Keys
	rank int
	// run is nil for an inbox whose consumer takes its tasks.
	run runFunc

	mu       sync.Mutex
	consumer Consumer
	// consumerName is the name the bound consumer gave to Bind. Each task it takes keeps the name.
	consumerName string
	// leasing holds the tasks that wait for admission, admitted the ones a consumer has not taken, and out the checked-out ones.
	leasing  map[string]context.CancelFunc
	admitted map[string]*grant
	out      map[string]*grant
	// held holds the tasks that gave back their lease to a better queued task. offer skips them until Take clears the set.
	held map[string]bool
	// avail is closed and replaced when leasing or admitted grows or the consumer changes.
	avail chan struct{}
	runs  sync.WaitGroup
	// closing stops a Run call that ends from recording its end, so the next open resumes the task.
	closing bool
	// isDefault marks the default inbox. The server's executor runs each task it admits.
	isDefault bool
	hooks     inboxHooks
}

// inboxHooks are the optional parts of an inbox that shape its leases.
type inboxHooks struct {
	// leaseFor builds the lease request of a task. Nil uses the task's name and policy. The core sets the ID.
	leaseFor func(j *Job) (LeaseRequest, error)
	// onQueue hears where a task waits for admission, and a zero QueueState once it is admitted.
	onQueue func(id string, q QueueState)
}

// pending reports whether a task waits for admission or holds a slot in the inbox.
func (c *inboxCore) pending(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, leasing := c.leasing[id]
	return leasing || c.admitted[id] != nil || c.out[id] != nil
}

// withdraw cancels every request for admission. The tasks stay queued, and the next offer asks again.
func (c *inboxCore) withdraw() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, cancel := range c.leasing {
		cancel()
		delete(c.leasing, id)
	}
}

// grant is one admitted task.
type grant struct {
	id       string
	lease    *Lease
	prio     priority.MultiPriority
	created  time.Time
	spec     json.RawMessage
	timeout  time.Duration
	consumer Consumer
	cancel   context.CancelFunc
	timer    *time.Timer
}

func newInboxCore(s *Server, name string, keys *priority.Keys, rank int, run runFunc) *inboxCore {
	return &inboxCore{s: s, name: name, keys: keys, rank: rank, run: run,
		leasing: map[string]context.CancelFunc{}, admitted: map[string]*grant{}, out: map[string]*grant{},
		held: map[string]bool{}, avail: make(chan struct{})}
}

// signal wakes every Take that waits. Caller holds c.mu.
func (c *inboxCore) signal() {
	close(c.avail)
	c.avail = make(chan struct{})
}

// offer asks for admission of a task whose trigger fired. A consumer inbox asks only while a consumer is bound.
func (c *inboxCore) offer(j *Job) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || (c.run == nil && c.consumer == "" && !c.isDefault) {
		return
	}
	if _, ok := c.leasing[j.ID]; ok {
		return
	}
	if c.admitted[j.ID] != nil || c.out[j.ID] != nil || c.held[j.ID] {
		return
	}
	prio, err := c.keys.FromValues(j.Priority)
	if err != nil {
		c.s.finish(j.ID, StateFailed, -1, err.Error(), nil, 0)
		return
	}
	req := LeaseRequest{ID: j.ID, Class: j.Name, Fallback: []string{"inbox:" + c.name}, Policy: j.Policy.clone()}
	switch {
	case c.isDefault:
		req = c.s.leaseFor(j)
	case c.hooks.leaseFor != nil:
		shaped, err := c.hooks.leaseFor(j)
		if err != nil {
			c.s.finish(j.ID, StateFailed, -1, "the lease request: "+err.Error(), nil, 0)
			return
		}
		shaped.ID = j.ID
		if shaped.Class == "" {
			shaped.Class = req.Class
		}
		if shaped.Fallback == nil {
			shaped.Fallback = req.Fallback
		}
		req = shaped
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.leasing[j.ID] = cancel
	g := &grant{id: j.ID, prio: prio, created: j.Created, spec: j.Spec, timeout: j.Timeout}
	if c.run == nil && !c.isDefault {
		c.signal()
	}
	go c.acquire(ctx, g, req)
}

// acquire waits for admission and hands the task to its runner or its consumer.
func (c *inboxCore) acquire(ctx context.Context, g *grant, req LeaseRequest) {
	order := &leaseOrder{keys: c.keys, prio: g.prio, rank: c.rank, arrival: g.created.UnixNano()}
	var notify func(QueueState)
	if c.hooks.onQueue != nil {
		notify = func(q QueueState) { c.hooks.onQueue(g.id, q) }
	}
	l, err := c.s.sched.acquire(ctx, req, order, notify)
	if err == nil && notify != nil {
		notify(QueueState{})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cancel, live := c.leasing[g.id]
	if !live || ctx.Err() != nil {
		if l != nil {
			l.Release()
		}
		return
	}
	delete(c.leasing, g.id)
	if err != nil {
		cancel()
		if !errors.Is(err, ErrSchedulerClosed) {
			c.s.cfg.Logf("go-jobserver: inbox %s: admit %s: %v", c.name, g.id, err)
		}
		return
	}
	g.lease = l
	if j, ok := c.s.store.Job(g.id); !ok || j.State != StateActive || c.s.store.Paused() {
		cancel()
		l.Release()
		c.s.Wake()
		return
	}
	if c.isDefault {
		cancel()
		// dispatch takes the lease. It runs under c.mu, so no tick offers the task again before it shows as running.
		c.s.dispatch(g.id, l)
		return
	}
	if c.run == nil {
		cancel()
		c.admitted[g.id] = g
		c.signal()
		return
	}
	if err := c.s.store.Start(g.id); err != nil {
		cancel()
		l.Release()
		return
	}
	g.cancel = cancel
	c.out[g.id] = g
	c.runs.Add(1)
	ctx = context.WithValue(ctx, leaseCtxKey{}, l)
	go c.runTask(context.WithValue(ctx, taskCtxKey{}, &Task{ID: g.id, b: localNamed{Server: c.s, c: c}}), g)
}

// runTask runs one task through the inbox's Run and records the outcome.
func (c *inboxCore) runTask(ctx context.Context, g *grant) {
	defer c.runs.Done()
	defer c.s.Wake()
	if g.timeout > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, g.timeout)
		defer stop()
	}
	go func() {
		select {
		case <-g.lease.Revoked():
			g.cancel()
		case <-ctx.Done():
		}
	}()
	result, err := c.run(ctx, g.spec)
	revoked := g.lease.RevokeReason()
	c.mu.Lock()
	_, still := c.out[g.id]
	delete(c.out, g.id)
	closing := c.closing
	c.mu.Unlock()
	c.s.settle(g.id, g.lease)
	defer g.cancel()
	if !still || (closing && ctx.Err() != nil) {
		return
	}
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		c.s.finish(g.id, StateFailed, -1, msgTimedOut, nil, 0)
	case ctx.Err() != nil && revoked != "":
		c.s.finish(g.id, StateFailed, -1, "revoked by the scheduler: "+revoked, nil, 0)
	case ctx.Err() != nil:
		c.s.finish(g.id, StateCancelled, -1, msgCancelled, nil, 0)
	default:
		c.s.settleTask(g.id, result, err)
	}
}

// checkedOut returns a task the consumer holds. Caller holds c.mu.
func (c *inboxCore) checkedOut(who Consumer, id string) (*grant, error) {
	if who == "" || c.consumer != who {
		return nil, ErrNotConsumer
	}
	g := c.out[id]
	if g == nil || g.consumer != who {
		return nil, fmt.Errorf("%w: %s", ErrNotRunning, id)
	}
	return g, nil
}

// started names the process a consumer runs a task in, so the governor can measure and control it.
func (c *inboxCore) started(who Consumer, id string, pid int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.checkedOut(who, id)
	if err != nil {
		return err
	}
	g.lease.AttachProcess(pid)
	return nil
}

// done settles a checked-out task.
func (c *inboxCore) done(who Consumer, id string, result json.RawMessage, taskErr error) error {
	c.mu.Lock()
	g, err := c.checkedOut(who, id)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	delete(c.out, id)
	c.mu.Unlock()
	if g.timer != nil {
		g.timer.Stop()
	}
	c.s.settle(id, g.lease)
	c.s.settleTask(id, result, taskErr)
	c.s.Wake()
	return nil
}

// expire fails a checked-out task whose timeout passed.
func (c *inboxCore) expire(id string) {
	c.mu.Lock()
	g := c.out[id]
	delete(c.out, id)
	c.mu.Unlock()
	if g == nil {
		return
	}
	c.s.settle(id, g.lease)
	c.s.finish(id, StateFailed, -1, msgTimedOut, nil, 0)
	c.s.Wake()
}

// bind attaches a new consumer under name. The one loses every task it held.
func (c *inboxCore) bind(name string) Consumer {
	var b [8]byte
	_, _ = rand.Read(b[:])
	who := Consumer("c-" + hex.EncodeToString(b[:]))
	c.mu.Lock()
	lost := c.dropConsumerLocked()
	c.consumer, c.consumerName = who, clip(name)
	c.signal()
	c.mu.Unlock()
	c.failLost(lost)
	c.s.Wake()
	return who
}

// unbind detaches a consumer. Its checked-out tasks fail. Its queued tasks stay for the next consumer.
func (c *inboxCore) unbind(who Consumer) error {
	c.mu.Lock()
	if who == "" || c.consumer != who {
		c.mu.Unlock()
		return ErrNotConsumer
	}
	lost := c.dropConsumerLocked()
	c.consumer, c.consumerName = "", ""
	c.signal()
	c.mu.Unlock()
	c.failLost(lost)
	return nil
}

// dropConsumerLocked gives back every lease the consumer's tasks held, and returns the checked-out ones. Caller holds c.mu.
func (c *inboxCore) dropConsumerLocked() []*grant {
	if c.consumer == "" {
		return nil
	}
	clear(c.held)
	for id, cancel := range c.leasing {
		cancel()
		delete(c.leasing, id)
	}
	for id, g := range c.admitted {
		g.lease.Release()
		delete(c.admitted, id)
	}
	var lost []*grant
	for id, g := range c.out {
		if g.consumer == c.consumer {
			lost = append(lost, g)
			delete(c.out, id)
		}
	}
	return lost
}

func (c *inboxCore) failLost(lost []*grant) {
	for _, g := range lost {
		if g.timer != nil {
			g.timer.Stop()
		}
		c.s.settle(g.id, g.lease)
		c.s.finish(g.id, StateFailed, -1, msgConsumerGone, nil, 0)
	}
	if len(lost) > 0 {
		c.s.Wake()
	}
}

// forget drops a task from every live set, and reports whether a Run call still owns it. A Run call records its own end.
func (c *inboxCore) forget(id string) (running bool) {
	c.mu.Lock()
	delete(c.held, id)
	if cancel, ok := c.leasing[id]; ok {
		cancel()
		delete(c.leasing, id)
	}
	if g := c.admitted[id]; g != nil {
		g.lease.Release()
		delete(c.admitted, id)
	}
	g := c.out[id]
	if g != nil && c.run != nil {
		c.mu.Unlock()
		g.cancel()
		return true
	}
	delete(c.out, id)
	c.mu.Unlock()
	if g != nil {
		if g.timer != nil {
			g.timer.Stop()
		}
		c.s.settle(id, g.lease)
	}
	return false
}

// close lets go of every lease and Run call, and records nothing. The next open of the inbox decides about the tasks.
func (c *inboxCore) close() {
	c.mu.Lock()
	c.closing = true
	clear(c.held)
	for id, cancel := range c.leasing {
		cancel()
		delete(c.leasing, id)
	}
	for id, g := range c.admitted {
		g.lease.Release()
		delete(c.admitted, id)
	}
	out := make([]*grant, 0, len(c.out))
	for _, g := range c.out {
		out = append(out, g)
	}
	c.mu.Unlock()
	for _, g := range out {
		if c.run != nil {
			g.cancel()
			continue
		}
		if g.timer != nil {
			g.timer.Stop()
		}
		g.lease.Release()
	}
	c.runs.Wait()
}

// stopAll cancels every task the inbox holds and waits for its Run calls.
func (c *inboxCore) stopAll() {
	c.mu.Lock()
	ids := make([]string, 0, len(c.leasing)+len(c.admitted)+len(c.out))
	for id := range c.leasing {
		ids = append(ids, id)
	}
	for id := range c.admitted {
		ids = append(ids, id)
	}
	for id := range c.out {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	for _, id := range ids {
		if !c.forget(id) {
			c.s.finishIfLive(id, StateCancelled, msgCancelled)
		}
	}
	c.runs.Wait()
}

// leaseCtxKey carries the lease a Run call runs under.
type leaseCtxKey struct{}

// LeaseFromContext returns the lease a Run method runs under, from the context it got. It is nil for another context.
func LeaseFromContext(ctx context.Context) *Lease {
	l, _ := ctx.Value(leaseCtxKey{}).(*Lease)
	return l
}
