// The server side of inboxes: opening them, routing tasks to them and the backends the Inbox type calls.

package jobserver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/wow-look-at-my/go-jobserver/priority"
)

// DefaultInboxName is the name of the inbox whose tasks the server's Runner runs.
const DefaultInboxName = "default"

// DefaultPriorityKey is the priority key of the default inbox. Lower goes first, and arrival breaks a tie.
var DefaultPriorityKey = &priority.MultiPriorityKey{Name: "priority"}

// defaultKeys is the key list of the default inbox: DefaultPriorityKey, then the open-batch key.
var defaultKeys = func() *priority.Keys {
	k, err := priority.NewKeys(DefaultPriorityKey, &priority.MultiPriorityKey{Name: KeyOpenBatch, DefaultPriority: 1})
	if err != nil {
		panic(err)
	}
	return k
}()

// withdrawLeases cancels every request for admission, so a paused server starts nothing.
func (s *Server) withdrawLeases() {
	for _, c := range s.allCores() {
		c.withdraw()
	}
}

// allCores is the default inbox and every open named inbox.
func (s *Server) allCores() []*inboxCore {
	s.mu.Lock()
	defer s.mu.Unlock()
	cores := []*inboxCore{s.defCore}
	for _, c := range s.inboxes {
		cores = append(cores, c)
	}
	return cores
}

// DefaultInbox returns the inbox whose tasks run as commands through the Runner.
func (s *Server) DefaultInbox() *Inbox[*CommandSpec, CommandResult] { return s.def }

// openInbox opens a named inbox and takes back its tasks from the journal.
// check says whether a spec still decodes. A task that does not fails.
func (s *Server) openInbox(name string, keys *priority.Keys, rank int, run runFunc, check func(json.RawMessage) error) (*inboxCore, error) {
	if name == "" || name == DefaultInboxName {
		return nil, fmt.Errorf("%w: %q", ErrInboxExists, name)
	}
	s.mu.Lock()
	if _, taken := s.inboxes[name]; taken {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrInboxExists, name)
	}
	c := newInboxCore(s, name, keys, rank, run)
	s.inboxes[name] = c
	s.mu.Unlock()
	for _, j := range s.store.liveIn(name) {
		if check != nil {
			if err := check(j.Spec); err != nil {
				s.finish(j.ID, StateFailed, -1, "the spec does not decode: "+err.Error(), nil, 0)
				continue
			}
		}
		if j.State != StateRunning {
			continue
		}
		if run != nil {
			if err := s.store.requeue(j.ID, "resumed after a restart"); err != nil {
				s.cfg.Logf("go-jobserver: inbox %s: requeue %s: %v", name, j.ID, err)
			}
			continue
		}
		s.finish(j.ID, StateFailed, -1, msgConsumerGone, nil, 0)
	}
	s.Wake()
	return c, nil
}

// inbox returns an open named inbox.
func (s *Server) inbox(name string) *inboxCore {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inboxes[name]
}

// signalTakers wakes each Take that waits, so it picks again from the tasks the store holds now.
func (s *Server) signalTakers() {
	s.mu.Lock()
	cores := make([]*inboxCore, 0, len(s.inboxes))
	for _, c := range s.inboxes {
		cores = append(cores, c)
	}
	s.mu.Unlock()
	for _, c := range cores {
		c.mu.Lock()
		if c.run == nil {
			c.signal()
		}
		c.mu.Unlock()
	}
}

// offerInbox hands a ready task to its inbox. A task of an inbox that is not open waits.
func (s *Server) offerInbox(j *Job) {
	if c := s.inbox(j.Inbox); c != nil {
		c.offer(j)
	}
}

// settleTask records the end of a task that ran: its result on success, its error otherwise.
func (s *Server) settleTask(id string, result json.RawMessage, err error) {
	if err != nil {
		s.mu.Lock()
		s.errs[id] = err
		s.mu.Unlock()
		s.finish(id, StateFailed, -1, err.Error(), nil, 0)
		return
	}
	if len(result) > 0 {
		if rerr := s.store.SetResult(id, result); rerr != nil {
			s.cfg.Logf("go-jobserver: result %s: %v", id, rerr)
		}
	}
	s.finish(id, StateCompleted, 0, "", nil, 0)
}

// finishIfLive ends a task that has not ended yet.
func (s *Server) finishIfLive(id string, st State, msg string) {
	if j, ok := s.store.Job(id); ok && !j.State.Terminal() {
		s.finish(id, st, -1, msg, nil, j.LogBytes)
	}
}

// cancel cancels a task, queued or running. A task that ended already stays as it is.
func (s *Server) cancel(id string) error {
	j, err := mustJob(s.store, id)
	if err != nil {
		return err
	}
	if j.State.Terminal() {
		return nil
	}
	if j.Inbox == "" {
		return s.Interrupt(id)
	}
	if c := s.inbox(j.Inbox); c != nil && c.forget(j.ID) {
		return nil
	}
	s.finishIfLive(j.ID, StateCancelled, msgCancelled)
	s.Wake()
	return nil
}

// drain cancels every queued task of an inbox. Running tasks go on.
func (s *Server) drain(name string) {
	c := s.inbox(name)
	if name == DefaultInboxName {
		c = s.defCore
	}
	for _, j := range s.store.liveIn(name) {
		if j.State == StateRunning {
			continue
		}
		if c != nil && c.forget(j.ID) {
			continue
		}
		s.finishIfLive(j.ID, StateCancelled, msgDrained)
	}
	s.Wake()
}

func (s *Server) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

// Stop refuses new tasks, cancels queued ones, cancels the context of running ones and waits for them. It is idempotent.
func (s *Server) Stop() {
	s.mu.Lock()
	s.stopped = true
	cores := make([]*inboxCore, 0, len(s.inboxes))
	for _, c := range s.inboxes {
		cores = append(cores, c)
	}
	s.mu.Unlock()
	s.drain(DefaultInboxName)
	for _, c := range cores {
		s.drain(c.name)
		c.stopAll()
	}
	s.InterruptAll()
}

// closeInboxes lets go of every task the open inboxes hold and waits for their Run calls. The journal keeps each task as it was.
func (s *Server) closeInboxes() {
	for _, c := range s.allCores() {
		c.close()
	}
}

// job reads a task's job.
func (s *Server) job(id string) (*Job, error) { return mustJob(s.store, id) }

// waitJob blocks until a task's job ends.
func (s *Server) waitJob(ctx context.Context, id string) (*Job, error) {
	if err := s.store.hold(id); err != nil {
		return nil, err
	}
	defer s.store.release(id)
	for {
		ch := s.store.Changed()
		j, err := mustJob(s.store, id)
		if err != nil {
			return nil, err
		}
		if j.State.Terminal() {
			return j, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// errOf is the error a task failed with in this process, or nil.
func (s *Server) errOf(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errs[id]
}

// localNamed is the backend of a named inbox in this process.
type localNamed struct {
	*Server
	c *inboxCore
}

func (b localNamed) add(raw json.RawMessage, values map[string]int, trigger, on string) (string, error) {
	return b.addNamed(b.c.name, raw, values, trigger, on)
}

// addNamed adds a task to a named inbox.
func (s *Server) addNamed(name string, raw json.RawMessage, values map[string]int, trigger, on string) (string, error) {
	if s.isStopped() {
		return "", ErrStopped
	}
	var ts TaskSpec
	if err := json.Unmarshal(raw, &ts); err != nil {
		return "", fmt.Errorf("jobserver: inbox %s: spec: %w", name, err)
	}
	spec := Spec{Name: ts.Title}
	if on != "" {
		spec.Deps = []string{on}
	}
	j, err := s.store.create(&spec, &taskMeta{Inbox: name, Trigger: trigger, Description: ts.Description,
		Timeout: ts.Timeout, Priority: values, Spec: raw})
	if err != nil {
		return "", err
	}
	s.Wake()
	return j.ID, nil
}

func (b localNamed) drain() error { b.Server.drain(b.c.name); return nil }
func (b localNamed) bind() (Consumer, error) {
	if b.c.run != nil {
		return "", fmt.Errorf("%w: inbox %s runs its own tasks", ErrBadInbox, b.c.name)
	}
	return b.c.bind(), nil
}
func (b localNamed) unbind(who Consumer) error { return b.c.unbind(who) }
func (b localNamed) take(ctx context.Context, who Consumer, wait time.Duration) (string, json.RawMessage, bool, error) {
	return b.c.take(ctx, who, wait)
}
func (b localNamed) started(who Consumer, id string, pid int) error { return b.c.started(who, id, pid) }
func (b localNamed) done(who Consumer, id string, result json.RawMessage, err error) error {
	return b.c.done(who, id, result, err)
}

// localDefault is the backend of the default inbox.
type localDefault struct{ *Server }

func (b localDefault) add(raw json.RawMessage, values map[string]int, trigger, on string) (string, error) {
	var cs CommandSpec
	if err := json.Unmarshal(raw, &cs); err != nil {
		return "", fmt.Errorf("jobserver: default inbox: spec: %w", err)
	}
	spec := cs.toSpec()
	if on != "" && len(spec.Deps) > 0 {
		return "", fmt.Errorf("%w: a triggered task takes no other dependency", ErrBadState)
	}
	if on != "" {
		spec.Deps = append(spec.Deps, on)
	}
	meta := &taskMeta{Trigger: trigger, Description: cs.Description, Timeout: cs.Timeout, Priority: values}
	j, _, err := b.enqueue(spec, meta)
	if err != nil {
		return "", err
	}
	return j.ID, nil
}

func (b localDefault) drain() error { b.Server.drain(DefaultInboxName); return nil }
func (b localDefault) bind() (Consumer, error) {
	return "", fmt.Errorf("%w: the default inbox runs its own tasks", ErrBadInbox)
}
func (b localDefault) unbind(Consumer) error { return ErrNotConsumer }
func (b localDefault) take(context.Context, Consumer, time.Duration) (string, json.RawMessage, bool, error) {
	return "", nil, false, ErrNotConsumer
}
func (b localDefault) started(Consumer, string, int) error { return ErrNotConsumer }
func (b localDefault) done(Consumer, string, json.RawMessage, error) error {
	return ErrNotConsumer
}
