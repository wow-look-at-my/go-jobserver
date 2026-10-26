// Inbox is a typed queue of tasks. An inbox type embeds it and adds its priority keys and methods.

package jobserver

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/wow-look-at-my/go-jobserver/priority"
)

// TaskRunner is the Run method an inbox type may have. With it, the server's workers run the tasks.
// Without it, a consumer uses Take and Done.
type TaskRunner[T iSpec, R any] interface {
	Run(ctx context.Context, spec T) (R, error)
}

// InboxPrioritizer is the InboxPriority method an inbox type may have. Lower goes first when tasks of several inboxes want a slot.
type InboxPrioritizer interface {
	InboxPriority() int
}

// inboxBackend is what an Inbox calls: the local server or a client.
type inboxBackend interface {
	taskBackend
	add(spec json.RawMessage, values map[string]int, trigger, on string) (string, error)
	drain() error
	bind() (Consumer, error)
	unbind(c Consumer) error
	take(ctx context.Context, c Consumer, wait time.Duration) (string, json.RawMessage, bool, error)
	started(c Consumer, id string, pid int) error
	done(c Consumer, id string, result json.RawMessage, err error) error
	detail(c Consumer, id string, op DetailOp) (int, error)
	details(id string) (*TaskDetails, error)
	tasks(since uint64) (TaskPage, error)
	counts() (TaskCounts, error)
	readLog(id string, offset int64, max int) ([]byte, error)
}

// Inbox holds tasks of spec type T with results of type R. An inbox type embeds it. Only DefaultInbox uses it bare.
type Inbox[T iSpec, R any] struct {
	b    inboxBackend
	name string
	keys *priority.Keys
}

// inboxMethods reads the optional methods of an inbox type. A method with the right name and the wrong signature is an error.
func inboxMethods[T iSpec, R any](self any) (TaskRunner[T, R], int, error) {
	t := reflect.TypeOf(self)
	var run TaskRunner[T, R]
	if _, has := t.MethodByName("Run"); has {
		r, ok := self.(TaskRunner[T, R])
		if !ok {
			return nil, 0, fmt.Errorf("%w: %v: Run must be func(context.Context, %v) (%v, error)",
				ErrBadInbox, t, reflect.TypeFor[T](), reflect.TypeFor[R]())
		}
		run = r
	}
	rank := 0
	if _, has := t.MethodByName("InboxPriority"); has {
		p, ok := self.(InboxPrioritizer)
		if !ok {
			return nil, 0, fmt.Errorf("%w: %v: InboxPriority must be func() int", ErrBadInbox, t)
		}
		rank = p.InboxPriority()
	}
	return run, rank, nil
}

// newSpec decodes a spec into a new value of T. T must be a pointer type.
func newSpec[T iSpec](raw json.RawMessage) (T, error) {
	var zero T
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Pointer {
		return zero, fmt.Errorf("%w: spec type %v is not a pointer", ErrBadInbox, t)
	}
	v := reflect.New(t.Elem()).Interface().(T)
	if err := json.Unmarshal(raw, v); err != nil {
		return zero, err
	}
	return v, nil
}

// setup opens the inbox on a server under name. NewInbox passes the outer value as self.
func (ib *Inbox[T, R]) setup(s *Server, name string, self any) error {
	keys, err := priority.FromFields(self)
	if err != nil {
		return err
	}
	runner, rank, err := inboxMethods[T, R](self)
	if err != nil {
		return err
	}
	if reflect.TypeFor[T]().Kind() != reflect.Pointer {
		return fmt.Errorf("%w: spec type %v is not a pointer", ErrBadInbox, reflect.TypeFor[T]())
	}
	var run runFunc
	if runner != nil {
		run = func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
			spec, err := newSpec[T](raw)
			if err != nil {
				return nil, err
			}
			r, err := runner.Run(ctx, spec)
			if err != nil {
				return nil, err
			}
			return json.Marshal(r)
		}
	}
	check := func(raw json.RawMessage) error { _, err := newSpec[T](raw); return err }
	c, err := s.openInbox(name, keys, rank, run, check)
	if err != nil {
		return err
	}
	ib.b, ib.name, ib.keys = localNamed{Server: s, c: c}, name, keys
	return nil
}

// NewInbox creates the inbox type I under name. A second one with the same name in one process is an error.
// After a restart, it takes back that name's tasks from the journal.
func NewInbox[I any, PI interface {
	*I
	setup(*Server, string, any) error
}](s *Server, name string) (*I, error) {
	v := new(I)
	if err := PI(v).setup(s, name, v); err != nil {
		return nil, err
	}
	return v, nil
}

// Name is the inbox's name.
func (ib *Inbox[T, R]) Name() string { return ib.name }

// Keys returns the inbox type's priority keys, in field order.
func (ib *Inbox[T, R]) Keys() []*priority.MultiPriorityKey { return ib.keys.List() }

// NewPriority returns a priority with every key at its default.
func (ib *Inbox[T, R]) NewPriority() priority.MultiPriority { return ib.keys.New() }

// Compare orders priorities by the key fields. Lower goes first.
func (ib *Inbox[T, R]) Compare(a, b priority.MultiPriority) int { return ib.keys.Compare(a, b) }

// AddTask adds the spec to the inbox as a queued task.
func (ib *Inbox[T, R]) AddTask(spec T, p priority.MultiPriority = priority.MultiPriority{}) (*Task, error) {
	return ib.add(spec, p, "", nil)
}

// add adds a task, triggered on another when on is set.
func (ib *Inbox[T, R]) add(spec T, p priority.MultiPriority, trigger string, on *Task) (*Task, error) {
	raw, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("jobserver: inbox %s: spec: %w", ib.name, err)
	}
	onID := ""
	if on != nil {
		onID = on.ID
	}
	id, err := ib.b.add(raw, ib.keys.Values(p), trigger, onID)
	if err != nil {
		return nil, err
	}
	return &Task{ID: id, b: ib.b}, nil
}

// OnSuccess adds spec to inbox as a task that waits until t succeeds. It is cancelled when t fails or is cancelled.
func OnSuccess[T iSpec, R any](t *Task, inbox *Inbox[T, R], spec T, p priority.MultiPriority = priority.MultiPriority{}) (*Task, error) {
	return inbox.add(spec, p, triggerSuccess, t)
}

// OnFail adds spec to inbox as a task that waits until t fails or is cancelled. It is cancelled when t succeeds.
func OnFail[T iSpec, R any](t *Task, inbox *Inbox[T, R], spec T, p priority.MultiPriority = priority.MultiPriority{}) (*Task, error) {
	return inbox.add(spec, p, triggerFail, t)
}

// OnEnded adds spec to inbox as a task that waits until t ends, whatever its state.
func OnEnded[T iSpec, R any](t *Task, inbox *Inbox[T, R], spec T, p priority.MultiPriority = priority.MultiPriority{}) (*Task, error) {
	return inbox.add(spec, p, triggerEnded, t)
}

// Drain cancels every queued task of the inbox. Running tasks go on.
func (ib *Inbox[T, R]) Drain() error { return ib.b.drain() }

// Bind attaches a new consumer. A refused bind gives the empty handle.
func (ib *Inbox[T, R]) Bind() Consumer {
	c, err := ib.b.bind()
	if err != nil {
		return ""
	}
	return c
}

// Unbind detaches the consumer. Its checked-out tasks fail with "the consumer went away".
func (ib *Inbox[T, R]) Unbind(c Consumer) error { return ib.b.unbind(c) }

// Take checks out the queued task that sorts first by Compare, then by arrival, with its spec. It waits for that task's admission. A wait of -waits with no limit. A wait of no returns at once.
// ok is false when no task was ready in time.
func (ib *Inbox[T, R]) Take(ctx context.Context, c Consumer, wait time.Duration) (*Task, T, bool, error) {
	var zero T
	id, raw, ok, err := ib.b.take(ctx, c, wait)
	if err != nil || !ok {
		return nil, zero, false, err
	}
	spec, err := newSpec[T](raw)
	if err != nil {
		return nil, zero, false, err
	}
	return &Task{ID: id, b: ib.b}, spec, true, nil
}

// Started names the process a consumer runs a task in, so the governor can measure and control it.
func (ib *Inbox[T, R]) Started(c Consumer, t *Task, pid int) error { return ib.b.started(c, t.ID, pid) }

// Done settles a checked-out task: Succeeded with its result when err is nil, else Failed.
func (ib *Inbox[T, R]) Done(c Consumer, t *Task, result R, err error) error {
	if err != nil {
		return ib.b.done(c, t.ID, nil, err)
	}
	raw, merr := json.Marshal(result)
	if merr != nil {
		return fmt.Errorf("jobserver: inbox %s: result: %w", ib.name, merr)
	}
	return ib.b.done(c, t.ID, raw, nil)
}

// Lookup returns the task of this inbox with the ID. It returns ErrNotFound for an ID the server does not know,
// or for a task of another inbox.
func (ib *Inbox[T, R]) Lookup(id string) (*Task, error) {
	j, err := ib.b.job(id)
	if err != nil {
		return nil, err
	}
	inbox := j.Inbox
	if inbox == "" {
		inbox = DefaultInboxName
	}
	if inbox != ib.name {
		return nil, fmt.Errorf("%w: %s is a task of inbox %s", ErrNotFound, id, inbox)
	}
	return &Task{ID: id, b: ib.b}, nil
}

// Spec returns the spec the task was made from. It is the zero value when the task cannot be read.
func (ib *Inbox[T, R]) Spec(t *Task) T {
	var zero T
	j, err := ib.b.job(t.ID)
	if err != nil {
		return zero
	}
	spec, err := newSpec[T](specOf(j))
	if err != nil {
		return zero
	}
	return spec
}

// Wait blocks until the task ends. The error is nil on success.
func (ib *Inbox[T, R]) Wait(ctx context.Context, t *Task) (R, *TaskError) {
	var zero R
	j, err := ib.b.waitJob(ctx, t.ID)
	if err != nil {
		return zero, &TaskError{State: Failed, Message: err.Error(), Err: err}
	}
	return ib.resultOf(t, j)
}

// TryResult is Wait with no wait. ok is false while the task is Queued or Running.
func (ib *Inbox[T, R]) TryResult(t *Task) (R, *TaskError, bool) {
	var zero R
	j, err := ib.b.job(t.ID)
	if err != nil {
		return zero, &TaskError{State: Failed, Message: err.Error(), Err: err}, true
	}
	if !j.State.Terminal() {
		return zero, nil, false
	}
	r, te := ib.resultOf(t, j)
	return r, te, true
}

func (ib *Inbox[T, R]) resultOf(t *Task, j *Job) (R, *TaskError) {
	var r R
	tk := &Task{ID: t.ID, b: ib.b}
	if te := tk.errorOf(j); te != nil {
		return r, te
	}
	if raw := resultOf(j); len(raw) > 0 {
		if err := json.Unmarshal(raw, &r); err != nil {
			return r, &TaskError{State: Failed, Message: "the result does not decode: " + err.Error(), Err: err}
		}
	}
	return r, nil
}
