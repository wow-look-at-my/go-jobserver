// The inbox ops of the request API, and the client side of an Inbox over them.

package jobserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wow-look-at-my/go-jobserver/priority"
)

// The inbox ops. Each names its inbox, because a socket cannot carry a Go pointer.
const (
	OpInboxAdd     = "inbox.add"
	OpInboxBind    = "inbox.bind"
	OpInboxUnbind  = "inbox.unbind"
	OpInboxTake    = "inbox.take"
	OpInboxDone    = "inbox.done"
	OpInboxStarted = "inbox.started"
	OpInboxDrain   = "inbox.drain"
)

// The error codes of the inbox ops.
const (
	// CodeNotConsumer means the call came through a consumer that is not bound now.
	CodeNotConsumer ErrorCode = "not_consumer"
	// CodeNotRunning means the task is no longer checked out.
	CodeNotRunning ErrorCode = "not_running"
	// CodeStopped means the server refuses new tasks.
	CodeStopped ErrorCode = "stopped"
)

// TaskRequest is the part of a Request the inbox ops read.
type TaskRequest struct {
	Inbox string `json:"inbox,omitempty"`
	// Keys are the priority keys of an inbox the server does not have open yet. The server opens it with them.
	Keys     []PriorityKey   `json:"keys,omitempty"`
	Task     json.RawMessage `json:"task,omitempty"`
	Priority map[string]int  `json:"priority,omitempty"`
	Trigger  string          `json:"trigger,omitempty"`
	// On is the task a triggered task waits on.
	On       string `json:"on,omitempty"`
	Consumer string `json:"consumer,omitempty"`
	// WaitMS is how long a take waits, in milliseconds. A negative value waits with no limit.
	WaitMS    int64           `json:"wait_ms,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	TaskError string          `json:"task_error,omitempty"`
	PID       int             `json:"pid,omitempty"`
}

// TaskResponse is the part of a Response the inbox ops fill.
type TaskResponse struct {
	Consumer string          `json:"consumer,omitempty"`
	Taken    bool            `json:"taken,omitempty"`
	TaskSpec json.RawMessage `json:"task_spec,omitempty"`
}

// remoteInbox opens a consumer inbox for a remote caller, with the keys the caller declares.
func (s *Server) remoteInbox(name string, keys []PriorityKey) (*inboxCore, error) {
	if c := s.inbox(name); c != nil {
		return c, nil
	}
	list := make([]*priority.MultiPriorityKey, 0, len(keys))
	for _, k := range keys {
		def, ok := wholeNumber(k.Default)
		if !ok {
			return nil, fmt.Errorf("jobserver: priority key %q: %w", k.Name, ErrBadPriorityValue)
		}
		list = append(list, &priority.MultiPriorityKey{Name: k.Name, DefaultPriority: def})
	}
	pk, err := priority.NewKeys(list...)
	if err != nil {
		return nil, err
	}
	c, err := s.openInbox(name, pk, 0, nil, nil)
	if errors.Is(err, ErrInboxExists) {
		if c := s.inbox(name); c != nil {
			return c, nil
		}
	}
	return c, err
}

// backendFor is the backend an inbox op runs through.
func (s *Server) backendFor(req Request) (inboxBackend, error) {
	if req.Inbox == DefaultInboxName {
		return localDefault{s}, nil
	}
	c, err := s.remoteInbox(req.Inbox, req.Keys)
	if err != nil {
		return nil, err
	}
	return localNamed{Server: s, c: c}, nil
}

// handleInbox runs an inbox op. ok is false for an op that is not one.
func (s *Server) handleInbox(req Request) (Response, bool) {
	switch req.Op {
	case OpInboxAdd, OpInboxBind, OpInboxUnbind, OpInboxTake, OpInboxDone, OpInboxStarted, OpInboxDrain:
	default:
		return Response{}, false
	}
	b, err := s.backendFor(req)
	if err != nil {
		return Failure(err), true
	}
	who := Consumer(req.Consumer)
	switch req.Op {
	case OpInboxAdd:
		id, err := b.add(req.Task, req.Priority, req.Trigger, req.On)
		if err != nil {
			return Failure(err), true
		}
		j, err := s.Get(id)
		if err != nil {
			return Failure(err), true
		}
		return Response{OK: true, Job: j}, true
	case OpInboxBind:
		c, err := b.bind()
		if err != nil {
			return Failure(err), true
		}
		return Response{OK: true, TaskResponse: TaskResponse{Consumer: string(c)}}, true
	case OpInboxUnbind:
		err = b.unbind(who)
	case OpInboxDrain:
		err = b.drain()
	case OpInboxStarted:
		err = b.started(who, req.ID, req.PID)
	case OpInboxDone:
		var taskErr error
		if req.TaskError != "" {
			taskErr = errors.New(req.TaskError)
		}
		err = b.done(who, req.ID, req.Result, taskErr)
	case OpInboxTake:
		wait := min(time.Duration(req.WaitMS)*time.Millisecond, remoteTakeCap)
		if req.WaitMS < 0 {
			wait = remoteTakeCap
		}
		id, spec, ok, err := b.take(context.Background(), who, wait)
		if err != nil {
			return Failure(err), true
		}
		resp := Response{OK: true, TaskResponse: TaskResponse{Taken: ok, TaskSpec: spec}}
		if ok {
			resp.Job, _ = s.Get(id)
		}
		return resp, true
	}
	if err != nil {
		return Failure(err), true
	}
	return Response{OK: true}, true
}

// remoteErr turns a failed response back into an error that wraps the sentinel its code names.
func remoteErr(resp Response) error {
	switch resp.Code {
	case CodeNotConsumer:
		return fmt.Errorf("%w: %s", ErrNotConsumer, resp.Error)
	case CodeNotRunning:
		return fmt.Errorf("%w: %s", ErrNotRunning, resp.Error)
	case CodeStopped:
		return fmt.Errorf("%w: %s", ErrStopped, resp.Error)
	case CodeNotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, resp.Error)
	}
	return errors.New(resp.Error)
}

// clientInbox is the backend of an inbox on a remote server.
type clientInbox struct {
	c    *Client
	name string
	keys []PriorityKey
}

func (b clientInbox) call(req Request) (Response, error) {
	req.Inbox, req.Keys = b.name, b.keys
	resp, err := b.c.Do(context.Background(), req)
	if err != nil {
		return resp, err
	}
	if !resp.OK {
		return resp, remoteErr(resp)
	}
	return resp, nil
}

func (b clientInbox) add(spec json.RawMessage, values map[string]int, trigger, on string) (string, error) {
	resp, err := b.call(Request{Op: OpInboxAdd, TaskRequest: TaskRequest{Task: spec, Priority: values, Trigger: trigger, On: on}})
	if err != nil {
		return "", err
	}
	return resp.Job.ID, nil
}

func (b clientInbox) drain() error {
	_, err := b.call(Request{Op: OpInboxDrain})
	return err
}

func (b clientInbox) bind() (Consumer, error) {
	resp, err := b.call(Request{Op: OpInboxBind})
	return Consumer(resp.Consumer), err
}

func (b clientInbox) unbind(who Consumer) error {
	_, err := b.call(Request{Op: OpInboxUnbind, TaskRequest: TaskRequest{Consumer: string(who)}})
	return err
}

// take asks the server in rounds of at most remoteTakeCap, so no single call waits with no limit.
func (b clientInbox) take(ctx context.Context, who Consumer, wait time.Duration) (string, json.RawMessage, bool, error) {
	var end time.Time
	if wait >= 0 {
		end = time.Now().Add(wait)
	}
	for {
		round := remoteTakeCap
		if wait >= 0 {
			round = min(round, max(time.Until(end), 0))
		}
		req := Request{Op: OpInboxTake, TaskRequest: TaskRequest{Inbox: b.name, Keys: b.keys, Consumer: string(who), WaitMS: round.Milliseconds()}}
		resp, err := b.c.Do(ctx, req)
		if err != nil {
			return "", nil, false, err
		}
		if !resp.OK {
			return "", nil, false, remoteErr(resp)
		}
		if resp.Taken && resp.Job != nil {
			return resp.Job.ID, resp.TaskSpec, true, nil
		}
		if wait >= 0 && !time.Now().Before(end) {
			return "", nil, false, nil
		}
		if err := ctx.Err(); err != nil {
			return "", nil, false, err
		}
	}
}

// remoteTakeCap bounds how long one take over the wire waits.
const remoteTakeCap = 25 * time.Second

func (b clientInbox) started(who Consumer, id string, pid int) error {
	_, err := b.call(Request{Op: OpInboxStarted, ID: id, TaskRequest: TaskRequest{Consumer: string(who), PID: pid}})
	return err
}

func (b clientInbox) done(who Consumer, id string, result json.RawMessage, taskErr error) error {
	req := Request{Op: OpInboxDone, ID: id, TaskRequest: TaskRequest{Consumer: string(who), Result: result}}
	if taskErr != nil {
		req.TaskError = taskErr.Error()
	}
	_, err := b.call(req)
	return err
}

func (b clientInbox) job(id string) (*Job, error) {
	resp, err := b.c.Get(context.Background(), id)
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, remoteErr(resp)
	}
	return resp.Job, nil
}

func (b clientInbox) waitJob(ctx context.Context, id string) (*Job, error) {
	return b.c.Wait(ctx, id, 0)
}

func (b clientInbox) cancel(id string) error {
	resp, err := b.c.Do(context.Background(), Request{Op: OpInterrupt, ID: id})
	if err != nil {
		return err
	}
	if !resp.OK && resp.Code != CodeBadRequest {
		return remoteErr(resp)
	}
	return nil
}

func (b clientInbox) errOf(string) error { return nil }

// attach points the inbox at a remote server. ClientInbox passes the outer value as self.
func (ib *Inbox[T, R]) attach(c *Client, name string, self any) error {
	keys, err := priority.FromFields(self)
	if err != nil {
		return err
	}
	runner, _, err := inboxMethods[T, R](self)
	if err != nil {
		return err
	}
	if runner != nil {
		return fmt.Errorf("%w: inbox %s has Run, so it must open in the server process", ErrBadInbox, name)
	}
	described := make([]PriorityKey, 0, len(keys.List()))
	for _, k := range keys.List() {
		described = append(described, PriorityKey{Name: k.Name, Default: float64(k.DefaultPriority)})
	}
	ib.b, ib.name, ib.keys = clientInbox{c: c, name: name, keys: described}, name, keys
	return nil
}

// ClientInbox opens the inbox type I under name on the server a client talks to.
// The server opens the inbox on first use, with the keys I declares. I must have no Run method.
func ClientInbox[I any, PI interface {
	*I
	attach(*Client, string, any) error
}](c *Client, name string) (*I, error) {
	v := new(I)
	if err := PI(v).attach(c, name, v); err != nil {
		return nil, err
	}
	return v, nil
}

// DefaultInbox returns the server's default inbox over this client.
func (c *Client) DefaultInbox() *Inbox[*CommandSpec, CommandResult] {
	return &Inbox[*CommandSpec, CommandResult]{b: clientInbox{c: c, name: DefaultInboxName}, name: DefaultInboxName, keys: defaultKeys}
}
