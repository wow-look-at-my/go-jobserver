// TaskSpec is what to do. Adding it to an inbox makes a Task.

package jobserver

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// TaskSpec is what to do. Adding it to an inbox makes a Task. Embed it to carry more fields.
type TaskSpec struct {
	Title       string        `json:"title"`
	Description string        `json:"description,omitempty"`
	Timeout     time.Duration `json:"timeout,omitempty"` // zero: none
}

// iSpec is the constraint for a spec type. Only a type that embeds TaskSpec has the spec method.
type iSpec interface{ spec() *TaskSpec }

func (s *TaskSpec) spec() *TaskSpec { return s }

// TaskState is where a task stands.
type TaskState uint8

const (
	// Queued is a task that waits on its trigger, on admission or on a consumer.
	Queued TaskState = iota
	// Running is a task that runs, or that a consumer holds.
	Running
	// Succeeded is a task that ended with no error.
	Succeeded
	// Failed is a task that ended with an error.
	Failed
	// Cancelled is a task that was cancelled, or whose trigger can no longer fire.
	Cancelled
)

// String names the state.
func (s TaskState) String() string {
	switch s {
	case Queued:
		return "queued"
	case Running:
		return "running"
	case Succeeded:
		return "succeeded"
	case Failed:
		return "failed"
	case Cancelled:
		return "cancelled"
	}
	return "unknown"
}

// taskStateOf maps a job state to a task state.
func taskStateOf(st State) TaskState {
	switch st {
	case StateRunning:
		return Running
	case StateCompleted, StateCached:
		return Succeeded
	case StateFailed:
		return Failed
	case StateCancelled, StateBlocked:
		return Cancelled
	}
	return Queued
}

// TaskError says why a task did not succeed. Err is set when State is Failed.
type TaskError struct {
	State   TaskState
	Message string
	Err     error
}

// Error renders the state and the message.
func (e *TaskError) Error() string { return e.State.String() + ": " + e.Message }

// Unwrap returns the error the task failed with.
func (e *TaskError) Unwrap() error { return e.Err }

// Trigger kinds name what a triggered task waits for.
const (
	triggerSuccess = "success"
	triggerFail    = "fail"
	triggerEnded   = "ended"
)

// Consumer is the handle of the consumer bound to an inbox.
type Consumer string

var (
	// ErrNotConsumer is returned for a call through a consumer that is not the bound now.
	ErrNotConsumer = errors.New("jobserver: not the bound consumer")
	// ErrNotRunning is returned for a Done or a Started on a task that is no longer checked out.
	ErrNotRunning = errors.New("jobserver: task is not running")
	// ErrInboxExists is returned for a second inbox with a name that is in use.
	ErrInboxExists = errors.New("jobserver: inbox name is in use")
	// ErrStopped is returned for a task added after Stop.
	ErrStopped = errors.New("jobserver: server is stopped")
	// ErrBadInbox is returned for an inbox type that is not valid.
	ErrBadInbox = errors.New("jobserver: bad inbox type")
)

// Messages for the ways a task ends that are not its own result.
const (
	msgConsumerGone = "the consumer went away"
	msgTimedOut     = "timed out"
	msgCancelled    = "cancelled"
	msgDrained      = "drained"
	msgNeverFires   = "the trigger can no longer fire"
)

// taskBackend is what a Task reads and controls its job through: the local server or a client.
type taskBackend interface {
	job(id string) (*Job, error)
	waitJob(ctx context.Context, id string) (*Job, error)
	cancel(id string) error
	errOf(id string) error
}

// Task is one task in an inbox. It is not generic, so a task from any inbox can trigger a task in any other.
type Task struct {
	ID string
	b  taskBackend
}

// State returns the task's state now. A task the backend cannot read is Failed.
func (t *Task) State() TaskState {
	j, err := t.b.job(t.ID)
	if err != nil {
		return Failed
	}
	return taskStateOf(j.State)
}

// Cancel cancels the task, queued or running.
func (t *Task) Cancel() error { return t.b.cancel(t.ID) }

// Wait blocks until the task ends. The error is nil on success.
func (t *Task) Wait(ctx context.Context) *TaskError {
	j, err := t.b.waitJob(ctx, t.ID)
	if err != nil {
		return &TaskError{State: Failed, Message: err.Error(), Err: err}
	}
	return t.errorOf(j)
}

// errorOf is the TaskError of an ended job, or nil when it succeeded.
func (t *Task) errorOf(j *Job) *TaskError {
	st := taskStateOf(j.State)
	switch st {
	case Succeeded:
		return nil
	case Failed:
		err := t.b.errOf(t.ID)
		if err == nil {
			err = errors.New(j.Error)
		}
		return &TaskError{State: st, Message: j.Error, Err: err}
	}
	return &TaskError{State: st, Message: j.Error}
}

// CommandSpec is a task of the default inbox: a command the Runner runs.
type CommandSpec struct {
	TaskSpec
	ID      string   `json:"id,omitempty"`
	Command []string `json:"command"`
	Deps    []string `json:"deps,omitempty"`
	Inputs  []string `json:"inputs,omitempty"`
	Outputs []string `json:"outputs,omitempty"`
	Env     []string `json:"env,omitempty"`
	Dir     string   `json:"dir,omitempty"`
	Key     string   `json:"key,omitempty"`
	// Draft creates the task held until Activate.
	Draft bool `json:"draft,omitempty"`
	// Force skips the dedup lookup.
	Force  bool      `json:"force,omitempty"`
	Policy JobPolicy `json:"policy,omitzero"`
}

// CommandResult is what a command task left.
type CommandResult struct {
	ExitCode  int        `json:"exit_code"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
	LogBytes  int64      `json:"log_bytes"`
	// CacheOf names the task whose outputs this reused.
	CacheOf string `json:"cache_of,omitempty"`
}

// toSpec is the job spec of a command task.
func (c *CommandSpec) toSpec() Spec {
	return Spec{ID: c.ID, Name: c.Title, Command: c.Command, Deps: c.Deps, Inputs: c.Inputs, Outputs: c.Outputs,
		Env: c.Env, Dir: c.Dir, Key: c.Key, Draft: c.Draft, Force: c.Force, Policy: c.Policy}
}

// commandSpecOf rebuilds the command spec of a default task.
func commandSpecOf(j *Job) *CommandSpec {
	return &CommandSpec{TaskSpec: TaskSpec{Title: j.Name, Description: j.Description, Timeout: j.Timeout},
		ID: j.ID, Command: j.Command, Deps: j.Deps, Inputs: j.Inputs, Outputs: j.Outputs, Env: j.Env,
		Dir: j.Dir, Key: j.Key, Draft: j.State == StateDraft, Force: j.Force, Policy: j.Policy}
}

// resultOf is the result JSON of an ended job.
func resultOf(j *Job) json.RawMessage {
	if j.Inbox != "" {
		return j.Result
	}
	b, _ := json.Marshal(CommandResult{ExitCode: j.ExitCode, Artifacts: j.Artifacts, LogBytes: j.LogBytes, CacheOf: j.CacheOf})
	return b
}

// specOf is the spec JSON of a job.
func specOf(j *Job) json.RawMessage {
	if j.Inbox != "" {
		return j.Spec
	}
	b, _ := json.Marshal(commandSpecOf(j))
	return b
}
