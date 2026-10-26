// Task details: what a consumer reports about a checked-out task, and how a reader gets it back.

package jobserver

import (
	"context"
	"fmt"
	"time"
)

// TaskDetails is what one task did: its title, its timings, its steps, its waits and the size of its log.
type TaskDetails struct {
	ID          string    `json:"id"`
	Inbox       string    `json:"inbox"`
	Seq         uint64    `json:"seq"`
	Title       string    `json:"title"`
	Description string    `json:"description,omitempty"`
	State       TaskState `json:"state"`
	Error       string    `json:"error,omitempty"`
	Consumer    string    `json:"consumer,omitempty"`
	// ConsumerName is the Bind name of the consumer that took the task last. It stays after that consumer is gone.
	ConsumerName string     `json:"consumer_name,omitempty"`
	Created      time.Time  `json:"created"`
	Started      time.Time  `json:"started,omitzero"`
	Finished     time.Time  `json:"finished,omitzero"`
	Attempts     int        `json:"attempts"`
	Steps        []TaskStep `json:"steps,omitempty"`
	Waits        []TaskWait `json:"waits,omitempty"`
	// StepsDropped and WaitsDropped count the closed steps and waits the bound removed.
	StepsDropped int `json:"steps_dropped,omitempty"`
	WaitsDropped int `json:"waits_dropped,omitempty"`
	// Queue is what a queued task waits on in the scheduler. It is nil when the task asks for no slot.
	Queue    *QueueState `json:"queue,omitempty"`
	LogBytes int64       `json:"log_bytes"`
	// Outcome is the short label the consumer set with SetOutcome. A reader tells the kind of result from it.
	Outcome string `json:"outcome,omitempty"`
}

// TaskPage is the tasks of one inbox that changed after a seq. A new Epoch or a new Pruned means the reader reads again from zero.
type TaskPage struct {
	Epoch string `json:"epoch"`
	Seq   uint64 `json:"seq"`
	// Pruned counts the prunes that dropped a task since the store opened.
	Pruned uint64         `json:"pruned"`
	Tasks  []*TaskDetails `json:"tasks"`
}

// MarshalText names the state.
func (s TaskState) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// UnmarshalText reads a state name.
func (s *TaskState) UnmarshalText(b []byte) error {
	for st := Queued; st <= Cancelled; st++ {
		if st.String() == string(b) {
			*s = st
			return nil
		}
	}
	return fmt.Errorf("jobserver: unknown task state %q", b)
}

// The kinds of a DetailOp.
const (
	DetailTitle   = "title"
	DetailStep    = "step"
	DetailEndStep = "end_step"
	DetailRename  = "rename_step"
	DetailWait    = "wait"
	DetailEndWait = "end_wait"
	DetailLog     = "log"
	DetailOutcome = "outcome"
)

// DetailOp is one change to the details of a checked-out task, as the wire carries it.
type DetailOp struct {
	Kind string `json:"kind"`
	// Text is the title, the step title, the wait reason, the log line or the message of a failed step.
	Text   string     `json:"text,omitempty"`
	Parent int        `json:"parent,omitempty"`
	Ref    int        `json:"ref,omitempty"`
	Status StepStatus `json:"status,omitempty"`
}

// applyDetail runs one detail change on a task. It returns the ID of a new step or wait.
func (s *Server) applyDetail(id string, op DetailOp) (int, error) {
	switch op.Kind {
	case DetailTitle:
		return 0, s.store.SetTitle(id, op.Text)
	case DetailStep:
		return s.store.OpenStep(id, op.Parent, op.Text)
	case DetailEndStep:
		return 0, s.store.EndStep(id, op.Ref, op.Status, op.Text)
	case DetailRename:
		return 0, s.store.RenameStep(id, op.Ref, op.Text)
	case DetailWait:
		return s.store.OpenWait(id, op.Text)
	case DetailEndWait:
		return 0, s.store.CloseWait(id, op.Ref)
	case DetailOutcome:
		return 0, s.store.SetOutcome(id, op.Text)
	case DetailLog:
		return 0, s.store.AppendLog(id, op.Text)
	}
	return 0, fmt.Errorf("%w: detail kind %q", ErrBadState, op.Kind)
}

// holds checks that the caller may report on a task. A consumer must hold it. In an inbox with Run, the empty consumer
// stands for the Run call.
func (c *inboxCore) holds(who Consumer, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.run != nil {
		if who != "" {
			return ErrNotConsumer
		}
		if c.out[id] == nil {
			return fmt.Errorf("%w: %s", ErrNotRunning, id)
		}
		return nil
	}
	_, err := c.checkedOut(who, id)
	return err
}

// queueStates maps each waiting lease to where it stands.
func (s *Server) queueStates() map[string]*QueueState {
	out := map[string]*QueueState{}
	for _, l := range s.sched.Status().Waiting {
		if l.Wait != nil {
			out[l.ID] = l.Wait
		}
	}
	return out
}

func (s *Server) detailsOf(j *Job, queue map[string]*QueueState) *TaskDetails {
	inbox := j.Inbox
	if inbox == "" {
		inbox = DefaultInboxName
	}
	return &TaskDetails{ID: j.ID, Inbox: inbox, Seq: j.Seq, Title: j.Name, Description: j.Description,
		State: taskStateOf(j.State), Error: j.Error, Consumer: j.Consumer, ConsumerName: j.ConsumerName, Created: j.Created, Started: j.Started,
		Finished: j.Finished, Attempts: j.Attempts, Steps: j.Steps, Waits: j.Waits, Queue: queue[j.ID],
		StepsDropped: j.StepsDropped, WaitsDropped: j.WaitsDropped,
		LogBytes: s.store.LogBytes(j.ID), Outcome: j.Outcome}
}

// taskDetails reads one task of an inbox.
func (s *Server) taskDetails(inbox, id string) (*TaskDetails, error) {
	j, err := mustJob(s.store, id)
	if err != nil {
		return nil, err
	}
	if inboxKey(inbox) != j.Inbox {
		return nil, fmt.Errorf("%w: %s is not a task of inbox %s", ErrNotFound, id, inbox)
	}
	var queue map[string]*QueueState
	if j.State == StateActive {
		queue = s.queueStates()
	}
	return s.detailsOf(j, queue), nil
}

// taskPage reads the tasks of an inbox that changed after since, oldest first.
func (s *Server) taskPage(inbox string, since uint64) TaskPage {
	jobs, page := s.store.inboxSince(inbox, since)
	queue := s.queueStates()
	page.Tasks = make([]*TaskDetails, 0, len(jobs))
	for _, j := range jobs {
		page.Tasks = append(page.Tasks, s.detailsOf(j, queue))
	}
	return page
}

// taskCtxKey carries the task a Run call runs.
type taskCtxKey struct{}

// TaskFromContext returns the task a Run method runs, from the context it
// got. It is nil for another context.
func TaskFromContext(ctx context.Context) *Task {
	t, _ := ctx.Value(taskCtxKey{}).(*Task)
	return t
}

// SetTitle replaces the title of a checked-out task.
func (ib *Inbox[T, R]) SetTitle(c Consumer, t *Task, title string) error {
	_, err := ib.b.detail(c, t.ID, DetailOp{Kind: DetailTitle, Text: title})
	return err
}

// Step opens a named step in a checked-out task and returns its ID. Parent is an open step to nest it in, or zero.
func (ib *Inbox[T, R]) Step(c Consumer, t *Task, parent int, title string) (int, error) {
	return ib.b.detail(c, t.ID, DetailOp{Kind: DetailStep, Parent: parent, Text: title})
}

// EndStep ends an open step as succeeded, failed, skipped or abandoned. A
// failed step needs a message that says what went wrong.
func (ib *Inbox[T, R]) EndStep(c Consumer, t *Task, step int, status StepStatus, msg string) error {
	_, err := ib.b.detail(c, t.ID, DetailOp{Kind: DetailEndStep, Ref: step, Status: status, Text: msg})
	return err
}

// RenameStep replaces the title of a step the task holds, open or closed.
func (ib *Inbox[T, R]) RenameStep(c Consumer, t *Task, step int, title string) error {
	_, err := ib.b.detail(c, t.ID, DetailOp{Kind: DetailRename, Ref: step, Text: title})
	return err
}

// OpenWait opens a span where a checked-out task waits, and returns its ID.
func (ib *Inbox[T, R]) OpenWait(c Consumer, t *Task, reason string) (int, error) {
	return ib.b.detail(c, t.ID, DetailOp{Kind: DetailWait, Text: reason})
}

// CloseWait ends an open wait.
func (ib *Inbox[T, R]) CloseWait(c Consumer, t *Task, wait int) error {
	_, err := ib.b.detail(c, t.ID, DetailOp{Kind: DetailEndWait, Ref: wait})
	return err
}

// SetOutcome sets a short label for the result of a checked-out task, such as "skipped". Tasks and Details carry it.
func (ib *Inbox[T, R]) SetOutcome(c Consumer, t *Task, outcome string) error {
	_, err := ib.b.detail(c, t.ID, DetailOp{Kind: DetailOutcome, Text: outcome})
	return err
}

// Log writes one line to the log of a checked-out task.
func (ib *Inbox[T, R]) Log(c Consumer, t *Task, line string) error {
	_, err := ib.b.detail(c, t.ID, DetailOp{Kind: DetailLog, Text: line})
	return err
}

// Details reads one task of the inbox.
func (ib *Inbox[T, R]) Details(t *Task) (*TaskDetails, error) { return ib.b.details(t.ID) }

// Tasks reads the tasks of the inbox that changed after since. Since zero reads every task the server holds.
func (ib *Inbox[T, R]) Tasks(since uint64) (TaskPage, error) { return ib.b.tasks(since) }

// Counts reports how many tasks of the inbox wait, run and ended. It reads only this inbox's tasks.
func (ib *Inbox[T, R]) Counts() (TaskCounts, error) { return ib.b.counts() }

// ReadLog returns up to max bytes of a task's log, from offset.
func (ib *Inbox[T, R]) ReadLog(t *Task, offset int64, max int) ([]byte, error) {
	return ib.b.readLog(t.ID, offset, max)
}

func (b localNamed) detail(who Consumer, id string, op DetailOp) (int, error) {
	if err := b.c.holds(who, id); err != nil {
		return 0, err
	}
	return b.applyDetail(id, op)
}
func (b localNamed) details(id string) (*TaskDetails, error) { return b.taskDetails(b.c.name, id) }
func (b localNamed) tasks(since uint64) (TaskPage, error)    { return b.taskPage(b.c.name, since), nil }
func (b localNamed) counts() (TaskCounts, error)             { return b.store.Counts(b.c.name), nil }
func (b localNamed) readLog(id string, offset int64, max int) ([]byte, error) {
	return b.Logs(id, offset, max)
}

func (b localDefault) detail(Consumer, string, DetailOp) (int, error) { return 0, ErrNotConsumer }
func (b localDefault) details(id string) (*TaskDetails, error) {
	return b.taskDetails(DefaultInboxName, id)
}
func (b localDefault) tasks(since uint64) (TaskPage, error) {
	return b.taskPage(DefaultInboxName, since), nil
}
func (b localDefault) counts() (TaskCounts, error) { return b.store.Counts(DefaultInboxName), nil }
func (b localDefault) readLog(id string, offset int64, max int) ([]byte, error) {
	return b.Logs(id, offset, max)
}
