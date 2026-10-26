// Task details: the title, the steps and the waits a task reports while it runs.

package jobserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// StepStatus is how a step ended. It is empty while the step is open.
type StepStatus string

const (
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
	// StepAbandoned is a step that was open when its task ended.
	StepAbandoned StepStatus = "abandoned"
)

// TaskStep is one named piece of work inside a task. Parent is the ID of the step it sits in, or zero.
type TaskStep struct {
	ID     int        `json:"id"`
	Parent int        `json:"parent,omitempty"`
	Title  string     `json:"title"`
	Start  time.Time  `json:"start"`
	End    time.Time  `json:"end,omitzero"`
	Status StepStatus `json:"status,omitempty"`
	Error  string     `json:"error,omitempty"`
}

// TaskWait is one span where a running task waited instead of worked.
type TaskWait struct {
	ID     int       `json:"id"`
	Reason string    `json:"reason"`
	Start  time.Time `json:"start"`
	End    time.Time `json:"end,omitzero"`
}

// The bounds of what one task reports.
const (
	maxTaskSteps    = 5000
	maxTaskWaits    = 1000
	maxTaskTitleLen = 200
)

var (
	// ErrNoStep is returned for a step or a wait the task does not hold open.
	ErrNoStep = errors.New("jobserver: no such open step or wait")
	// ErrTooMany is returned when a task holds the most steps or waits it may.
	ErrTooMany = errors.New("jobserver: the task holds the most steps or waits it may")
)

// detailRec is the journal form of a detail change. A step or a wait replaces the one with its ID.
type detailRec struct {
	Title *string    `json:"title,omitempty"`
	Steps []TaskStep `json:"steps,omitempty"`
	Waits []TaskWait `json:"waits,omitempty"`
}

// merge folds a detail change into a job.
func (d detailRec) merge(j *Job) {
	if d.Title != nil {
		j.Name = *d.Title
	}
	for _, st := range d.Steps {
		if st.ID >= 1 && st.ID <= len(j.Steps) {
			j.Steps[st.ID-1] = st
		} else if st.ID == len(j.Steps)+1 {
			j.Steps = append(j.Steps, st)
		}
	}
	for _, w := range d.Waits {
		if w.ID >= 1 && w.ID <= len(j.Waits) {
			j.Waits[w.ID-1] = w
		} else if w.ID == len(j.Waits)+1 {
			j.Waits = append(j.Waits, w)
		}
	}
}

// applyDetail folds a detail record into the table. A blob that does not decode is left out.
func (s *Store) applyDetail(r record) {
	j, ok := s.jobs[r.id]
	if !ok {
		return
	}
	var d detailRec
	if json.Unmarshal([]byte(r.blob), &d) == nil {
		d.merge(j)
	}
}

// closeOpen ends each open step and wait of a task that ended.
func closeOpen(j *Job, at time.Time) {
	for i := range j.Steps {
		if j.Steps[i].End.IsZero() {
			j.Steps[i].End, j.Steps[i].Status = at, StepAbandoned
		}
	}
	for i := range j.Waits {
		if j.Waits[i].End.IsZero() {
			j.Waits[i].End = at
		}
	}
}

// liveJob returns a job that has not ended. Caller holds s.mu.
func (s *Store) liveJob(id string) (*Job, error) {
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if j.State.Terminal() {
		return nil, fmt.Errorf("%w: %s is %s", ErrNotRunning, j.ID, j.State)
	}
	return j, nil
}

// noteDetail applies a detail change and appends it to the journal. Caller holds s.mu.
func (s *Store) noteDetail(j *Job, d detailRec) error {
	blob, err := json.Marshal(d)
	if err != nil {
		return err
	}
	d.merge(j)
	return s.note(record{kind: recDetail, id: j.ID, blob: string(blob)})
}

// clip cuts a title to its bound.
func clip(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxTaskTitleLen {
		return s[:maxTaskTitleLen]
	}
	return s
}

// SetTitle replaces the title of a task that has not ended.
func (s *Store) SetTitle(id, title string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return err
	}
	title = clip(title)
	return s.noteDetail(j, detailRec{Title: &title})
}

// OpenStep opens a step in a task and returns its ID. Parent is an open step of the task, or zero.
func (s *Store) OpenStep(id string, parent int, title string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return 0, err
	}
	if parent != 0 && (parent < 0 || parent > len(j.Steps) || !j.Steps[parent-1].End.IsZero()) {
		return 0, fmt.Errorf("%w: parent step %d", ErrNoStep, parent)
	}
	if len(j.Steps) >= maxTaskSteps {
		return 0, fmt.Errorf("%w: %s", ErrTooMany, j.ID)
	}
	st := TaskStep{ID: len(j.Steps) + 1, Parent: parent, Title: clip(title), Start: time.Now()}
	return st.ID, s.noteDetail(j, detailRec{Steps: []TaskStep{st}})
}

// EndStep ends an open step. A failed step needs a message that says what went wrong.
func (s *Store) EndStep(id string, step int, status StepStatus, msg string) error {
	switch status {
	case StepSucceeded, StepSkipped:
	case StepFailed:
		if strings.TrimSpace(msg) == "" {
			return fmt.Errorf("%w: a failed step needs a message", ErrBadState)
		}
	default:
		return fmt.Errorf("%w: step status %q", ErrBadState, status)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return err
	}
	if step < 1 || step > len(j.Steps) || !j.Steps[step-1].End.IsZero() {
		return fmt.Errorf("%w: step %d", ErrNoStep, step)
	}
	st := j.Steps[step-1]
	st.End, st.Status, st.Error = time.Now(), status, msg
	return s.noteDetail(j, detailRec{Steps: []TaskStep{st}})
}

// OpenWait opens a wait in a task and returns its ID.
func (s *Store) OpenWait(id, reason string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return 0, err
	}
	if len(j.Waits) >= maxTaskWaits {
		return 0, fmt.Errorf("%w: %s", ErrTooMany, j.ID)
	}
	w := TaskWait{ID: len(j.Waits) + 1, Reason: clip(reason), Start: time.Now()}
	return w.ID, s.noteDetail(j, detailRec{Waits: []TaskWait{w}})
}

// CloseWait ends an open wait.
func (s *Store) CloseWait(id string, wait int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return err
	}
	if wait < 1 || wait > len(j.Waits) || !j.Waits[wait-1].End.IsZero() {
		return fmt.Errorf("%w: wait %d", ErrNoStep, wait)
	}
	w := j.Waits[wait-1]
	w.End = time.Now()
	return s.noteDetail(j, detailRec{Waits: []TaskWait{w}})
}

// AppendLog writes one line to a task's log.
func (s *Store) AppendLog(id, line string) error {
	s.mu.Lock()
	j, err := s.liveJob(id)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	w, err := s.LogWriter(j.ID)
	if err != nil {
		return err
	}
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	_, err = w.Write([]byte(line))
	return err
}
