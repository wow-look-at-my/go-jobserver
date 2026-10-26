// Task details: the title, the steps and the waits a task reports while it runs.

package jobserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/wow-look-at-my/go-containers/set"
	"maps"
	"slices"
	"strings"
	"time"
)

// StepStatus is how a step ended. It is empty while the step is open.
type StepStatus string

const (
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
	// StepAbandoned is a step that was open when its task or its parent step ended, or that a consumer gave up.
	StepAbandoned StepStatus = "abandoned"
)

// msgParentEnded is the message of a sub-step that was open when its parent step ended.
const msgParentEnded = "its parent step ended first"

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

// The bounds of what one task holds. Past a bound, the oldest closed step or wait goes, and the task counts it.
const (
	maxTaskSteps    = 5000
	maxTaskWaits    = 1000
	maxTaskTitleLen = 200
	maxOutcomeLen   = 64
)

var (
	// ErrNoStep is returned for a step or a wait the task does not hold open.
	ErrNoStep = errors.New("jobserver: no such open step or wait")
	// ErrAllOpen is returned when a task is at its bound and every step or wait it holds is still open.
	ErrAllOpen = errors.New("jobserver: every step or wait the task holds is open")
)

// detailRec is the journal form of a detail change. A step or a wait replaces the one with its ID, or adds it.
type detailRec struct {
	Title   *string `json:"title,omitempty"`
	Outcome *string `json:"outcome,omitempty"`
	// Priority replaces the task's priority values. An empty map sets every key to its default.
	Priority *map[string]int `json:"priority,omitempty"`
	// ConsumerName is the Bind name of the consumer that took the task.
	ConsumerName *string    `json:"consumer_name,omitempty"`
	Steps        []TaskStep `json:"steps,omitempty"`
	Waits        []TaskWait `json:"waits,omitempty"`
	// DropSteps and DropWaits are the IDs a bound removed. The task counts each.
	DropSteps []int `json:"drop_steps,omitempty"`
	DropWaits []int `json:"drop_waits,omitempty"`
	// StepsDropped and WaitsDropped set the counts outright. A compaction writes them.
	StepsDropped *int `json:"steps_dropped,omitempty"`
	WaitsDropped *int `json:"waits_dropped,omitempty"`
}

func stepAt(j *Job, id int) int {
	for i := len(j.Steps) - 1; i >= 0; i-- {
		if j.Steps[i].ID == id {
			return i
		}
	}
	return -1
}

func waitAt(j *Job, id int) int {
	for i := len(j.Waits) - 1; i >= 0; i-- {
		if j.Waits[i].ID == id {
			return i
		}
	}
	return -1
}

// merge folds a detail change into a job.
func (d detailRec) merge(j *Job) {
	if d.Title != nil {
		j.Name = *d.Title
	}
	if d.Outcome != nil {
		j.Outcome = *d.Outcome
	}
	if d.ConsumerName != nil {
		j.ConsumerName = *d.ConsumerName
	}
	if d.Priority != nil {
		j.Priority = maps.Clone(*d.Priority)
	}
	if d.StepsDropped != nil {
		j.StepsDropped = *d.StepsDropped
	}
	if d.WaitsDropped != nil {
		j.WaitsDropped = *d.WaitsDropped
	}
	for _, id := range d.DropSteps {
		if i := stepAt(j, id); i >= 0 {
			j.Steps = slices.Delete(j.Steps, i, i+1)
			j.StepsDropped++
		}
	}
	for _, id := range d.DropWaits {
		if i := waitAt(j, id); i >= 0 {
			if j.Waits[i].End.IsZero() {
				j.openWaits--
			}
			j.Waits = slices.Delete(j.Waits, i, i+1)
			j.WaitsDropped++
		}
	}
	for _, st := range d.Steps {
		if i := stepAt(j, st.ID); i >= 0 {
			j.Steps[i] = st
		} else {
			j.Steps = append(j.Steps, st)
		}
	}
	for _, w := range d.Waits {
		if w.End.IsZero() {
			j.openWaits++
		}
		if i := waitAt(j, w.ID); i >= 0 {
			if j.Waits[i].End.IsZero() {
				j.openWaits--
			}
			j.Waits[i] = w
		} else {
			j.Waits = append(j.Waits, w)
		}
	}
}

// oldestClosed is the ID of the oldest closed entry, or zero when every entry is open.
func oldestClosed[E any](list []E, id func(E) int, open func(E) bool) int {
	for _, e := range list {
		if !open(e) {
			return id(e)
		}
	}
	return 0
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
	j.openWaits = 0
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
	if parent != 0 {
		if i := stepAt(j, parent); i < 0 || !j.Steps[i].End.IsZero() {
			return 0, fmt.Errorf("%w: parent step %d", ErrNoStep, parent)
		}
	}
	// Each ID the task gave out is a held step or a dropped one, so this ID is new.
	d := detailRec{Steps: []TaskStep{{ID: len(j.Steps) + j.StepsDropped + 1, Parent: parent, Title: clip(title), Start: time.Now()}}}
	if len(j.Steps) >= s.stepCap {
		old := oldestClosed(j.Steps, func(st TaskStep) int { return st.ID }, func(st TaskStep) bool { return st.End.IsZero() })
		if old == 0 {
			return 0, fmt.Errorf("%w: %s", ErrAllOpen, j.ID)
		}
		d.DropSteps = []int{old}
	}
	return d.Steps[0].ID, s.noteDetail(j, d)
}

// RenameStep replaces the title of a step the task holds, open or closed.
func (s *Store) RenameStep(id string, step int, title string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return err
	}
	i := stepAt(j, step)
	if i < 0 {
		return fmt.Errorf("%w: step %d", ErrNoStep, step)
	}
	st := j.Steps[i]
	st.Title = clip(title)
	return s.noteDetail(j, detailRec{Steps: []TaskStep{st}})
}

// hasOpenStep reports whether a step of the task is open.
func hasOpenStep(j *Job) bool {
	return slices.ContainsFunc(j.Steps, func(st TaskStep) bool { return st.End.IsZero() })
}

// EndStep ends an open step. A failed step needs a message that says what went wrong.
// Each sub-step still open under it ends as abandoned, with the message msgParentEnded.
func (s *Store) EndStep(id string, step int, status StepStatus, msg string) error {
	switch status {
	case StepSucceeded, StepSkipped, StepAbandoned:
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
	i := stepAt(j, step)
	if i < 0 || !j.Steps[i].End.IsZero() {
		return fmt.Errorf("%w: step %d", ErrNoStep, step)
	}
	now := time.Now()
	st := j.Steps[i]
	st.End, st.Status, st.Error = now, status, msg
	changed := []TaskStep{st}
	// A sub-step has a higher ID than its parent, so one pass in ID order finds each descendant.
	under := set.Of[int](step)
	for _, sub := range j.Steps[i+1:] {
		if !under.Contains(sub.Parent) {
			continue
		}
		under.Add(sub.ID)
		if sub.End.IsZero() {
			sub.End, sub.Status, sub.Error = now, StepAbandoned, msgParentEnded
			changed = append(changed, sub)
		}
	}
	return s.noteDetail(j, detailRec{Steps: changed})
}

// SetOutcome sets the short outcome label of a task that has not ended.
func (s *Store) SetOutcome(id, outcome string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return err
	}
	outcome = strings.TrimSpace(outcome)
	if len(outcome) > maxOutcomeLen {
		return fmt.Errorf("%w: an outcome is at most %d bytes", ErrBadState, maxOutcomeLen)
	}
	return s.noteDetail(j, detailRec{Outcome: &outcome})
}

// OpenWait opens a wait in a task and returns its ID.
func (s *Store) OpenWait(id, reason string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return 0, err
	}
	d := detailRec{Waits: []TaskWait{{ID: len(j.Waits) + j.WaitsDropped + 1, Reason: clip(reason), Start: time.Now()}}}
	if len(j.Waits) >= s.waitCap {
		old := oldestClosed(j.Waits, func(w TaskWait) int { return w.ID }, func(w TaskWait) bool { return w.End.IsZero() })
		if old == 0 {
			return 0, fmt.Errorf("%w: %s", ErrAllOpen, j.ID)
		}
		d.DropWaits = []int{old}
	}
	return d.Waits[0].ID, s.noteDetail(j, d)
}

// CloseWait ends an open wait.
func (s *Store) CloseWait(id string, wait int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return err
	}
	i := waitAt(j, wait)
	if i < 0 || !j.Waits[i].End.IsZero() {
		return fmt.Errorf("%w: wait %d", ErrNoStep, wait)
	}
	w := j.Waits[i]
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
