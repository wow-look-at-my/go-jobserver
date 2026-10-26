// taskMeta is the part of a task the job record has no field for. The journal keeps it as JSON.

package jobserver

import (
	"encoding/json"
	"fmt"
	"time"
)

// taskMeta is the part of a task the job record has no field for. The journal keeps it as JSON.
type taskMeta struct {
	Inbox       string          `json:"inbox,omitempty"`
	Trigger     string          `json:"trigger,omitempty"`
	Description string          `json:"description,omitempty"`
	Timeout     time.Duration   `json:"timeout,omitempty"`
	Priority    map[string]int  `json:"priority,omitempty"`
	Spec        json.RawMessage `json:"spec,omitempty"`
}

// commitTask records the task fields of a new job. Caller holds s.mu.
func (s *Store) commitTask(j *Job, m taskMeta) error {
	blob, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("jobserver: task %s: %w", j.ID, err)
	}
	if err := s.commit(record{kind: recTask, id: j.ID, blob: string(blob)}); err != nil {
		return err
	}
	m.applyTo(j)
	return nil
}

func (m taskMeta) applyTo(j *Job) {
	j.Inbox, j.Trigger, j.Description, j.Timeout = m.Inbox, m.Trigger, m.Description, m.Timeout
	j.Priority, j.Spec = m.Priority, m.Spec
}

// applyTask folds a task or result record into the table. A blob that does not decode is left out.
func (s *Store) applyTask(r record) {
	j, ok := s.jobs[r.id]
	if !ok {
		return
	}
	if r.kind == recResult {
		j.Result = json.RawMessage(r.blob)
		return
	}
	var m taskMeta
	if json.Unmarshal([]byte(r.blob), &m) == nil {
		m.applyTo(j)
	}
}

// bump wakes everything that waits on a change. Caller holds s.mu.
func (s *Store) bump() {
	close(s.watch)
	s.watch = make(chan struct{})
}

// Changed returns a channel that closes at the next commit.
func (s *Store) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watch
}

// SetResult records a task's result.
func (s *Store) SetResult(id string, result json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[CanonicalID(id)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	j.Result = append(json.RawMessage(nil), result...)
	return s.commit(record{kind: recResult, id: j.ID, blob: string(result)})
}

// setConsumer names the consumer that holds a task. Only memory holds it: a restart ends every consumer.
func (s *Store) setConsumer(id, consumer string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[CanonicalID(id)]; ok {
		j.Consumer = consumer
	}
}

// requeue puts a running or blocked task back to active.
func (s *Store) requeue(id, msg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setState(id, StateActive, msg)
}
