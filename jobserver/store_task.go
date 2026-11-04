// taskMeta is the part of a task the job record has no field for. The journal keeps it as JSON.

package jobserver

import (
	"database/sql"
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

func (m taskMeta) applyTo(j *Job) {
	j.Inbox, j.Trigger, j.Description, j.Timeout = m.Inbox, m.Trigger, m.Description, m.Timeout
	j.Priority, j.Spec = m.Priority, m.Spec
}

// liveRecord returns the record of a task, or nil. Read its fields only inside Read, because the store changes it under s.mu.
func (s *Store) liveRecord(id string) *Job {
	j, _ := s.Job(id)
	return j
}

// readRecord runs fn on a record under the read lock. A pruned record still reads.
func (s *Store) readRecord(rec *Job, fn func(j *Job)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(rec)
}

// setErr keeps the error a task failed with on its record.
func (s *Store) setErr(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j := s.held(CanonicalID(id)); j != nil {
		j.err = err
	}
}

// setMonitorStatus records what a running monitor's checks found. Nil clears it. The database does not keep it.
func (s *Store) setMonitorStatus(id string, st *MonitorStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j := s.held(CanonicalID(id)); j != nil {
		j.MonitorStatus = st
	}
}

// bump wakes everything that waits on a change. Caller holds s.mu.
func (s *Store) bump() {
	close(s.watch)
	s.watch = make(chan struct{})
}

// Changed returns a channel that closes at the next change.
func (s *Store) Changed() <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.watch
}

// SetResult records a task's result.
func (s *Store) SetResult(id string, result json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	if len(result) > 0 && !json.Valid(result) {
		return fmt.Errorf("jobserver: task %s: the result is not valid JSON", j.ID)
	}
	j.Result = append(json.RawMessage(nil), result...)
	if err := s.inTx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO task_results (id, result) VALUES (?, ?)`, j.ID, []byte(j.Result))
		return err
	}); err != nil {
		return err
	}
	return s.save(j, nil)
}

// startTaken starts a task that a consumer took. Only memory holds the handle, because a restart ends every consumer.
// The journal holds the consumer's name.
func (s *Store) startTaken(id, consumer, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	if j.ConsumerName != name {
		if err := s.noteDetail(j, detailRec{ConsumerName: &name}); err != nil {
			return err
		}
	}
	j.Consumer = consumer
	return s.startLocked(j)
}

// requeue puts a running or blocked task back to active. It ends the open steps and waits of the run that stopped.
func (s *Store) requeue(id, msg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j := s.held(CanonicalID(id)); j != nil && (j.openWaits > 0 || hasOpenStep(j)) {
		if err := s.noteDetail(j, openEnded(j, time.Now())); err != nil {
			return err
		}
	}
	return s.setState(id, StateActive, msg)
}
