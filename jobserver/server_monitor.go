// The server's monitor inbox: Watch adds a task to it, and its run watches the condition until it holds.

package jobserver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wow-look-at-my/go-jobserver/priority"
)

// MonitorInboxName is the name of the built-in inbox that holds the monitor tasks. The server opens it when Config.Monitors is set.
const MonitorInboxName = "monitor"

// monitorTask is the spec of a task in the monitor inbox.
type monitorTask struct {
	TaskSpec
	Monitor MonitorSpec `json:"monitor"`
}

var monitorKeys = func() *priority.Keys {
	k, err := priority.NewKeys()
	if err != nil {
		panic(err)
	}
	return k
}()

// openMonitorInbox opens the monitor inbox when the server has monitor definitions.
func (s *Server) openMonitorInbox() error {
	if s.cfg.Monitors == nil {
		return nil
	}
	check := func(raw json.RawMessage) error {
		var m monitorTask
		return json.Unmarshal(raw, &m)
	}
	// An empty class takes the task's title, which offer reads under the store lock.
	hooks := inboxHooks{leaseFor: func(*Job) (LeaseRequest, error) {
		return LeaseRequest{Fallback: []string{"inbox:" + MonitorInboxName}, Monitor: true}, nil
	}}
	_, err := s.openInbox(MonitorInboxName, monitorKeys, 0, s.runMonitor, check, hooks)
	return err
}

// Watch adds a monitor task that watches spec until its condition holds. The task succeeds then, and fails at the
// spec's deadline. When an unfinished monitor task already watches the same condition, Watch returns that task and true.
// A dependent task waits on it through OnSuccess.
func (s *Server) Watch(spec MonitorSpec, title string) (*Task, bool, error) {
	c := s.inbox(MonitorInboxName)
	if s.cfg.Monitors == nil || c == nil {
		return nil, false, fmt.Errorf("%w %q: this server loads no monitor definitions", ErrUnknownMonitorKind, spec.Kind)
	}
	if err := s.cfg.Monitors.Definitions().Validate(spec); err != nil {
		return nil, false, err
	}
	if s.isStopped() {
		return nil, false, ErrStopped
	}
	key := spec.Key()
	if title == "" {
		title = key
	}
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for _, t := range s.store.liveIn(MonitorInboxName) {
		if t.key == key {
			return newTask(localNamed{Server: s, c: c}, t.id), true, nil
		}
	}
	raw, err := json.Marshal(monitorTask{TaskSpec: TaskSpec{Title: title}, Monitor: spec.clone()})
	if err != nil {
		return nil, false, err
	}
	j, err := s.store.create(&Spec{Name: title, Key: key}, &taskMeta{Inbox: MonitorInboxName, Spec: raw})
	if err != nil {
		return nil, false, err
	}
	s.Wake()
	return newTask(localNamed{Server: s, c: c}, j.ID), false, nil
}

// runMonitor is the run of the monitor inbox. Its result is the last status.
func (s *Server) runMonitor(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var m monitorTask
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	t := TaskFromContext(ctx)
	if t == nil {
		return nil, fmt.Errorf("jobserver: a monitor runs only in the monitor inbox")
	}
	w, err := s.store.LogWriter(t.ID)
	if err != nil {
		return nil, err
	}
	lease := LeaseFromContext(ctx)
	started := func(pid int) {
		if lease != nil {
			lease.AttachProcess(pid)
		}
	}
	var last MonitorStatus
	defer s.store.setMonitorStatus(t.ID, nil)
	err = s.cfg.Monitors.Watch(ctx, m.Monitor, w, started, func(st MonitorStatus) {
		last = st
		// Each report is a new value, so a reader that holds the last pointer keeps a status that does not change.
		s.store.setMonitorStatus(t.ID, &st)
		if st.LastError != "" {
			s.cfg.Logf("go-jobserver: ERROR: monitor %s (%s) check %d failed, retrying: %s", t.ID, m.Monitor.Key(), st.Checks, st.LastError)
		}
	})
	if serr := w.Sync(); serr != nil {
		s.cfg.Logf("go-jobserver: sync log %s: %v", t.ID, serr)
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(last)
}

// monitorOf decodes the monitor spec of a task in the monitor inbox. It is nil for any other task. The spec does not
// change, so the store decodes it once, when it makes the record.
func monitorOf(j *Job) *MonitorSpec {
	if j.Inbox != MonitorInboxName {
		return nil
	}
	var m monitorTask
	if err := json.Unmarshal(j.Spec, &m); err != nil {
		return nil
	}
	return &m.Monitor
}

// Monitors returns the record of every monitor task, oldest first. Read the status of each inside Read.
func (s *Server) Monitors() []*Job {
	var out []*Job
	for _, j := range s.store.InboxJobs(MonitorInboxName) {
		if j.Monitor != nil {
			out = append(out, j)
		}
	}
	return out
}

// MonitorKinds returns the definitions this server loads.
func (s *Server) MonitorKinds() []*MonitorKind {
	if s.cfg.Monitors == nil {
		return nil
	}
	return s.cfg.Monitors.Definitions().Kinds()
}
