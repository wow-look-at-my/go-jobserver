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
	hooks := inboxHooks{leaseFor: func(j *Job) (LeaseRequest, error) {
		return LeaseRequest{Class: j.Name, Fallback: []string{"inbox:" + MonitorInboxName}, Monitor: true}, nil
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
	for _, j := range s.store.liveIn(MonitorInboxName) {
		if j.Key == key {
			return &Task{ID: j.ID, b: localNamed{Server: s, c: c}}, true, nil
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
	return &Task{ID: j.ID, b: localNamed{Server: s, c: c}}, false, nil
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
	lease := leaseFrom(ctx)
	started := func(pid int) {
		if lease != nil {
			lease.AttachProcess(pid)
		}
	}
	var last MonitorStatus
	defer func() {
		s.mu.Lock()
		delete(s.watching, t.ID)
		s.mu.Unlock()
	}()
	err = s.cfg.Monitors.Watch(ctx, m.Monitor, w, started, func(st MonitorStatus) {
		last = st
		s.mu.Lock()
		s.watching[t.ID] = st
		s.mu.Unlock()
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

// monitorOf decodes the monitor spec of a task in the monitor inbox. It is nil for any other task.
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

// withStatus fills the monitor spec of each monitor task, and the last status of each running one.
func (s *Server) withStatus(jobs []*Job) []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range jobs {
		j.Monitor = monitorOf(j)
		if st, ok := s.watching[j.ID]; ok {
			c := st
			j.MonitorStatus = &c
		}
	}
	return jobs
}

// Monitors returns every monitor job, oldest first, each with its last status.
func (s *Server) Monitors() []*Job {
	var out []*Job
	for _, j := range s.List() {
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
