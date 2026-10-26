// The daemon's side of monitor jobs: checking a spec at enqueue, running the watch, and keeping each monitor's last status.

package jobserver

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// checkMonitor refuses a monitor job this server cannot run, and gives an accepted one its dedup key. An
// unfinished monitor for the same condition answers the enqueue.
func (s *Server) checkMonitor(spec *Spec) error {
	if spec.Monitor == nil {
		return nil
	}
	if s.cfg.Monitors == nil {
		return fmt.Errorf("%w %q: this server loads no monitor definitions", ErrUnknownMonitorKind, spec.Monitor.Kind)
	}
	if err := s.cfg.Monitors.Defs.Validate(*spec.Monitor); err != nil {
		return err
	}
	spec.Key = spec.Monitor.Key()
	return nil
}

// run runs one job: a monitor watches its condition, and every other job goes to the runner.
func (s *Server) run(ctx context.Context, j *Job, out io.Writer, started func(pid int)) (int, error) {
	if j.Task != nil {
		return s.runTask(ctx, j, out)
	}
	if j.Monitor == nil {
		return s.runner.Run(ctx, j, out, started)
	}
	if s.cfg.Monitors == nil {
		return -1, fmt.Errorf("%w %q: this server loads no monitor definitions", ErrUnknownMonitorKind, j.Monitor.Kind)
	}
	defer func() {
		s.mu.Lock()
		delete(s.watching, j.ID)
		s.mu.Unlock()
	}()
	err := s.cfg.Monitors.Watch(ctx, *j.Monitor, out, started, func(st MonitorStatus) {
		s.mu.Lock()
		s.watching[j.ID] = st
		s.mu.Unlock()
		if st.LastError != "" {
			s.cfg.Logf("go-jobserver: ERROR: monitor %s (%s) check %d failed, retrying: %s", j.ID, j.Monitor.Key(), st.Checks, st.LastError)
		}
	})
	switch {
	case err == nil:
		return 0, nil
	case errors.Is(err, context.Canceled) && ctx.Err() != nil:
		return -1, ctx.Err()
	}
	return -1, err
}

// withStatus copies each running monitor's last status onto its job.
func (s *Server) withStatus(jobs []*Job) []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range jobs {
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
	return s.cfg.Monitors.Defs.Kinds()
}
