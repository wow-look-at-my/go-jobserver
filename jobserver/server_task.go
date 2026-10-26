// Host tasks: a job kind the embedding program runs itself, so its own work can sit in the job graph.

package jobserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// TaskSpec names a host task kind and carries its data. The host decodes Data itself.
type TaskSpec struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data,omitempty"`
}

// A TaskRunner runs one host task. It writes what it wants in the job's log to out. A nil error completes the job.
type TaskRunner func(ctx context.Context, j *Job, out io.Writer) error

// ErrUnknownTaskKind is returned for a task kind no runner is registered for.
var ErrUnknownTaskKind = errors.New("jobserver: no runner for this task kind")

func (t TaskSpec) clone() TaskSpec {
	out := t
	out.Data = append(json.RawMessage(nil), t.Data...)
	return out
}

// checkTask refuses a task job this server has no runner for.
func (s *Server) checkTask(spec *Spec) error {
	if spec.Task == nil {
		return nil
	}
	if _, ok := s.cfg.Tasks[spec.Task.Kind]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownTaskKind, spec.Task.Kind)
	}
	return nil
}

// runTask runs one host task through its registered runner.
func (s *Server) runTask(ctx context.Context, j *Job, out io.Writer) (int, error) {
	run, ok := s.cfg.Tasks[j.Task.Kind]
	if !ok {
		return -1, fmt.Errorf("%w: %q", ErrUnknownTaskKind, j.Task.Kind)
	}
	if err := run(ctx, j, out); err != nil {
		return -1, err
	}
	return 0, nil
}

// Detached returns a lease the scheduler never counts or governs.
func (s *Scheduler) Detached(id string) *Lease {
	l := newLease(s, LeaseRequest{ID: id}, 0, s.now().UTC(), nil)
	l.ended = true
	close(l.ready)
	return l
}
