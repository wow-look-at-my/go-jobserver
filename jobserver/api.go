// The wire API is one request shape and one response shape, carried by every transport.

package jobserver

import (
	"errors"
	"fmt"
)

// The wire API is one request shape and one response shape, carried by every
// transport. The go-ipc service, the file socket, the HTTP server and the
// spool watcher all end up in Server.Handle.
const (
	OpEnqueue      = "enqueue"
	OpActivate     = "activate"
	OpDepend       = "depend"
	OpSetDeps      = "deps"
	OpList         = "list"
	OpGet          = "get"
	OpLogs         = "logs"
	OpPause        = "pause"
	OpResume       = "resume"
	OpInterrupt    = "interrupt"
	OpInterruptAll = "interrupt-all"
	OpStats        = "stats"
	OpPolicy       = "policy"
	OpMonitors     = "monitors"
	OpKinds        = "kinds"
	OpWhy          = "why"
	OpGraph        = "graph"
	OpScheduler    = "scheduler"
)

// Request is one API call.
type Request struct {
	Op     string   `json:"op"`
	Spec   *Spec    `json:"spec,omitempty"`
	ID     string   `json:"id,omitempty"`
	Dep    string   `json:"dep,omitempty"`
	Deps   []string `json:"deps,omitempty"`
	Offset int64    `json:"offset,omitempty"`
	Max    int      `json:"max,omitempty"`
	// Policy carries a job's CPU cost and process-control settings.
	Policy *JobPolicy `json:"policy,omitempty"`
}

// ErrorCode classifies a failed call, so a transport can map it to a status without matching on the message text.
type ErrorCode string

const (
	// CodeNotFound means no job has that ID.
	CodeNotFound ErrorCode = "not_found"
	// CodeBadRequest means the request or the job it names is wrong.
	CodeBadRequest ErrorCode = "bad_request"
	// CodeInternal means the server failed on its own.
	CodeInternal ErrorCode = "internal"
)

// Response is the answer to one API call.
type Response struct {
	OK       bool      `json:"ok"`
	Error    string    `json:"error,omitempty"`
	Code     ErrorCode `json:"code,omitempty"`
	Job      *Job      `json:"job,omitempty"`
	Jobs     []*Job    `json:"jobs,omitempty"`
	Stats    *Stats    `json:"stats,omitempty"`
	Output   string    `json:"output,omitempty"`
	Existing bool      `json:"existing,omitempty"`
	Count    int       `json:"count,omitempty"`
	// Summary is the line that says what runs, what watches and what the waiting jobs wait on.
	Summary   string           `json:"summary,omitempty"`
	Waits     []Wait           `json:"waits,omitempty"`
	JobWaits  map[string][]Wait `json:"job_waits,omitempty"`
	Monitors  []MonitorView    `json:"monitors,omitempty"`
	Kinds     []*MonitorKind   `json:"kinds,omitempty"`
	Graph     *JobGraph        `json:"graph,omitempty"`
	Scheduler *SchedulerStatus `json:"scheduler,omitempty"`
}

// Failure returns a response carrying an error.
func Failure(err error) Response {
	return Response{OK: false, Error: err.Error(), Code: codeFor(err)}
}

// codeFor classifies an error by the sentinel it wraps.
func codeFor(err error) ErrorCode {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrNotFound):
		return CodeNotFound
	case errors.Is(err, ErrDuplicate), errors.Is(err, ErrCycle), errors.Is(err, ErrMissingDep),
		errors.Is(err, ErrBadState), errors.Is(err, ErrNoCommand),
		errors.Is(err, ErrUnknownMonitorKind), errors.Is(err, ErrBadMonitorArgs):
		return CodeBadRequest
	default:
		return CodeInternal
	}
}

// Handle runs one API call against the server.
func (s *Server) Handle(req Request) Response {
	if resp, ok := s.handleView(req); ok {
		return resp
	}
	switch req.Op {
	case OpEnqueue:
		if req.Spec == nil {
			return Failure(fmt.Errorf("jobserver: enqueue needs a spec"))
		}
		j, existing, err := s.Enqueue(*req.Spec)
		if err != nil {
			return Failure(err)
		}
		return Response{OK: true, Job: j, Existing: existing}

	case OpActivate:
		j, err := s.Activate(req.ID)
		if err != nil {
			return Failure(err)
		}
		return Response{OK: true, Job: j}

	case OpDepend:
		j, err := s.AddDep(req.ID, req.Dep)
		if err != nil {
			return Failure(err)
		}
		return Response{OK: true, Job: j}

	case OpSetDeps:
		j, err := s.SetDeps(req.ID, req.Deps)
		if err != nil {
			return Failure(err)
		}
		return Response{OK: true, Job: j}

	case OpList:
		jobs := s.List()
		waits := s.Explain()
		return Response{OK: true, Jobs: jobs, JobWaits: waits, Summary: Summary(jobs, waits)}

	case OpGet:
		j, err := s.Get(req.ID)
		if err != nil {
			return Failure(err)
		}
		return Response{OK: true, Job: j}

	case OpLogs:
		out, err := s.Logs(req.ID, req.Offset, req.Max)
		if err != nil {
			return Failure(err)
		}
		return Response{OK: true, Output: string(out)}

	case OpPause:
		if err := s.Pause(); err != nil {
			return Failure(err)
		}
		st := s.Stats()
		return Response{OK: true, Stats: &st}

	case OpResume:
		if err := s.Resume(); err != nil {
			return Failure(err)
		}
		st := s.Stats()
		return Response{OK: true, Stats: &st}

	case OpInterrupt:
		if err := s.Interrupt(req.ID); err != nil {
			return Failure(err)
		}
		return Response{OK: true}

	case OpInterruptAll:
		return Response{OK: true, Count: s.InterruptAll()}

	case OpPolicy:
		if req.Policy == nil {
			return Failure(fmt.Errorf("jobserver: policy needs a policy"))
		}
		j, err := s.SetPolicy(req.ID, *req.Policy)
		if err != nil {
			return Failure(err)
		}
		return Response{OK: true, Job: j}

	case OpStats:
		st := s.Stats()
		return Response{OK: true, Stats: &st}

	default:
		return Failure(fmt.Errorf("jobserver: unknown op %q", req.Op))
	}
}
