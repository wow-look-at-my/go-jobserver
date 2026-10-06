package jobserver

import "fmt"

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
	OpWait         = "wait"
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
}

// Response is the answer to one API call.
type Response struct {
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	Job      *Job   `json:"job,omitempty"`
	Jobs     []*Job `json:"jobs,omitempty"`
	Stats    *Stats `json:"stats,omitempty"`
	Output   string `json:"output,omitempty"`
	Existing bool   `json:"existing,omitempty"`
	Count    int    `json:"count,omitempty"`
}

// Failure returns a response carrying an error.
func Failure(err error) Response {
	return Response{OK: false, Error: err.Error()}
}

// Handle runs one API call against the server.
func (s *Server) Handle(req Request) Response {
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
		return Response{OK: true, Jobs: s.List()}

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

	case OpStats:
		st := s.Stats()
		return Response{OK: true, Stats: &st}

	default:
		return Failure(fmt.Errorf("jobserver: unknown op %q", req.Op))
	}
}
