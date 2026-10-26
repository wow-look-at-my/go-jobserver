// The views that answer "what runs, what watches, and why does this wait": the job graph, the monitors, the waits.

package jobserver

import (
	"net/http"
	"sort"
)

// GraphNode is one job in the graph view.
type GraphNode struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	// Kind is "monitor" or "job".
	Kind  string `json:"kind"`
	State State  `json:"state"`
	// Waiting says what the job waits on, one line per wait. It is empty for a job that runs or has ended.
	Waiting []string `json:"waiting,omitempty"`
	// Status is a monitor's last check, in words.
	Status string `json:"status,omitempty"`
}

// GraphEdge runs from a dependency to the job that waits for it.
type GraphEdge struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Label string `json:"label"`
}

// JobGraph is every job, every dependency, and the summary line.
type JobGraph struct {
	Summary string      `json:"summary"`
	Nodes   []GraphNode `json:"nodes"`
	Edges   []GraphEdge `json:"edges"`
}

// Graph returns the whole job graph with each node's waits.
func (s *Server) Graph() JobGraph {
	jobs := SortedJobs(s.List())
	waits := s.Explain()
	g := JobGraph{Summary: Summary(jobs, waits), Nodes: []GraphNode{}, Edges: []GraphEdge{}}
	for _, j := range jobs {
		n := GraphNode{ID: j.ID, Label: jobLabel(j), Kind: "job", State: j.State}
		if j.Monitor != nil {
			n.Kind, n.Label, n.Status = "monitor", j.Monitor.Key(), monitorLine(j)
		}
		for _, w := range waits[j.ID] {
			n.Waiting = append(n.Waiting, w.Detail)
		}
		g.Nodes = append(g.Nodes, n)
		for _, d := range j.Deps {
			label := "before"
			if dep, ok := Index(jobs)[d]; ok && dep.Monitor != nil {
				label = "gates"
			}
			g.Edges = append(g.Edges, GraphEdge{From: d, To: j.ID, Label: label})
		}
	}
	return g
}

// MonitorView is one monitor and the jobs that wait on it.
type MonitorView struct {
	*Job
	// Waiting are the unfinished jobs that depend on this monitor.
	Waiting []string `json:"waiting_jobs"`
	// Line is where the monitor stands, in one line.
	Line string `json:"line"`
}

// MonitorViews returns every monitor, watching ones first, each with the jobs that wait on it.
func (s *Server) MonitorViews() []MonitorView {
	jobs := s.List()
	children := Children(jobs)
	byID := Index(jobs)
	var out []MonitorView
	for _, j := range jobs {
		if j.Monitor == nil {
			continue
		}
		v := MonitorView{Job: j, Line: monitorLine(j), Waiting: []string{}}
		for _, c := range children[j.ID] {
			if child, ok := byID[c]; ok && !child.State.Terminal() {
				v.Waiting = append(v.Waiting, c)
			}
		}
		out = append(out, v)
	}
	sort.SliceStable(out, func(i, k int) bool {
		return !out[i].State.Terminal() && out[k].State.Terminal()
	})
	return out
}

// viewRoutes adds the view routes to the HTTP API.
func (s *Server) viewRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/graph", func(w http.ResponseWriter, r *http.Request) {
		g := s.Graph()
		writeJSON(w, Response{OK: true, Graph: &g, Summary: g.Summary})
	})
	mux.HandleFunc("GET /api/monitors", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.Handle(Request{Op: OpMonitors}))
	})
	mux.HandleFunc("GET /api/kinds", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.Handle(Request{Op: OpKinds}))
	})
	mux.HandleFunc("GET /api/why/{id}", func(w http.ResponseWriter, r *http.Request) {
		resp := s.Handle(Request{Op: OpWhy, ID: r.PathValue("id")})
		if resp.Code == CodeNotFound {
			writeJSONStatus(w, http.StatusNotFound, resp)
			return
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("GET /api/scheduler", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.Handle(Request{Op: OpScheduler}))
	})
}

// handleView answers the view ops. It reports false for any other op.
func (s *Server) handleView(req Request) (Response, bool) {
	switch req.Op {
	case OpMonitors:
		jobs := s.List()
		return Response{OK: true, Monitors: s.MonitorViews(), Summary: Summary(jobs, s.Explain())}, true
	case OpKinds:
		return Response{OK: true, Kinds: s.MonitorKinds()}, true
	case OpWhy:
		ws, err := s.Why(req.ID)
		if err != nil {
			return Failure(err), true
		}
		return Response{OK: true, Waits: ws}, true
	case OpGraph:
		g := s.Graph()
		return Response{OK: true, Graph: &g, Summary: g.Summary}, true
	case OpScheduler:
		st := s.SchedulerStatus()
		return Response{OK: true, Scheduler: &st}, true
	}
	return Response{}, false
}
