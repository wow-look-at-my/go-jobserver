package jobserver

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

//go:embed dashboard.html
var dashboardFiles embed.FS

// pages is parsed once; a parse failure is a build-time mistake, so it panics
// at first use rather than serving a broken page.
var pages = template.Must(template.New("dashboard.html").Funcs(template.FuncMap{
	"short":   shortTime,
	"dur":     shortDuration,
	"elapsed": elapsed,
	"args":    strings.Join,
}).ParseFS(dashboardFiles, "dashboard.html"))

// Mux returns the HTTP routes the dashboard and the JSON API answer on.
func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.pageDashboard)
	mux.HandleFunc("GET /job/{id}", s.pageJob)
	mux.HandleFunc("GET /api/jobs", s.apiList)
	mux.HandleFunc("POST /api/jobs", s.apiEnqueue)
	mux.HandleFunc("POST /api/", s.apiCall)
	mux.HandleFunc("GET /api/jobs/{id}", s.apiGet)
	mux.HandleFunc("GET /api/jobs/{id}/logs", s.apiLogs)
	mux.HandleFunc("GET /api/stats", s.apiStats)
	return mux
}

// startHTTP listens on the configured address.
func (s *Server) startHTTP() error {
	if s.cfg.HTTPAddr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("go-jobserver: http: %w", err)
	}
	srv := &http.Server{Handler: s.Mux(), ReadHeaderTimeout: 10 * time.Second}
	s.addCloser(&httpServer{ln: ln, srv: srv})
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.cfg.Logf("go-jobserver: http: %v", err)
		}
	}()
	s.cfg.Logf("go-jobserver: dashboard on http://%s", ln.Addr())
	return nil
}

// startUnix serves the same routes over a file socket.
func (s *Server) startUnix() error {
	if s.cfg.UnixSocket == "" {
		return nil
	}
	if n := len(s.cfg.UnixSocket); n > unixPathLimit {
		return fmt.Errorf("go-jobserver: unix socket path is %d bytes and %s allows %d: pass -socket with a shorter path, or -no-socket to use the ipc service", n, runtime.GOOS, unixPathLimit)
	}
	if err := os.MkdirAll(filepath.Dir(s.cfg.UnixSocket), 0o755); err != nil {
		return err
	}
	ln, err := listenUnix(s.cfg.UnixSocket)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s.Mux(), ReadHeaderTimeout: 10 * time.Second}
	s.addCloser(&unixServer{ln: ln, srv: srv, path: s.cfg.UnixSocket})
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.cfg.Logf("go-jobserver: unix: %v", err)
		}
	}()
	s.cfg.Logf("go-jobserver: socket %s", s.cfg.UnixSocket)
	return nil
}

// listenUnix binds a file socket, clearing a leftover one that no process
// answers on. A socket another daemon holds is left alone and reported.
func listenUnix(path string) (net.Listener, error) {
	ln, err := net.Listen("unix", path)
	if err == nil {
		return ln, nil
	}
	if !isAddrInUse(err) {
		return nil, fmt.Errorf("go-jobserver: unix socket %s: %w", path, err)
	}
	conn, derr := net.DialTimeout("unix", path, time.Second)
	if derr == nil {
		conn.Close()
		return nil, fmt.Errorf("go-jobserver: unix socket %s: another daemon is listening", path)
	}
	if rerr := os.Remove(path); rerr != nil {
		return nil, fmt.Errorf("go-jobserver: unix socket %s: stale and %w", path, rerr)
	}
	ln, err = net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("go-jobserver: unix socket %s: %w", path, err)
	}
	return ln, nil
}

func isAddrInUse(err error) bool {
	return errors.Is(err, os.ErrExist) || strings.Contains(err.Error(), "address already in use")
}

// httpServer closes a TCP listener and its server.
type httpServer struct {
	ln  net.Listener
	srv *http.Server
}

func (h *httpServer) Close() error {
	_ = h.srv.Close()
	if err := h.ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

// unixServer closes a file socket, its server and the socket file.
type unixServer struct {
	ln   net.Listener
	srv  *http.Server
	path string
}

func (u *unixServer) Close() error {
	_ = u.srv.Close()
	var err error
	if cerr := u.ln.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
		err = cerr
	}
	if rerr := os.Remove(u.path); rerr != nil && !errors.Is(rerr, os.ErrNotExist) && err == nil {
		err = rerr
	}
	return err
}

// pageDashboard renders the job table.
func (s *Server) pageDashboard(w http.ResponseWriter, r *http.Request) {
	data := s.pageData(r)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := pages.Execute(w, data); err != nil {
		s.cfg.Logf("go-jobserver: dashboard: %v", err)
	}
}

// pageJob renders one job with its output.
func (s *Server) pageJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.Get(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	out, _ := s.Logs(id, 0, pageLogLimit)
	data := s.pageData(r)
	data.Job = j
	data.Output = string(out)
	data.Truncated = len(out) >= pageLogLimit
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := pages.ExecuteTemplate(w, "dashboard.html", data); err != nil {
		s.cfg.Logf("go-jobserver: job page: %v", err)
	}
}

// pageLogLimit bounds how much output a job page shows.
const pageLogLimit = 256 << 10

// pageData is what the dashboard template renders.
type pageData struct {
	Stats     Stats
	Jobs      []*Job
	Job       *Job
	Output    string
	Truncated bool
	Graph     []GraphRow
	Now       time.Time
}

// GraphRow is one job's place in the dependency order.
type GraphRow struct {
	Job      *Job
	Depth    int
	Children []string
}

// pageData assembles the dashboard's view of the world.
func (s *Server) pageData(*http.Request) pageData {
	jobs := SortedJobs(s.List())
	byID := Index(jobs)
	rows := make([]GraphRow, 0, len(jobs))
	for _, j := range jobs {
		row := GraphRow{Job: j, Depth: depthOf(j, byID, 0)}
		for _, other := range jobs {
			for _, d := range other.Deps {
				if d == j.ID {
					row.Children = append(row.Children, other.ID)
				}
			}
		}
		rows = append(rows, row)
	}
	return pageData{Stats: s.Stats(), Jobs: jobs, Graph: rows, Now: time.Now()}
}

// depthOf is how many dependency levels deep a job sits, bounded to stop a
// malformed graph from recursing forever.
func depthOf(j *Job, byID map[string]*Job, depth int) int {
	if depth > 64 || len(j.Deps) == 0 {
		return depth
	}
	best := depth
	for _, d := range j.Deps {
		dep, ok := byID[d]
		if !ok {
			continue
		}
		if got := depthOf(dep, byID, depth+1); got > best {
			best = got
		}
	}
	return best
}

// apiList answers GET /api/jobs.
func (s *Server) apiList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, Response{OK: true, Jobs: SortedJobs(s.List())})
}

// apiGet answers GET /api/jobs/{id}.
func (s *Server) apiGet(w http.ResponseWriter, r *http.Request) {
	j, err := s.Get(r.PathValue("id"))
	if err != nil {
		writeJSONStatus(w, http.StatusNotFound, Failure(err))
		return
	}
	writeJSON(w, Response{OK: true, Job: j})
}

// apiLogs answers GET /api/jobs/{id}/logs, honoring ?offset= and ?max=.
func (s *Server) apiLogs(w http.ResponseWriter, r *http.Request) {
	var offset int64
	var max int
	fmt.Sscanf(r.URL.Query().Get("offset"), "%d", &offset)
	fmt.Sscanf(r.URL.Query().Get("max"), "%d", &max)
	out, err := s.Logs(r.PathValue("id"), offset, max)
	if err != nil {
		writeJSONStatus(w, http.StatusNotFound, Failure(err))
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(out)
}

// apiStats answers GET /api/stats.
func (s *Server) apiStats(w http.ResponseWriter, r *http.Request) {
	st := s.Stats()
	writeJSON(w, Response{OK: true, Stats: &st})
}

// apiEnqueue answers POST /api/jobs with a job spec in the body.
func (s *Server) apiEnqueue(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, Failure(err))
		return
	}
	spec, err := decodeSpec(body)
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, Failure(err))
		return
	}
	resp := s.Handle(Request{Op: OpEnqueue, Spec: spec})
	writeJSONStatus(w, statusFor(resp), resp)
}

// apiCall answers POST /api/<op> with either a request envelope or a spec.
func (s *Server) apiCall(w http.ResponseWriter, r *http.Request) {
	op := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/"), "/")
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, Failure(err))
		return
	}
	req := Request{Op: op}
	if len(body) > 0 {
		if json.Valid(body) && strings.Contains(op, "/") {
			writeJSONStatus(w, http.StatusNotFound, Failure(fmt.Errorf("go-jobserver: no route %s", r.URL.Path)))
			return
		}
		if err := json.Unmarshal(body, &req); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, Failure(err))
			return
		}
	}
	if req.Op == "" {
		req.Op = op
	}
	resp := s.Handle(req)
	writeJSONStatus(w, statusFor(resp), resp)
}

// decodeSpec accepts either a full Spec or a bare argv array.
func decodeSpec(body []byte) (*Spec, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("go-jobserver: empty job body")
	}
	var spec Spec
	if err := json.Unmarshal(body, &spec); err == nil && len(spec.Command) > 0 {
		return &spec, nil
	}
	var argv []string
	if err := json.Unmarshal(body, &argv); err == nil && len(argv) > 0 {
		return &Spec{Command: argv}, nil
	}
	// A text body is a command line.
	text := strings.TrimSpace(string(body))
	if text != "" {
		if argv := splitCommand(text); len(argv) > 0 {
			return &Spec{Command: argv}, nil
		}
	}
	return nil, fmt.Errorf("go-jobserver: body carries no command")
}

// statusFor maps a failed response to an HTTP status.
func statusFor(resp Response) int {
	if resp.OK {
		return http.StatusOK
	}
	switch resp.Code {
	case CodeNotFound:
		return http.StatusNotFound
	case CodeBadRequest:
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func writeJSON(w http.ResponseWriter, resp Response) {
	writeJSONStatus(w, statusFor(resp), resp)
}

func writeJSONStatus(w http.ResponseWriter, status int, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(resp); err != nil {
		// The status line is already sent; the log is the only place left.
		_ = err
	}
}

func shortTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("15:04:05")
}

func shortDuration(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	return d.Round(time.Millisecond).String()
}

// elapsed is how long a job ran, for the pages.
func elapsed(j *Job) string {
	if j.Started.IsZero() {
		return "-"
	}
	if j.Finished.IsZero() {
		return shortDuration(time.Since(j.Started)) + " (running)"
	}
	return shortDuration(j.Finished.Sub(j.Started))
}
