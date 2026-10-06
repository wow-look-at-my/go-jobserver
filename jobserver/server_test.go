package jobserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRunner stands in for a command. It records what ran, can hold a run
// open until the test releases it, and writes declared outputs.
type fakeRunner struct {
	mu       sync.Mutex
	calls    []string
	commands []string
	hold     chan struct{}
	fail     int
	out      string
	writes   map[string]string
	entered  chan string
}

func (f *fakeRunner) Run(ctx context.Context, j *Job, out io.Writer) (int, error) {
	f.mu.Lock()
	f.calls = append(f.calls, j.ID)
	f.commands = append(f.commands, j.Command[0])
	hold, fail, text, writes, entered := f.hold, f.fail, f.out, f.writes, f.entered
	f.mu.Unlock()

	if text != "" {
		io.WriteString(out, text)
	}
	for path, body := range writes {
		if err := os.WriteFile(resolvePath(j.Dir, path), []byte(body), 0o644); err != nil {
			return -1, err
		}
	}
	if entered != nil {
		select {
		case entered <- j.ID:
		default:
		}
	}
	if hold != nil {
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-hold:
		}
	}
	return fail, nil
}

func (f *fakeRunner) ran(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == id {
			return true
		}
	}
	return false
}

func (f *fakeRunner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// newTestServer starts a server whose transports are off unless the test
// turns them on.
func newTestServer(t *testing.T, mutate func(*Config)) *Server {
	t.Helper()
	cfg := DefaultConfig(t.TempDir())
	cfg.HTTPAddr = ""
	cfg.UnixSocket = ""
	cfg.SpoolDir = ""
	cfg.IPC = false
	cfg.MaxConcurrent = 4
	cfg.SpoolInterval = 10 * time.Millisecond
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		require.NoError(t, srv.Close())
	})
	return srv
}

// waitState polls until a job reaches the wanted state.
func waitState(t *testing.T, srv *Server, id string, want State) *Job {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		j, err := srv.Get(id)
		require.NoError(t, err)
		if j.State == want {
			return j
		}
		if j.State.Terminal() && want != j.State {
			t.Fatalf("job %s reached %s (%s), want %s", id, j.State, j.Error, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached %s", id, want)
	return nil
}

func TestServerRunsAJobToCompletion(t *testing.T) {
	srv := newTestServer(t, nil)
	j, existing, err := srv.Enqueue(Spec{Command: []string{"sh", "-c", "echo done"}})
	require.NoError(t, err)
	assert.False(t, existing)
	done := waitState(t, srv, j.ID, StateCompleted)
	assert.Equal(t, "done\n", string(mustLog(t, srv, j.ID)))
	assert.NotZero(t, done.LogBytes)
	assert.False(t, done.Started.IsZero())
	assert.False(t, done.Finished.IsZero())
}

func mustLog(t *testing.T, srv *Server, id string) []byte {
	t.Helper()
	out, err := srv.Logs(id, 0, 1<<20)
	require.NoError(t, err)
	return out
}

func TestServerRunsADependencyChain(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, nil)
	a, _, err := srv.Enqueue(Spec{
		Command: []string{"sh", "-c", "echo one > one.txt"},
		Dir:     dir,
		Outputs: []string{"one.txt"},
	})
	require.NoError(t, err)
	b, _, err := srv.Enqueue(Spec{
		Command: []string{"sh", "-c", "cat one.txt > two.txt"},
		Dir:     dir,
		Outputs: []string{"two.txt"},
		Deps:    []string{a.ID},
	})
	require.NoError(t, err)
	waitState(t, srv, b.ID, StateCompleted)
	body, err := os.ReadFile(filepath.Join(dir, "two.txt"))
	require.NoError(t, err)
	assert.Equal(t, "one\n", string(body))

	bj, err := srv.Get(b.ID)
	require.NoError(t, err)
	require.Len(t, bj.Artifacts, 1)
	assert.Equal(t, "two.txt", bj.Artifacts[0].Path)
	assert.NotEmpty(t, bj.Identity)
}

func TestServerHoldsADraftUntilActivated(t *testing.T) {
	runner := &fakeRunner{}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	j, _, err := srv.Enqueue(Spec{Command: []string{"x"}, Draft: true})
	require.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	assert.False(t, runner.ran(j.ID), "a draft job ran")

	_, err = srv.Activate(j.ID)
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateCompleted)
	assert.True(t, runner.ran(j.ID))
}

func TestServerFailsADependentWhenItsDependencyFails(t *testing.T) {
	runner := &fakeRunner{fail: 2}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	a, _, err := srv.Enqueue(Spec{Command: []string{"x"}})
	require.NoError(t, err)
	b, _, err := srv.Enqueue(Spec{Command: []string{"y"}, Deps: []string{a.ID}})
	require.NoError(t, err)
	failed := waitState(t, srv, a.ID, StateFailed)
	assert.Equal(t, 2, failed.ExitCode)
	blocked := waitState(t, srv, b.ID, StateBlocked)
	assert.Contains(t, blocked.Error, "dependency")
	assert.False(t, runner.ran(b.ID))
}

func TestServerFailsAJobWhoseDeclaredOutputIsMissing(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Runner = &fakeRunner{} })
	j, _, err := srv.Enqueue(Spec{Command: []string{"x"}, Outputs: []string{"never-written.txt"}})
	require.NoError(t, err)
	done := waitState(t, srv, j.ID, StateFailed)
	assert.Contains(t, done.Error, "never-written.txt")
}

func TestServerRecordsArtifacts(t *testing.T) {
	runner := &fakeRunner{writes: map[string]string{"out.bin": "payload"}}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	j, _, err := srv.Enqueue(Spec{Command: []string{"x"}, Outputs: []string{"out.bin"}})
	require.NoError(t, err)
	done := waitState(t, srv, j.ID, StateCompleted)
	require.Len(t, done.Artifacts, 1)
	assert.Equal(t, int64(7), done.Artifacts[0].Size)
	assert.NotEmpty(t, done.Artifacts[0].SHA256)
}

func TestServerReusesIdenticalWork(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, nil)
	command := []string{"sh", "-c", "echo run >> " + filepath.Join(dir, "runs.txt")}
	first, _, err := srv.Enqueue(Spec{Command: command, Dir: dir})
	require.NoError(t, err)
	waitState(t, srv, first.ID, StateCompleted)

	second, _, err := srv.Enqueue(Spec{Command: command, Dir: dir})
	require.NoError(t, err)
	cached := waitState(t, srv, second.ID, StateCached)
	assert.Equal(t, first.ID, cached.CacheOf)

	body, err := os.ReadFile(filepath.Join(dir, "runs.txt"))
	require.NoError(t, err)
	assert.Equal(t, "run\n", string(body), "the identical job ran a second time")
}

func TestServerForceSkipsTheCache(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, nil)
	command := []string{"sh", "-c", "echo run >> " + filepath.Join(dir, "runs.txt")}
	first, _, err := srv.Enqueue(Spec{Command: command, Dir: dir})
	require.NoError(t, err)
	waitState(t, srv, first.ID, StateCompleted)
	second, _, err := srv.Enqueue(Spec{Command: command, Dir: dir, Force: true})
	require.NoError(t, err)
	waitState(t, srv, second.ID, StateCompleted)
	body, err := os.ReadFile(filepath.Join(dir, "runs.txt"))
	require.NoError(t, err)
	assert.Equal(t, "run\nrun\n", string(body))
}

func TestServerCacheFollowsInputContent(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "in.txt")
	require.NoError(t, os.WriteFile(input, []byte("one"), 0o644))
	srv := newTestServer(t, nil)
	spec := Spec{
		Command: []string{"sh", "-c", "echo run >> " + filepath.Join(dir, "runs.txt")},
		Dir:     dir,
		Inputs:  []string{"in.txt"},
	}
	first, _, err := srv.Enqueue(spec)
	require.NoError(t, err)
	waitState(t, srv, first.ID, StateCompleted)

	same, _, err := srv.Enqueue(spec)
	require.NoError(t, err)
	waitState(t, srv, same.ID, StateCached)

	require.NoError(t, os.WriteFile(input, []byte("two"), 0o644))
	changed, _, err := srv.Enqueue(spec)
	require.NoError(t, err)
	waitState(t, srv, changed.ID, StateCompleted)

	body, err := os.ReadFile(filepath.Join(dir, "runs.txt"))
	require.NoError(t, err)
	assert.Equal(t, "run\nrun\n", string(body))
}

func TestServerReusesAJobByKey(t *testing.T) {
	runner := &fakeRunner{hold: make(chan struct{})}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	first, existing, err := srv.Enqueue(Spec{Command: []string{"x"}, Key: "nightly"})
	require.NoError(t, err)
	assert.False(t, existing)
	waitState(t, srv, first.ID, StateRunning)

	second, existing, err := srv.Enqueue(Spec{Command: []string{"x"}, Key: "nightly"})
	require.NoError(t, err)
	assert.True(t, existing, "enqueueing a key that is already running added a job")
	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, 1, len(srv.List()))
	close(runner.hold)
	waitState(t, srv, first.ID, StateCompleted)
}

func TestServerCachesFollowDependencyIdentity(t *testing.T) {
	dir := t.TempDir()
	runner := &fakeRunner{writes: map[string]string{"out.txt": "v"}}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	producer := Spec{Command: []string{"produce", "A"}, Dir: dir, Outputs: []string{"out.txt"}}
	a, _, err := srv.Enqueue(producer)
	require.NoError(t, err)
	waitState(t, srv, a.ID, StateCompleted)
	consumer := Spec{Command: []string{"consume"}, Dir: dir, Deps: []string{a.ID}}
	c, _, err := srv.Enqueue(consumer)
	require.NoError(t, err)
	waitState(t, srv, c.ID, StateCompleted)

	// The same command under a different producer command is different work.
	producer.Command = []string{"produce", "B"}
	b, _, err := srv.Enqueue(producer)
	require.NoError(t, err)
	waitState(t, srv, b.ID, StateCompleted)
	c2, _, err := srv.Enqueue(Spec{Command: []string{"consume"}, Dir: dir, Deps: []string{b.ID}})
	require.NoError(t, err)
	waitState(t, srv, c2.ID, StateCompleted)
	assert.Equal(t, 2, runner.countCommand("consume"), "the consumer was reused across different producers")

	// The same producer and consumer again is a cache hit.
	b2, _, err := srv.Enqueue(producer)
	require.NoError(t, err)
	waitState(t, srv, b2.ID, StateCached)
	c3, _, err := srv.Enqueue(Spec{Command: []string{"consume"}, Dir: dir, Deps: []string{b2.ID}})
	require.NoError(t, err)
	waitState(t, srv, c3.ID, StateCached)
	assert.Equal(t, 2, runner.countCommand("consume"))
}

// countCommand counts the runs whose command starts with a word.
func (f *fakeRunner) countCommand(first string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.commands {
		if c == first {
			n++
		}
	}
	return n
}

func TestServerPauseStopsSchedulingUntilResume(t *testing.T) {
	runner := &fakeRunner{}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	require.NoError(t, srv.Pause())
	assert.True(t, srv.Paused())
	j, _, err := srv.Enqueue(Spec{Command: []string{"x"}})
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	assert.False(t, runner.ran(j.ID), "a job ran while the server was paused")
	assert.Equal(t, StateActive, currentState(t, srv, j.ID))

	require.NoError(t, srv.Resume())
	waitState(t, srv, j.ID, StateCompleted)
}

func TestServerPauseIsDurable(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.HTTPAddr, cfg.UnixSocket, cfg.SpoolDir, cfg.IPC = "", "", "", false
	srv, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	require.NoError(t, srv.Pause())
	require.NoError(t, srv.Close())

	srv2, err := New(cfg)
	require.NoError(t, err)
	defer srv2.Close()
	assert.True(t, srv2.Paused(), "the paused flag did not survive a restart")
}

func TestServerInterruptCancelsARunningJob(t *testing.T) {
	runner := &fakeRunner{hold: make(chan struct{})}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	j, _, err := srv.Enqueue(Spec{Command: []string{"x"}})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	require.NoError(t, srv.Interrupt(j.ID))
	waitState(t, srv, j.ID, StateCancelled)
}

func TestServerInterruptAll(t *testing.T) {
	runner := &fakeRunner{hold: make(chan struct{})}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner; c.MaxConcurrent = 2 })
	ids := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		j, _, err := srv.Enqueue(Spec{Command: []string{"x"}})
		require.NoError(t, err)
		ids = append(ids, j.ID)
	}
	for _, id := range ids {
		waitState(t, srv, id, StateRunning)
	}
	assert.Equal(t, 2, srv.InterruptAll())
	for _, id := range ids {
		waitState(t, srv, id, StateCancelled)
	}
}

func TestServerInterruptOfAFinishedJobIsRefused(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Runner = &fakeRunner{} })
	j, _, err := srv.Enqueue(Spec{Command: []string{"x"}})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateCompleted)
	assert.ErrorIs(t, srv.Interrupt(j.ID), ErrBadState)
	assert.ErrorIs(t, srv.Interrupt("ghost"), ErrNotFound)
}

func TestServerRunsJobsConcurrentlyUpToTheLimit(t *testing.T) {
	runner := &fakeRunner{hold: make(chan struct{})}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner; c.MaxConcurrent = 1 })
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		j, _, err := srv.Enqueue(Spec{Command: []string{"x"}})
		require.NoError(t, err)
		ids = append(ids, j.ID)
	}
	waitState(t, srv, ids[0], StateRunning)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, runner.count(), "more jobs ran than the limit allows")
	assert.Equal(t, StateActive, currentState(t, srv, ids[1]))
	close(runner.hold)
	for _, id := range ids {
		waitState(t, srv, id, StateCompleted)
	}
}

func currentState(t *testing.T, srv *Server, id string) State {
	t.Helper()
	j, err := srv.Get(id)
	require.NoError(t, err)
	return j.State
}

func TestServerLimitsConcurrency(t *testing.T) {
	runner := &fakeRunner{}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner; c.MaxConcurrent = 0 })
	j, _, err := srv.Enqueue(Spec{Command: []string{"x"}})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateCompleted)
}

func TestServerAddDepAndSetDeps(t *testing.T) {
	runner := &fakeRunner{}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	a, _, err := srv.Enqueue(Spec{Command: []string{"a"}, Draft: true})
	require.NoError(t, err)
	b, _, err := srv.Enqueue(Spec{Command: []string{"b"}, Draft: true})
	require.NoError(t, err)
	got, err := srv.AddDep(b.ID, a.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{a.ID}, got.Deps)
	got, err = srv.SetDeps(b.ID, nil)
	require.NoError(t, err)
	assert.Empty(t, got.Deps)
	_, err = srv.AddDep(b.ID, "ghost")
	assert.ErrorIs(t, err, ErrMissingDep)
}

func TestServerDuplicateExplicitIDIsRefused(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Runner = &fakeRunner{} })
	_, _, err := srv.Enqueue(Spec{ID: "once", Command: []string{"a"}, Draft: true})
	require.NoError(t, err)
	_, _, err = srv.Enqueue(Spec{ID: "once", Command: []string{"a"}, Draft: true})
	assert.ErrorIs(t, err, ErrDuplicate)
}

func TestServerSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.HTTPAddr, cfg.UnixSocket, cfg.SpoolDir, cfg.IPC = "", "", "", false
	srv, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	j, _, err := srv.Enqueue(Spec{Command: []string{"sh", "-c", "echo first-run"}, Draft: true})
	require.NoError(t, err)
	require.NoError(t, srv.Close())

	srv2, err := New(cfg)
	require.NoError(t, err)
	defer srv2.Close()
	require.NoError(t, srv2.Start())
	got, err := srv2.Get(j.ID)
	require.NoError(t, err)
	assert.Equal(t, StateDraft, got.State)
	_, err = srv2.Activate(j.ID)
	require.NoError(t, err)
	waitState(t, srv2, j.ID, StateCompleted)
	log, err := srv2.Logs(j.ID, 0, 1<<20)
	require.NoError(t, err)
	assert.Equal(t, "first-run\n", string(log))
}

func TestServerStatsCountsStates(t *testing.T) {
	srv := newTestServer(t, nil)
	j, _, err := srv.Enqueue(Spec{Command: []string{"true"}})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateCompleted)
	st := srv.Stats()
	assert.Equal(t, 1, st.Total)
	assert.Equal(t, 1, st.ByState[string(StateCompleted)])
	assert.Equal(t, srv.Config().Dir, st.Dir)
	assert.NotZero(t, st.PID)
}

func TestServerHoldsManyJobsInTheScheduler(t *testing.T) {
	runner := &fakeRunner{}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner; c.MaxConcurrent = 8 })
	ids := make([]string, 0, 25)
	prev := ""
	for i := 0; i < 25; i++ {
		spec := Spec{Command: []string{"x"}}
		if prev != "" {
			spec.Deps = []string{prev}
		}
		j, _, err := srv.Enqueue(spec)
		require.NoError(t, err)
		ids = append(ids, j.ID)
		prev = j.ID
	}
	waitState(t, srv, ids[len(ids)-1], StateCompleted)
	assert.Equal(t, 25, runner.count())
}

func TestAPIHandleAnswersEveryOperation(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Runner = &fakeRunner{} })
	resp := srv.Handle(Request{Op: OpEnqueue, Spec: &Spec{Command: []string{"x"}, Draft: true}})
	require.True(t, resp.OK, resp.Error)
	id := resp.Job.ID

	require.True(t, srv.Handle(Request{Op: OpGet, ID: id}).OK)
	require.True(t, srv.Handle(Request{Op: OpActivate, ID: id}).OK)
	waitState(t, srv, id, StateCompleted)
	require.True(t, srv.Handle(Request{Op: OpLogs, ID: id}).OK)
	require.True(t, srv.Handle(Request{Op: OpList}).OK)
	require.True(t, srv.Handle(Request{Op: OpStats}).OK)
	require.True(t, srv.Handle(Request{Op: OpPause}).OK)
	require.True(t, srv.Handle(Request{Op: OpResume}).OK)
	require.True(t, srv.Handle(Request{Op: OpInterruptAll}).OK)

	bad := srv.Handle(Request{Op: "nonsense"})
	assert.False(t, bad.OK)
	assert.Contains(t, bad.Error, "unknown op")
	noSpec := srv.Handle(Request{Op: OpEnqueue})
	assert.False(t, noSpec.OK)
	missing := srv.Handle(Request{Op: OpGet, ID: "ghost"})
	assert.False(t, missing.OK)
}

func TestHTTPTransport(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Runner = &fakeRunner{} })
	front := httptest.NewServer(srv.Mux())
	defer front.Close()

	body, err := json.Marshal(Spec{Command: []string{"x"}, Draft: true})
	require.NoError(t, err)
	res, err := http.Post(front.URL+"/api/jobs", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	var created Response
	require.NoError(t, json.NewDecoder(res.Body).Decode(&created))
	res.Body.Close()
	require.True(t, created.OK, created.Error)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// A bare argv array is accepted too.
	res, err = http.Post(front.URL+"/api/jobs", "application/json", bytes.NewReader([]byte(`["y"]`)))
	require.NoError(t, err)
	var second Response
	require.NoError(t, json.NewDecoder(res.Body).Decode(&second))
	res.Body.Close()
	require.True(t, second.OK, second.Error)
	require.Equal(t, []string{"y"}, second.Job.Command)

	// The generic endpoint takes a request envelope.
	reqBody, err := json.Marshal(Request{Op: OpActivate, ID: created.Job.ID})
	require.NoError(t, err)
	res, err = http.Post(front.URL+"/api/activate", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	waitState(t, srv, created.Job.ID, StateCompleted)

	res, err = http.Get(front.URL + "/api/jobs")
	require.NoError(t, err)
	var listed Response
	require.NoError(t, json.NewDecoder(res.Body).Decode(&listed))
	res.Body.Close()
	assert.Len(t, listed.Jobs, 2)

	res, err = http.Get(front.URL + "/api/jobs/" + created.Job.ID + "/logs")
	require.NoError(t, err)
	log, _ := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Empty(t, log)

	res, err = http.Get(front.URL + "/api/stats")
	require.NoError(t, err)
	res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)

	res, err = http.Get(front.URL + "/api/jobs/ghost")
	require.NoError(t, err)
	res.Body.Close()
	assert.Equal(t, http.StatusNotFound, res.StatusCode)

	res, err = http.Get(front.URL + "/")
	require.NoError(t, err)
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Contains(t, string(page), "go-jobserver")
	assert.Contains(t, string(page), created.Job.ID)

	res, err = http.Get(front.URL + "/job/" + created.Job.ID)
	require.NoError(t, err)
	detail, _ := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Contains(t, string(detail), created.Job.ID)
}

func TestHTTPRejectsAnEmptyBody(t *testing.T) {
	srv := newTestServer(t, nil)
	front := httptest.NewServer(srv.Mux())
	defer front.Close()
	res, err := http.Post(front.URL+"/api/jobs", "application/json", bytes.NewReader(nil))
	require.NoError(t, err)
	defer res.Body.Close()
	assert.Equal(t, http.StatusBadRequest, res.StatusCode)
}

func TestClientOverTheUnixSocket(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, func(c *Config) {
		c.Dir = dir
		c.UnixSocket = filepath.Join(dir, "go-jobserver.sock")
		c.Runner = &fakeRunner{}
	})
	require.FileExists(t, filepath.Join(dir, "go-jobserver.sock"))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Config())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Ping(ctx))

	resp, err := c.Enqueue(ctx, Spec{Command: []string{"x"}, Draft: true})
	require.NoError(t, err)
	require.True(t, resp.OK, resp.Error)
	_, err = c.Do(ctx, Request{Op: OpActivate, ID: resp.Job.ID})
	require.NoError(t, err)
	done, err := c.Wait(ctx, resp.Job.ID, 10*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, StateCompleted, done.State)

	logs, err := c.Logs(ctx, resp.Job.ID, 0, 1<<20)
	require.NoError(t, err)
	assert.True(t, logs.OK)
}

func TestClientOverIPC(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, func(c *Config) {
		c.Dir = dir
		c.IPC = true
		c.IPCName = IPCName(dir)
		c.Runner = &fakeRunner{}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Config())
	require.NoError(t, err)
	require.NotNil(t, c.ipc, "the client did not use the ipc service")
	defer c.Close()

	resp, err := c.Enqueue(ctx, Spec{Command: []string{"x"}})
	require.NoError(t, err)
	require.True(t, resp.OK, resp.Error)
	done, err := c.Wait(ctx, resp.Job.ID, 10*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, StateCompleted, done.State)

	stats, err := c.Do(ctx, Request{Op: OpStats})
	require.NoError(t, err)
	require.True(t, stats.OK)
	assert.Equal(t, 1, stats.Stats.Total)
}

func TestDialReportsAMissingDaemon(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := Dial(ctx, Config{Dir: dir, IPC: true, IPCName: "go-jobserver-no-such-daemon-" + nameSuffix(dir)})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoDaemon)
}

func TestSpoolTransport(t *testing.T) {
	spool := t.TempDir()
	runner := &fakeRunner{}
	srv := newTestServer(t, func(c *Config) {
		c.SpoolDir = spool
		c.Runner = runner
		c.SpoolInterval = 10 * time.Millisecond
	})
	require.NoError(t, os.WriteFile(filepath.Join(spool, "hello.cmd"), []byte("echo hello\n"), 0o644))
	waitForJobs(t, srv, 1)
	jobs := srv.List()
	require.Len(t, jobs, 1)
	assert.Equal(t, []string{"echo", "hello"}, jobs[0].Command)
	assert.Equal(t, "hello", jobs[0].Name)
	waitState(t, srv, jobs[0].ID, StateCompleted)
	require.FileExists(t, filepath.Join(spool, spoolDoneDir, "hello.cmd"))

	spec := Spec{Command: []string{"one", "two"}, Name: "spec-job", Draft: true}
	body, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(spool, "spec.json"), body, 0o644))
	waitForJobs(t, srv, 2)

	require.NoError(t, os.WriteFile(filepath.Join(spool, "bad.json"), []byte("{not json"), 0o644))
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(spool, spoolFailedDir, "bad.json"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.FileExists(t, filepath.Join(spool, spoolFailedDir, "bad.json.error"))
}

func waitForJobs(t *testing.T, srv *Server, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return len(srv.List()) >= want }, 5*time.Second, 10*time.Millisecond)
}

func TestSpoolAcceptsAnArgvArray(t *testing.T) {
	spool := t.TempDir()
	srv := newTestServer(t, func(c *Config) {
		c.SpoolDir = spool
		c.Runner = &fakeRunner{}
		c.SpoolInterval = 10 * time.Millisecond
	})
	require.NoError(t, os.WriteFile(filepath.Join(spool, "argv.json"), []byte(`["echo","hi"]`), 0o644))
	waitForJobs(t, srv, 1)
	assert.Equal(t, []string{"echo", "hi"}, srv.List()[0].Command)
}

func TestSpecFromFile(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		want    []string
		wantErr bool
	}{
		{"cmd", "echo hi\n", []string{"echo", "hi"}, false},
		{"json spec", `{"command":["make","-j4"]}`, []string{"make", "-j4"}, false},
		{"json array", `["ls","-l"]`, []string{"ls", "-l"}, false},
		{"json string", `"echo quoted"`, []string{"echo", "quoted"}, false},
		{"empty", "   ", nil, true},
		{"no command", `{"name":"x"}`, nil, true},
		{"bad json", `{"command":`, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec, err := specFromFile(c.name, []byte(c.body))
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.want, spec.Command)
		})
	}
}

func TestDecodeSpec(t *testing.T) {
	spec, err := decodeSpec([]byte(`{"command":["a"]}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, spec.Command)
	spec, err = decodeSpec([]byte(`["b"]`))
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, spec.Command)
	spec, err = decodeSpec([]byte("c d\n"))
	require.NoError(t, err)
	assert.Equal(t, []string{"c", "d"}, spec.Command)
	_, err = decodeSpec(nil)
	assert.Error(t, err)
	_, err = decodeSpec([]byte("   \n"))
	assert.Error(t, err)
}

func TestStatusForMapsErrors(t *testing.T) {
	assert.Equal(t, http.StatusOK, statusFor(Response{OK: true}))
	assert.Equal(t, http.StatusNotFound, statusFor(Failure(ErrNotFound)))
	assert.Equal(t, http.StatusBadRequest, statusFor(Failure(ErrNoCommand)))
	assert.Equal(t, http.StatusInternalServerError, statusFor(Failure(io.EOF)))
}

func TestPageHelpers(t *testing.T) {
	assert.Equal(t, "-", shortTime(time.Time{}))
	assert.NotEqual(t, "-", shortTime(time.Now()))
	assert.Equal(t, "-", shortDuration(0))
	assert.Equal(t, "5ms", shortDuration(5*time.Millisecond))
}
