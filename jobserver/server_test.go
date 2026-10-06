package jobserver

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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

func (f *fakeRunner) Run(ctx context.Context, j *Job, out io.Writer, started func(pid int)) (int, error) {
	if started != nil {
		started(os.Getpid())
	}
	f.mu.Lock()
	f.calls = append(f.calls, j.ID)
	f.commands = append(f.commands, j.Command[0])
	hold, fail, text, writes, entered := f.hold, f.fail, f.out, f.writes, f.entered
	f.mu.Unlock()

	if text != "" {
		io.WriteString(out, text)
	}
	for path, body := range writes {
		if j.Dir == "" {
			// Writing without a job directory would land in the package directory, which is the repository during a test run.
			return -1, errors.New("the fake runner needs a job directory to write an output")
		}
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

// shortDir is a scratch directory with a path short enough to hold a socket.
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "goj")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
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
		if j.State.Terminal() {
			require.FailNow(t, "job reached the wrong terminal state",
				"job %s is %s (%s), want %s", id, j.State, j.Error, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.FailNow(t, "job never reached the wanted state", "job %s never reached %s", id, want)
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
	j, _, err := srv.Enqueue(Spec{Command: []string{"x"}, Dir: t.TempDir(), Outputs: []string{"never-written.txt"}})
	require.NoError(t, err)
	done := waitState(t, srv, j.ID, StateFailed)
	assert.Contains(t, done.Error, "never-written.txt")
}

func TestServerRecordsArtifacts(t *testing.T) {
	dir := t.TempDir()
	runner := &fakeRunner{writes: map[string]string{"out.bin": "payload"}}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	j, _, err := srv.Enqueue(Spec{Command: []string{"x"}, Dir: dir, Outputs: []string{"out.bin"}})
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
	// The job is settled by the time Interrupt returns.
	got, err := srv.Get(j.ID)
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.State)
	assert.Equal(t, -1, got.ExitCode)
	assert.Contains(t, got.Error, "interrupted")
}

func TestServerInterruptsAJobWhoseChildHoldsThePipe(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, nil)
	j, _, err := srv.Enqueue(Spec{
		Command: []string{"sh", "-c", "mkfifo -m 600 hold; cat hold"},
		Dir:     dir,
	})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dir, "hold"))
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "the job never started")

	require.NoError(t, srv.Interrupt(j.ID))
	got, err := srv.Get(j.ID)
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.State)
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
		j, _, err := srv.Enqueue(Spec{Command: []string{"x", strconv.Itoa(i)}})
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

func TestSchedulerHoldsAJobThatWouldOversubscribeTheBudget(t *testing.T) {
	hold := make(chan struct{})
	runner := &fakeRunner{hold: hold}
	srv := newTestServer(t, func(c *Config) {
		c.Runner = runner
		c.CPUBudget = 2
		c.DefaultCost = 2
		c.MaxConcurrent = 4
	})
	first, _, err := srv.Enqueue(Spec{Command: []string{"first"}})
	require.NoError(t, err)
	waitState(t, srv, first.ID, StateRunning)

	second, _, err := srv.Enqueue(Spec{Command: []string{"second"}})
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	waiting, err := srv.Get(second.ID)
	require.NoError(t, err)
	assert.Equal(t, StateActive, waiting.State, "a job that does not fit must stay runnable")
	assert.False(t, runner.ran(second.ID), "the budget must hold the second job back")

	close(hold)
	waitState(t, srv, second.ID, StateCompleted)
}

func TestSchedulerRunsAJobLargerThanTheWholeBudget(t *testing.T) {
	runner := &fakeRunner{}
	srv := newTestServer(t, func(c *Config) {
		c.Runner = runner
		c.CPUBudget = 1
		c.DefaultCost = 4
	})
	j, _, err := srv.Enqueue(Spec{Command: []string{"big"}})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateCompleted)
}

func TestStatsCarryTheCPUReport(t *testing.T) {
	srv := newTestServer(t, func(c *Config) {
		c.CPUBudget = 3
		c.SampleInterval = 20 * time.Millisecond
	})
	st := srv.Stats()
	assert.InDelta(t, 3, st.CPU.Budget, 1e-9)
	assert.Equal(t, runtime.NumCPU(), st.CPU.CPUs)
	assert.False(t, st.CPU.OverBudget)
	assert.Empty(t, st.Controls)

	off := newTestServer(t, func(c *Config) { c.DisableCPU = true })
	assert.Contains(t, off.Stats().CPU.Note, "cpu sampling is off")
}

func TestGovernorThrottlesAnOversubscribedJobAndSparesAnExemptProcess(t *testing.T) {
	srv := newTestServer(t, func(c *Config) {
		c.CPUBudget = 0.001
		c.SampleInterval = 20 * time.Millisecond
	})
	j, _, err := srv.Enqueue(Spec{
		Command: []string{"sh", "-c", "sleep 30 & while :; do :; done"},
		Policy: JobPolicy{
			Cost:   1,
			Freeze: FreezeSettings{Enabled: true, Exempt: ParseSelector("sleep")},
		},
	})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	t.Cleanup(func() { srv.InterruptAll() })

	var controls []Control
	require.Eventually(t, func() bool {
		controls = srv.Controls()
		return len(controls) > 0
	}, 20*time.Second, 20*time.Millisecond, "the governor never applied a control")

	for _, c := range controls {
		assert.Equal(t, j.ID, c.Job)
		assert.Equal(t, MechFreeze, c.Mechanism)
		assert.Equal(t, OutcomeApplied, c.Outcome, c.Detail)
		assert.NotEqual(t, "sleep", filepath.Base(c.Name), "an exempt process must be left alone")
	}
	assert.NotEmpty(t, srv.Stats().Controls)
}

func TestGovernorGivesAControlBackOnceTheJobIsUnderBudgetAgain(t *testing.T) {
	srv := newTestServer(t, func(c *Config) {
		c.CPUBudget = 0.001
		c.SampleInterval = 20 * time.Millisecond
	})
	j, _, err := srv.Enqueue(Spec{
		Command: []string{"sh", "-c", "head -c 4000000000 /dev/zero > /dev/null; sleep 30"},
		Policy: JobPolicy{
			Cost:     1,
			Priority: PrioritySettings{Enabled: true, Nice: 10},
		},
	})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	t.Cleanup(func() { srv.InterruptAll() })

	require.Eventually(t, func() bool { return len(srv.Controls()) > 0 },
		20*time.Second, 20*time.Millisecond, "the governor never applied a control")

	// The heavy command finishes and the job goes quiet, which is where the
	// daemon hands its processes back.
	require.Eventually(t, func() bool {
		for _, c := range srv.Events() {
			if c.Job == j.ID && !c.Active {
				return true
			}
		}
		return false
	}, 20*time.Second, 20*time.Millisecond, "the governor never released the control")
	assert.Empty(t, srv.Controls(), "nothing may still be in effect once the job is under budget")
}

func TestGovernorLeavesAJobAloneWhereFreezeCannotSuspend(t *testing.T) {
	srv := newTestServer(t, func(c *Config) {
		c.CPUBudget = 0.001
		c.SampleInterval = 20 * time.Millisecond
	})
	// A host whose stop signal ends a process cannot be asked to stop a job.
	srv.gov.mu.Lock()
	srv.gov.suspend = suspendFacts{known: true, supported: false, detail: "the stop signal ended the process"}
	srv.gov.mu.Unlock()

	j, _, err := srv.Enqueue(Spec{
		Command: []string{"sh", "-c", "while :; do :; done"},
		Policy:  JobPolicy{Cost: 1, Freeze: FreezeSettings{Enabled: true}},
	})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	t.Cleanup(func() { srv.InterruptAll() })

	var told bool
	require.Eventually(t, func() bool {
		for _, c := range srv.Events() {
			if c.Job == j.ID && c.Mechanism == MechFreeze {
				told = true
				assert.Equal(t, OutcomeNotApplicable, c.Outcome)
				assert.Equal(t, "the stop signal ended the process", c.Detail)
			}
		}
		return told
	}, 20*time.Second, 20*time.Millisecond, "the governor never said why it did not stop the job")

	assert.Empty(t, srv.Controls(), "nothing may be in effect where freeze cannot suspend")
	running, err := srv.Get(j.ID)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, running.State, "the job must keep running")
}

func TestServerRecordsTheCPUAJobUsed(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.SampleInterval = 10 * time.Millisecond })
	j, _, err := srv.Enqueue(Spec{Command: []string{"sh", "-c", "head -c 4000000000 /dev/zero > /dev/null"}})
	require.NoError(t, err)
	done := waitState(t, srv, j.ID, StateCompleted)
	assert.Greater(t, done.CPUSeconds, 0.0, "the daemon must measure what the job's tree consumed")
}

func TestServerReplacesAJobPolicyAndReportsIt(t *testing.T) {
	srv := newTestServer(t, nil)
	j, _, err := srv.Enqueue(Spec{Command: []string{"sh", "-c", "sleep 30"}, Draft: true})
	require.NoError(t, err)

	updated, err := srv.SetPolicy(j.ID, JobPolicy{
		Cost:     2,
		Freeze:   FreezeSettings{Enabled: true},
		Priority: PrioritySettings{Enabled: true, Nice: 5},
	})
	require.NoError(t, err)
	assert.InDelta(t, 2, updated.Policy.Cost, 1e-9)
	assert.Equal(t, []Mechanism{MechPriority, MechFreeze}, updated.Policy.Enabled())

	stored, err := srv.Get(j.ID)
	require.NoError(t, err)
	assert.True(t, stored.Policy.equal(updated.Policy))
}
