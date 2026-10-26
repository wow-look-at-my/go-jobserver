package jobserver

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMonitorServer runs one job at a time and checks monitors every few milliseconds.
func newMonitorServer(t *testing.T, runner Runner) *Server {
	t.Helper()
	defs, err := ParseMonitorDefs([]byte(goodDefs))
	require.NoError(t, err)
	return newTestServer(t, func(c *Config) {
		c.MaxConcurrent = 1
		c.Runner = runner
		c.Monitors = &MonitorEnv{Defs: defs, Sleep: func(ctx context.Context, _ time.Duration) bool {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(5 * time.Millisecond):
				return true
			}
		}}
	})
}

// A monitor starts while the run cap is full. A job that depends on it waits and says why, the same monitor is not
// started twice. The job runs once the condition holds.
func TestAMonitorGatesAJobWithoutASlot(t *testing.T) {
	hold := make(chan struct{})
	runner := &fakeRunner{hold: hold}
	srv := newMonitorServer(t, runner)

	busy, _, err := srv.Enqueue(Spec{ID: "busy", Command: []string{"long"}})
	require.NoError(t, err)
	waitState(t, srv, busy.ID, StateRunning)

	flag := filepath.Join(t.TempDir(), "flag")
	spec := MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": flag}}
	mon, existing, err := srv.Enqueue(Spec{ID: "mon", Monitor: &spec})
	require.NoError(t, err)
	assert.False(t, existing)
	waitState(t, srv, mon.ID, StateRunning)

	again, existing, err := srv.Enqueue(Spec{Monitor: &MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": flag}}})
	require.NoError(t, err)
	assert.True(t, existing, "the same condition answers with the monitor already watching it")
	assert.Equal(t, mon.ID, again.ID)

	after, _, err := srv.Enqueue(Spec{ID: "after", Command: []string{"deploy"}, Deps: []string{mon.ID}})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ws, err := srv.Why(after.ID)
		require.NoError(t, err)
		// The detail names the last check, so wait until the first one reported.
		return len(ws) == 1 && ws[0].Kind == WaitMonitor && ws[0].ID == mon.ID && strings.Contains(ws[0].Detail, "exit 1") &&
			assert.ObjectsAreEqual(StateRunning, ws[0].State)
	}, 5*time.Second, 5*time.Millisecond)
	ws, _ := srv.Why(after.ID)
	assert.Contains(t, ws[0].Detail, "monitor:file-exists path="+flag)
	assert.Contains(t, ws[0].Detail, "exit 1")

	monitors := srv.Monitors()
	require.Len(t, monitors, 1)
	require.NotNil(t, monitors[0].MonitorStatus, "a running monitor carries its last status")
	assert.Positive(t, monitors[0].MonitorStatus.Checks)
	sum := Summary(srv.List(), srv.Explain())
	assert.Equal(t, "1 running, 1 monitor watching; 1 waits on 1 monitor", sum)

	st := srv.SchedulerStatus()
	assert.Len(t, st.Monitors, 1, "the monitor holds a lease of its own")
	assert.Equal(t, 1, st.Running, "and takes no slot")

	require.NoError(t, os.WriteFile(flag, nil, 0o644))
	waitState(t, srv, mon.ID, StateCompleted)
	ws, _ = srv.Why(after.ID)
	require.Len(t, ws, 1)
	assert.Equal(t, WaitCap, ws[0].Kind, "with the monitor done, the run cap is what holds it")
	assert.Contains(t, ws[0].Detail, "run cap")
	close(hold)
	waitState(t, srv, after.ID, StateCompleted)
	assert.True(t, runner.ran(after.ID))
	assert.False(t, runner.ran(mon.ID), "a monitor never reaches the command runner")
}

func TestAMonitorJobIsRefusedWhenItCannotRun(t *testing.T) {
	srv := newMonitorServer(t, &fakeRunner{})
	_, _, err := srv.Enqueue(Spec{Monitor: &MonitorSpec{Kind: "nope"}})
	assert.ErrorIs(t, err, ErrUnknownMonitorKind)
	_, _, err = srv.Enqueue(Spec{Monitor: &MonitorSpec{Kind: "file-exists"}})
	assert.ErrorIs(t, err, ErrBadMonitorArgs)
	_, _, err = srv.Enqueue(Spec{Command: []string{"x"}, Monitor: &MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": "/x"}}})
	assert.ErrorIs(t, err, ErrBadMonitorArgs, "a monitor runs no command")

	bare := newTestServer(t, nil)
	_, _, err = bare.Enqueue(Spec{Monitor: &MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": "/x"}}})
	assert.ErrorIs(t, err, ErrUnknownMonitorKind, "a server with no definitions says so")
	assert.Nil(t, bare.MonitorKinds())
	assert.Len(t, srv.MonitorKinds(), 3)
}

// A restart puts a watching monitor back to active, where a command job fails.
func TestARestartResumesAMonitor(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	deadline := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = st.Create(&Spec{ID: "mon", Monitor: &MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": "/x"}, Deadline: deadline}})
	require.NoError(t, err)
	_, err = st.Create(&Spec{ID: "cmd", Command: []string{"true"}})
	require.NoError(t, err)
	require.NoError(t, st.Start("mon"))
	require.NoError(t, st.Start("cmd"))
	require.NoError(t, st.Close())

	st2, err := OpenStore(dir)
	require.NoError(t, err)
	defer st2.Close()
	mon, _ := st2.Job("mon")
	assert.Equal(t, StateActive, mon.State)
	assert.Equal(t, "resumed after a daemon restart", mon.Error)
	require.NotNil(t, mon.Monitor, "the monitor spec is journaled")
	assert.Equal(t, map[string]string{"path": "/x"}, mon.Monitor.Args)
	assert.True(t, deadline.Equal(mon.Monitor.Deadline))
	assert.Equal(t, "monitor:file-exists path=/x", mon.Key)
	cmd, _ := st2.Job("cmd")
	assert.Equal(t, StateFailed, cmd.State)
}

func TestAMonitorLeaseStartsWithEveryGateFull(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	s.SetGroups(map[string]int{"g": 1})
	hog, err := s.Acquire(context.Background(), LeaseRequest{ID: "hog", Group: "g"}, nil)
	require.NoError(t, err)
	defer hog.Release()
	l, ok, err := s.TryAcquire(LeaseRequest{ID: "m", Group: "g", Monitor: true, Policy: JobPolicy{Cost: 99}})
	require.NoError(t, err)
	require.True(t, ok, "a monitor never queues")
	defer l.Release()
	req, _, _ := l.snapshot()
	assert.Empty(t, req.Group, "a monitor takes no group slot")
	assert.True(t, req.Policy.Priority.Enabled, "it runs under the monitor policy")
	assert.Equal(t, 19, req.Policy.Priority.Nice)
	st := s.Status()
	assert.Equal(t, 1, st.Running)
	require.Len(t, st.Monitors, 1)
	assert.Equal(t, "m", st.Monitors[0].ID)
	_, ok, err = s.TryAcquire(LeaseRequest{ID: "job"})
	require.NoError(t, err)
	assert.False(t, ok, "the cap still holds back an ordinary lease")
}

// A host's own work sits in the graph as a task. It waits on a monitor, takes no slot of the host's full scheduler,
// and runs through the host's runner. The server leaves the host's scheduler running when it closes.
func TestAHostTaskWaitsOnAMonitorOnTheHostsScheduler(t *testing.T) {
	sched := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	hog := admit(t, sched, LeaseRequest{ID: "hog"})
	defer hog.Release()
	defs, err := ParseMonitorDefs([]byte(goodDefs))
	require.NoError(t, err)
	ran := make(chan string, 1)
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.HTTPAddr, cfg.UnixSocket, cfg.SpoolDir, cfg.IPC = "", "", "", false
	cfg.Scheduler = sched
	cfg.Monitors = &MonitorEnv{Defs: defs, Sleep: func(ctx context.Context, _ time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Millisecond):
			return true
		}
	}}
	cfg.Tasks = map[string]TaskRunner{"spawn": func(ctx context.Context, j *Job, out io.Writer) error {
		fmt.Fprintf(out, "started %s\n", j.Task.Data)
		ran <- string(j.Task.Data)
		return nil
	}}
	srv, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())

	flag := filepath.Join(t.TempDir(), "flag")
	mon, _, err := srv.Enqueue(Spec{Monitor: &MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": flag}}})
	require.NoError(t, err)
	task, _, err := srv.Enqueue(Spec{ID: "spawn-1", Task: &TaskSpec{Kind: "spawn", Data: []byte(`{"hook":"h"}`)}, Deps: []string{mon.ID}})
	require.NoError(t, err)
	_, _, err = srv.Enqueue(Spec{Task: &TaskSpec{Kind: "nope"}})
	assert.ErrorIs(t, err, ErrUnknownTaskKind)

	waitState(t, srv, mon.ID, StateRunning)
	g := srv.Graph()
	var node GraphNode
	for _, n := range g.Nodes {
		if n.ID == task.ID {
			node = n
		}
	}
	assert.Equal(t, "spawn", node.Kind)
	require.Eventually(t, func() bool {
		ws, _ := srv.Why(task.ID)
		return len(ws) == 1 && ws[0].Kind == WaitMonitor
	}, 5*time.Second, 5*time.Millisecond)

	require.NoError(t, os.WriteFile(flag, nil, 0o644))
	select {
	case data := <-ran:
		assert.Equal(t, `{"hook":"h"}`, data, "the task runs with the host's own full scheduler")
	case <-time.After(10 * time.Second):
		t.Fatal("the task never ran")
	}
	waitState(t, srv, task.ID, StateCompleted)
	require.NoError(t, srv.Close())

	_, ok, err := sched.TryAcquire(LeaseRequest{ID: "after", Monitor: true})
	require.NoError(t, err)
	assert.True(t, ok, "the host's scheduler still runs after the server closed")

	st, err := OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	j, _ := st.Job(task.ID)
	require.NotNil(t, j.Task, "the task is journaled")
	assert.Equal(t, "spawn", j.Task.Kind)
	assert.JSONEq(t, `{"hook":"h"}`, string(j.Task.Data))
}

// Why names each kind of wait in words: a draft, a running dependency, the run cap and a pause.
func TestWhyNamesEveryWaitKind(t *testing.T) {
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	srv := newMonitorServer(t, &fakeRunner{hold: hold})
	busy, _, err := srv.Enqueue(Spec{ID: "busy", Command: []string{"long"}})
	require.NoError(t, err)
	waitState(t, srv, busy.ID, StateRunning)

	why := func(spec Spec) Wait {
		t.Helper()
		j, _, err := srv.Enqueue(spec)
		require.NoError(t, err)
		ws, err := srv.Why(j.ID)
		require.NoError(t, err)
		require.Len(t, ws, 1, "%s waits on one thing", j.ID)
		return ws[0]
	}
	w := why(Spec{ID: "draft", Command: []string{"x"}, Draft: true})
	assert.Equal(t, WaitDraft, w.Kind)
	assert.Contains(t, w.Detail, "go-jobserver activate draft")

	w = why(Spec{ID: "next", Command: []string{"x"}, Deps: []string{busy.ID}})
	assert.Equal(t, WaitDep, w.Kind)
	assert.Equal(t, "busy is running", w.Detail)

	w = why(Spec{ID: "queued", Command: []string{"x"}})
	assert.Equal(t, WaitCap, w.Kind)
	assert.Contains(t, w.Detail, "run cap")

	require.NoError(t, srv.Pause())
	ws, err := srv.Why("queued")
	require.NoError(t, err)
	require.Len(t, ws, 1)
	assert.Equal(t, WaitPaused, ws[0].Kind)
	assert.Contains(t, ws[0].Detail, "go-jobserver resume")

	assert.Equal(t, "1 running, 0 monitors watching; 1 waits on unfinished jobs, 1 waits as drafts, 1 waits because the server is paused",
		Summary(srv.List(), srv.Explain()))
}

func TestSummaryWords(t *testing.T) {
	jobs := []*Job{{ID: "a", State: StateRunning}, {ID: "m", State: StateRunning, Monitor: &MonitorSpec{Kind: "k"}}}
	assert.Equal(t, "1 running, 1 monitor watching; nothing waits", Summary(jobs, nil))
	waits := map[string][]Wait{
		"b": {{Kind: WaitCap}},
		"c": {{Kind: WaitCap}},
		"d": {{Kind: WaitMonitor, ID: "m"}, {Kind: WaitDep, ID: "a"}},
		"e": {{Kind: WaitDraft}},
	}
	assert.Equal(t, "1 running, 1 monitor watching; 2 wait for the run cap, 1 waits on unfinished jobs, 1 waits as drafts, 1 waits on 1 monitor", Summary(jobs, waits))
}
