package jobserver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fastMonitors is a monitor env on goodDefs that checks every few milliseconds.
func fastMonitors(t *testing.T) *MonitorEnv {
	t.Helper()
	defs, err := ParseMonitorDefs([]byte(goodDefs))
	require.NoError(t, err)
	return &MonitorEnv{Defs: defs, Sleep: func(ctx context.Context, _ time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Millisecond):
			return true
		}
	}}
}

// newMonitorServer runs one task at a time and checks monitors every few milliseconds.
func newMonitorServer(t *testing.T, runner Runner) *Server {
	t.Helper()
	env := fastMonitors(t)
	return newTestServer(t, func(c *Config) {
		c.MaxConcurrent = 1
		c.Runner = runner
		c.Monitors = env
	})
}

// fileMonitor watches for a file.
func fileMonitor(path string) MonitorSpec {
	return MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": path}}
}

// holdBusy starts a default task that holds the only run slot. The fake runner holds it.
func holdBusy(t *testing.T, srv *Server) *Job {
	t.Helper()
	busy, _, err := srv.Enqueue(Spec{ID: "busy", Command: []string{"long"}})
	require.NoError(t, err)
	waitState(t, srv, busy.ID, StateRunning)
	return busy
}

// A monitor starts while the run cap is full. A task that triggers on it waits and says why. The same monitor is not
// started twice. The task runs once the condition holds.
func TestAMonitorGatesATaskWithoutASlot(t *testing.T) {
	hold := make(chan struct{})
	runner := &fakeRunner{hold: hold}
	srv := newMonitorServer(t, runner)
	run, err := NewInbox[runInbox](srv, "run")
	require.NoError(t, err)
	holdBusy(t, srv)

	flag := filepath.Join(t.TempDir(), "flag")
	mon, existing, err := srv.Watch(fileMonitor(flag), "")
	require.NoError(t, err)
	assert.False(t, existing)
	waitState(t, srv, mon.ID, StateRunning)

	again, existing, err := srv.Watch(fileMonitor(flag), "another title")
	require.NoError(t, err)
	assert.True(t, existing, "the same condition answers with the monitor already watching it")
	assert.Equal(t, mon.ID, again.ID)

	after, err := OnSuccess(mon, &run.Inbox, work(3))
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
	require.Eventually(t, func() bool {
		ws, err := srv.Why(after.ID)
		return err == nil && len(ws) == 1 && ws[0].Kind == WaitCap && strings.Contains(ws[0].Detail, "run cap")
	}, 5*time.Second, 5*time.Millisecond, "with the monitor done, the run cap is what holds the task")
	close(hold)
	r, te := run.Wait(context.Background(), after)
	require.Nil(t, te)
	assert.Equal(t, 6, r)
	assert.False(t, runner.ran(mon.ID), "a monitor never reaches the command runner")
	j, err := srv.Get(mon.ID)
	require.NoError(t, err)
	assert.Contains(t, string(j.Result), `"checks"`, "the result is the last status")
}

func TestAMonitorTaskIsRefusedWhenItCannotRun(t *testing.T) {
	srv := newMonitorServer(t, &fakeRunner{})
	_, _, err := srv.Watch(MonitorSpec{Kind: "nope"}, "")
	assert.ErrorIs(t, err, ErrUnknownMonitorKind)
	_, _, err = srv.Watch(MonitorSpec{Kind: "file-exists"}, "")
	assert.ErrorIs(t, err, ErrBadMonitorArgs)
	resp := srv.Handle(Request{Op: OpWatch, Monitor: &MonitorSpec{Kind: "nope"}})
	assert.Equal(t, CodeBadRequest, resp.Code)
	_, err = NewInbox[runInbox](srv, MonitorInboxName)
	assert.ErrorIs(t, err, ErrInboxExists, "the monitor inbox name is taken")

	bare := newTestServer(t, nil)
	_, _, err = bare.Watch(fileMonitor("/x"), "")
	assert.ErrorIs(t, err, ErrUnknownMonitorKind, "a server with no definitions says so")
	assert.Nil(t, bare.MonitorKinds())
	assert.Len(t, srv.MonitorKinds(), 3)
}

// A monitor whose deadline passes fails.
func TestAMonitorPastItsDeadlineFails(t *testing.T) {
	srv := newMonitorServer(t, &fakeRunner{})
	spec := fileMonitor(filepath.Join(t.TempDir(), "never"))
	spec.Deadline = time.Now().Add(-time.Second)
	mon, _, err := srv.Watch(spec, "late")
	require.NoError(t, err)
	te := waitTask(t, mon)
	require.NotNil(t, te)
	assert.Equal(t, Failed, te.State)
	assert.ErrorIs(t, te, ErrMonitorDeadline)
}

// A restart resumes a watching monitor: the task runs again and succeeds when the condition holds.
func TestARestartResumesAMonitor(t *testing.T) {
	dir := t.TempDir()
	open := func() *Server {
		cfg := DefaultConfig(dir)
		cfg.HTTPAddr, cfg.UnixSocket, cfg.IPC = "", "", false
		cfg.Monitors = fastMonitors(t)
		srv, err := New(cfg)
		require.NoError(t, err)
		require.NoError(t, srv.Start())
		return srv
	}
	srv := open()
	flag := filepath.Join(t.TempDir(), "flag")
	mon, _, err := srv.Watch(fileMonitor(flag), "")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		j, err := srv.Get(mon.ID)
		return err == nil && j.MonitorStatus != nil && j.MonitorStatus.Checks > 0
	}, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, srv.Close())

	srv = open()
	t.Cleanup(func() { srv.Close() })
	waitState(t, srv, mon.ID, StateRunning)
	again, existing, err := srv.Watch(fileMonitor(flag), "")
	require.NoError(t, err)
	assert.True(t, existing, "the resumed monitor still answers for its condition")
	assert.Equal(t, mon.ID, again.ID)
	require.NoError(t, os.WriteFile(flag, nil, 0o644))
	waitState(t, srv, mon.ID, StateCompleted)
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

// A host task that asks the scheduler for a lease under its own job ID waits for it in the open: Why, the summary.
// And the running count all say it waits for the run cap.
func TestAHostTaskQueuedOnTheSchedulerSaysSo(t *testing.T) {
	hold := make(chan struct{})
	var srv *Server
	srv = newTestServer(t, func(c *Config) {
		c.MaxConcurrent = 1
		c.Runner = &fakeRunner{hold: hold}
		c.Tasks = map[string]TaskRunner{"run": func(ctx context.Context, j *Job, out io.Writer) error {
			l, err := srv.sched.Acquire(ctx, LeaseRequest{ID: j.ID, Class: j.Name}, nil)
			if err != nil {
				return err
			}
			l.Release()
			return nil
		}}
	})
	busy, _, err := srv.Enqueue(Spec{ID: "busy", Command: []string{"long"}})
	require.NoError(t, err)
	waitState(t, srv, busy.ID, StateRunning)
	task, _, err := srv.Enqueue(Spec{ID: "hook-run-1", Task: &TaskSpec{Kind: "run"}})
	require.NoError(t, err)
	waitState(t, srv, task.ID, StateRunning)

	require.Eventually(t, func() bool {
		ws, err := srv.Why(task.ID)
		require.NoError(t, err)
		return len(ws) == 1 && ws[0].Kind == WaitCap
	}, 5*time.Second, 5*time.Millisecond)
	ws, _ := srv.Why(task.ID)
	assert.Contains(t, ws[0].Detail, "run cap")
	assert.Equal(t, "1 running, 0 monitors watching; 1 waits for the run cap", Summary(srv.List(), srv.Explain()))

	close(hold)
	waitState(t, srv, task.ID, StateCompleted)
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
