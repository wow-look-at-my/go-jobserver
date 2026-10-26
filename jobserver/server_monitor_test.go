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

// A task of a run inbox that waits for admission says so under its own ID: Why, the summary and the running count.
func TestANamedTaskQueuedOnTheSchedulerSaysSo(t *testing.T) {
	hold := make(chan struct{})
	srv := newMonitorServer(t, &fakeRunner{hold: hold})
	run, err := NewInbox[runInbox](srv, "run")
	require.NoError(t, err)
	holdBusy(t, srv)
	task, err := run.AddTask(work(1))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ws, err := srv.Why(task.ID)
		require.NoError(t, err)
		return len(ws) == 1 && ws[0].Kind == WaitCap && strings.Contains(ws[0].Detail, "run cap")
	}, 5*time.Second, 5*time.Millisecond)
	assert.Equal(t, "1 running, 0 monitors watching; 1 waits for the run cap", Summary(srv.List(), srv.Explain()))

	close(hold)
	require.Nil(t, waitTask(t, task))
}

// groupInbox names a group in LeaseFor and records what OnQueue hears.
type groupInbox struct {
	Inbox[*workSpec, int]
	hold  chan struct{}
	mu    sync.Mutex
	heard map[string][]QueueState
}

func (i *groupInbox) Run(ctx context.Context, s *workSpec) (int, error) {
	select {
	case <-i.hold:
		return s.N, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (i *groupInbox) LeaseFor(s *workSpec) LeaseRequest {
	return LeaseRequest{ID: "ignored", Group: "g", Monitor: true}
}

func (i *groupInbox) OnQueue(id string, q QueueState) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.heard[id] = append(i.heard[id], q)
}

func (i *groupInbox) last(id string) (QueueState, int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	h := i.heard[id]
	if len(h) == 0 {
		return QueueState{}, 0
	}
	return h[len(h)-1], len(h)
}

// LeaseFor shapes the lease of each task: a group limit of one holds back the second task. OnQueue hears where the
// second task waits, and a zero QueueState when each task is admitted.
func TestLeaseForShapesTheLeaseOfAnInboxTask(t *testing.T) {
	srv := newTestServer(t, nil)
	srv.Scheduler().SetGroups(map[string]int{"g": 1})
	in, err := NewInbox[groupInbox](srv, "grouped")
	require.NoError(t, err)
	in.hold, in.heard = make(chan struct{}), map[string][]QueueState{}

	first, err := in.AddTask(work(1))
	require.NoError(t, err)
	waitState(t, srv, first.ID, StateRunning)
	second, err := in.AddTask(work(2))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		q, _ := in.last(second.ID)
		return q.Kind == WaitGroup
	}, 5*time.Second, 5*time.Millisecond, "OnQueue hears that the second task waits for the group")
	q, _ := in.last(second.ID)
	assert.Equal(t, "g", q.Key)
	assert.Equal(t, []string{first.ID}, q.Holders, "the lease ID is the task ID, whatever LeaseFor said")
	q, n := in.last(first.ID)
	assert.Equal(t, 1, n)
	assert.Zero(t, q, "an admitted task hears a zero QueueState")
	ws, err := srv.Why(second.ID)
	require.NoError(t, err)
	require.Len(t, ws, 1)
	assert.Equal(t, WaitGroup, ws[0].Kind)
	assert.Equal(t, "g", ws[0].ID)
	assert.Empty(t, srv.SchedulerStatus().Monitors, "LeaseFor cannot make a monitor lease")
	assert.Equal(t, 1, srv.SchedulerStatus().Running)

	close(in.hold)
	require.Nil(t, waitTask(t, second))
	q, _ = in.last(second.ID)
	assert.Zero(t, q, "the second task hears a zero QueueState once admitted")
}

// Why names each kind of wait in words: a draft, a running dependency, a trigger, a consumer, the run cap and a pause.
func TestWhyNamesEveryWaitKind(t *testing.T) {
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	srv := newMonitorServer(t, &fakeRunner{hold: hold})
	queue, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	busy := holdBusy(t, srv)
	busyTask := &Task{ID: busy.ID, b: srv}

	why := func(id string) Wait {
		t.Helper()
		ws, err := srv.Why(id)
		require.NoError(t, err)
		require.Len(t, ws, 1, "%s waits on one thing", id)
		return ws[0]
	}
	enqueue := func(spec Spec) string {
		t.Helper()
		j, _, err := srv.Enqueue(spec)
		require.NoError(t, err)
		return j.ID
	}
	w := why(enqueue(Spec{ID: "draft", Command: []string{"x"}, Draft: true}))
	assert.Equal(t, WaitDraft, w.Kind)
	assert.Contains(t, w.Detail, "go-jobserver activate draft")

	w = why(enqueue(Spec{ID: "next", Command: []string{"x"}, Deps: []string{busy.ID}}))
	assert.Equal(t, WaitDep, w.Kind)
	assert.Equal(t, "busy is running", w.Detail)

	trig, err := OnFail(busyTask, &queue.Inbox, work(1))
	require.NoError(t, err)
	w = why(trig.ID)
	assert.Equal(t, WaitDep, w.Kind)
	assert.Equal(t, "trigger fail: it runs when busy fails; that task is running", w.Detail)

	taken, err := queue.AddTask(work(2))
	require.NoError(t, err)
	w = why(taken.ID)
	assert.Equal(t, WaitConsumer, w.Kind)
	assert.Equal(t, "no consumer is bound to inbox q", w.Detail)

	w = why(enqueue(Spec{ID: "queued", Command: []string{"x"}}))
	assert.Equal(t, WaitCap, w.Kind)
	assert.Contains(t, w.Detail, "run cap")
	assert.Equal(t, "1 running, 0 monitors watching; 2 wait on unfinished tasks, 1 waits for the run cap, 1 waits for a consumer, 1 waits as drafts",
		Summary(srv.List(), srv.Explain()))

	require.NoError(t, srv.Pause())
	w = why("queued")
	assert.Equal(t, WaitPaused, w.Kind)
	assert.Contains(t, w.Detail, "go-jobserver resume")

	ws, err := srv.Why(busy.ID)
	require.NoError(t, err)
	assert.Empty(t, ws, "a running task waits on nothing")
	_, err = srv.Why("no-such-task")
	assert.ErrorIs(t, err, ErrNotFound)
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
	assert.Equal(t, "1 running, 1 monitor watching; 2 wait for the run cap, 1 waits on unfinished tasks, 1 waits as drafts, 1 waits on 1 monitor", Summary(jobs, waits))
}

// monitorLine says where a monitor stands for each status it can have.
func TestMonitorLineWords(t *testing.T) {
	spec := &MonitorSpec{Kind: "k"}
	line := func(st State, ms *MonitorStatus) string { return monitorLine(&Job{State: st, Monitor: spec, MonitorStatus: ms}) }
	assert.Equal(t, "monitor:k: starting", line(StateActive, nil))
	assert.Equal(t, "monitor:k: no check has run yet", line(StateRunning, nil))
	assert.Equal(t, "monitor:k: failed", line(StateFailed, nil))
	assert.Equal(t, "monitor:k: check 3 FAILED (2 in a row): boom", line(StateRunning, &MonitorStatus{Checks: 3, ErrorsInARow: 2, LastError: "boom"}))
	assert.Equal(t, "monitor:k: not yet after 4 checks: exit 1", line(StateRunning, &MonitorStatus{Checks: 4, LastDetail: "exit 1"}))
}
