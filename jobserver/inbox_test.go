package jobserver

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/priority"
)

type workSpec struct {
	TaskSpec
	N    int  `json:"n"`
	Fail bool `json:"fail,omitempty"`
	Hang bool `json:"hang,omitempty"`
}

// queueInbox has no Run, so a consumer takes its tasks.
type queueInbox struct {
	Inbox[*workSpec, int]
	Urgent priority.MultiPriorityKey `priority:"urgent,default=10"`
}

// runInbox has a Run, so the server's workers run its tasks.
type runInbox struct {
	Inbox[*workSpec, int]
}

func (i *runInbox) Run(ctx context.Context, s *workSpec) (int, error) {
	if s.Hang {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if s.Fail {
		return 0, errors.New("boom")
	}
	return s.N * 2, nil
}

type wrongRun struct{ Inbox[*workSpec, int] }

func (*wrongRun) Run(s *workSpec) int { return s.N }

type wrongPriority struct{ Inbox[*workSpec, int] }

func (*wrongPriority) InboxPriority() string { return "first" }

func work(n int) *workSpec { return &workSpec{TaskSpec: TaskSpec{Title: "work"}, N: n} }

// waitTask waits for a task to end and fails the test past a deadline.
func waitTask(t *testing.T, task *Task) *TaskError {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	te := task.Wait(ctx)
	require.False(t, te != nil && errors.Is(te.Err, context.DeadlineExceeded), "the task never ended")
	return te
}

func TestEnqueueLandsInTheDefaultInboxAndRuns(t *testing.T) {
	srv := newTestServer(t, nil)
	j, _, err := srv.Enqueue(Spec{Command: []string{"sh", "-c", "exit 0"}})
	require.NoError(t, err)
	def := srv.DefaultInbox()
	res, te := def.Wait(context.Background(), &Task{ID: j.ID, b: srv})
	require.Nil(t, te)
	assert.Equal(t, 0, res.ExitCode)

	task, err := def.AddTask(&CommandSpec{TaskSpec: TaskSpec{Title: "fails"}, Command: []string{"sh", "-c", "exit 3"}})
	require.NoError(t, err)
	te = waitTask(t, task)
	require.NotNil(t, te)
	assert.Equal(t, Failed, te.State)
	assert.Equal(t, "fails", def.Spec(task).Title)
	_, te, ok := def.TryResult(task)
	assert.True(t, ok)
	assert.Equal(t, "exit status 3", te.Message)
}

func TestDefaultTaskTimesOut(t *testing.T) {
	runner := &fakeRunner{hold: make(chan struct{})}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	task, err := srv.DefaultInbox().AddTask(&CommandSpec{TaskSpec: TaskSpec{Timeout: 50 * time.Millisecond}, Command: []string{"hold"}})
	require.NoError(t, err)
	te := waitTask(t, task)
	require.NotNil(t, te)
	assert.Equal(t, Failed, te.State)
	assert.Equal(t, msgTimedOut, te.Message)
}

func TestRunInboxRunsItsTasks(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[runInbox](srv, "run")
	require.NoError(t, err)
	task, err := in.AddTask(work(21))
	require.NoError(t, err)
	got, te := in.Wait(context.Background(), task)
	require.Nil(t, te)
	assert.Equal(t, 42, got)
	assert.Equal(t, Succeeded, task.State())

	failing, err := in.AddTask(&workSpec{Fail: true})
	require.NoError(t, err)
	_, te = in.Wait(context.Background(), failing)
	require.NotNil(t, te)
	assert.Equal(t, "boom", te.Message)
	assert.EqualError(t, te.Err, "boom")
}

func TestRunInboxTimeoutAndCancel(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[runInbox](srv, "run")
	require.NoError(t, err)
	slow, err := in.AddTask(&workSpec{TaskSpec: TaskSpec{Timeout: 50 * time.Millisecond}, Hang: true})
	require.NoError(t, err)
	te := waitTask(t, slow)
	require.NotNil(t, te)
	assert.Equal(t, msgTimedOut, te.Message)

	hung, err := in.AddTask(&workSpec{Hang: true})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return hung.State() == Running }, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, hung.Cancel())
	te = waitTask(t, hung)
	require.NotNil(t, te)
	assert.Equal(t, Cancelled, te.State)
}

func TestConsumerTakesAndSettles(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	c := in.Bind()
	task, err := in.AddTask(work(5))
	require.NoError(t, err)
	got, spec, ok, err := in.Take(context.Background(), c, -1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, 5, spec.N)
	assert.Equal(t, Running, got.State())
	require.NoError(t, in.Started(c, got, 1))
	require.NoError(t, in.Done(c, got, 50, nil))
	r, te := in.Wait(context.Background(), task)
	require.Nil(t, te)
	assert.Equal(t, 50, r)

	_, _, ok, err = in.Take(context.Background(), c, 0)
	require.NoError(t, err)
	assert.False(t, ok, "a wait of zero returns at once when nothing is ready")
	require.ErrorIs(t, in.Done(c, got, 1, nil), ErrNotRunning)
}

func TestInboxesDrainIndependently(t *testing.T) {
	srv := newTestServer(t, nil)
	a, err := NewInbox[queueInbox](srv, "a")
	require.NoError(t, err)
	b, err := NewInbox[queueInbox](srv, "b")
	require.NoError(t, err)
	ta, err := a.AddTask(work(1))
	require.NoError(t, err)
	tb, err := b.AddTask(work(2))
	require.NoError(t, err)

	cb := b.Bind()
	got, _, ok, err := b.Take(context.Background(), cb, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok, "b drains while a has no consumer")
	assert.Equal(t, tb.ID, got.ID)
	assert.Equal(t, Queued, ta.State())

	require.NoError(t, a.Drain())
	te := waitTask(t, ta)
	require.NotNil(t, te)
	assert.Equal(t, Cancelled, te.State)
	assert.Equal(t, Running, tb.State(), "a drain of a leaves b alone")
	require.NoError(t, b.Done(cb, got, 0, nil))
}

func TestTakeOrdersByPriorityThenArrival(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.MaxConcurrent = 1 })
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	hold, err := srv.Scheduler().Acquire(context.Background(), LeaseRequest{ID: "hold"}, nil)
	require.NoError(t, err)
	c := in.Bind()
	add := func(n, urgent int) {
		p := in.NewPriority()
		p.Set(&in.Urgent, urgent)
		_, err := in.AddTask(work(n), p)
		require.NoError(t, err)
	}
	add(1, 10)
	add(2, 5)
	add(3, 10)
	add(4, 5)
	require.Eventually(t, func() bool { return len(srv.Scheduler().Status().Waiting) == 4 }, 5*time.Second, 5*time.Millisecond)
	hold.Release()
	var order []int
	for range 4 {
		task, spec, ok, err := in.Take(context.Background(), c, 5*time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		order = append(order, spec.N)
		require.NoError(t, in.Done(c, task, 0, nil))
	}
	assert.Equal(t, []int{2, 4, 1, 3}, order, "lower urgent first, then arrival")
}

func TestTriggers(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[runInbox](srv, "run")
	require.NoError(t, err)
	failing, err := in.AddTask(&workSpec{Fail: true})
	require.NoError(t, err)
	onSuccess, err := OnSuccess(failing, &in.Inbox, work(1))
	require.NoError(t, err)
	onFail, err := OnFail(failing, &in.Inbox, work(2))
	require.NoError(t, err)
	onEnded, err := OnEnded(failing, srv.DefaultInbox(), &CommandSpec{Command: []string{"sh", "-c", "exit 0"}})
	require.NoError(t, err)

	te := waitTask(t, onSuccess)
	require.NotNil(t, te)
	assert.Equal(t, Cancelled, te.State)
	assert.Equal(t, msgNeverFires, te.Message)
	r, te := in.Wait(context.Background(), onFail)
	require.Nil(t, te)
	assert.Equal(t, 4, r)
	assert.Nil(t, waitTask(t, onEnded), "an ended trigger fires on a failure, across inboxes")

	ok, err := in.AddTask(work(3))
	require.NoError(t, err)
	failAfterOK, err := OnFail(ok, &in.Inbox, work(4))
	require.NoError(t, err)
	te = waitTask(t, failAfterOK)
	require.NotNil(t, te)
	assert.Equal(t, Cancelled, te.State, "a fail trigger on a success can never fire")
}

func TestStaleConsumerIsRefused(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	old := in.Bind()
	fresh := in.Bind()
	_, _, _, err = in.Take(context.Background(), old, 0)
	require.ErrorIs(t, err, ErrNotConsumer)
	require.ErrorIs(t, in.Unbind(old), ErrNotConsumer)
	_, _, _, err = in.Take(context.Background(), fresh, 0)
	require.NoError(t, err)
}

func TestRebindAbandonsCheckedOutTasks(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	old := in.Bind()
	task, err := in.AddTask(work(1))
	require.NoError(t, err)
	queued, err := in.AddTask(work(2))
	require.NoError(t, err)
	got, _, ok, err := in.Take(context.Background(), old, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	held, other := task, queued
	if got.ID != task.ID {
		held, other = queued, task
	}
	fresh := in.Bind()
	te := waitTask(t, held)
	require.NotNil(t, te)
	assert.Equal(t, Failed, te.State)
	assert.Equal(t, msgConsumerGone, te.Message)
	require.ErrorIs(t, in.Done(old, got, 0, nil), ErrNotConsumer)

	next, _, ok, err := in.Take(context.Background(), fresh, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok, "a queued task stays for the next consumer")
	assert.Equal(t, other.ID, next.ID)
	require.NoError(t, in.Unbind(fresh))
	te = waitTask(t, next)
	require.NotNil(t, te)
	assert.Equal(t, msgConsumerGone, te.Message)
}

func TestConsumerTaskTimesOut(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	c := in.Bind()
	task, err := in.AddTask(&workSpec{TaskSpec: TaskSpec{Timeout: 50 * time.Millisecond}})
	require.NoError(t, err)
	got, _, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	te := waitTask(t, task)
	require.NotNil(t, te)
	assert.Equal(t, msgTimedOut, te.Message)
	require.ErrorIs(t, in.Done(c, got, 0, nil), ErrNotRunning)
}

func TestJournalReplaysInboxTasksAfterARestart(t *testing.T) {
	dir := t.TempDir()
	open := func() *Server {
		cfg := DefaultConfig(dir)
		cfg.HTTPAddr, cfg.UnixSocket, cfg.SpoolDir, cfg.IPC = "", "", "", false
		srv, err := New(cfg)
		require.NoError(t, err)
		require.NoError(t, srv.Start())
		return srv
	}
	srv := open()
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	c := in.Bind()
	finished, err := in.AddTask(work(1))
	require.NoError(t, err)
	got, _, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, finished.ID, got.ID)
	require.NoError(t, in.Done(c, got, 11, nil))
	dying, err := in.AddTask(work(4))
	require.NoError(t, err)
	_, _, ok, err = in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	p := in.NewPriority()
	p.Set(&in.Urgent, 3)
	queued, err := in.AddTask(work(5), p)
	require.NoError(t, err)
	require.NoError(t, srv.Close())

	srv = open()
	t.Cleanup(func() { srv.Close() })
	again, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	r, te := again.Wait(context.Background(), &Task{ID: finished.ID, b: srv})
	require.Nil(t, te)
	assert.Equal(t, 11, r, "the result decodes after the restart")
	te = waitTask(t, &Task{ID: dying.ID, b: srv})
	require.NotNil(t, te)
	assert.Equal(t, msgConsumerGone, te.Message, "a task a consumer held fails at the reopen")
	c = again.Bind()
	got, spec, ok, err := again.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, queued.ID, got.ID)
	assert.Equal(t, 5, spec.N)
	j, err := srv.Get(queued.ID)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"urgent": 3}, j.Priority)
}

func TestRunInboxResumesAfterARestart(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.HTTPAddr, cfg.UnixSocket, cfg.SpoolDir, cfg.IPC = "", "", "", false
	srv, err := New(cfg)
	require.NoError(t, err)
	queued, err := srv.addNamed("run", []byte(`{"title":"later","n":4}`), nil, "", "")
	require.NoError(t, err)
	require.NoError(t, srv.Close())

	srv, err = New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(func() { srv.Close() })
	in, err := NewInbox[runInbox](srv, "run")
	require.NoError(t, err)
	r, te := in.Wait(context.Background(), &Task{ID: queued, b: srv})
	require.Nil(t, te)
	assert.Equal(t, 8, r)
}

func TestNewInboxRefusesBadTypesAndNames(t *testing.T) {
	srv := newTestServer(t, nil)
	_, err := NewInbox[wrongRun](srv, "a")
	require.ErrorIs(t, err, ErrBadInbox)
	assert.Contains(t, err.Error(), "Run must be func(context.Context, *jobserver.workSpec) (int, error)")
	_, err = NewInbox[wrongPriority](srv, "b")
	require.ErrorIs(t, err, ErrBadInbox)
	assert.Contains(t, err.Error(), "InboxPriority must be func() int")
	_, err = NewInbox[queueInbox](srv, "c")
	require.NoError(t, err)
	_, err = NewInbox[queueInbox](srv, "c")
	require.ErrorIs(t, err, ErrInboxExists)
	_, err = NewInbox[queueInbox](srv, DefaultInboxName)
	require.ErrorIs(t, err, ErrInboxExists)
}

func TestStopRefusesNewTasksAndCancelsQueued(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	task, err := in.AddTask(work(1))
	require.NoError(t, err)
	srv.Stop()
	srv.Stop()
	assert.Equal(t, Cancelled, task.State())
	_, err = in.AddTask(work(2))
	require.ErrorIs(t, err, ErrStopped)
	_, _, err = srv.Enqueue(Spec{Command: []string{"true"}})
	require.ErrorIs(t, err, ErrStopped)
}

func TestInboxOverTheRequestAPI(t *testing.T) {
	dir := shortDir(t)
	srv := newTestServer(t, func(c *Config) { c.UnixSocket = filepath.Join(dir, "s.sock") })
	ctx := context.Background()
	client, err := Dial(ctx, srv.Config())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	in, err := ClientInbox[queueInbox](client, "remote")
	require.NoError(t, err)
	p := in.NewPriority()
	p.Set(&in.Urgent, 1)
	task, err := in.AddTask(work(7), p)
	require.NoError(t, err)
	c := in.Bind()
	require.NotEmpty(t, c)
	got, spec, ok, err := in.Take(ctx, c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, 7, spec.N)
	require.NoError(t, in.Started(c, got, 1))
	require.NoError(t, in.Done(c, got, 70, nil))
	r, te := in.Wait(ctx, task)
	require.Nil(t, te)
	assert.Equal(t, 70, r)
	require.ErrorIs(t, in.Done(c, got, 1, nil), ErrNotRunning)
	require.ErrorIs(t, in.Done("c-stale", got, 1, nil), ErrNotConsumer)

	def := client.DefaultInbox()
	cmd, err := def.AddTask(&CommandSpec{Command: []string{"sh", "-c", "exit 0"}})
	require.NoError(t, err)
	res, te := def.Wait(ctx, cmd)
	require.Nil(t, te)
	assert.Equal(t, 0, res.ExitCode)

	_, err = ClientInbox[runInbox](client, "nope")
	require.ErrorIs(t, err, ErrBadInbox)
}
