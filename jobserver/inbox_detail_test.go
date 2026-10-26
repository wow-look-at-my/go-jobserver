package jobserver

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTakeAndCountsReadOnlyTheirOwnInbox(t *testing.T) {
	srv := newTestServer(t, nil)
	big, err := NewInbox[queueInbox](srv, "big")
	require.NoError(t, err)
	small, err := NewInbox[queueInbox](srv, "small")
	require.NoError(t, err)
	for range 2000 {
		addEnded(t, srv.store, "big")
	}
	for range 500 {
		_, err := big.AddTask(work(1))
		require.NoError(t, err)
	}
	task, err := small.AddTask(work(2))
	require.NoError(t, err)
	c := small.Bind()

	before := srv.store.visits.Load()
	got, _, ok, err := small.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, task.ID, got.ID)
	visited := srv.store.visits.Load() - before
	assert.LessOrEqual(t, visited, int64(10), "a take reads the tasks of its own inbox, not the others")

	before = srv.store.visits.Load()
	counts, err := small.Counts()
	require.NoError(t, err)
	assert.Equal(t, TaskCounts{Running: 1}, counts)
	assert.Equal(t, int64(1), srv.store.visits.Load()-before)
	counts, err = big.Counts()
	require.NoError(t, err)
	assert.Equal(t, TaskCounts{Queued: 500, Ended: 2000}, counts)
}

func TestRestartReplaysQueuedTasksAfterAPrune(t *testing.T) {
	dir := t.TempDir()
	open := func() *Server {
		cfg := DefaultConfig(dir)
		cfg.HTTPAddr, cfg.UnixSocket, cfg.IPC = "", "", false
		cfg.RetainCount = 1
		srv, err := New(cfg)
		require.NoError(t, err)
		require.NoError(t, srv.Start())
		return srv
	}
	srv := open()
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	for range 3 {
		addEnded(t, srv.store, "q")
	}
	queued, err := in.AddTask(work(9))
	require.NoError(t, err)
	srv.prune()
	counts, err := in.Counts()
	require.NoError(t, err)
	assert.Equal(t, TaskCounts{Queued: 1, Ended: 1}, counts)
	require.NoError(t, srv.Close())

	srv = open()
	t.Cleanup(func() { srv.Close() })
	again, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	c := again.Bind()
	got, spec, ok, err := again.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok, "the next consumer gets the queued task")
	assert.Equal(t, queued.ID, got.ID)
	assert.Equal(t, 9, spec.N)
	counts, err = again.Counts()
	require.NoError(t, err)
	assert.Equal(t, TaskCounts{Running: 1, Ended: 1}, counts)
}

// reportOn runs every detail call on a checked-out task and checks what reads back.
func reportOn(t *testing.T, in *queueInbox) {
	t.Helper()
	ctx := context.Background()
	task, err := in.AddTask(work(3))
	require.NoError(t, err)
	before, err := in.Tasks(0)
	require.NoError(t, err)
	c := in.Bind()
	got, _, ok, err := in.Take(ctx, c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	require.ErrorIs(t, in.SetTitle("c-stale", got, "x"), ErrNotConsumer)
	require.NoError(t, in.SetTitle(c, got, "pr 12"))
	outer, err := in.Step(c, got, 0, "fetch")
	require.NoError(t, err)
	inner, err := in.Step(c, got, outer, "clone")
	require.NoError(t, err)
	require.NoError(t, in.EndStep(c, got, inner, StepSucceeded, ""))
	require.ErrorIs(t, in.EndStep(c, got, inner, StepSucceeded, ""), ErrNoStep)
	require.ErrorContains(t, in.EndStep(c, got, outer, StepFailed, ""), "a failed step needs a message")
	left, err := in.Step(c, got, 0, "left open")
	require.NoError(t, err)
	wait, err := in.OpenWait(c, got, "a lock")
	require.NoError(t, err)
	require.NoError(t, in.CloseWait(c, got, wait))
	require.NoError(t, in.Log(c, got, "hello"))

	d, err := in.Details(got)
	require.NoError(t, err)
	assert.Equal(t, "pr 12", d.Title)
	assert.Equal(t, Running, d.State)
	assert.Equal(t, in.Name(), d.Inbox)
	require.Len(t, d.Steps, 3)
	assert.Equal(t, TaskStep{ID: inner, Parent: outer, Title: "clone", Start: d.Steps[1].Start, End: d.Steps[1].End, Status: StepSucceeded}, d.Steps[1])
	assert.True(t, d.Steps[0].End.IsZero())
	require.Len(t, d.Waits, 1)
	assert.Equal(t, "a lock", d.Waits[0].Reason)
	assert.False(t, d.Waits[0].End.IsZero())
	assert.Equal(t, int64(len("hello\n")), d.LogBytes)
	out, err := in.ReadLog(got, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, "hello\n", string(out))

	changed, err := in.Tasks(before.Seq)
	require.NoError(t, err)
	require.Len(t, changed.Tasks, 1, "only the task that changed")
	assert.Equal(t, task.ID, changed.Tasks[0].ID)

	require.NoError(t, in.Done(c, got, 1, nil))
	d, err = in.Details(got)
	require.NoError(t, err)
	assert.Equal(t, Succeeded, d.State)
	assert.False(t, d.Finished.IsZero())
	assert.Equal(t, StepAbandoned, d.Steps[left-1].Status, "a step open at the end is abandoned")
	require.ErrorIs(t, in.Log(c, got, "late"), ErrNotRunning)

	all, err := in.Tasks(0)
	require.NoError(t, err)
	require.Len(t, all.Tasks, 1)
	assert.Equal(t, "pr 12", all.Tasks[0].Title)
}

func TestTaskDetailsLocally(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	reportOn(t, in)
}

func TestTaskDetailsOverTheClient(t *testing.T) {
	dir := shortDir(t)
	srv := newTestServer(t, func(c *Config) { c.UnixSocket = filepath.Join(dir, "s.sock") })
	client, err := Dial(context.Background(), srv.Config())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	in, err := ClientInbox[queueInbox](client, "remote")
	require.NoError(t, err)
	reportOn(t, in)
}

// titledInbox has a Run that names its task through the context.
type titledInbox struct {
	Inbox[*workSpec, int]
}

func (i *titledInbox) Run(ctx context.Context, s *workSpec) (int, error) {
	t := TaskFromContext(ctx)
	if err := i.SetTitle("", t, "run titled"); err != nil {
		return 0, err
	}
	step, err := i.Step("", t, 0, "work")
	if err != nil {
		return 0, err
	}
	return s.N, i.EndStep("", t, step, StepSucceeded, "")
}

func TestRunReportsOnItsTaskThroughTheContext(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[titledInbox](srv, "titled")
	require.NoError(t, err)
	task, err := in.AddTask(work(4))
	require.NoError(t, err)
	r, te := in.Wait(context.Background(), task)
	require.Nil(t, te)
	assert.Equal(t, 4, r)
	d, err := in.Details(task)
	require.NoError(t, err)
	assert.Equal(t, "run titled", d.Title)
	require.Len(t, d.Steps, 1)
	assert.Equal(t, StepSucceeded, d.Steps[0].Status)
}

func TestNegativeRetentionIsRefused(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())
	cfg.Retention = -time.Second
	_, err := New(cfg)
	require.Error(t, err)
}
