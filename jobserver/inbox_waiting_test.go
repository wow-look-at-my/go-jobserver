package jobserver

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reportMore checks the waiting count, the step rules and the outcome label through an inbox.
func reportMore(t *testing.T, in *queueInbox) {
	t.Helper()
	ctx := context.Background()
	for range 2 {
		_, err := in.AddTask(work(1))
		require.NoError(t, err)
	}
	c := in.Bind()
	a, _, ok, err := in.Take(ctx, c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	b, _, ok, err := in.Take(ctx, c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	counts := func() TaskCounts {
		t.Helper()
		got, err := in.Counts()
		require.NoError(t, err)
		return got
	}
	assert.Equal(t, TaskCounts{Running: 2}, counts())
	w1, err := in.OpenWait(c, a, "a lock")
	require.NoError(t, err)
	w2, err := in.OpenWait(c, a, "another lock")
	require.NoError(t, err)
	assert.Equal(t, 1, counts().Waiting, "a task with two open waits counts once")
	require.NoError(t, in.CloseWait(c, a, w1))
	assert.Equal(t, 1, counts().Waiting, "one wait of the task is still open")
	require.NoError(t, in.CloseWait(c, a, w2))
	assert.Equal(t, 0, counts().Waiting)
	_, err = in.OpenWait(c, b, "held at the end")
	require.NoError(t, err)
	assert.Equal(t, 1, counts().Waiting)

	parent, err := in.Step(c, a, 0, "parent")
	require.NoError(t, err)
	child, err := in.Step(c, a, parent, "child")
	require.NoError(t, err)
	grandchild, err := in.Step(c, a, child, "grandchild")
	require.NoError(t, err)
	done, err := in.Step(c, a, parent, "done child")
	require.NoError(t, err)
	require.NoError(t, in.EndStep(c, a, done, StepSucceeded, ""))
	gaveUp, err := in.Step(c, a, 0, "given up")
	require.NoError(t, err)
	require.NoError(t, in.EndStep(c, a, gaveUp, StepAbandoned, ""), "a consumer can end a step as abandoned")
	require.NoError(t, in.EndStep(c, a, parent, StepSucceeded, ""))
	require.ErrorIs(t, in.EndStep(c, a, child, StepSucceeded, ""), ErrNoStep, "the sub-step ended with its parent")
	require.ErrorContains(t, in.EndStep(c, a, 0, StepStatus("lost"), ""), "step status")

	require.NoError(t, in.SetOutcome(c, a, "skipped"))
	require.ErrorContains(t, in.SetOutcome(c, a, strings.Repeat("x", maxOutcomeLen+1)), "an outcome is at most")
	require.NoError(t, in.Done(c, a, 0, nil))
	require.NoError(t, in.Done(c, b, 0, nil))
	assert.Equal(t, TaskCounts{Ended: 2}, counts(), "an ended task does not wait")

	d, err := in.Details(a)
	require.NoError(t, err)
	byID := map[int]TaskStep{}
	for _, st := range d.Steps {
		byID[st.ID] = st
	}
	assert.Equal(t, StepSucceeded, byID[parent].Status)
	for _, id := range []int{child, grandchild} {
		assert.Equal(t, StepAbandoned, byID[id].Status)
		assert.Equal(t, msgParentEnded, byID[id].Error)
	}
	assert.Equal(t, StepSucceeded, byID[done].Status, "a closed sub-step keeps its status")
	assert.Equal(t, StepAbandoned, byID[gaveUp].Status)
	assert.Empty(t, byID[gaveUp].Error)

	page, err := in.Tasks(0)
	require.NoError(t, err)
	outcomes := map[string]string{}
	for _, task := range page.Tasks {
		outcomes[task.ID] = task.Outcome
	}
	assert.Equal(t, map[string]string{a.ID: "skipped", b.ID: ""}, outcomes, "the list carries the outcome")
}

func TestWaitingStepsAndOutcomeLocally(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	reportMore(t, in)
}

func TestWaitingStepsAndOutcomeOverTheClient(t *testing.T) {
	dir := shortDir(t)
	srv := newTestServer(t, func(c *Config) { c.UnixSocket = filepath.Join(dir, "s.sock") })
	client, err := Dial(context.Background(), srv.Config())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	in, err := ClientInbox[queueInbox](client, "remote")
	require.NoError(t, err)
	reportMore(t, in)
}

func TestTheJournalKeepsTheWaitingCountAndTheOutcome(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	held := addQueued(t, st, "n")
	require.NoError(t, st.Start(held))
	_, err = st.OpenWait(held, "open")
	require.NoError(t, err)
	free := addQueued(t, st, "n")
	require.NoError(t, st.Start(free))
	w, err := st.OpenWait(free, "closed")
	require.NoError(t, err)
	require.NoError(t, st.CloseWait(free, w))
	require.NoError(t, st.SetOutcome(free, "partial"))
	want := TaskCounts{Running: 2, Waiting: 1}
	assert.Equal(t, want, st.Counts("n"))
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	assert.Equal(t, want, st.Counts("n"), "a replay counts the same waits")
	st.mu.Lock()
	err = st.compactLocked()
	st.mu.Unlock()
	require.NoError(t, err)
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	assert.Equal(t, want, st.Counts("n"), "a compaction keeps the open waits")
	j, _ := st.Job(free)
	assert.Equal(t, "partial", j.Outcome, "a compaction keeps the outcome")

	require.NoError(t, st.requeue(held, "resumed"))
	assert.Equal(t, TaskCounts{Queued: 1, Running: 1}, st.Counts("n"))
	require.NoError(t, st.Start(held))
	assert.Equal(t, 0, st.Counts("n").Waiting, "a requeue ends the waits of the run that stopped")
}

func TestTasksOfAnInboxNotOpenedAgainEnd(t *testing.T) {
	dir := t.TempDir()
	open := func(retention time.Duration) *Server {
		cfg := DefaultConfig(dir)
		cfg.HTTPAddr, cfg.UnixSocket, cfg.IPC = "", "", false
		cfg.Retention, cfg.PruneInterval = retention, time.Hour
		srv, err := New(cfg)
		require.NoError(t, err)
		require.NoError(t, srv.Start())
		return srv
	}
	srv := open(time.Hour)
	gone, err := NewInbox[queueInbox](srv, "gone")
	require.NoError(t, err)
	kept, err := NewInbox[queueInbox](srv, "kept")
	require.NoError(t, err)
	queued, err := gone.AddTask(work(1))
	require.NoError(t, err)
	_, err = gone.AddTask(work(2))
	require.NoError(t, err)
	c := gone.Bind()
	_, _, ok, err := gone.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	stays, err := kept.AddTask(work(3))
	require.NoError(t, err)
	require.NoError(t, srv.Close())

	srv = open(200 * time.Millisecond)
	t.Cleanup(func() { srv.Close() })
	_, err = NewInbox[queueInbox](srv, "kept")
	require.NoError(t, err)
	srv.prune()
	assert.Equal(t, TaskCounts{Queued: 1, Running: 1}, srv.store.Counts("gone"), "inside the retention period the tasks stay")

	time.Sleep(250 * time.Millisecond)
	srv.prune()
	assert.Equal(t, TaskCounts{Ended: 2}, srv.store.Counts("gone"), "past it, the queued and the running task end")
	j, ok := srv.store.Job(queued.ID)
	require.True(t, ok)
	assert.Equal(t, StateCancelled, j.State)
	assert.Equal(t, msgInboxGone, j.Error)
	j, ok = srv.store.Job(stays.ID)
	require.True(t, ok)
	assert.False(t, j.State.Terminal(), "a task of an open inbox stays")

	time.Sleep(250 * time.Millisecond)
	srv.prune()
	assert.Equal(t, TaskCounts{}, srv.store.Counts("gone"), "a later prune drops the ended tasks")
}
