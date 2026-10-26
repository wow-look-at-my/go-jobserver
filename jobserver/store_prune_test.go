package jobserver

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// addEnded adds a task to an inbox of the store and ends it.
func addEnded(t *testing.T, st *Store, inbox string) string {
	t.Helper()
	j, err := st.create(&Spec{Name: "t"}, &taskMeta{Inbox: inbox, Spec: []byte(`{"title":"t"}`)})
	require.NoError(t, err)
	require.NoError(t, st.Finish(j.ID, StateCompleted, 0, "", nil, 0))
	return j.ID
}

func addQueued(t *testing.T, st *Store, inbox string, deps ...string) string {
	t.Helper()
	j, err := st.create(&Spec{Name: "q", Deps: deps}, &taskMeta{Inbox: inbox, Spec: []byte(`{"title":"q","n":5}`)})
	require.NoError(t, err)
	return j.ID
}

func ids(jobs []*Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.ID)
	}
	return out
}

func TestPruneDropsEndedTasksPastTheRetention(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	old := addEnded(t, st, "a")
	w, err := st.LogWriter(old)
	require.NoError(t, err)
	_, err = w.Write([]byte("line\n"))
	require.NoError(t, err)
	live := addQueued(t, st, "a")

	dropped, err := st.Prune(time.Now(), time.Hour, 1000)
	require.NoError(t, err)
	assert.Empty(t, dropped, "a task that ended now stays")

	dropped, err = st.Prune(time.Now().Add(2*time.Hour), time.Hour, 1000)
	require.NoError(t, err)
	assert.Equal(t, []string{old}, dropped)
	_, ok := st.Job(old)
	assert.False(t, ok)
	_, err = os.Stat(st.LogPath(old))
	assert.True(t, errors.Is(err, os.ErrNotExist), "the log goes with the task")
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	assert.Equal(t, []string{live}, st.Order(), "the compacted journal holds only what stayed")
}

func TestPruneKeepsTheNewestEndedTasksOfEachInbox(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	var a []string
	for range 5 {
		a = append(a, addEnded(t, st, "a"))
		time.Sleep(time.Millisecond)
	}
	b := addEnded(t, st, "b")
	dropped, err := st.Prune(time.Now(), time.Hour, 2)
	require.NoError(t, err)
	assert.ElementsMatch(t, a[:3], dropped, "the oldest go first")
	assert.Equal(t, append(append([]string(nil), a[3:]...), b), st.Order())
	assert.Equal(t, 2, st.Counts("a").Ended)
	assert.Equal(t, 1, st.Counts("b").Ended, "another inbox has its own bound")
}

func TestPruneKeepsATaskATriggerOrAWaiterNeeds(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	dep := addEnded(t, st, "a")
	j, err := st.create(&Spec{Name: "after", Deps: []string{dep}}, &taskMeta{Inbox: "b", Trigger: triggerFail})
	require.NoError(t, err)
	waited := addEnded(t, st, "a")
	require.NoError(t, st.hold(waited))
	later := time.Now().Add(2 * time.Hour)

	dropped, err := st.Prune(later, time.Hour, 0)
	require.NoError(t, err)
	assert.Empty(t, dropped, "a live trigger and a waiter keep their tasks")

	require.NoError(t, st.Finish(j.ID, StateCancelled, -1, msgNeverFires, nil, 0))
	st.release(waited)
	dropped, err = st.Prune(later, time.Hour, 0)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{dep, waited, j.ID}, dropped)
}

func TestCompactionRebuildsEveryField(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	gone := addEnded(t, st, "a")
	done, err := st.create(&Spec{Name: "done", Command: []string{"true"}, Env: []string{"A=1"}, Key: "k",
		Policy: JobPolicy{Cost: 2}}, &taskMeta{Description: "d", Timeout: time.Minute, Priority: map[string]int{"priority": 3}})
	require.NoError(t, err)
	require.NoError(t, st.Start(done.ID))
	require.NoError(t, st.SetIdentity(done.ID, "ident", ""))
	require.NoError(t, st.FinishCPU(done.ID, 1.5))
	require.NoError(t, st.Finish(done.ID, StateFailed, 3, "exit 3", []Artifact{{Path: "o", Size: 1, SHA256: "x"}}, 0))
	named := addQueued(t, st, "n")
	require.NoError(t, st.Start(named))
	require.NoError(t, st.SetTitle(named, "renamed"))
	step, err := st.OpenStep(named, 0, "outer")
	require.NoError(t, err)
	_, err = st.OpenStep(named, step, "inner")
	require.NoError(t, err)
	require.NoError(t, st.EndStep(named, step, StepSucceeded, ""))
	w, err := st.OpenWait(named, "a lock")
	require.NoError(t, err)
	require.NoError(t, st.CloseWait(named, w))
	result := addQueued(t, st, "n")
	require.NoError(t, st.SetResult(result, []byte(`42`)))
	require.NoError(t, st.Finish(result, StateCompleted, 0, "", nil, 0))
	_, err = st.create(&Spec{Name: "draft", Command: []string{"true"}, Draft: true}, nil)
	require.NoError(t, err)
	blocked := addQueued(t, st, "n")
	st.mu.Lock()
	require.NoError(t, st.setState(blocked, StateBlocked, "a dependency failed"))
	st.mu.Unlock()
	require.NoError(t, st.SetPaused(true))
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	before := st.Jobs()
	for _, id := range []string{done.ID, result, blocked} {
		require.NoError(t, st.hold(id))
	}
	dropped, err := st.Prune(time.Now(), time.Hour, 1000)
	require.NoError(t, err)
	require.Empty(t, dropped)
	dropped, err = st.Prune(time.Now().Add(2*time.Hour), time.Hour, 1000)
	require.NoError(t, err)
	require.Equal(t, []string{gone}, dropped)
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	after := st.Jobs()
	assert.True(t, st.Paused())
	want := before[1:]
	require.Equal(t, ids(want), ids(after))
	for i := range want {
		want[i].Seq, after[i].Seq = 0, 0
		assert.Equal(t, want[i], after[i], "job %s", want[i].ID)
	}
}

func TestCompactionThatStopsBeforeTheRenameKeepsTheJournal(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	gone := addEnded(t, st, "a")
	kept := addQueued(t, st, "a")
	st.compactHook = func(string) error { return errors.New("the process died here") }
	_, err = st.Prune(time.Now().Add(2*time.Hour), time.Hour, 1000)
	require.ErrorContains(t, err, "the process died here")
	st.compactHook = nil
	_, err = os.Stat(st.compactPath())
	assert.True(t, errors.Is(err, os.ErrNotExist), "the half compaction is removed")
	later := addQueued(t, st, "a")
	require.NoError(t, st.Close())

	// A crash leaves a half-written compaction file behind. The next open ignores it.
	require.NoError(t, os.WriteFile(st.compactPath(), []byte("half a frame"), 0o644))
	st, err = OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	assert.Equal(t, []string{gone, kept, later}, st.Order(), "the old journal is whole, and appends after the failure stay")
	_, err = os.Stat(st.compactPath())
	assert.True(t, errors.Is(err, os.ErrNotExist))
}
