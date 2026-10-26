package jobserver

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func finished(t *testing.T, st *Store, spec Spec, state State) *Job {
	t.Helper()
	j, err := st.Create(&spec)
	require.NoError(t, err)
	require.NoError(t, st.Start(j.ID))
	w, err := st.LogWriter(j.ID)
	require.NoError(t, err)
	_, err = w.Write([]byte("output\n"))
	require.NoError(t, err)
	require.NoError(t, st.Finish(j.ID, state, 0, "", nil, 7))
	return j
}

// A finished job leaves the table and its log goes with it, unless an unfinished job still depends on it.
func TestForgetKeepsWhatAnUnfinishedJobNeeds(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	done := finished(t, st, Spec{ID: "done", Command: []string{"x"}}, StateCompleted)
	needed := finished(t, st, Spec{ID: "needed", Command: []string{"x"}}, StateCompleted)
	_, err = st.Create(&Spec{ID: "waits", Command: []string{"x"}, Deps: []string{needed.ID}})
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"done", "needed"}, Expired(st.Jobs(), time.Now().Add(time.Hour)))
	assert.Empty(t, Expired(st.Jobs(), time.Now().Add(-time.Hour)), "nothing finished an hour ago")

	gone, err := st.Forget([]string{"done", "needed", "waits"})
	require.NoError(t, err)
	assert.Equal(t, []string{"done"}, gone, "a job an unfinished job depends on stays, and an unfinished job is never forgotten")
	_, ok := st.Job(done.ID)
	assert.False(t, ok)
	_, err = os.Stat(st.LogPath(done.ID))
	assert.True(t, os.IsNotExist(err), "the log of a forgotten job is removed")
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	_, ok = st.Job(done.ID)
	assert.False(t, ok, "the forget is journaled")
	assert.Equal(t, []string{"needed", "waits"}, st.Order())
	assert.Equal(t, 1, st.Forgotten())
}

// The rewritten journal rebuilds every job as it stood, and holds nothing of the forgotten ones.
func TestCompactRewritesTheJournalFromTheTable(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		finished(t, st, Spec{Command: []string{"old"}}, StateCompleted)
	}
	failed := finished(t, st, Spec{ID: "failed", Command: []string{"x"}, Policy: JobPolicy{Cost: 2}}, StateFailed)
	require.NoError(t, st.FinishCPU(failed.ID, 1.5))
	task, err := st.Create(&Spec{ID: "task", Task: &TaskSpec{Kind: "hook-run", Data: json.RawMessage(`{"a":1}`)}, Deps: []string{failed.ID}})
	require.NoError(t, err)
	mon, err := st.Create(&Spec{ID: "mon", Monitor: &MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": "/x"}}})
	require.NoError(t, err)
	require.NoError(t, st.SetPaused(true))
	var old []string
	for _, j := range st.Jobs() {
		if j.Command != nil && j.Command[0] == "old" {
			old = append(old, j.ID)
		}
	}
	_, err = st.Forget(old)
	require.NoError(t, err)
	before := st.Jobs()
	big, err := os.Stat(st.journalPath())
	require.NoError(t, err)

	require.NoError(t, st.Compact())
	assert.Zero(t, st.Forgotten())
	small, err := os.Stat(st.journalPath())
	require.NoError(t, err)
	assert.Less(t, small.Size(), big.Size())
	require.NoError(t, st.SetPolicy(task.ID, JobPolicy{Cost: 3}), "the store appends to the new file")
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	assert.True(t, st.Paused())
	after := st.Jobs()
	require.Len(t, after, len(before))
	for i, j := range after {
		want := before[i]
		if j.ID == task.ID {
			want.Policy = JobPolicy{Cost: 3}
		}
		assert.Equal(t, want.ID, j.ID)
		assert.Equal(t, want.State, j.State, j.ID)
		assert.Equal(t, want.Deps, j.Deps, j.ID)
		assert.Equal(t, want.Task, j.Task, j.ID)
		assert.Equal(t, want.Monitor, j.Monitor, j.ID)
		assert.Equal(t, want.Policy, j.Policy, j.ID)
		assert.Equal(t, want.CPUSeconds, j.CPUSeconds, j.ID)
		assert.Equal(t, want.Attempts, j.Attempts, j.ID)
		assert.True(t, want.Finished.Equal(j.Finished), j.ID)
	}
	_, ok := st.Job(mon.ID)
	assert.True(t, ok)
}

// The server's retention pass forgets what finished before the cutoff.
func TestTheServerForgetsJobsPastTheRetention(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Retention = time.Hour })
	j, _, err := srv.Enqueue(Spec{ID: "quick", Command: []string{"true"}})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateCompleted)

	srv.prune(time.Now())
	_, err = srv.Get(j.ID)
	require.NoError(t, err, "a job inside the retention stays")
	srv.prune(time.Now().Add(2 * time.Hour))
	_, err = srv.Get(j.ID)
	assert.ErrorIs(t, err, ErrNotFound)
}
