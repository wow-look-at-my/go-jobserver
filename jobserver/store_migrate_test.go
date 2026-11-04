package jobserver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// copyLegacyFixture copies the journal and the log that the release before the database wrote into a new directory.
// testdata/legacy holds the output of that release's own code, not of legacyRecords.
func copyLegacyFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, logsDirName), 0o755))
	for _, name := range []string{journalName, filepath.Join(logsDirName, "cmd-done.log")} {
		b, err := os.ReadFile(filepath.Join("testdata", "legacy", name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), b, 0o644))
	}
	return dir
}

// assertLegacyTasks checks every task of the fixture, as the store reads it
// after the move.
func assertLegacyTasks(t *testing.T, st *Store) {
	t.Helper()
	assert.Equal(t, []string{"cmd-done", "cmd-cached", "cmd-running", "cmd-draft", "cmd-blocked", "n-done", "n-running",
		"n-next", "old-failed", "after"}, st.Order())
	assert.True(t, st.Paused())

	get := func(id string) *Job {
		t.Helper()
		j, ok := st.Job(id)
		require.True(t, ok, "task %s", id)
		return j
	}
	st.Read(func() {
		done := get("cmd-done")
		assert.Equal(t, StateCompleted, done.State)
		assert.Equal(t, "build", done.Name)
		assert.Equal(t, []string{"make", "all"}, done.Command)
		assert.Equal(t, []string{"in.c"}, done.Inputs)
		assert.Equal(t, []string{"out.o"}, done.Outputs)
		assert.Equal(t, []string{"A=1"}, done.Env)
		assert.Equal(t, "/work", done.Dir)
		assert.Equal(t, "k1", done.Key)
		assert.Equal(t, 2.0, done.Policy.Cost)
		assert.Equal(t, "builds it", done.Description)
		assert.Equal(t, time.Minute, done.Timeout)
		assert.Equal(t, map[string]int{"priority": 3}, done.Priority)
		assert.Equal(t, "sha256:ident", done.Identity)
		assert.Equal(t, []Artifact{{Path: "out.o", Size: 3, SHA256: "sha256:o"}}, done.Artifacts)
		assert.Equal(t, 1.5, done.CPUSeconds)
		assert.Equal(t, int64(6), done.LogBytes)
		assert.Equal(t, 1, done.Attempts)
		assert.False(t, done.Started.IsZero())
		assert.False(t, done.Finished.IsZero())

		cached := get("cmd-cached")
		assert.Equal(t, StateCached, cached.State)
		assert.Equal(t, "cmd-done", cached.CacheOf)

		running := get("cmd-running")
		assert.Equal(t, StateFailed, running.State, "a command task that ran when the process died fails")
		assert.Equal(t, "interrupted by daemon restart", running.Error)

		assert.Equal(t, StateDraft, get("cmd-draft").State)
		blocked := get("cmd-blocked")
		assert.Equal(t, StateBlocked, blocked.State)
		assert.Equal(t, "a dependency failed", blocked.Error)

		nd := get("n-done")
		assert.Equal(t, StateCompleted, nd.State)
		assert.Equal(t, "events", nd.Inbox)
		assert.Equal(t, "event renamed", nd.Name)
		assert.JSONEq(t, `{"title":"event","n":7}`, string(nd.Spec))
		assert.JSONEq(t, `{"ok":true}`, string(nd.Result))
		assert.Equal(t, map[string]int{"urgency": 1}, nd.Priority)
		assert.Equal(t, "partial", nd.Outcome)
		assert.Equal(t, "worker", nd.ConsumerName)
		require.Len(t, nd.Steps, 2)
		assert.Equal(t, "outer", nd.Steps[0].Title)
		assert.Equal(t, StepSucceeded, nd.Steps[0].Status)
		assert.Equal(t, StepAbandoned, nd.Steps[1].Status)
		require.Len(t, nd.Waits, 1)
		assert.Equal(t, "lock", nd.Waits[0].Kind)
		assert.Equal(t, map[string]string{"by": "x"}, nd.Waits[0].Attrs)

		nr := get("n-running")
		assert.Equal(t, StateRunning, nr.State, "a named inbox decides about its running task when it opens")
		require.Len(t, nr.Steps, 1)
		assert.True(t, nr.Steps[0].End.IsZero())
		assert.Equal(t, 1, nr.openWaits)

		next := get("n-next")
		assert.Equal(t, StateActive, next.State)
		assert.Equal(t, triggerSuccess, next.Trigger)
		assert.Equal(t, []string{"n-done"}, next.Deps)

		old := get("old-failed")
		assert.Equal(t, StateFailed, old.State)
		assert.Equal(t, "boom", old.Error)
		assert.Equal(t, StateActive, get("after").State)
	})
	assert.Equal(t, TaskCounts{Queued: 1, Running: 1, Waiting: 1, Ended: 1}, st.Counts("events"))
	assert.Equal(t, "cmd-cached", st.IdentityIndex()["sha256:ident"])
	out, err := st.ReadLog("cmd-done", 0, 100)
	require.NoError(t, err)
	assert.Equal(t, "hello\n", string(out))
}

func TestOpenMovesAJournalOfTheReleaseBeforeIntoTheDatabase(t *testing.T) {
	dir := copyLegacyFixture(t)
	st, err := OpenStore(dir)
	require.NoError(t, err)
	assertLegacyTasks(t, st)
	assert.NoFileExists(t, filepath.Join(dir, journalName))
	assert.FileExists(t, filepath.Join(dir, migratedName))
	assert.FileExists(t, filepath.Join(dir, dbName))
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	assertLegacyTasks(t, st)
}

// A large journal moves in one transaction, and every task reads back from the database.
func TestALargeJournalMovesWhole(t *testing.T) {
	dir := t.TempDir()
	const n = 3000
	spec := bigSpec(4096)
	start := time.Now().Add(-time.Hour)
	jobs := make([]*Job, 0, n)
	for i := range n {
		at := start.Add(time.Duration(i) * time.Millisecond)
		jobs = append(jobs, &Job{ID: fmt.Sprintf("t-%05d", i), Name: "t", Inbox: "big", Spec: spec, State: StateCompleted,
			Created: at, Started: at, Attempts: 1, Finished: at.Add(time.Millisecond), Result: json.RawMessage(`1`),
			Steps: []TaskStep{{ID: 1, Title: "s", Start: at, End: at, Status: StepSucceeded}}})
	}
	writeLegacyJournal(t, dir, jobs)
	st, err := OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	assert.Equal(t, n, st.Counts("big").Ended)
	got := st.InboxJobs("big")
	require.Len(t, got, n)
	st.Read(func() {
		assert.Equal(t, "t-02999", got[n-1].ID)
		assert.JSONEq(t, string(spec), string(got[n-1].Spec))
		assert.Len(t, got[n-1].Steps, 1)
	})
	st.mu.RLock()
	held := len(st.jobs)
	st.mu.RUnlock()
	assert.Zero(t, held, "memory keeps no ended task after the move")
}
