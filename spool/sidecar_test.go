package spool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

// serverIn starts a server on dir. The caller closes it.
func serverIn(t *testing.T, dir string) *jobserver.Server {
	t.Helper()
	cfg := jobserver.DefaultConfig(dir)
	cfg.HTTPAddr, cfg.UnixSocket, cfg.IPC = "", "", false
	srv, err := jobserver.New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	return srv
}

// tasksIn lists the tasks of one inbox.
func tasksIn(srv *jobserver.Server, inbox string) []*jobserver.Job {
	var out []*jobserver.Job
	for _, j := range srv.List() {
		if j.Inbox == inbox {
			out = append(out, j)
		}
	}
	return out
}

// waitActive waits until the spool waits on the task of a file.
func waitActive(t *testing.T, s *Spool, name string) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.active[name]
	}, 10*time.Second, 5*time.Millisecond)
}

// takeOne takes one task, settles it, and returns its hook.
func takeOne(t *testing.T, in *deliveryInbox) string {
	t.Helper()
	c := in.Bind()
	task, spec, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, in.Done(c, task, "ok", nil))
	return spec.Hook
}

func TestReopenWaitsOnTheTaskAFileBecame(t *testing.T) {
	srv := newServer(t)
	in, err := jobserver.NewInbox[deliveryInbox](srv, "hooks")
	require.NoError(t, err)
	dir := t.TempDir()
	s := open(t, dir, in)
	write(t, filepath.Join(dir, "d.json"), `{"hook":"once"}`, time.Time{})
	waitActive(t, s, "d.json")
	require.FileExists(t, filepath.Join(dir, "d.json"+taskSuffix), "the task ID is recorded before the wait")
	require.NoError(t, s.Close())

	s2 := open(t, dir, in)
	waitActive(t, s2, "d.json")
	assert.Len(t, tasksIn(srv, "hooks"), 1, "the reopened spool added the file again")
	assert.Equal(t, "once", takeOne(t, in))
	eventuallyExists(t, filepath.Join(dir, DoneDir, "d.json"))
	assert.NoFileExists(t, filepath.Join(dir, "d.json"+taskSuffix), "the sidecar goes with the file")
}

func TestRestartWaitsOnTheJournaledTask(t *testing.T) {
	state := t.TempDir()
	dir := t.TempDir()
	srv := serverIn(t, state)
	in, err := jobserver.NewInbox[deliveryInbox](srv, "hooks")
	require.NoError(t, err)
	s, err := Open(dir, in, WithInterval(10*time.Millisecond))
	require.NoError(t, err)
	write(t, filepath.Join(dir, "d.json"), `{"hook":"crash"}`, time.Time{})
	waitActive(t, s, "d.json")
	// The process dies with the task queued: the spool and the server stop, and the task stays in the journal.
	require.NoError(t, s.Close())
	require.NoError(t, srv.Close())

	srv2 := serverIn(t, state)
	t.Cleanup(func() { srv2.Close() })
	in2, err := jobserver.NewInbox[deliveryInbox](srv2, "hooks")
	require.NoError(t, err)
	require.Len(t, tasksIn(srv2, "hooks"), 1, "the journal holds the task")
	s2 := open(t, dir, in2)
	waitActive(t, s2, "d.json")
	assert.Len(t, tasksIn(srv2, "hooks"), 1, "exactly one task after the restart")
	assert.Equal(t, "crash", takeOne(t, in2))
	eventuallyExists(t, filepath.Join(dir, DoneDir, "d.json"))
}

func writeSidecar(t *testing.T, dir, name, task string, mtime int64) {
	t.Helper()
	data, err := json.Marshal(sidecarBody{Task: task, Mtime: mtime})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+taskSuffix), data, 0o644))
}

func TestSidecarForAnUnknownTaskAddsTheFile(t *testing.T) {
	srv := newServer(t)
	dir := t.TempDir()
	stamp := time.Now().Add(-time.Minute)
	write(t, filepath.Join(dir, "lost.cmd"), "echo lost\n", stamp)
	// The add never reached the journal.
	writeSidecar(t, dir, "lost.cmd", "j-never", stamp.UnixNano())
	open(t, dir, srv.DefaultInbox())

	eventuallyExists(t, filepath.Join(dir, DoneDir, "lost.cmd"))
	assert.Len(t, srv.List(), 1)
	assert.NoFileExists(t, filepath.Join(dir, "lost.cmd"+taskSuffix))
}

func TestSidecarForAnotherVersionOfTheFileAddsItAgain(t *testing.T) {
	srv := newServer(t)
	old, err := srv.DefaultInbox().AddTask(&jobserver.CommandSpec{Command: []string{"echo", "old"}})
	require.NoError(t, err)
	dir := t.TempDir()
	stamp := time.Now().Add(-time.Minute)
	write(t, filepath.Join(dir, "v.cmd"), "echo new\n", stamp)
	writeSidecar(t, dir, "v.cmd", old.ID, stamp.Add(-time.Hour).UnixNano())
	open(t, dir, srv.DefaultInbox())

	eventuallyExists(t, filepath.Join(dir, DoneDir, "v.cmd"))
	assert.Len(t, srv.List(), 2, "a changed file is a new task, not the old one")
}

func TestSidecarWithNoLookupAddsTheFile(t *testing.T) {
	dir := t.TempDir()
	stamp := time.Now().Add(-time.Minute)
	write(t, filepath.Join(dir, "x.cmd"), "echo x\n", stamp)
	writeSidecar(t, dir, "x.cmd", "j-1", stamp.UnixNano())
	var logs lineLog
	stopped := &stoppedInbox{}
	idle(t, dir, stopped, &logs)
	assert.Equal(t, 1, stopped.count())
	assert.Len(t, logs.containing("no Lookup"), 1)
}

type badLookup struct{ stoppedInbox }

func (*badLookup) Lookup(int) (*jobserver.Task, error) { return nil, nil }

func TestOpenRefusesALookupOfTheWrongShape(t *testing.T) {
	_, err := Open(t.TempDir(), &badLookup{})
	assert.ErrorIs(t, err, ErrBadInbox)
}
