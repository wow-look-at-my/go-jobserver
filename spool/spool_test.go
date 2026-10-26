package spool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
	"github.com/wow-look-at-my/go-jobserver/priority"
)

// deliverySpec is a task spec with more fields than TaskSpec, as a parked webhook delivery has.
type deliverySpec struct {
	jobserver.TaskSpec
	Hook    string              `json:"hook"`
	Headers map[string][]string `json:"headers"`
	Body    []byte              `json:"body"`
}

// deliveryInbox has no Run, so the test takes its tasks.
type deliveryInbox struct {
	jobserver.Inbox[*deliverySpec, string]
}

// stoppedInbox refuses every task as a stopped server does.
type stoppedInbox struct {
	mu    sync.Mutex
	calls int
}

func (i *stoppedInbox) AddTask(*jobserver.CommandSpec) (*jobserver.Task, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
	return nil, jobserver.ErrStopped
}

func (i *stoppedInbox) count() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.calls
}

// lineLog collects the status lines a spool writes.
type lineLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *lineLog) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *lineLog) containing(sub string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range l.lines {
		if strings.Contains(line, sub) {
			out = append(out, line)
		}
	}
	return out
}

func newServer(t *testing.T) *jobserver.Server {
	t.Helper()
	cfg := jobserver.DefaultConfig(t.TempDir())
	cfg.HTTPAddr, cfg.UnixSocket, cfg.IPC = "", "", false
	srv, err := jobserver.New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(func() { srv.Close() })
	return srv
}

func open(t *testing.T, dir string, inbox any, opts ...Option) *Spool {
	t.Helper()
	s, err := Open(dir, inbox, append([]Option{WithInterval(10 * time.Millisecond)}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// idle opens a spool that scans only when the test calls scan.
func idle(t *testing.T, dir string, inbox any, logs *lineLog) *Spool {
	t.Helper()
	return open(t, dir, inbox, WithInterval(time.Hour), WithLogf(logs.logf))
}

// settle waits until no task of the spool is still running.
func settle(t *testing.T, s *Spool) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.active) == 0
	}, 10*time.Second, 5*time.Millisecond)
}

func eventuallyExists(t *testing.T, paths ...string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, p := range paths {
			if _, err := os.Stat(p); err != nil {
				return false
			}
		}
		return true
	}, 10*time.Second, 5*time.Millisecond, "missing one of %v", paths)
}

func write(t *testing.T, path, body string, stamp time.Time) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	if !stamp.IsZero() {
		require.NoError(t, os.Chtimes(path, stamp, stamp))
	}
}

func TestDefaultInboxReadsEachForm(t *testing.T) {
	srv := newServer(t)
	dir := t.TempDir()
	open(t, dir, srv.DefaultInbox())
	write(t, filepath.Join(dir, "hello.cmd"), "echo hello\n", time.Time{})
	write(t, filepath.Join(dir, "spec.json"), `{"title":"spec-job","command":["true"]}`, time.Time{})
	write(t, filepath.Join(dir, "argv.json"), `["echo","hi"]`, time.Time{})
	write(t, filepath.Join(dir, "quoted.json"), `"echo quoted"`, time.Time{})

	eventuallyExists(t,
		filepath.Join(dir, DoneDir, "hello.cmd"),
		filepath.Join(dir, DoneDir, "spec.json"),
		filepath.Join(dir, DoneDir, "argv.json"),
		filepath.Join(dir, DoneDir, "quoted.json"))
	got := map[string][]string{}
	for _, j := range srv.List() {
		got[j.Name] = j.Command
	}
	assert.Equal(t, map[string][]string{
		"hello":    {"echo", "hello"},
		"spec-job": {"true"},
		"argv":     {"echo", "hi"},
		"quoted":   {"echo", "quoted"},
	}, got, "each form makes one task, titled by its file when it has no title")
}

func TestFailedTaskMovesToFailedWithTheError(t *testing.T) {
	srv := newServer(t)
	dir := t.TempDir()
	open(t, dir, srv.DefaultInbox())
	write(t, filepath.Join(dir, "fail.cmd"), "false\n", time.Time{})

	reason := filepath.Join(dir, FailedDir, "fail.cmd"+errorSuffix)
	eventuallyExists(t, filepath.Join(dir, FailedDir, "fail.cmd"), reason)
	body, err := os.ReadFile(reason)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(body), jobserver.Failed.String()+": "), "the reason is the TaskError text: %q", body)
	assert.NoFileExists(t, filepath.Join(dir, DoneDir, "fail.cmd"))
}

func TestRefusedSpecMovesToFailedWithTheError(t *testing.T) {
	srv := newServer(t)
	dir := t.TempDir()
	open(t, dir, srv.DefaultInbox())
	write(t, filepath.Join(dir, "nocmd.json"), `{"title":"x"}`, time.Time{})

	reason := filepath.Join(dir, FailedDir, "nocmd.json"+errorSuffix)
	eventuallyExists(t, filepath.Join(dir, FailedDir, "nocmd.json"), reason)
	body, err := os.ReadFile(reason)
	require.NoError(t, err)
	assert.Contains(t, string(body), jobserver.ErrNoCommand.Error())
	assert.Empty(t, srv.List(), "a refused spec makes no task")
}

func TestCorruptFileIsQuarantinedNotBlocking(t *testing.T) {
	srv := newServer(t)
	dir := t.TempDir()
	old := time.Now().Add(-time.Hour)
	write(t, filepath.Join(dir, "bad.json"), "{not json", old)
	write(t, filepath.Join(dir, "empty.cmd"), "   \n", old.Add(time.Second))
	write(t, filepath.Join(dir, "good.cmd"), "echo good\n", old.Add(2*time.Second))
	open(t, dir, srv.DefaultInbox())

	eventuallyExists(t, filepath.Join(dir, DoneDir, "good.cmd"))
	for _, name := range []string{"bad.json", "empty.cmd"} {
		assert.FileExists(t, filepath.Join(dir, name+corruptSuffix), "the corrupt file is set aside, not deleted")
		assert.FileExists(t, filepath.Join(dir, name+corruptSuffix+errorSuffix), "the reason sits beside it")
		assert.NoFileExists(t, filepath.Join(dir, name))
	}
	require.Len(t, srv.List(), 1, "only the good file made a task")
	n, err := (&Spool{dir: dir}).Count()
	require.NoError(t, err)
	assert.Zero(t, n, "a quarantined file does not wait")
}

func TestOpenReplaysInAgeOrder(t *testing.T) {
	srv := newServer(t)
	in, err := jobserver.NewInbox[deliveryInbox](srv, "hooks")
	require.NoError(t, err)
	dir := t.TempDir()
	base := time.Now().Add(-time.Hour)
	write(t, filepath.Join(dir, "a.json"), `{"hook":"third"}`, base.Add(2*time.Second))
	write(t, filepath.Join(dir, "b.json"), `{"hook":"first"}`, base)
	write(t, filepath.Join(dir, "c.json"), `{"hook":"second"}`, base.Add(time.Second))
	open(t, dir, in)

	c := in.Bind()
	var order, titles []string
	for range 3 {
		task, spec, ok, err := in.Take(context.Background(), c, 5*time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		order = append(order, spec.Hook)
		titles = append(titles, spec.Title)
		require.NoError(t, in.Done(c, task, "ok", nil))
	}
	assert.Equal(t, []string{"first", "second", "third"}, order, "the oldest file is added first")
	assert.Equal(t, []string{"b", "c", "a"}, titles)
	eventuallyExists(t, filepath.Join(dir, DoneDir, "a.json"), filepath.Join(dir, DoneDir, "b.json"), filepath.Join(dir, DoneDir, "c.json"))
}

func TestRecreatesDirectoriesRemovedWhileRunning(t *testing.T) {
	srv := newServer(t)
	dir := t.TempDir()
	open(t, dir, srv.DefaultInbox())
	require.NoError(t, os.RemoveAll(filepath.Join(dir, DoneDir)))
	require.NoError(t, os.RemoveAll(filepath.Join(dir, FailedDir)))

	write(t, filepath.Join(dir, "late.cmd"), "echo late\n", time.Time{})
	write(t, filepath.Join(dir, "nocmd.json"), `{"title":"x"}`, time.Time{})

	eventuallyExists(t,
		filepath.Join(dir, DoneDir, "late.cmd"),
		filepath.Join(dir, FailedDir, "nocmd.json"),
		filepath.Join(dir, FailedDir, "nocmd.json"+errorSuffix))
	assert.Len(t, srv.List(), 1, "the good file made more than one task")
}

func TestSkipsAFileItCannotMove(t *testing.T) {
	cases := []struct {
		name     string
		file     string
		body     string
		blocked  string
		reason   string
		wantJobs int
	}{
		{"done", "once.cmd", "echo once\n", filepath.Join(DoneDir, "once.cmd"), "", 1},
		{"failed", "nocmd.json", `{"title":"x"}`, filepath.Join(FailedDir, "nocmd.json"), filepath.Join(FailedDir, "nocmd.json"+errorSuffix), 0},
		{"corrupt", "bad.json", "{not json", "bad.json" + corruptSuffix, "bad.json" + corruptSuffix + errorSuffix, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := newServer(t)
			dir := t.TempDir()
			var logs lineLog
			write(t, filepath.Join(dir, c.file), c.body, time.Time{})
			// A directory in the file's place makes the rename fail on every try.
			require.NoError(t, os.MkdirAll(filepath.Join(dir, c.blocked), 0o755))
			s := idle(t, dir, srv.DefaultInbox(), &logs)

			require.Eventually(t, func() bool { return len(logs.containing("rename")) > 0 }, 10*time.Second, 5*time.Millisecond)
			settle(t, s)
			for range 5 {
				s.scan()
			}
			settle(t, s)

			assert.Len(t, srv.List(), c.wantJobs, "scans added the file more than once")
			assert.FileExists(t, filepath.Join(dir, c.file))
			assert.Len(t, logs.containing("rename"), 1, "the failed move was not logged exactly once")
			if c.reason != "" {
				assert.NoFileExists(t, filepath.Join(dir, c.reason), "the reason stayed behind a failed move")
			}
		})
	}
}

func TestReadsAStuckFileAgainOnceItChanges(t *testing.T) {
	srv := newServer(t)
	dir := t.TempDir()
	var logs lineLog
	file := filepath.Join(dir, "once.cmd")
	write(t, file, "echo once\n", time.Time{})
	require.NoError(t, os.MkdirAll(filepath.Join(dir, DoneDir, "once.cmd"), 0o755))
	s := idle(t, dir, srv.DefaultInbox(), &logs)
	settle(t, s)
	s.scan()
	settle(t, s)
	require.Len(t, srv.List(), 1)

	write(t, file, "echo twice\n", time.Now().Add(time.Hour))
	s.scan()
	settle(t, s)
	s.scan()
	settle(t, s)

	assert.Len(t, srv.List(), 2, "a changed file was not read again")
}

func TestForgetsAStuckFileThatLeaves(t *testing.T) {
	srv := newServer(t)
	dir := t.TempDir()
	var logs lineLog
	file := filepath.Join(dir, "once.cmd")
	stamp := time.Now().Add(-time.Hour)
	write(t, file, "echo once\n", stamp)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, DoneDir, "once.cmd"), 0o755))
	s := idle(t, dir, srv.DefaultInbox(), &logs)
	settle(t, s)
	require.Len(t, srv.List(), 1)

	require.NoError(t, os.Remove(file))
	s.scan()
	write(t, file, "echo again\n", stamp)
	s.scan()
	settle(t, s)

	assert.Len(t, srv.List(), 2, "a new file with the old name and time was skipped")
}

func TestStoppedInboxKeepsTheFileForTheNextStart(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "keep.cmd"), "echo keep\n", time.Time{})
	var logs lineLog
	stopped := &stoppedInbox{}
	s := idle(t, dir, stopped, &logs)
	for range 3 {
		s.scan()
	}
	assert.Equal(t, 1, stopped.count(), "a kept file is not offered again until it changes")
	assert.Len(t, logs.containing("next start"), 1)
	assert.FileExists(t, filepath.Join(dir, "keep.cmd"), "a refused add must never lose the file")
	require.NoError(t, s.Close())

	srv := newServer(t)
	open(t, dir, srv.DefaultInbox())
	eventuallyExists(t, filepath.Join(dir, DoneDir, "keep.cmd"))
}

func TestCloseLeavesTheFileOfATaskThatDidNotEnd(t *testing.T) {
	srv := newServer(t)
	in, err := jobserver.NewInbox[deliveryInbox](srv, "hooks")
	require.NoError(t, err)
	dir := t.TempDir()
	s := open(t, dir, in)
	name, err := s.Park(&deliverySpec{Hook: "h"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.active[name]
	}, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "Close twice is safe")

	assert.FileExists(t, filepath.Join(dir, name))
	n, err := s.Count()
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestTempFilesAreNeverReplayed(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, ".spool-halfwritten.tmp"), `{"command":["true"]`, time.Time{})
	stopped := &stoppedInbox{}
	s := idle(t, dir, stopped, &lineLog{})
	assert.Zero(t, stopped.count(), "an uncommitted temp file is not an entry")
	n, err := s.Count()
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestOpenOnAnEmptySpoolAddsNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	stopped := &stoppedInbox{}
	s := idle(t, dir, stopped, &lineLog{})
	assert.Zero(t, stopped.count())
	assert.Equal(t, dir, s.Dir())
	assert.DirExists(t, filepath.Join(dir, DoneDir))
	assert.DirExists(t, filepath.Join(dir, FailedDir))
}

func TestCountReportsAnUnreadableDirectory(t *testing.T) {
	dir := t.TempDir()
	s := idle(t, dir, &stoppedInbox{}, &lineLog{})
	require.NoError(t, os.RemoveAll(dir))
	_, err := s.Count()
	assert.Error(t, err, "an unreadable spool is an error, never a count of zero")
}

// twoArgInbox and variadicInbox take a priority after the spec, as AddTask can.
type twoArgInbox struct{ stoppedInbox }

func (i *twoArgInbox) AddTask(s *jobserver.CommandSpec, _ priority.MultiPriority) (*jobserver.Task, error) {
	return i.stoppedInbox.AddTask(s)
}

type variadicInbox struct{ stoppedInbox }

func (i *variadicInbox) AddTask(s *jobserver.CommandSpec, _ ...priority.MultiPriority) (*jobserver.Task, error) {
	return i.stoppedInbox.AddTask(s)
}

// noTaskInbox returns neither a task nor an error.
type noTaskInbox struct{}

func (noTaskInbox) AddTask(*jobserver.CommandSpec) (*jobserver.Task, error) { return nil, nil }

func TestOpenCallsEachShapeOfAddTask(t *testing.T) {
	for _, in := range []interface{ count() int }{&twoArgInbox{}, &variadicInbox{}} {
		dir := t.TempDir()
		write(t, filepath.Join(dir, "x.cmd"), "echo x\n", time.Time{})
		idle(t, dir, in, &lineLog{})
		assert.Equal(t, 1, in.count(), "%T", in)
	}

	dir := t.TempDir()
	write(t, filepath.Join(dir, "x.cmd"), "echo x\n", time.Time{})
	idle(t, dir, noTaskInbox{}, &lineLog{})
	assert.FileExists(t, filepath.Join(dir, FailedDir, "x.cmd"+errorSuffix), "no task and no error is a failure")
}

type noAddTask struct{}

type wrongResult struct{}

func (wrongResult) AddTask(*jobserver.CommandSpec) error { return nil }

type notAStruct struct{}

func (notAStruct) AddTask(*int) (*jobserver.Task, error) { return nil, nil }

type noSpec struct{}

func (noSpec) AddTask(...int) (*jobserver.Task, error) { return nil, nil }

func TestOpenRefusesABadInbox(t *testing.T) {
	for _, in := range []any{nil, noAddTask{}, wrongResult{}, notAStruct{}, noSpec{}} {
		_, err := Open(t.TempDir(), in)
		assert.ErrorIs(t, err, ErrBadInbox, "%T", in)
	}
}
