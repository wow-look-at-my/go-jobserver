package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

// startDaemon runs a server whose file socket and spool live in dir.
func startDaemon(t *testing.T, dir string) *jobserver.Server {
	t.Helper()
	cfg := jobserver.DefaultConfig(dir)
	cfg.HTTPAddr = ""
	cfg.SpoolDir = filepath.Join(dir, "spool")
	cfg.SpoolInterval = 10 * time.Millisecond
	srv, err := jobserver.New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(func() { srv.Close() })
	return srv
}

// capture runs the CLI and returns what it wrote to stdout.
func capture(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fnErr := fn()
	w.Close()
	os.Stdout = old
	return <-done, fnErr
}

func TestCLIQueueWaitsForAJob(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	out, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "--", "sh", "-c", "echo from-cli"})
	})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2, "output was %q", out)
	assert.True(t, strings.HasPrefix(lines[0], "j-"), "the first line is the job id, got %q", lines[0])
	assert.Contains(t, lines[1], string(jobserver.StateCompleted))
}

func TestCLIQueueReportsAFailure(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	out, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "--", "sh", "-c", "echo bad >&2; exit 4"})
	})
	require.Error(t, err)
	assert.Contains(t, out, "exit 4")
	assert.Contains(t, out, "bad")
}

func TestCLIQueueDraftThenActivate(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	out, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "-draft", "-name", "later", "--", "sh", "-c", "echo later"})
	})
	require.NoError(t, err)
	id := strings.TrimSpace(out)
	require.True(t, strings.HasPrefix(id, "j-"))

	out, err = capture(t, func() error { return run([]string{"-dir", dir, "status", id}) })
	require.NoError(t, err)
	assert.Contains(t, out, string(jobserver.StateDraft))
	assert.Contains(t, out, "later")

	_, err = capture(t, func() error { return run([]string{"-dir", dir, "activate", id}) })
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, err := capture(t, func() error { return run([]string{"-dir", dir, "status", id}) })
		return err == nil && strings.Contains(out, string(jobserver.StateCompleted))
	}, 10*time.Second, 20*time.Millisecond)
}

func TestCLIDependBuildsAChain(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	var producer, consumer string
	out, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "-draft", "--", "sh", "-c", "echo one"})
	})
	require.NoError(t, err)
	producer = strings.TrimSpace(out)
	out, err = capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "-draft", "--", "sh", "-c", "echo two"})
	})
	require.NoError(t, err)
	consumer = strings.TrimSpace(out)

	_, err = capture(t, func() error { return run([]string{"-dir", dir, "depend", consumer, producer}) })
	require.NoError(t, err)

	out, err = capture(t, func() error { return run([]string{"-dir", dir, "status", consumer}) })
	require.NoError(t, err)
	assert.Contains(t, out, "deps      "+producer)

	_, err = capture(t, func() error { return run([]string{"-dir", dir, "activate", producer}) })
	require.NoError(t, err)
	_, err = capture(t, func() error { return run([]string{"-dir", dir, "activate", consumer}) })
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, err := capture(t, func() error { return run([]string{"-dir", dir, "status", consumer}) })
		return err == nil && strings.Contains(out, string(jobserver.StateCompleted))
	}, 10*time.Second, 20*time.Millisecond)
}

func TestCLIListShowsJobs(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	out, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "--", "sh", "-c", "true"})
	})
	require.NoError(t, err)
	id := strings.Split(strings.TrimSpace(out), "\n")[0]

	out, err = capture(t, func() error { return run([]string{"-dir", dir, "list"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "ID")
	assert.Contains(t, out, id)
	assert.Contains(t, out, string(jobserver.StateCompleted))

	out, err = capture(t, func() error { return run([]string{"-dir", dir, "list", "-json"}) })
	require.NoError(t, err)
	var listed []jobserver.Job
	require.NoError(t, json.Unmarshal([]byte(out), &listed))
	require.Len(t, listed, 1)
	assert.Equal(t, id, listed[0].ID)
	assert.Equal(t, jobserver.StateCompleted, listed[0].State)
}

func TestCLILogsPrintsTheOutput(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	out, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "--", "sh", "-c", "echo logged-line"})
	})
	require.NoError(t, err)
	id := strings.Split(strings.TrimSpace(out), "\n")[0]

	out, err = capture(t, func() error { return run([]string{"-dir", dir, "logs", id}) })
	require.NoError(t, err)
	assert.Equal(t, "logged-line\n", out)
}

func TestCLIPauseResumeAndInterrupt(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	out, err := capture(t, func() error { return run([]string{"-dir", dir, "pause"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "paused")

	out, err = capture(t, func() error { return run([]string{"-dir", dir, "stats"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "paused   true")

	_, err = capture(t, func() error { return run([]string{"-dir", dir, "resume"}) })
	require.NoError(t, err)

	idOut, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "--", "sh", "-c", "sleep 30"})
	})
	require.NoError(t, err)
	id := strings.TrimSpace(idOut)
	require.Eventually(t, func() bool {
		out, _ := capture(t, func() error { return run([]string{"-dir", dir, "status", id}) })
		return strings.Contains(out, string(jobserver.StateRunning))
	}, 10*time.Second, 20*time.Millisecond)

	out, err = capture(t, func() error { return run([]string{"-dir", dir, "interrupt", id}) })
	require.NoError(t, err)
	assert.Contains(t, out, "interrupted")

	require.Eventually(t, func() bool {
		out, _ := capture(t, func() error { return run([]string{"-dir", dir, "status", id}) })
		return strings.Contains(out, string(jobserver.StateCancelled))
	}, 10*time.Second, 20*time.Millisecond)
}

func TestCLIQueueReadsTheSpool(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	spool := filepath.Join(dir, "spool")
	require.NoError(t, os.MkdirAll(spool, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(spool, "spooled.cmd"), []byte("echo spooled\n"), 0o644))
	require.Eventually(t, func() bool {
		out, err := capture(t, func() error { return run([]string{"-dir", dir, "list"}) })
		return err == nil && strings.Contains(out, "echo spooled")
	}, 10*time.Second, 20*time.Millisecond)
}

func TestCLIWithoutADaemonReportsIt(t *testing.T) {
	dir := t.TempDir()
	_, err := capture(t, func() error { return run([]string{"-dir", dir, "list"}) })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no daemon")
}

func TestCLIVersionAndHelp(t *testing.T) {
	out, err := capture(t, func() error { return run([]string{"version"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "go-jobserver")

	out, err = capture(t, func() error { return run([]string{"help"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "usage:")

	out, err = capture(t, func() error { return run(nil) })
	require.NoError(t, err)
	assert.Contains(t, out, "usage:")
}

func TestCLIQueueNeedsACommand(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	_, err := capture(t, func() error { return run([]string{"-dir", dir, "queue"}) })
	require.Error(t, err)
}

func TestCLIRejectsAnUnknownCommand(t *testing.T) {
	_, err := capture(t, func() error { return run([]string{"frobnicate"}) })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown command")
}

func TestCLIQueueThroughAShell(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	out, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "-shell", "echo a && echo b"})
	})
	require.NoError(t, err)
	assert.Contains(t, out, string(jobserver.StateCompleted))
}

func TestCLIInterruptAll(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	out, err := capture(t, func() error { return run([]string{"-dir", dir, "interrupt", "all"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "interrupted 0 job(s)")
}

func TestCLIStatusJSON(t *testing.T) {
	dir := t.TempDir()
	startDaemon(t, dir)
	idOut, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "--", "sh", "-c", "true"})
	})
	require.NoError(t, err)
	id := strings.Split(strings.TrimSpace(idOut), "\n")[0]
	out, err := capture(t, func() error { return run([]string{"-dir", dir, "status", "-json", id}) })
	require.NoError(t, err)
	assert.Contains(t, out, `"state": "completed"`)
}

func TestCLIQueueWithInputsAndOutputs(t *testing.T) {
	dir := t.TempDir()
	work := t.TempDir()
	startDaemon(t, dir)
	out, err := capture(t, func() error {
		return run([]string{
			"-dir", dir, "queue", "-wait",
			"-workdir", work,
			"-output", "made.txt",
			"-env", "JOBSERVER_CLI=1",
			"--", "sh", "-c", "echo $JOBSERVER_CLI > made.txt",
		})
	})
	require.NoError(t, err)
	id := strings.Split(strings.TrimSpace(out), "\n")[0]
	status, err := capture(t, func() error { return run([]string{"-dir", dir, "status", id}) })
	require.NoError(t, err)
	assert.Contains(t, status, "made.txt")
	assert.Contains(t, status, "artifact")
	body, err := os.ReadFile(filepath.Join(work, "made.txt"))
	require.NoError(t, err)
	assert.Equal(t, "1\n", string(body))
}
