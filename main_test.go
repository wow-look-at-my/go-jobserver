package main

import (
	"encoding/json"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

// TestMain lets this test binary stand in for the daemon as the process the
// suspend probe runs, which starts os.Executable() with -hold.
func TestMain(m *testing.M) {
	for _, arg := range os.Args[1:] {
		if arg == "-hold" {
			_, _ = io.Copy(io.Discard, os.Stdin)
			os.Exit(0)
		}
	}
	os.Exit(m.Run())
}

// shortDir is a scratch directory with a path short enough to hold a socket.
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "goj")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

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
	out, _, err := captureBoth(t, fn)
	return out, err
}

// captureBoth runs the CLI and returns what it wrote to each stream.
func captureBoth(t *testing.T, fn func() error) (string, string, error) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	outR, outW, err := os.Pipe()
	require.NoError(t, err)
	errR, errW, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout, os.Stderr = outW, errW
	outDone := make(chan string, 1)
	errDone := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(outR)
		outDone <- string(b)
	}()
	go func() {
		b, _ := io.ReadAll(errR)
		errDone <- string(b)
	}()
	fnErr := fn()
	outW.Close()
	errW.Close()
	os.Stdout, os.Stderr = oldOut, oldErr
	return <-outDone, <-errDone, fnErr
}

func TestCLIQueueWaitsForAJob(t *testing.T) {
	dir := shortDir(t)
	startDaemon(t, dir)
	out, errText, err := captureBoth(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "--", "sh", "-c", "echo from-cli"})
	})
	require.NoError(t, err)
	// stdout carries the id alone, so a script can capture it.
	id := strings.TrimSpace(out)
	assert.True(t, strings.HasPrefix(id, "j-"), "stdout was %q", out)
	assert.NotContains(t, id, "\n", "stdout carries more than the id: %q", out)
	assert.Contains(t, errText, string(jobserver.StateCompleted))
}

func TestCLIQueueReportsAFailure(t *testing.T) {
	dir := shortDir(t)
	startDaemon(t, dir)
	out, errText, err := captureBoth(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "--", "sh", "-c", "echo bad >&2; exit 4"})
	})
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(strings.TrimSpace(out), "j-"), "stdout was %q", out)
	assert.Contains(t, errText, "exit 4")
	assert.Contains(t, errText, "bad")
}

func TestCLIQueueDraftThenActivate(t *testing.T) {
	dir := shortDir(t)
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
	dir := shortDir(t)
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
	dir := shortDir(t)
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
	dir := shortDir(t)
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
	dir := shortDir(t)
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
	dir := shortDir(t)
	startDaemon(t, dir)
	spool := filepath.Join(dir, "spool")
	require.NoError(t, os.MkdirAll(spool, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(spool, "spooled.cmd"), []byte("echo spooled\n"), 0o644))
	require.Eventually(t, func() bool {
		out, err := capture(t, func() error { return run([]string{"-dir", dir, "list"}) })
		return err == nil && strings.Contains(out, "echo spooled")
	}, 10*time.Second, 20*time.Millisecond)
}

func TestCLIQueueOverIPC(t *testing.T) {
	dir := shortDir(t)
	cfg := jobserver.DefaultConfig(dir)
	cfg.HTTPAddr = ""
	// No socket: the client has to reach the daemon over its ipc service.
	cfg.UnixSocket = ""
	cfg.SpoolDir = filepath.Join(dir, "spool")
	cfg.SpoolInterval = 10 * time.Millisecond
	srv, err := jobserver.New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(func() { srv.Close() })

	out, errText, err := captureBoth(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "--", "sh", "-c", "echo over-ipc"})
	})
	require.NoError(t, err, errText)
	assert.True(t, strings.HasPrefix(strings.TrimSpace(out), "j-"), "stdout was %q", out)
	assert.Contains(t, errText, string(jobserver.StateCompleted))

	logs, err := capture(t, func() error {
		return run([]string{"-dir", dir, "logs", strings.TrimSpace(out)})
	})
	require.NoError(t, err)
	assert.Equal(t, "over-ipc\n", logs)
}

func TestCLIWithoutADaemonReportsIt(t *testing.T) {
	dir := shortDir(t)
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
	dir := shortDir(t)
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
	dir := shortDir(t)
	startDaemon(t, dir)
	out, errText, err := captureBoth(t, func() error {
		return run([]string{"-dir", dir, "queue", "-wait", "-shell", "echo a && echo b"})
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(strings.TrimSpace(out), "j-"))
	assert.Contains(t, errText, string(jobserver.StateCompleted))
}

func TestCLIInterruptAll(t *testing.T) {
	dir := shortDir(t)
	startDaemon(t, dir)
	out, err := capture(t, func() error { return run([]string{"-dir", dir, "interrupt", "all"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "interrupted 0 job(s)")
}

func TestCLIStatusJSON(t *testing.T) {
	dir := shortDir(t)
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
	dir := shortDir(t)
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

func TestCLICarriesAJobPolicyAndReplacesIt(t *testing.T) {
	dir := shortDir(t)
	startDaemon(t, dir)
	out, err := capture(t, func() error {
		return run([]string{
			"-dir", dir, "queue", "-draft",
			"-cpu", "2.5",
			"-priority", "-nice", "5",
			"-freeze", "-exempt", "1234,sleep",
			"-affinity", "0,1",
			"--", "sh", "-c", "true",
		})
	})
	require.NoError(t, err)
	id := strings.Split(strings.TrimSpace(out), "\n")[0]

	raw, err := capture(t, func() error { return run([]string{"-dir", dir, "status", "-json", id}) })
	require.NoError(t, err)
	var job jobserver.Job
	require.NoError(t, json.Unmarshal([]byte(raw), &job))
	assert.InDelta(t, 2.5, job.Policy.Cost, 1e-9)
	assert.Equal(t, []jobserver.Mechanism{jobserver.MechAffinity, jobserver.MechPriority, jobserver.MechFreeze},
		job.Policy.Enabled())
	assert.Equal(t, []int{0, 1}, job.Policy.Affinity.CPUs)
	assert.Equal(t, 5, job.Policy.Priority.Nice)
	assert.Equal(t, []int{1234}, job.Policy.Freeze.Exempt.PIDs)
	assert.Equal(t, []string{"sleep"}, job.Policy.Freeze.Exempt.Names)

	replaced, err := capture(t, func() error {
		return run([]string{"-dir", dir, "policy", "-cpu", "1", "-freeze-exempt", "99", id})
	})
	require.NoError(t, err)
	assert.Contains(t, replaced, "freeze")
	assert.Contains(t, replaced, "exempt 99")

	raw, err = capture(t, func() error { return run([]string{"-dir", dir, "status", "-json", id}) })
	require.NoError(t, err)
	var after jobserver.Job
	require.NoError(t, json.Unmarshal([]byte(raw), &after))
	assert.InDelta(t, 1, after.Policy.Cost, 1e-9)
	assert.Empty(t, after.Policy.Enabled(), "the replacement turns every mechanism off")
	assert.Equal(t, []int{99}, after.Policy.Freeze.Exempt.PIDs)
}

func TestCLIRunsTheDaemonAndReportsCPU(t *testing.T) {
	dir := shortDir(t)
	// The signal that stops the daemon travels through this process.
	quiet := make(chan os.Signal, 4)
	signal.Notify(quiet, os.Interrupt)
	defer signal.Stop(quiet)

	daemon := make(chan error, 1)
	go func() { daemon <- run([]string{"-dir", dir, "-no-http", "-ipc=false", "run"}) }()

	sock := filepath.Join(dir, "go-jobserver.sock")
	require.Eventually(t, func() bool {
		_, err := os.Stat(sock)
		return err == nil
	}, 30*time.Second, 10*time.Millisecond, "the daemon must publish its socket")

	idOut, err := capture(t, func() error {
		return run([]string{"-dir", dir, "queue", "--", "sh", "-c", "while :; do :; done"})
	})
	require.NoError(t, err)
	id := strings.TrimSpace(idOut)

	// The shipped daemon reports the host's use, and the use of the job it is running, while that job burns.
	var stats jobserver.Stats
	require.Eventually(t, func() bool {
		raw, err := capture(t, func() error { return run([]string{"-dir", dir, "stats", "-json"}) })
		if err != nil {
			return false
		}
		return json.Unmarshal([]byte(raw), &stats) == nil && stats.CPU.Jobs[id] > 0
	}, 30*time.Second, 50*time.Millisecond, "stats must report the CPU the job uses")
	assert.GreaterOrEqual(t, stats.CPU.CPUs, 1)
	assert.Greater(t, stats.CPU.Budget, 0.0)
	assert.GreaterOrEqual(t, stats.CPU.HostBusy, 0.0)

	_, err = capture(t, func() error { return run([]string{"-dir", dir, "interrupt", id}) })
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		raw, err := capture(t, func() error { return run([]string{"-dir", dir, "status", "-json", id}) })
		if err != nil {
			return false
		}
		var j jobserver.Job
		return json.Unmarshal([]byte(raw), &j) == nil && j.State.Terminal()
	}, 30*time.Second, 50*time.Millisecond, "the interrupted job must reach a terminal state")

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGINT))
	select {
	case err := <-daemon:
		require.NoError(t, err, "the daemon must shut down cleanly")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the daemon never returned")
	}
}

func TestCLIStatsPrintsTheCPUReport(t *testing.T) {
	dir := shortDir(t)
	startDaemon(t, dir)
	out, err := capture(t, func() error { return run([]string{"-dir", dir, "stats"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "budget")
	assert.Contains(t, out, "measured")
	assert.Contains(t, out, "host")
}
