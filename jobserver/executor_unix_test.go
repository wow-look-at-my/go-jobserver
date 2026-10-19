//go:build unix

package jobserver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// runWithBackgroundChild runs a shell that starts a sleeping child and exits
// with a code. The child keeps the output pipe open past the wait delay, and
// the test kills it afterwards.
func runWithBackgroundChild(t *testing.T, exit int) (int, error) {
	t.Helper()
	old := waitDelay
	waitDelay = 200 * time.Millisecond
	t.Cleanup(func() { waitDelay = old })
	dir := t.TempDir()
	t.Cleanup(func() {
		raw, err := os.ReadFile(filepath.Join(dir, "child.pid"))
		if err != nil {
			return
		}
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	script := fmt.Sprintf("sleep 30 & echo $! > child.pid; exit %d", exit)
	return ExecRunner{}.Run(context.Background(), &Job{Command: []string{"sh", "-c", script}, Dir: dir}, &bytes.Buffer{}, nil)
}

func TestExecRunnerCountsExitZeroAsSuccessWhileAChildHoldsThePipe(t *testing.T) {
	code, err := runWithBackgroundChild(t, 0)
	require.NoError(t, err)
	require.Zero(t, code)
}

func TestExecRunnerKeepsTheExitCodeWhileAChildHoldsThePipe(t *testing.T) {
	code, err := runWithBackgroundChild(t, 3)
	require.NoError(t, err)
	require.Equal(t, 3, code)
}

func TestExecRunnerKillsAJobsWholeProcessGroup(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "hold")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := ExecRunner{}.Run(ctx, &Job{
			Command: []string{"sh", "-c", "mkfifo -m 600 hold; cat hold & cat hold & wait"},
			Dir:     dir,
		}, &bytes.Buffer{}, nil)
		done <- err
	}()
	require.Eventually(t, func() bool {
		_, err := os.Stat(fifo)
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "the job never started")
	require.Eventually(t, func() bool { return fifoHasReader(fifo) }, 10*time.Second, 10*time.Millisecond,
		"the job's helpers never opened the fifo")

	cancel()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		require.FailNow(t, "the runner did not return after its context was cancelled")
	}
	require.Eventually(t, func() bool { return !fifoHasReader(fifo) }, 10*time.Second, 20*time.Millisecond,
		"a process from the job outlived the interrupt")
}

// fifoHasReader reports whether a process still holds a fifo open for
// reading. Opening the write end without a reader fails with ENXIO.
func fifoHasReader(path string) bool {
	f, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return false
	}
	f.Close()
	return true
}
