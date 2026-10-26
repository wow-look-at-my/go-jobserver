package jobserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// lineLog collects the status lines a server writes.
type lineLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *lineLog) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

// containing returns the lines that hold a substring.
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

// fakeRunner stands in for a command. It records what ran, can hold a run
// open until the test releases it, and writes declared outputs.
type fakeRunner struct {
	mu       sync.Mutex
	calls    []string
	commands []string
	hold     chan struct{}
	fail     int
	out      string
	writes   map[string]string
	entered  chan string
}

func (f *fakeRunner) Run(ctx context.Context, j *Job, out io.Writer, started func(pid int)) (int, error) {
	if started != nil {
		started(os.Getpid())
	}
	f.mu.Lock()
	f.calls = append(f.calls, j.ID)
	f.commands = append(f.commands, j.Command[0])
	hold, fail, text, writes, entered := f.hold, f.fail, f.out, f.writes, f.entered
	f.mu.Unlock()

	if text != "" {
		io.WriteString(out, text)
	}
	for path, body := range writes {
		if j.Dir == "" {
			// Writing without a job directory would land in the package directory, which is the repository during a test run.
			return -1, errors.New("the fake runner needs a job directory to write an output")
		}
		if err := os.WriteFile(resolvePath(j.Dir, path), []byte(body), 0o644); err != nil {
			return -1, err
		}
	}
	if entered != nil {
		select {
		case entered <- j.ID:
		default:
		}
	}
	if hold != nil {
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-hold:
		}
	}
	return fail, nil
}

func (f *fakeRunner) ran(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == id {
			return true
		}
	}
	return false
}

func (f *fakeRunner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// shortDir is a scratch directory with a path short enough to hold a socket.
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "goj")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// newTestServer starts a server whose transports are off unless the test
// turns them on.
func newTestServer(t *testing.T, mutate func(*Config)) *Server {
	t.Helper()
	return newTunedServer(t, mutate, nil)
}

// newTunedServer is newTestServer with a tune step that changes the server
// before it starts, while no governor reads its settings yet.
func newTunedServer(t *testing.T, mutate func(*Config), tune func(*Server)) *Server {
	t.Helper()
	cfg := DefaultConfig(t.TempDir())
	cfg.HTTPAddr = ""
	cfg.UnixSocket = ""
	cfg.IPC = false
	cfg.MaxConcurrent = 4
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := New(cfg)
	require.NoError(t, err)
	if tune != nil {
		tune(srv)
	}
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		require.NoError(t, srv.Close())
	})
	return srv
}

// waitState polls until a job reaches the wanted state.
func waitState(t *testing.T, srv *Server, id string, want State) *Job {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		j, err := srv.Get(id)
		require.NoError(t, err)
		if j.State == want {
			return j
		}
		if j.State.Terminal() {
			require.FailNow(t, "job reached the wrong terminal state",
				"job %s is %s (%s), want %s", id, j.State, j.Error, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.FailNow(t, "job never reached the wanted state", "job %s never reached %s", id, want)
	return nil
}
