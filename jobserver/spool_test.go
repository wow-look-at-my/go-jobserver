package jobserver

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newIdleServer builds a server without starting it, so a test drives the spool
// scan itself.
func newIdleServer(t *testing.T, mutate func(*Config)) *Server {
	t.Helper()
	cfg := DefaultConfig(t.TempDir())
	cfg.HTTPAddr = ""
	cfg.UnixSocket = ""
	cfg.IPC = false
	cfg.Runner = &fakeRunner{}
	mutate(&cfg)
	srv, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, srv.Close()) })
	return srv
}

func TestSpoolRecreatesDirectoriesRemovedWhileRunning(t *testing.T) {
	spool := t.TempDir()
	srv := newTestServer(t, func(c *Config) {
		c.SpoolDir = spool
		c.Runner = &fakeRunner{}
		c.SpoolInterval = 10 * time.Millisecond
	})
	// The watcher makes done/ and then failed/ when it starts.
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(spool, spoolFailedDir))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, os.RemoveAll(filepath.Join(spool, spoolDoneDir)))
	require.NoError(t, os.RemoveAll(filepath.Join(spool, spoolFailedDir)))

	require.NoError(t, os.WriteFile(filepath.Join(spool, "late.cmd"), []byte("echo late\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(spool, "bad.json"), []byte("{not json"), 0o644))

	require.Eventually(t, func() bool {
		for _, path := range []string{
			filepath.Join(spool, spoolDoneDir, "late.cmd"),
			filepath.Join(spool, spoolFailedDir, "bad.json"),
			filepath.Join(spool, spoolFailedDir, "bad.json.error"),
		} {
			if _, err := os.Stat(path); err != nil {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond, "the files never reached done/ and failed/")
	assert.Len(t, srv.List(), 1, "the good file made more than one job")
}

func TestSpoolSkipsAFileItCannotMove(t *testing.T) {
	cases := []struct {
		name     string
		file     string
		body     string
		dir      string
		wantJobs int
	}{
		{"done", "once.cmd", "echo once\n", spoolDoneDir, 1},
		{"failed", "bad.json", "{not json", spoolFailedDir, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spool := t.TempDir()
			var logs lineLog
			srv := newIdleServer(t, func(cfg *Config) {
				cfg.SpoolDir = spool
				cfg.Logf = logs.logf
			})
			require.NoError(t, os.WriteFile(filepath.Join(spool, c.file), []byte(c.body), 0o644))
			// A directory in the file's place makes the rename fail on every scan.
			require.NoError(t, os.MkdirAll(filepath.Join(spool, c.dir, c.file), 0o755))

			for i := 0; i < 5; i++ {
				srv.scanSpool()
			}

			assert.Len(t, srv.List(), c.wantJobs, "scans enqueued the file more than once")
			assert.FileExists(t, filepath.Join(spool, c.file))
			assert.Len(t, logs.containing("rename"), 1, "the failed move was not logged exactly once")
			assert.NoFileExists(t, filepath.Join(spool, c.dir, c.file+".error"), "the reason stayed behind a failed move")
		})
	}
}

func TestSpoolReadsAStuckFileAgainOnceItChanges(t *testing.T) {
	spool := t.TempDir()
	srv := newIdleServer(t, func(c *Config) { c.SpoolDir = spool })
	file := filepath.Join(spool, "once.cmd")
	require.NoError(t, os.WriteFile(file, []byte("echo once\n"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(spool, spoolDoneDir, "once.cmd"), 0o755))
	srv.scanSpool()
	srv.scanSpool()
	require.Len(t, srv.List(), 1)

	later := time.Now().Add(time.Hour)
	require.NoError(t, os.WriteFile(file, []byte("echo twice\n"), 0o644))
	require.NoError(t, os.Chtimes(file, later, later))
	srv.scanSpool()
	srv.scanSpool()

	assert.Len(t, srv.List(), 2, "a changed file was not read again")
}

func TestSpoolForgetsAStuckFileThatLeaves(t *testing.T) {
	spool := t.TempDir()
	srv := newIdleServer(t, func(c *Config) { c.SpoolDir = spool })
	file := filepath.Join(spool, "once.cmd")
	stamp := time.Now().Add(-time.Hour)
	require.NoError(t, os.WriteFile(file, []byte("echo once\n"), 0o644))
	require.NoError(t, os.Chtimes(file, stamp, stamp))
	require.NoError(t, os.MkdirAll(filepath.Join(spool, spoolDoneDir, "once.cmd"), 0o755))
	srv.scanSpool()
	require.Len(t, srv.List(), 1)

	require.NoError(t, os.Remove(file))
	srv.scanSpool()
	require.NoError(t, os.WriteFile(file, []byte("echo again\n"), 0o644))
	require.NoError(t, os.Chtimes(file, stamp, stamp))
	srv.scanSpool()

	assert.Len(t, srv.List(), 2, "a new file with the old name and time was skipped")
}
