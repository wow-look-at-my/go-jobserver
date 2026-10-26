package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

// The CLI answers the questions a monitor raises: which monitors watch, did one already start for this, and what a
// job waits on. The job runs once the monitor's condition holds.
func TestCLIMonitors(t *testing.T) {
	dir := shortDir(t)
	defs, err := jobserver.LoadMonitorDefs(filepath.Join("testdata", "cli-monitors.xml"))
	require.NoError(t, err)
	cfg := jobserver.DefaultConfig(dir)
	cfg.HTTPAddr = ""
	cfg.Monitors = &jobserver.MonitorEnv{Defs: defs}
	srv, err := jobserver.New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(func() { srv.Close() })

	out, err := capture(t, func() error { return run([]string{"--dir", dir, "kinds"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "file-exists")
	assert.Contains(t, out, "path*", "a required arg is marked")

	flag := filepath.Join(t.TempDir(), "flag")
	enqueue := func() (string, string) {
		o, e, err := captureBoth(t, func() error {
			return run([]string{"--dir", dir, "queue", "--monitor", "file-exists", "--arg", "path=" + flag})
		})
		require.NoError(t, err)
		return strings.TrimSpace(o), e
	}
	mon, _ := enqueue()
	again, stderr := enqueue()
	assert.Equal(t, mon, again, "the same condition answers with the same monitor")
	assert.Contains(t, stderr, "already watches or runs this")

	out, err = capture(t, func() error { return run([]string{"--dir", dir, "queue", "--dep", mon, "--", "true"}) })
	require.NoError(t, err)
	job := strings.TrimSpace(out)

	require.Eventually(t, func() bool {
		out, err := capture(t, func() error { return run([]string{"--dir", dir, "why", job}) })
		return err == nil && strings.Contains(out, "monitor "+mon+": monitor:file-exists path="+flag+": not yet after")
	}, 10*time.Second, 50*time.Millisecond)

	out, err = capture(t, func() error { return run([]string{"--dir", dir, "monitors"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "0 running, 1 monitor watching; 1 waits on 1 monitor")
	assert.Contains(t, out, mon)
	assert.Contains(t, out, job, "the monitor names the job that waits on it")

	out, err = capture(t, func() error { return run([]string{"--dir", dir, "list"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "[monitor:file-exists path="+flag+"]")

	require.NoError(t, os.WriteFile(flag, nil, 0o644))
	require.Eventually(t, func() bool {
		j, err := srv.Get(job)
		return err == nil && j.State == jobserver.StateCompleted
	}, 10*time.Second, 50*time.Millisecond)
	out, err = capture(t, func() error { return run([]string{"--dir", dir, "why", job}) })
	require.NoError(t, err)
	assert.Contains(t, out, "waits on nothing")

	_, _, err = captureBoth(t, func() error {
		return run([]string{"--dir", dir, "queue", "--monitor", "file-exists", "--", "true"})
	})
	assert.ErrorContains(t, err, "a monitor job runs no command")
	_, _, err = captureBoth(t, func() error {
		return run([]string{"--dir", dir, "queue", "--monitor", "file-exists", "--arg", "nonsense"})
	})
	assert.ErrorContains(t, err, "is not name=value")
}
