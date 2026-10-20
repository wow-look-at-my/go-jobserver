package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func TestCLIHelpListsCommandsAndLongFlags(t *testing.T) {
	out, err := capture(t, func() error { return run([]string{"--help"}) })
	require.NoError(t, err)
	for _, want := range []string{"queue", "policy", "--dir", "--budget", "-j, --jobs"} {
		assert.Contains(t, out, want)
	}

	out, err = capture(t, func() error { return run([]string{"queue", "--help"}) })
	require.NoError(t, err)
	for _, want := range []string{"--wait", "--dep", "--freeze-exempt", "--dir"} {
		assert.Contains(t, out, want)
	}
}

func TestCLIQueueStopsAtTheCommand(t *testing.T) {
	dir := shortDir(t)
	startDaemon(t, dir)
	// No "--": the "-c" belongs to sh, not to queue.
	out, err := capture(t, func() error {
		return run([]string{"--dir", dir, "queue", "--wait", "sh", "-c", "echo no-dash"})
	})
	require.NoError(t, err)
	logs, err := capture(t, func() error { return run([]string{"--dir", dir, "logs", strings.TrimSpace(out)}) })
	require.NoError(t, err)
	assert.Equal(t, "no-dash\n", logs)
}

func TestCLIQueueDepAndDependsShareOneList(t *testing.T) {
	dir := shortDir(t)
	srv := startDaemon(t, dir)
	var ids []string
	for range 2 {
		j, _, err := srv.Enqueue(jobserver.Spec{Command: []string{"true"}, Draft: true})
		require.NoError(t, err)
		ids = append(ids, j.ID)
	}
	out, err := capture(t, func() error {
		return run([]string{"--dir", dir, "queue", "--draft", "--dep", ids[0], "--depends", ids[1], "--", "true"})
	})
	require.NoError(t, err)
	raw, err := capture(t, func() error { return run([]string{"--dir", dir, "status", "--json", strings.TrimSpace(out)}) })
	require.NoError(t, err)
	var job jobserver.Job
	require.NoError(t, json.Unmarshal([]byte(raw), &job))
	assert.ElementsMatch(t, ids, job.Deps)
}
