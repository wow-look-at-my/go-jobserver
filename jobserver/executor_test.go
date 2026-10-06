package jobserver

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecRunnerReportsTheExitCodeAndOutput(t *testing.T) {
	var out bytes.Buffer
	j := &Job{Command: []string{"sh", "-c", "echo hi; exit 3"}}
	code, err := ExecRunner{}.Run(context.Background(), j, &out)
	require.NoError(t, err)
	assert.Equal(t, 3, code)
	assert.Equal(t, "hi\n", out.String())
}

func TestExecRunnerCapturesStdoutAndStderr(t *testing.T) {
	var out bytes.Buffer
	j := &Job{Command: []string{"sh", "-c", "echo to-out; echo to-err >&2"}}
	code, err := ExecRunner{}.Run(context.Background(), j, &out)
	require.NoError(t, err)
	assert.Zero(t, code)
	assert.Contains(t, out.String(), "to-out")
	assert.Contains(t, out.String(), "to-err")
}

func TestExecRunnerUsesTheJobDirectoryAndEnvironment(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	j := &Job{
		Command: []string{"sh", "-c", "pwd; echo $JOBSERVER_TEST"},
		Dir:     dir,
		Env:     []string{"JOBSERVER_TEST=yes"},
	}
	code, err := ExecRunner{}.Run(context.Background(), j, &out)
	require.NoError(t, err)
	assert.Zero(t, code)
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n"))
	require.Len(t, lines, 2)
	assert.True(t, filepath.Base(string(lines[0])) == filepath.Base(dir) || len(dir) == 0,
		"the command ran in %q, want %q", lines[0], dir)
	assert.Equal(t, "yes", string(lines[1]))
}

func TestExecRunnerStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var out bytes.Buffer
	j := &Job{Command: []string{"sh", "-c", "sleep 30"}}
	start := time.Now()
	_, err := ExecRunner{}.Run(ctx, j, &out)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 20*time.Second)
}

func TestExecRunnerStopsAJobThatLeavesAChildOnThePipe(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := ExecRunner{}.Run(ctx, &Job{
			Command: []string{"sh", "-c", "mkfifo -m 600 hold; cat hold"},
			Dir:     dir,
		}, &bytes.Buffer{})
		done <- err
	}()
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dir, "hold"))
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "the job never started")

	cancel()
	select {
	case err := <-done:
		require.Error(t, err, "a cancelled run must report an error")
	case <-time.After(20 * time.Second):
		require.FailNow(t, "the runner did not return after its context was cancelled")
	}
}

func TestExecRunnerReportsAMissingBinary(t *testing.T) {
	var out bytes.Buffer
	_, err := ExecRunner{}.Run(context.Background(), &Job{Command: []string{"definitely-not-a-real-binary-xyz"}}, &out)
	assert.Error(t, err)
}

func TestExecRunnerRejectsAnEmptyCommand(t *testing.T) {
	_, err := ExecRunner{}.Run(context.Background(), &Job{}, &bytes.Buffer{})
	assert.ErrorIs(t, err, ErrNoCommand)
}

func TestHashArtifacts(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "one.txt"), []byte("hello"), 0o644))
	j := &Job{Dir: dir, Outputs: []string{"one.txt"}}
	arts, err := hashArtifacts(j)
	require.NoError(t, err)
	require.Len(t, arts, 1)
	assert.Equal(t, "one.txt", arts[0].Path)
	assert.Equal(t, int64(5), arts[0].Size)
	assert.Equal(t, "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", arts[0].SHA256)
}

func TestHashArtifactsReportsAMissingOutput(t *testing.T) {
	j := &Job{Dir: t.TempDir(), Outputs: []string{"nope.txt"}}
	_, err := hashArtifacts(j)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope.txt")
}

func TestHashArtifactsUsesAnAbsolutePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "abs.txt")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
	arts, err := hashArtifacts(&Job{Dir: t.TempDir(), Outputs: []string{path}})
	require.NoError(t, err)
	require.Len(t, arts, 1)
	assert.Equal(t, int64(1), arts[0].Size)
}

func TestHashInputsTracksContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "in.txt")
	require.NoError(t, os.WriteFile(path, []byte("one"), 0o644))
	first := hashInputs(dir, []string{"in.txt"})
	require.Len(t, first, 1)
	assert.Contains(t, first[0], "sha256:")

	require.NoError(t, os.WriteFile(path, []byte("two"), 0o644))
	second := hashInputs(dir, []string{"in.txt"})
	assert.NotEqual(t, first[0], second[0])

	missing := hashInputs(dir, []string{"ghost.txt"})
	assert.Equal(t, []string{"ghost.txt=missing"}, missing)
}

func TestResolvePath(t *testing.T) {
	assert.Equal(t, "/tmp/a", resolvePath("/tmp", "/tmp/a"))
	assert.Equal(t, filepath.Join("/tmp", "a"), resolvePath("/tmp", "a"))
}

func TestResolveDir(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	assert.Equal(t, wd, resolveDir(""))
	assert.Equal(t, "/nowhere", resolveDir("/nowhere"))
}

func TestSplitCommand(t *testing.T) {
	assert.Equal(t, []string{"make", "-j4"}, splitCommand("make -j4"))
	assert.Nil(t, splitCommand("   "))
	assert.Nil(t, splitCommand(""))
}
