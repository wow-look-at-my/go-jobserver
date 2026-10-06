package jobserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// waitDelay bounds how long a killed child may hold its output pipe open.
const waitDelay = 5 * time.Second

// A Runner runs one job's command into out and reports its exit code.
// started receives the child's process id once it is running.
type Runner interface {
	Run(ctx context.Context, j *Job, out io.Writer, started func(pid int)) (int, error)
}

// ExecRunner runs the job's command as a child process.
type ExecRunner struct{}

// Run starts the command, streams both of its output streams into out, and
// kills it when ctx ends.
func (ExecRunner) Run(ctx context.Context, j *Job, out io.Writer, started func(pid int)) (int, error) {
	if len(j.Command) == 0 {
		return -1, ErrNoCommand
	}
	cmd := exec.CommandContext(ctx, j.Command[0], j.Command[1:]...)
	cmd.Dir = j.Dir
	if len(j.Env) > 0 {
		cmd.Env = append(os.Environ(), j.Env...)
	}
	cmd.Stdout = out
	cmd.Stderr = out
	// Cancellation kills the job's whole process group.
	prepareProcess(cmd)
	cmd.WaitDelay = waitDelay
	if err := cmd.Start(); err != nil {
		return -1, err
	}
	if started != nil {
		started(cmd.Process.Pid)
	}
	err := cmd.Wait()
	if err == nil {
		return 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if ctx.Err() != nil {
			return -1, ctx.Err()
		}
		return exitErr.ExitCode(), nil
	}
	return -1, err
}

// resolveDir turns a job's working directory into an absolute path.
func resolveDir(dir string) string {
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			return "."
		}
		return wd
	}
	return dir
}

// resolvePath resolves a declared path against the job's directory.
func resolvePath(jobDir, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(resolveDir(jobDir), p)
}

// hashArtifacts hashes every declared output. A declared output that no run
// produced is an error: the job said it would write it.
func hashArtifacts(j *Job) ([]Artifact, error) {
	out := make([]Artifact, 0, len(j.Outputs))
	for _, o := range j.Outputs {
		p := resolvePath(j.Dir, o)
		f, err := os.Open(p)
		if err != nil {
			return nil, fmt.Errorf("declared output %s was not produced: %w", o, err)
		}
		h := sha256.New()
		n, err := io.Copy(h, f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("reading declared output %s: %w", o, err)
		}
		out = append(out, Artifact{Path: o, Size: n, SHA256: "sha256:" + hex.EncodeToString(h.Sum(nil))})
	}
	return out, nil
}

// hashInputs describes declared input files by content. A job whose inputs
// changed has a different identity, so it is not served from cache.
func hashInputs(dir string, inputs []string) []string {
	out := make([]string, 0, len(inputs))
	for _, in := range inputs {
		p := resolvePath(dir, in)
		f, err := os.Open(p)
		if err != nil {
			out = append(out, in+"=missing")
			continue
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			out = append(out, in+"=unreadable")
			continue
		}
		f.Close()
		out = append(out, in+"=sha256:"+hex.EncodeToString(h.Sum(nil)))
	}
	sort.Strings(out)
	return out
}

// splitCommand parses a single command string into argv on whitespace.
func splitCommand(cmdline string) []string {
	fields := strings.Fields(cmdline)
	if len(fields) == 0 {
		return nil
	}
	return fields
}
