// Package jobserver runs a dependency-ordered queue of jobs as a daemon.
package jobserver

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// State is where a job sits in its lifecycle.
type State string

const (
	// StateDraft holds a job that is not yet eligible to run.
	StateDraft State = "draft"
	// StateActive holds a job the scheduler may run once its dependencies finish.
	StateActive State = "active"
	// StateRunning holds a job whose command is executing.
	StateRunning State = "running"
	// StateCompleted holds a job whose command exited zero.
	StateCompleted State = "completed"
	// StateCached holds a job whose work an earlier job with the same identity already did.
	StateCached State = "cached"
	// StateFailed holds a job whose command exited non-zero or could not start.
	StateFailed State = "failed"
	// StateCancelled holds a job whose run was interrupted.
	StateCancelled State = "cancelled"
	// StateBlocked holds a job a failed, cancelled or blocked dependency will never let run.
	StateBlocked State = "blocked"
)

// Terminal reports whether a state never changes again.
func (s State) Terminal() bool {
	switch s {
	case StateCompleted, StateCached, StateFailed, StateCancelled, StateBlocked:
		return true
	}
	return false
}

// succeeded reports whether a state counts as "its outputs exist".
func (s State) succeeded() bool {
	return s == StateCompleted || s == StateCached
}

// Artifact is one declared output file a finished job produced.
type Artifact struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Job is one unit of work and everything known about it.
type Job struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Command []string `json:"command"`
	Deps    []string `json:"deps"`
	Inputs  []string `json:"inputs"`
	Outputs []string `json:"outputs"`
	Env     []string `json:"env"`
	Dir     string   `json:"dir"`
	Key     string   `json:"key"`
	// Force marks a job that runs even when its work was already done.
	Force    bool      `json:"force,omitempty"`
	State    State     `json:"state"`
	Created  time.Time `json:"created"`
	Started  time.Time `json:"started,omitempty"`
	Finished time.Time `json:"finished,omitempty"`
	Attempts int       `json:"attempts"`
	ExitCode int       `json:"exit_code"`
	Error    string    `json:"error,omitempty"`
	// Identity is the value dedup compares.
	Identity string `json:"identity,omitempty"`
	// CacheOf names the job whose outputs this reused.
	CacheOf   string     `json:"cache_of,omitempty"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
	// LogBytes is how many bytes of output the job wrote.
	LogBytes int64 `json:"log_bytes"`
}

// Spec is the caller-supplied half of a job.
type Spec struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Command []string `json:"command"`
	Deps    []string `json:"deps"`
	Inputs  []string `json:"inputs"`
	Outputs []string `json:"outputs"`
	Env     []string `json:"env"`
	Dir     string   `json:"dir"`
	Key     string   `json:"key"`
	// Draft creates the job in StateDraft instead of StateActive.
	Draft bool `json:"draft"`
	// Force skips the dedup lookup, so the job runs even when an identical one already did.
	Force bool `json:"force"`
}

var (
	// ErrNotFound is returned for an ID no job has.
	ErrNotFound = errors.New("jobserver: no such job")
	// ErrCycle is returned when adding a dependency would close a loop.
	ErrCycle = errors.New("jobserver: dependency cycle")
	// ErrDuplicate is returned when a job's ID is already taken.
	ErrDuplicate = errors.New("jobserver: duplicate job id")
	// ErrBadState is returned for a transition the lifecycle forbids.
	ErrBadState = errors.New("jobserver: job is in the wrong state")
	// ErrMissingDep is returned when a dependency ID does not exist.
	ErrMissingDep = errors.New("jobserver: no such dependency")
	// ErrNoCommand is returned when a job carries nothing to run.
	ErrNoCommand = errors.New("jobserver: job has no command")
)

// CanonicalID normalizes a caller-supplied job ID.
func CanonicalID(id string) string { return strings.TrimSpace(id) }

// NewID returns an unused job ID of the shape "j-<12 hex>". taken reports
// which IDs are in use, so the caller's store decides uniqueness.
func NewID(taken func(string) bool) string {
	for i := 0; ; i++ {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d/%d", time.Now().UnixNano(), i)))
		id := "j-" + hex.EncodeToString(sum[:6])
		if !taken(id) {
			return id
		}
	}
}

// SortedDeps returns the job's dependencies in a stable order.
func SortedDeps(deps []string) []string {
	out := append([]string(nil), deps...)
	sort.Strings(out)
	return out
}

// ValidDeps normalizes a dependency list: trimmed, de-duplicated, sorted.
func ValidDeps(deps []string) []string {
	seen := make(map[string]bool, len(deps))
	out := make([]string, 0, len(deps))
	for _, d := range deps {
		d = CanonicalID(d)
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// identityInput is everything that decides whether jobs do the same work.
type identityInput struct {
	Key     string
	Command []string
	Dir     string
	Env     []string
	Outputs []string
	Inputs  []string
	Deps    []string
}

// computeIdentity hashes the job's work plus the identities of its finished
// dependencies. Jobs with the same value produce the same outputs, so the
// second one may reuse the first one's.
func computeIdentity(in identityInput) string {
	var sb strings.Builder
	writeList := func(tag string, values []string) {
		sb.WriteString(tag)
		sb.WriteByte(0)
		for _, v := range values {
			sb.WriteString(v)
			sb.WriteByte(0)
		}
		sb.WriteByte('\n')
	}
	if in.Key != "" {
		writeList("key", []string{in.Key})
	}
	writeList("cmd", in.Command)
	writeList("dir", []string{in.Dir})
	writeList("env", in.Env)
	writeList("outputs", in.Outputs)
	// Inputs are content-addressed, so their hashes are what matter.
	writeList("inputs", in.Inputs)
	writeList("deps", in.Deps)
	sum := sha256.Sum256([]byte(sb.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}
