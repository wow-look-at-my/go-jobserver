package jobserver

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
	"strings"
	"testing"
	"time"
)

func TestNewIDIsUnique(t *testing.T) {
	taken := set.New[string]()
	reg := func(id string) bool { return taken.Contains(id) }
	for i := 0; i < 500; i++ {
		id := NewID(reg)
		require.False(t, taken.Contains(id))

		require.False(t, !strings.HasPrefix(id, "j-") || len(id) != 14)
		taken.Add(id)
	}
}

func TestNewIDSkipsTaken(t *testing.T) {
	seen := map[string]int{}
	first := NewID(func(string) bool { return false })
	seen[first]++
	second := NewID(func(id string) bool { return id == first })
	require.NotEqual(t, first, second)

}

func TestStateTerminal(t *testing.T) {
	cases := []struct {
		state State
		want  bool
	}{
		{StateDraft, false},
		{StateActive, false},
		{StateRunning, false},
		{StateCompleted, true},
		{StateCached, true},
		{StateFailed, true},
		{StateCancelled, true},
		{StateBlocked, true},
	}
	for _, c := range cases {
		got := c.state.Terminal()
		assert.Equal(t, c.want, got)

	}
}

func TestStateSucceeded(t *testing.T) {
	for _, c := range []struct {
		state State
		want  bool
	}{
		{StateCompleted, true},
		{StateCached, true},
		{StateFailed, false},
		{StateActive, false},
		{StateBlocked, false},
	} {
		got := c.state.succeeded()
		assert.Equal(t, c.want, got)

	}
}

func TestValidDepsNormalizes(t *testing.T) {
	got := ValidDeps([]string{" b ", "a", "b", "", "a"})
	want := []string{"a", "b"}
	require.Equal(t, len(want), len(got))

	for i := range want {
		require.Equal(t, want[i], got[i])

	}
}

func TestComputeIdentityIsStable(t *testing.T) {
	base := identityInput{Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"out"}}
	require.Equal(t, computeIdentity(base), computeIdentity(base))

	variants := map[string]identityInput{
		"other command": {Command: []string{"echo", "bye"}, Dir: "/tmp", Outputs: []string{"out"}},
		"other dir":     {Command: []string{"echo", "hi"}, Dir: "/elsewhere", Outputs: []string{"out"}},
		"other outputs": {Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"other"}},
		"other env":     {Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"out"}, Env: []string{"A=1"}},
		"other deps":    {Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"out"}, Deps: []string{"a=1"}},
		"other inputs":  {Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"out"}, Inputs: []string{"in=2"}},
	}
	want := computeIdentity(base)
	for _, in := range variants {
		got := computeIdentity(in)
		assert.NotEqual(t, want, got)

	}
}

func TestComputeIdentityUsesTheKey(t *testing.T) {
	withKey := computeIdentity(identityInput{Key: "k"})
	without := computeIdentity(identityInput{Command: []string{"k"}})
	require.NotEqual(t, without, withKey)

}

func TestJobIdentityNeedsDependencyIdentities(t *testing.T) {
	a := &Job{ID: "a", Command: []string{"one"}}
	b := &Job{ID: "b", Command: []string{"two"}, Deps: []string{"a"}}
	byID := map[string]*Job{"a": a, "b": b}
	id := JobIdentity(b, byID)
	require.Equal(t, "", id)

	a.Identity = "sha256:aa"
	first := JobIdentity(b, byID)
	require.NotEqual(t, "", first)

	a.Identity = "sha256:bb"
	require.NotEqual(t, first, JobIdentity(b, byID))

}

func TestSortedJobsOrdersByCreation(t *testing.T) {
	now := time.Now()
	jobs := []*Job{
		{ID: "b", Created: now.Add(time.Second)},
		{ID: "a", Created: now},
		{ID: "c", Created: now},
	}
	got := SortedJobs(jobs)
	want := []string{"a", "c", "b"}
	for i, id := range want {
		require.Equal(t, id, got[i].ID)
	}
}
