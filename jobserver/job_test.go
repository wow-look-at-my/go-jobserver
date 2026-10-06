package jobserver

import (
	"strings"
	"testing"
	"time"
)

func TestNewIDIsUnique(t *testing.T) {
	taken := map[string]bool{}
	reg := func(id string) bool { return taken[id] }
	for i := 0; i < 500; i++ {
		id := NewID(reg)
		if taken[id] {
			t.Fatalf("NewID returned a taken id %q", id)
		}
		if !strings.HasPrefix(id, "j-") || len(id) != 14 {
			t.Fatalf("NewID returned %q, want j- followed by 12 hex digits", id)
		}
		taken[id] = true
	}
}

func TestNewIDSkipsTaken(t *testing.T) {
	seen := map[string]int{}
	first := NewID(func(string) bool { return false })
	seen[first]++
	second := NewID(func(id string) bool { return id == first })
	if second == first {
		t.Fatalf("NewID returned the taken id %q", first)
	}
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
		if got := c.state.Terminal(); got != c.want {
			t.Errorf("%s.Terminal() = %v, want %v", c.state, got, c.want)
		}
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
		if got := c.state.succeeded(); got != c.want {
			t.Errorf("%s.succeeded() = %v, want %v", c.state, got, c.want)
		}
	}
}

func TestValidDepsNormalizes(t *testing.T) {
	got := ValidDeps([]string{" b ", "a", "b", "", "a"})
	want := []string{"a", "b"}
	if len(got) != len(want) {
		t.Fatalf("ValidDeps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ValidDeps = %v, want %v", got, want)
		}
	}
}

func TestComputeIdentityIsStable(t *testing.T) {
	base := identityInput{Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"out"}}
	if computeIdentity(base) != computeIdentity(base) {
		t.Fatal("the same input hashed differently twice")
	}
	variants := map[string]identityInput{
		"other command": {Command: []string{"echo", "bye"}, Dir: "/tmp", Outputs: []string{"out"}},
		"other dir":     {Command: []string{"echo", "hi"}, Dir: "/elsewhere", Outputs: []string{"out"}},
		"other outputs": {Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"other"}},
		"other env":     {Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"out"}, Env: []string{"A=1"}},
		"other deps":    {Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"out"}, Deps: []string{"a=1"}},
		"other inputs":  {Command: []string{"echo", "hi"}, Dir: "/tmp", Outputs: []string{"out"}, Inputs: []string{"in=2"}},
	}
	want := computeIdentity(base)
	for name, in := range variants {
		if got := computeIdentity(in); got == want {
			t.Errorf("%s hashed the same as the base input", name)
		}
	}
}

func TestComputeIdentityUsesTheKey(t *testing.T) {
	withKey := computeIdentity(identityInput{Key: "k"})
	without := computeIdentity(identityInput{Command: []string{"k"}})
	if withKey == without {
		t.Fatal("a key collides with a command of the same text")
	}
}

func TestJobIdentityNeedsDependencyIdentities(t *testing.T) {
	a := &Job{ID: "a", Command: []string{"one"}}
	b := &Job{ID: "b", Command: []string{"two"}, Deps: []string{"a"}}
	byID := map[string]*Job{"a": a, "b": b}
	if id := JobIdentity(b, byID); id != "" {
		t.Fatalf("JobIdentity = %q, want empty while a dependency has no identity", id)
	}
	a.Identity = "sha256:aa"
	first := JobIdentity(b, byID)
	if first == "" {
		t.Fatal("JobIdentity stayed empty after the dependency got an identity")
	}
	a.Identity = "sha256:bb"
	if JobIdentity(b, byID) == first {
		t.Fatal("a dependency's identity change did not change the dependent")
	}
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
		if got[i].ID != id {
			t.Fatalf("SortedJobs = %s, want %v", ids(got), want)
		}
	}
}

func ids(jobs []*Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.ID)
	}
	return out
}
