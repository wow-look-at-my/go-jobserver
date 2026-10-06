package jobserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// job builds a job with a state and dependencies for the table tests.
func job(id string, state State, deps ...string) *Job {
	return &Job{ID: id, State: state, Deps: deps, Created: time.Unix(0, 0)}
}

func TestReady(t *testing.T) {
	cases := []struct {
		name string
		jobs []*Job
		want []string
	}{
		{
			name: "an active job with no dependencies is ready",
			jobs: []*Job{job("a", StateActive)},
			want: []string{"a"},
		},
		{
			name: "a draft job is never ready",
			jobs: []*Job{job("a", StateDraft)},
			want: nil,
		},
		{
			name: "a job waits for its dependency",
			jobs: []*Job{job("a", StateRunning), job("b", StateActive, "a")},
			want: nil,
		},
		{
			name: "a completed dependency releases its dependent",
			jobs: []*Job{job("a", StateCompleted), job("b", StateActive, "a")},
			want: []string{"b"},
		},
		{
			name: "a cached dependency releases its dependent",
			jobs: []*Job{job("a", StateCached), job("b", StateActive, "a")},
			want: []string{"b"},
		},
		{
			name: "a failed dependency does not release its dependent",
			jobs: []*Job{job("a", StateFailed), job("b", StateActive, "a")},
			want: nil,
		},
		{
			name: "every dependency must succeed",
			jobs: []*Job{job("a", StateCompleted), job("b", StateFailed), job("c", StateActive, "a", "b")},
			want: nil,
		},
		{
			name: "a missing dependency blocks",
			jobs: []*Job{job("b", StateActive, "ghost")},
			want: nil,
		},
		{
			name: "ready jobs come back in creation order",
			jobs: []*Job{
				{ID: "late", State: StateActive, Created: time.Unix(10, 0)},
				{ID: "early", State: StateActive, Created: time.Unix(1, 0)},
			},
			want: []string{"early", "late"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, Ready(c.jobs))
		})
	}
}

func TestBlocked(t *testing.T) {
	t.Run("a failed dependency blocks its dependent", func(t *testing.T) {
		jobs := []*Job{job("a", StateFailed), job("b", StateActive, "a")}
		assert.Equal(t, []string{"b"}, Blocked(jobs))
	})
	t.Run("blocking reaches through the graph", func(t *testing.T) {
		jobs := []*Job{
			job("a", StateFailed),
			job("b", StateActive, "a"),
			job("c", StateActive, "b"),
			job("d", StateActive, "c"),
		}
		assert.Equal(t, []string{"b", "c", "d"}, Blocked(jobs))
	})
	t.Run("a cancelled dependency blocks", func(t *testing.T) {
		jobs := []*Job{job("a", StateCancelled), job("b", StateActive, "a")}
		assert.Equal(t, []string{"b"}, Blocked(jobs))
	})
	t.Run("a blocked dependency keeps blocking", func(t *testing.T) {
		jobs := []*Job{job("a", StateBlocked), job("b", StateActive, "a")}
		assert.Equal(t, []string{"b"}, Blocked(jobs))
	})
	t.Run("successful dependencies block nothing", func(t *testing.T) {
		jobs := []*Job{job("a", StateCompleted), job("b", StateCached), job("c", StateActive, "a", "b")}
		assert.Empty(t, Blocked(jobs))
	})
	t.Run("a terminal dependent is left alone", func(t *testing.T) {
		jobs := []*Job{job("a", StateFailed), job("b", StateCompleted, "a")}
		assert.Empty(t, Blocked(jobs))
	})
}

func TestDeadSets(t *testing.T) {
	jobs := []*Job{
		job("ok", StateCompleted),
		job("bad", StateFailed),
		job("mid", StateActive, "bad"),
		job("leaf", StateActive, "mid"),
		job("free", StateActive),
	}
	dead := Dead(jobs)
	assert.False(t, dead["ok"])
	assert.True(t, dead["bad"])
	assert.True(t, dead["mid"])
	assert.True(t, dead["leaf"])
	assert.False(t, dead["free"])
}

func TestCyclePath(t *testing.T) {
	t.Run("an acyclic graph has none", func(t *testing.T) {
		jobs := []*Job{job("a", StateActive), job("b", StateActive, "a"), job("c", StateActive, "b", "a")}
		assert.Empty(t, CyclePath(jobs))
	})
	t.Run("a two job loop is found", func(t *testing.T) {
		jobs := []*Job{job("a", StateActive, "b"), job("b", StateActive, "a")}
		path := CyclePath(jobs)
		require.NotEmpty(t, path)
		assert.Equal(t, path[0], path[len(path)-1])
	})
	t.Run("a three job loop is found", func(t *testing.T) {
		jobs := []*Job{job("a", StateActive, "c"), job("b", StateActive, "a"), job("c", StateActive, "b")}
		path := CyclePath(jobs)
		require.Len(t, path, 4)
		assert.Equal(t, path[0], path[3])
	})
	t.Run("a job outside the loop still gets visited", func(t *testing.T) {
		jobs := []*Job{job("x", StateActive), job("a", StateActive, "b"), job("b", StateActive, "a")}
		assert.NotEmpty(t, CyclePath(jobs))
	})
}

func TestTopoOrder(t *testing.T) {
	t.Run("dependencies come first", func(t *testing.T) {
		jobs := []*Job{job("c", StateActive, "b"), job("b", StateActive, "a"), job("a", StateActive)}
		order := TopoOrder(jobs)
		require.Len(t, order, 3)
		pos := map[string]int{}
		for i, id := range order {
			pos[id] = i
		}
		assert.Less(t, pos["a"], pos["b"])
		assert.Less(t, pos["b"], pos["c"])
	})
	t.Run("a cycle has no order", func(t *testing.T) {
		jobs := []*Job{job("a", StateActive, "b"), job("b", StateActive, "a")}
		assert.Nil(t, TopoOrder(jobs))
	})
	t.Run("independent jobs are ordered by id", func(t *testing.T) {
		jobs := []*Job{job("c", StateActive), job("a", StateActive), job("b", StateActive)}
		assert.Equal(t, []string{"a", "b", "c"}, TopoOrder(jobs))
	})
}

func TestCacheHit(t *testing.T) {
	mk := func(id string, state State, command string) *Job {
		j := job(id, state)
		j.Command = []string{command}
		return j
	}
	a := mk("a", StateCompleted, "echo hi")
	a.Identity = JobIdentity(a, Index([]*Job{a}))
	require.NotEmpty(t, a.Identity)
	b := mk("b", StateActive, "echo hi")
	b.Identity = a.Identity
	idx := IdentityIndex{a.Identity: "a"}

	t.Run("an identical finished job is a hit", func(t *testing.T) {
		assert.Equal(t, a.Identity, JobIdentity(b, Index([]*Job{a, b})))
		assert.Equal(t, "a", CacheHit(a.Identity, idx, b.ID))
	})
	t.Run("a different command is a miss", func(t *testing.T) {
		c := mk("c", StateActive, "echo bye")
		assert.Empty(t, CacheHit(JobIdentity(c, Index([]*Job{c})), idx, c.ID))
	})
	t.Run("a job is not its own cache", func(t *testing.T) {
		assert.Empty(t, CacheHit(a.Identity, idx, a.ID))
	})
	t.Run("an empty identity is a miss", func(t *testing.T) {
		assert.Empty(t, CacheHit("", idx, "b"))
	})
	t.Run("a job whose dependencies have no identity is a miss", func(t *testing.T) {
		dep := job("dep", StateRunning)
		d := mk("d", StateActive, "echo hi")
		d.Deps = []string{"dep"}
		assert.Empty(t, JobIdentity(d, Index([]*Job{dep, d})))
		assert.Empty(t, CacheHit(JobIdentity(d, Index([]*Job{dep, d})), idx, d.ID))
	})
	t.Run("a dependency identity change is a miss", func(t *testing.T) {
		dep := job("dep", StateCompleted)
		dep.Identity = "sha256:one"
		d := mk("d", StateActive, "echo hi")
		d.Deps = []string{"dep"}
		byID := Index([]*Job{dep, d})
		first := JobIdentity(d, byID)
		require.NotEmpty(t, first)
		dep.Identity = "sha256:two"
		assert.NotEqual(t, first, JobIdentity(d, byID))
	})
}

func TestIndex(t *testing.T) {
	jobs := []*Job{job("a", StateActive), job("b", StateDraft)}
	idx := Index(jobs)
	require.Len(t, idx, 2)
	assert.Equal(t, StateDraft, idx["b"].State)
}
