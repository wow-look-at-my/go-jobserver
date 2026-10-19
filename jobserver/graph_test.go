package jobserver

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func depJob(id string, deps ...string) *Job { return &Job{ID: id, Deps: deps} }

// inASecond runs fn and fails the test if it needs more than a second.
func inASecond[T any](t *testing.T, fn func() T) T {
	t.Helper()
	done := make(chan T, 1)
	go func() { done <- fn() }()
	select {
	case v := <-done:
		return v
	case <-time.After(time.Second):
	}
	require.FailNow(t, "the call needed more than a second")
	var zero T
	return zero
}

// diamondJobs stacks n diamonds, in creation order. Diamond k has a bottom
// b<k>, a left side l<k> and a right side r<k>. Both sides depend on the
// bottom b<k-1> of the diamond below, and b<k> depends on both sides. The
// longest dependency path from b<k> has 2k links, and b0 has none.
func diamondJobs(n int) []*Job {
	jobs := []*Job{depJob("b0")}
	for k := 1; k <= n; k++ {
		below := fmt.Sprintf("b%d", k-1)
		left, right := fmt.Sprintf("l%d", k), fmt.Sprintf("r%d", k)
		jobs = append(jobs, depJob(left, below), depJob(right, below), depJob(fmt.Sprintf("b%d", k), left, right))
	}
	return jobs
}

func enqueueDiamonds(t *testing.T, srv *Server, n int) {
	t.Helper()
	for _, j := range diamondJobs(n) {
		_, _, err := srv.Enqueue(Spec{ID: j.ID, Command: []string{"x"}, Deps: j.Deps, Draft: true})
		require.NoError(t, err)
	}
}

// assertDiamondDepths checks every depth against the longest path.
func assertDiamondDepths(t *testing.T, n int, depth map[string]int) {
	t.Helper()
	require.Len(t, depth, 3*n+1)
	assert.Equal(t, 0, depth["b0"])
	for k := 1; k <= n; k++ {
		assert.Equal(t, 2*k-1, depth[fmt.Sprintf("l%d", k)], "left side of diamond %d", k)
		assert.Equal(t, 2*k-1, depth[fmt.Sprintf("r%d", k)], "right side of diamond %d", k)
		assert.Equal(t, 2*k, depth[fmt.Sprintf("b%d", k)], "bottom of diamond %d", k)
	}
}

// referenceDepth is the recursive original that Depths replaced. It is
// exponential on stacked diamonds, so the tests run it on small graphs only.
func referenceDepth(j *Job, byID map[string]*Job, depth int) int {
	if depth > 64 || len(j.Deps) == 0 {
		return depth
	}
	best := depth
	for _, d := range j.Deps {
		dep, ok := byID[d]
		if !ok {
			continue
		}
		if got := referenceDepth(dep, byID, depth+1); got > best {
			best = got
		}
	}
	return best
}

// referenceChildren is the loop over all jobs that Children replaced.
func referenceChildren(jobs []*Job, id string) []string {
	var out []string
	for _, other := range jobs {
		for _, d := range other.Deps {
			if d == id {
				out = append(out, other.ID)
			}
		}
	}
	return out
}

func TestDashboardOrdersStackedDiamondsQuickly(t *testing.T) {
	srv := newTestServer(t, nil)
	const n = 30
	enqueueDiamonds(t, srv, n)

	data := inASecond(t, func() pageData { return srv.pageData(nil) })

	depth := map[string]int{}
	for _, row := range data.Graph {
		depth[row.Job.ID] = row.Depth
	}
	assertDiamondDepths(t, n, depth)
}

func TestDashboardListsEachJobsDependents(t *testing.T) {
	srv := newTestServer(t, nil)
	enqueueDiamonds(t, srv, 3)

	children := map[string][]string{}
	for _, row := range srv.pageData(nil).Graph {
		children[row.Job.ID] = row.Children
	}

	assert.Equal(t, []string{"l1", "r1"}, children["b0"])
	assert.Equal(t, []string{"b1"}, children["l1"])
	assert.Equal(t, []string{"b1"}, children["r1"])
	assert.Equal(t, []string{"l2", "r2"}, children["b1"])
	assert.Empty(t, children["b3"])
}

func TestDepthsOfStackedDiamondsAreTheLongestPath(t *testing.T) {
	const n = 30
	jobs := diamondJobs(n)

	depth := inASecond(t, func() map[string]int { return Depths(jobs) })

	assertDiamondDepths(t, n, depth)
}

func TestDepthsStopAtTheLimit(t *testing.T) {
	for _, links := range []int{1, 64, 65, 66, 200} {
		jobs := []*Job{depJob("c0")}
		for i := 1; i <= links; i++ {
			jobs = append(jobs, depJob(fmt.Sprintf("c%d", i), fmt.Sprintf("c%d", i-1)))
		}
		top := jobs[len(jobs)-1]

		got := Depths(jobs)[top.ID]

		assert.Equal(t, min(links, maxDepth), got, "a chain of %d links", links)
		assert.Equal(t, referenceDepth(top, Index(jobs), 0), got, "a chain of %d links against the original", links)
	}
}

func TestDepthsAndChildrenMatchTheOriginalOnRandomGraphs(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for round := 0; round < 200; round++ {
		size := 1 + rng.Intn(12)
		jobs := make([]*Job, 0, size)
		for i := 0; i < size; i++ {
			j := depJob(fmt.Sprintf("j%d", i))
			for k := rng.Intn(4); k > 0 && i > 0; k-- {
				j.Deps = append(j.Deps, fmt.Sprintf("j%d", rng.Intn(i)))
			}
			if rng.Intn(5) == 0 {
				j.Deps = append(j.Deps, "ghost")
			}
			jobs = append(jobs, j)
		}
		byID := Index(jobs)
		depth, children := Depths(jobs), Children(jobs)
		for _, j := range jobs {
			require.Equal(t, referenceDepth(j, byID, 0), depth[j.ID], "round %d, depth of %s", round, j.ID)
			require.Equal(t, referenceChildren(jobs, j.ID), children[j.ID], "round %d, children of %s", round, j.ID)
		}
	}
}

func TestChildrenKeepRepeatedAndMissingDependencies(t *testing.T) {
	jobs := []*Job{
		depJob("a"),
		depJob("b", "a"),
		depJob("c", "a", "b"),
		depJob("d", "c", "c", "ghost"),
		depJob("e", "e"),
	}

	children := Children(jobs)

	assert.Equal(t, []string{"b", "c"}, children["a"])
	assert.Equal(t, []string{"d", "d"}, children["c"])
	assert.Equal(t, []string{"d"}, children["ghost"])
	assert.Equal(t, []string{"e"}, children["e"])
	for _, j := range jobs {
		assert.Equal(t, referenceChildren(jobs, j.ID), children[j.ID], "children of %s", j.ID)
	}
}

func TestDepthsEndOnACycle(t *testing.T) {
	jobs := []*Job{
		depJob("base"),
		depJob("aside", "base"),
		depJob("self", "self"),
		depJob("a", "b", "base"),
		depJob("b", "c"),
		depJob("c", "a"),
		depJob("tail", "a"),
	}

	depth := inASecond(t, func() map[string]int { return Depths(jobs) })

	want := map[string]int{"base": 0, "aside": 1, "self": maxDepth, "a": maxDepth, "b": maxDepth, "c": maxDepth, "tail": maxDepth}
	assert.Equal(t, want, depth)
}

func TestDepthsOfASimpleCycleMatchTheOriginal(t *testing.T) {
	jobs := []*Job{depJob("a", "b"), depJob("b", "c"), depJob("c", "a"), depJob("d", "a", "c")}
	byID := Index(jobs)

	depth := Depths(jobs)

	for _, j := range jobs {
		assert.Equal(t, referenceDepth(j, byID, 0), depth[j.ID], "depth of %s", j.ID)
	}
}
