package jobserver

import (
	"sort"
)

// The scheduler is pure: every function here reads a snapshot of the job
// table and returns what should happen next.

// Index maps job IDs to jobs.
func Index(jobs []*Job) map[string]*Job {
	out := make(map[string]*Job, len(jobs))
	for _, j := range jobs {
		out[j.ID] = j
	}
	return out
}

// Ready returns the IDs of active jobs whose dependencies have all succeeded,
// in creation order. A job with no dependencies is ready as soon as it is
// active.
func Ready(jobs []*Job) []string {
	byID := Index(jobs)
	var out []string
	for _, j := range SortedJobs(jobs) {
		if j.State != StateActive {
			continue
		}
		ok := true
		for _, d := range j.Deps {
			dep, found := byID[d]
			if !found || !dep.State.succeeded() {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, j.ID)
		}
	}
	return out
}

// Dead returns the IDs of jobs that will never succeed: the ones that already
// failed or were cancelled, plus everything downstream of them.
func Dead(jobs []*Job) map[string]bool {
	dead := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		if j.State.Terminal() && !j.State.succeeded() {
			dead[j.ID] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for _, j := range jobs {
			if j.State.Terminal() || dead[j.ID] {
				continue
			}
			for _, d := range j.Deps {
				if dead[d] {
					dead[j.ID] = true
					changed = true
					break
				}
			}
		}
	}
	return dead
}

// Blocked returns the IDs of jobs that should move to StateBlocked: not
// terminal yet, and downstream of a job that will never succeed.
func Blocked(jobs []*Job) []string {
	dead := Dead(jobs)
	var out []string
	for _, j := range SortedJobs(jobs) {
		if j.State.Terminal() || !dead[j.ID] {
			continue
		}
		hasDeadDep := false
		for _, d := range j.Deps {
			if dead[d] {
				hasDeadDep = true
				break
			}
		}
		if hasDeadDep {
			out = append(out, j.ID)
		}
	}
	return out
}

// CyclePath returns the IDs forming one dependency cycle, or nil when the
// graph is acyclic.
func CyclePath(jobs []*Job) []string {
	byID := Index(jobs)
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(jobs))
	var stack []string
	var cycle []string

	var visit func(id string) bool
	visit = func(id string) bool {
		color[id] = gray
		stack = append(stack, id)
		j, ok := byID[id]
		if !ok {
			stack = stack[:len(stack)-1]
			color[id] = black
			return false
		}
		for _, d := range j.Deps {
			switch color[d] {
			case gray:
				// Found the loop: the stack from d's first appearance onward.
				for i, s := range stack {
					if s == d {
						cycle = append([]string(nil), stack[i:]...)
						cycle = append(cycle, d)
						return true
					}
				}
			case white:
				if visit(d) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return false
	}

	for _, id := range sortedIDs(jobs) {
		if color[id] == white {
			if visit(id) {
				return cycle
			}
		}
	}
	return nil
}

// TopoOrder returns job IDs with every dependency before its dependents, or
// nil when the graph has a cycle.
func TopoOrder(jobs []*Job) []string {
	byID := Index(jobs)
	indeg := make(map[string]int, len(jobs))
	dependents := make(map[string][]string, len(jobs))
	for _, j := range jobs {
		if _, ok := indeg[j.ID]; !ok {
			indeg[j.ID] = 0
		}
		for _, d := range j.Deps {
			if _, ok := byID[d]; !ok {
				continue
			}
			indeg[j.ID]++
			dependents[d] = append(dependents[d], j.ID)
		}
	}
	var queue []string
	for _, id := range sortedIDs(jobs) {
		if indeg[id] == 0 {
			queue = append(queue, id)
		}
	}
	out := make([]string, 0, len(jobs))
	for len(queue) > 0 {
		// Pop the smallest ID so the order is stable.
		sort.Strings(queue)
		id := queue[0]
		queue = queue[1:]
		out = append(out, id)
		for _, dep := range dependents[id] {
			indeg[dep]--
			if indeg[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}
	if len(out) != len(jobs) {
		return nil
	}
	return out
}

// CacheHit returns the ID of a finished job that already produced this
// identity, or "" when there is none.
func CacheHit(identity string, idx IdentityIndex, selfID string) string {
	if identity == "" {
		return ""
	}
	cand, ok := idx[identity]
	if !ok || cand == selfID {
		return ""
	}
	return cand
}

// JobIdentity computes what a job's work is worth deduping on: its command,
// directory, environment, outputs, hashed inputs, and the identities of its
// dependencies. A dependency contributes its identity rather than its name, so
// the same work reaches the same identity through a cached copy of it. An
// empty string means the identity cannot be computed yet.
func JobIdentity(j *Job, byID map[string]*Job) string {
	deps := make([]string, 0, len(j.Deps))
	for _, d := range j.Deps {
		dep, ok := byID[d]
		if !ok || dep.Identity == "" {
			return ""
		}
		deps = append(deps, dep.Identity)
	}
	sort.Strings(deps)
	return computeIdentity(identityInput{
		Key:     j.Key,
		Command: j.Command,
		Dir:     j.Dir,
		Env:     append([]string(nil), j.Env...),
		Outputs: sortedCopy(j.Outputs),
		Inputs:  sortedCopy(j.Inputs),
		Deps:    deps,
	})
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func sortedIDs(jobs []*Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.ID)
	}
	sort.Strings(out)
	return out
}
