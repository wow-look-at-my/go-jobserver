package jobserver

// maxDepth caps Depths. A job that deep, or one that depends on a cycle, reports it.
const maxDepth = 65

// Children maps each job ID to the jobs that list it as a dependency, in the
// order of jobs. A job that lists a dependency twice appears twice.
func Children(jobs []*Job) map[string][]string {
	out := make(map[string][]string, len(jobs))
	for _, j := range jobs {
		for _, d := range j.Deps {
			out[d] = append(out[d], j.ID)
		}
	}
	return out
}

// Depths maps each job ID to the length of its longest dependency path, capped at maxDepth.
func Depths(jobs []*Job) map[string]int {
	return depthsFrom(jobs, Children(jobs))
}

// depthsFrom is Depths for a children index that the caller already built. It
// settles each job once, after the jobs it depends on, so the work grows
// linearly with the graph. A job that never settles depends on a cycle.
func depthsFrom(jobs []*Job, children map[string][]string) map[string]int {
	byID := Index(jobs)
	open := make(map[string]int, len(jobs))
	depth := make(map[string]int, len(jobs))
	var settled []string
	for _, j := range jobs {
		depth[j.ID] = 0
		for _, d := range j.Deps {
			if _, ok := byID[d]; ok {
				open[j.ID]++
			}
		}
		if open[j.ID] == 0 {
			settled = append(settled, j.ID)
		}
	}
	for len(settled) > 0 {
		id := settled[len(settled)-1]
		settled = settled[:len(settled)-1]
		for _, child := range children[id] {
			depth[child] = max(depth[child], min(depth[id]+1, maxDepth))
			if open[child]--; open[child] == 0 {
				settled = append(settled, child)
			}
		}
	}
	for _, j := range jobs {
		if open[j.ID] > 0 {
			depth[j.ID] = maxDepth
		}
	}
	return depth
}
