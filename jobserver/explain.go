// Why a job is not running: the waits every view shows, and the one-line summary that leads each view.

package jobserver

import (
	"fmt"
	"sort"
	"strings"
)

// Wait kinds beyond the scheduler's own cap, group and cpu.
const (
	// WaitDep is a dependency that has not finished.
	WaitDep = "dep"
	// WaitMonitor is a monitor dependency that has not seen its condition hold.
	WaitMonitor = "monitor"
	// WaitDraft is a job nobody activated yet.
	WaitDraft = "draft"
	// WaitPaused is a paused server.
	WaitPaused = "paused"
)

// Wait is one thing a job waits on.
type Wait struct {
	Kind string `json:"kind"`
	// ID names the dependency, for a dep or monitor wait.
	ID    string `json:"id,omitempty"`
	State State  `json:"state,omitempty"`
	// Detail says what the wait is, in words.
	Detail string `json:"detail"`
}

// Explain lists what each waiting job waits on. A job that runs, or will never run, is left out.
func (s *Server) Explain() map[string][]Wait {
	jobs := s.List()
	byID := Index(jobs)
	paused := s.store.Paused()
	out := map[string][]Wait{}
	for _, j := range jobs {
		if j.State.Terminal() || j.State == StateRunning {
			continue
		}
		if w := s.waitsOf(j, byID, paused); len(w) > 0 {
			out[j.ID] = w
		}
	}
	return out
}

// Why lists what one job waits on. It is empty for a job that runs or has ended.
func (s *Server) Why(id string) ([]Wait, error) {
	j, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	if j.State.Terminal() || j.State == StateRunning {
		return nil, nil
	}
	return s.waitsOf(j, Index(s.List()), s.store.Paused()), nil
}

func (s *Server) waitsOf(j *Job, byID map[string]*Job, paused bool) []Wait {
	if j.State == StateDraft {
		return []Wait{{Kind: WaitDraft, Detail: "a draft: it runs only after go-jobserver activate " + j.ID}}
	}
	var out []Wait
	for _, d := range j.Deps {
		dep, ok := byID[d]
		switch {
		case !ok:
			out = append(out, Wait{Kind: WaitDep, ID: d, Detail: "no such job"})
		case dep.State.succeeded():
		case dep.Monitor != nil:
			out = append(out, Wait{Kind: WaitMonitor, ID: d, State: dep.State, Detail: monitorLine(dep)})
		default:
			out = append(out, Wait{Kind: WaitDep, ID: d, State: dep.State, Detail: fmt.Sprintf("%s is %s", jobLabel(dep), dep.State)})
		}
	}
	if len(out) > 0 {
		return out
	}
	if paused {
		return []Wait{{Kind: WaitPaused, Detail: "the server is paused: go-jobserver resume starts it again"}}
	}
	a, err := s.sched.Admits(s.leaseFor(j))
	switch {
	case err != nil:
		return []Wait{{Kind: WaitCap, Detail: err.Error()}}
	case a.Starts:
		return []Wait{{Kind: WaitCap, Detail: "ready: it starts at the next scheduling pass"}}
	}
	return []Wait{{Kind: a.Wait.Kind, ID: a.Wait.Key, Detail: a.Wait.Reason}}
}

// monitorLine says where a monitor stands, in one line.
func monitorLine(m *Job) string {
	key := m.Monitor.Key()
	st := m.MonitorStatus
	switch {
	case m.State == StateActive:
		return key + ": starting"
	case st == nil:
		return key + ": " + string(m.State)
	case st.LastError != "":
		return fmt.Sprintf("%s: check %d FAILED (%d in a row): %s", key, st.Checks, st.ErrorsInARow, st.LastError)
	case st.Checks == 0:
		return key + ": no check has run yet"
	}
	return fmt.Sprintf("%s: not yet after %d checks: %s", key, st.Checks, st.LastDetail)
}

func jobLabel(j *Job) string {
	if j.Name != "" && j.Name != j.ID {
		return j.ID + " (" + j.Name + ")"
	}
	return j.ID
}

// Summary is the line that leads every view. It says what the waiting jobs wait on, so an empty run table explains itself.
func Summary(jobs []*Job, waits map[string][]Wait) string {
	watching, running := 0, 0
	for _, j := range jobs {
		if j.State != StateRunning {
			continue
		}
		if j.Monitor != nil {
			watching++
		} else {
			running++
		}
	}
	monitors := map[string]int{}
	byKind := map[string]int{}
	waiting := 0
	for _, ws := range waits {
		waiting++
		seen := map[string]int{}
		for _, w := range ws {
			if w.Kind == WaitMonitor {
				monitors[w.ID]++
			}
			seen[w.Kind]++
		}
		for k := range seen {
			byKind[k]++
		}
	}
	parts := []string{fmt.Sprintf("%d running", running), fmt.Sprintf("%d %s watching", watching, plural(watching, "monitor", "monitors"))}
	if waiting == 0 {
		return strings.Join(parts, ", ") + "; nothing waits"
	}
	words := map[string]string{
		WaitMonitor: fmt.Sprintf("on %d %s", len(monitors), plural(len(monitors), "monitor", "monitors")),
		WaitDep:     "on unfinished jobs",
		WaitCap:     "for the run cap",
		WaitGroup:   "for a group slot",
		WaitCPU:     "for CPU",
		WaitDraft:   "as drafts",
		WaitPaused:  "because the server is paused",
	}
	kinds := make([]string, 0, len(byKind))
	for k := range byKind {
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(i, k int) bool {
		return byKind[kinds[i]] > byKind[kinds[k]] || byKind[kinds[i]] == byKind[kinds[k]] && kinds[i] < kinds[k]
	})
	var why []string
	for _, k := range kinds {
		why = append(why, fmt.Sprintf("%d %s %s", byKind[k], plural(byKind[k], "waits", "wait"), words[k]))
	}
	return strings.Join(parts, ", ") + "; " + strings.Join(why, ", ")
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
