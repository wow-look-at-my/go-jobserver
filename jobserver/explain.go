// Why a task is not running: the waits every view shows, and the one-line summary that leads each view.

package jobserver

import (
	"fmt"
	"sort"
	"strings"
)

// Wait kinds beyond the scheduler's own cap, group and cpu.
const (
	// WaitDep is a dependency or a trigger that has not fired.
	WaitDep = "dep"
	// WaitMonitor is a monitor dependency that has not seen its condition hold.
	WaitMonitor = "monitor"
	// WaitDraft is a task nobody activated yet.
	WaitDraft = "draft"
	// WaitPaused is a paused server.
	WaitPaused = "paused"
	// WaitConsumer is a task of a consumer inbox that waits for a consumer, or for its inbox to open.
	WaitConsumer = "consumer"
)

// Wait is one thing a task waits on.
type Wait struct {
	Kind string `json:"kind"`
	// ID names the dependency, for a dep or monitor wait.
	ID    string `json:"id,omitempty"`
	State State  `json:"state,omitempty"`
	// Detail says what the wait is, in words.
	Detail string `json:"detail"`
}

// Explain lists what each waiting task waits on. A running task that waits for a scheduler lease under its own ID
// counts as waiting. A task that runs, or will never run, is left out.
func (s *Server) Explain() map[string][]Wait {
	jobs := s.List()
	byID := Index(jobs)
	paused := s.store.Paused()
	queued := s.queued()
	out := map[string][]Wait{}
	for _, j := range jobs {
		if w := s.waitsOf(j, byID, paused, queued); len(w) > 0 {
			out[j.ID] = w
		}
	}
	return out
}

// queued maps each lease that waits on the scheduler to where it stands.
func (s *Server) queued() map[string]QueueState {
	out := map[string]QueueState{}
	for _, l := range s.sched.Status().Waiting {
		if l.Wait != nil {
			out[l.ID] = *l.Wait
		}
	}
	return out
}

// queueWait is the wait of a task whose lease waits on the scheduler under the task's own ID.
func queueWait(q QueueState, j *Job) []Wait {
	return []Wait{{Kind: q.Kind, ID: q.Key, State: j.State, Detail: q.Reason}}
}

// Why lists what one task waits on. It is empty for a task that has ended, and for a running task unless the
// task waits for a scheduler lease. That lease is under its own ID.
func (s *Server) Why(id string) ([]Wait, error) {
	j, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	return s.waitsOf(j, Index(s.List()), s.store.Paused(), s.queued()), nil
}

// triggerWords says what a trigger waits for.
var triggerWords = map[string]string{triggerSuccess: "succeeds", triggerFail: "fails", triggerEnded: "ends"}

func (s *Server) waitsOf(j *Job, byID map[string]*Job, paused bool, queued map[string]QueueState) []Wait {
	switch {
	case j.State.Terminal():
		return nil
	case j.State == StateRunning:
		if q, ok := queued[j.ID]; ok {
			return queueWait(q, j)
		}
		return nil
	case j.State == StateDraft:
		return []Wait{{Kind: WaitDraft, Detail: "a draft: it runs only after go-jobserver activate " + j.ID}}
	}
	var out []Wait
	for _, d := range j.Deps {
		dep, ok := byID[d]
		switch {
		case !ok:
			out = append(out, Wait{Kind: WaitDep, ID: d, Detail: "no such task"})
		case dep.Monitor != nil && !dep.State.Terminal():
			out = append(out, Wait{Kind: WaitMonitor, ID: d, State: dep.State, Detail: monitorLine(dep)})
		case j.Trigger != "" && !dep.State.Terminal():
			out = append(out, Wait{Kind: WaitDep, ID: d, State: dep.State,
				Detail: fmt.Sprintf("trigger %s: it runs when %s %s; that task is %s", j.Trigger, jobLabel(dep), triggerWords[j.Trigger], dep.State)})
		case j.Trigger != "", dep.State.succeeded():
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
	if q, ok := queued[j.ID]; ok {
		return queueWait(q, j)
	}
	if j.Inbox == "" {
		a, err := s.sched.Admits(s.leaseFor(j))
		switch {
		case err != nil:
			return []Wait{{Kind: WaitCap, Detail: err.Error()}}
		case a.Starts:
			return []Wait{{Kind: WaitCap, Detail: "ready: it starts at the next scheduling pass"}}
		}
		return []Wait{{Kind: a.Wait.Kind, ID: a.Wait.Key, Detail: a.Wait.Reason}}
	}
	c := s.inbox(j.Inbox)
	if c == nil {
		return []Wait{{Kind: WaitConsumer, ID: j.Inbox, Detail: "inbox " + j.Inbox + " is not open in this process"}}
	}
	return []Wait{c.idleWait(j.ID)}
}

// idleWait is the wait of a ready task that holds no place in the scheduler's queue.
func (c *inboxCore) idleWait(id string) Wait {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.admitted[id] != nil:
		return Wait{Kind: WaitConsumer, ID: c.name, Detail: "admitted: it waits for a consumer of inbox " + c.name + " to take it"}
	case c.run == nil && c.consumer == "":
		return Wait{Kind: WaitConsumer, ID: c.name, Detail: "no consumer is bound to inbox " + c.name}
	}
	return Wait{Kind: WaitCap, Detail: "ready: it starts at the next scheduling pass"}
}

// monitorLine says where a monitor stands, in one line.
func monitorLine(m *Job) string {
	key := m.Monitor.Key()
	st := m.MonitorStatus
	switch {
	case m.State == StateActive:
		return key + ": starting"
	case st == nil && m.State == StateRunning, st != nil && st.Checks == 0 && st.LastError == "":
		return key + ": no check has run yet"
	case st == nil:
		return key + ": " + string(m.State)
	case st.LastError != "":
		return fmt.Sprintf("%s: check %d FAILED (%d in a row): %s", key, st.Checks, st.ErrorsInARow, st.LastError)
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
		// A running task that waits for its lease counts as waiting, not as running.
		if _, waits := waits[j.ID]; j.State != StateRunning || waits {
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
		WaitDep:      "on unfinished tasks",
		WaitConsumer: "for a consumer",
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
