// The index of each inbox, so a Take or a count reads the tasks of one inbox and no others.

package jobserver

import (
	"maps"
	"sort"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// inboxIndex holds, for each inbox, every task the store holds and the tasks that have not ended.
type inboxIndex struct {
	// where is the inbox each task is indexed under.
	where   map[string]string
	members map[string]*set.Set[string]
	live    map[string]*set.Set[string]
}

func newInboxIndex() inboxIndex {
	return inboxIndex{where: map[string]string{}, members: map[string]*set.Set[string]{}, live: map[string]*set.Set[string]{}}
}

func addTo(m map[string]*set.Set[string], inbox, id string) {
	ids := m[inbox]
	if ids == nil {
		v := set.New[string]()
		ids = &v
		m[inbox] = ids
	}
	ids.Add(id)
}

func dropFrom(m map[string]*set.Set[string], inbox, id string) {
	if ids := m[inbox]; ids != nil {
		ids.Remove(id)
		if ids.IsEmpty() {
			delete(m, inbox)
		}
	}
}

// size is the length of a set that can be missing.
func size(ids *set.Set[string]) int {
	if ids == nil {
		return 0
	}
	return ids.Len()
}

// each yields the members of a set that can be missing.
func each(ids *set.Set[string]) func(yield func(string) bool) {
	if ids == nil {
		return func(func(string) bool) {}
	}
	return ids.All()
}

// put indexes a job under its inbox now. A task that moved to another inbox
// leaves the one.
func (x inboxIndex) put(j *Job) {
	if old, ok := x.where[j.ID]; ok && old != j.Inbox {
		x.remove(j.ID)
	}
	x.where[j.ID] = j.Inbox
	addTo(x.members, j.Inbox, j.ID)
	if j.State.Terminal() {
		dropFrom(x.live, j.Inbox, j.ID)
		return
	}
	addTo(x.live, j.Inbox, j.ID)
}

// remove forgets a job.
func (x inboxIndex) remove(id string) {
	inbox, ok := x.where[id]
	if !ok {
		return
	}
	delete(x.where, id)
	dropFrom(x.members, inbox, id)
	dropFrom(x.live, inbox, id)
}

// TaskCounts says how many tasks of an inbox wait and how many run or are checked out.
type TaskCounts struct {
	Queued  int `json:"queued"`
	Running int `json:"running"`
	// Ended is how many ended tasks the store still holds.
	Ended int `json:"ended"`
}

// inboxKey is the index key of an inbox name. The default inbox is the empty key.
func inboxKey(name string) string {
	if name == DefaultInboxName {
		return ""
	}
	return name
}

// Counts reports the tasks of one inbox. It reads only that inbox's tasks.
func (s *Store) Counts(inbox string) TaskCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := inboxKey(inbox)
	var c TaskCounts
	for id := range each(s.idx.live[key]) {
		s.visits.Add(1)
		if s.jobs[id].State == StateRunning {
			c.Running++
		} else {
			c.Queued++
		}
	}
	c.Ended = size(s.idx.members[key]) - size(s.idx.live[key])
	return c
}

// readyTask is a task of an inbox whose trigger fired and that has not started.
type readyTask struct {
	id       string
	priority map[string]int
	created  time.Time
}

// readyIn lists the ready tasks of one inbox. It reads only that inbox's tasks and their dependencies.
func (s *Store) readyIn(inbox string) []readyTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []readyTask
	for id := range each(s.idx.live[inboxKey(inbox)]) {
		s.visits.Add(1)
		j := s.jobs[id]
		if j.State != StateActive {
			continue
		}
		ok := true
		for _, d := range j.Deps {
			dep, found := s.jobs[d]
			if !found || !fired(j.Trigger, dep) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, readyTask{id: id, priority: maps.Clone(j.Priority), created: j.Created})
		}
	}
	return out
}

// liveIn returns a copy of each task of one inbox that has not ended, in creation order.
func (s *Store) liveIn(inbox string) []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sortedLocked(s.idx.live[inboxKey(inbox)])
}

// InboxJobs returns a copy of each task the store holds for one inbox, in creation order.
func (s *Store) InboxJobs(inbox string) []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sortedLocked(s.idx.members[inboxKey(inbox)])
}

// inboxSince returns a copy of each task of one inbox that changed after since, and the page header, under one lock.
func (s *Store) inboxSince(inbox string, since uint64) ([]*Job, TaskPage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := set.New[string]()
	for id := range each(s.idx.members[inboxKey(inbox)]) {
		if s.jobs[id].Seq > since {
			ids.Add(id)
		}
	}
	return s.sortedLocked(&ids), TaskPage{Epoch: s.epoch, Seq: s.seq, Pruned: s.prunes}
}

func (s *Store) sortedLocked(ids *set.Set[string]) []*Job {
	out := make([]*Job, 0, size(ids))
	for id := range each(ids) {
		s.visits.Add(1)
		out = append(out, s.jobs[id].clone())
	}
	sort.Slice(out, func(a, b int) bool {
		if !out[a].Created.Equal(out[b].Created) {
			return out[a].Created.Before(out[b].Created)
		}
		return out[a].ID < out[b].ID
	})
	return out
}
