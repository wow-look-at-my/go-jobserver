// The index of each inbox, so a Take or a count reads the tasks of one inbox and no others.

package jobserver

import (
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// inboxIndex holds, for each inbox, the tasks that have not ended, the running tasks and the running tasks that hold
// an open wait. The database holds the ended tasks.
type inboxIndex struct {
	// where is the inbox each indexed task is under.
	where   map[string]string
	live    map[string]*set.Set[string]
	running map[string]*set.Set[string]
	waiting map[string]*set.Set[string]
}

func newInboxIndex() inboxIndex {
	return inboxIndex{where: map[string]string{}, live: map[string]*set.Set[string]{},
		running: map[string]*set.Set[string]{}, waiting: map[string]*set.Set[string]{}}
}

// mark adds the task to a set when on is true, and takes it out when on is false.
func mark(m map[string]*set.Set[string], on bool, inbox, id string) {
	if on {
		addTo(m, inbox, id)
		return
	}
	dropFrom(m, inbox, id)
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

// put indexes a task under its inbox now. A task that ended leaves the index.
func (x inboxIndex) put(j *Job) {
	if j.State.Terminal() {
		x.remove(j.ID)
		return
	}
	x.where[j.ID] = j.Inbox
	running := j.State == StateRunning
	mark(x.live, !j.State.Terminal(), j.Inbox, j.ID)
	mark(x.running, running, j.Inbox, j.ID)
	mark(x.waiting, running && j.openWaits > 0, j.Inbox, j.ID)
}

// remove forgets a job.
func (x inboxIndex) remove(id string) {
	inbox, ok := x.where[id]
	if !ok {
		return
	}
	delete(x.where, id)
	dropFrom(x.live, inbox, id)
	dropFrom(x.running, inbox, id)
	dropFrom(x.waiting, inbox, id)
}

// TaskCounts says how many tasks of an inbox wait and how many run or are checked out.
type TaskCounts struct {
	Queued  int `json:"queued"`
	Running int `json:"running"`
	// Waiting is how many running tasks hold at least one open wait.
	Waiting int `json:"waiting"`
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

// Counts reports the tasks of one inbox from the sizes of its index sets, and the ended ones from a count in the
// database. It reads no task.
func (s *Store) Counts(inbox string) TaskCounts {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := inboxKey(inbox)
	live, running := size(s.idx.live[key]), size(s.idx.running[key])
	c := TaskCounts{Queued: live - running, Running: running, Waiting: size(s.idx.waiting[key])}
	if s.db == nil {
		return c
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE inbox = ? AND ended IS NOT NULL`, key).Scan(&c.Ended); err != nil {
		s.readErr("the count of the ended tasks of inbox "+inbox, err)
	}
	return c
}

// liveInboxes returns the key of each inbox that holds a task that has not ended.
func (s *Store) liveInboxes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Collect(maps.Keys(s.idx.live))
}

// readyTask is a task of an inbox whose trigger fired and that has not started.
type readyTask struct {
	id       string
	priority map[string]int
	created  time.Time
}

// readyIn lists the ready tasks of one inbox. It reads only that inbox's tasks and their dependencies.
func (s *Store) readyIn(inbox string) []readyTask {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
			out = append(out, readyTask{id: id, priority: j.Priority, created: j.Created})
		}
	}
	return out
}

// liveTask is what a caller outside the lock reads of a task that has not ended.
type liveTask struct {
	id    string
	key   string
	state State
	spec  json.RawMessage
}

// liveIn returns each task of one inbox that has not ended, in creation order. It copies the fields a caller needs,
// and no step or wait.
func (s *Store) liveIn(inbox string) []liveTask {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := s.sortedLocked(s.idx.live[inboxKey(inbox)])
	out := make([]liveTask, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, liveTask{id: j.ID, key: j.Key, state: j.State, spec: j.Spec})
	}
	return out
}

// RunningIn returns the IDs of the running tasks of one inbox, in creation order. It reads the index and copies no task.
func (s *Store) RunningIn(inbox string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := slices.Collect(each(s.idx.running[inboxKey(inbox)]))
	sort.Slice(ids, func(a, b int) bool {
		ja, jb := s.jobs[ids[a]], s.jobs[ids[b]]
		if !ja.Created.Equal(jb.Created) {
			return ja.Created.Before(jb.Created)
		}
		return ids[a] < ids[b]
	})
	return ids
}

// InboxJobs returns the record of each task the store holds for one inbox, in creation order. It reads the ended
// tasks from the database.
func (s *Store) InboxJobs(inbox string) []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inboxLocked(inbox, 0)
}

// inboxLocked returns the record of each task of one inbox that changed after since, in creation order. Caller holds s.mu.
func (s *Store) inboxLocked(inbox string, since uint64) []*Job {
	key := inboxKey(inbox)
	out, err := s.loadWhere("t.inbox = ? AND t.seq > ?", "t.created, t.id", key, int64(since))
	if err != nil {
		s.readErr("the tasks of inbox "+inbox, err)
		var mem []*Job
		for _, j := range s.jobs {
			if j.Inbox == key && j.Seq > since {
				mem = append(mem, j)
			}
		}
		return SortedJobs(mem)
	}
	s.visits.Add(int64(len(out)))
	return out
}

// inboxSince runs fn under one read lock with the record of each task of one inbox that changed after since, and
// returns the page header.
func (s *Store) inboxSince(inbox string, since uint64, fn func(jobs []*Job)) TaskPage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(s.inboxLocked(inbox, since))
	return TaskPage{Epoch: s.epoch, Seq: s.seq, Pruned: s.prunes}
}

func (s *Store) sortedLocked(ids *set.Set[string]) []*Job {
	out := make([]*Job, 0, size(ids))
	for id := range each(ids) {
		s.visits.Add(1)
		out = append(out, s.jobs[id])
	}
	sort.Slice(out, func(a, b int) bool {
		if !out[a].Created.Equal(out[b].Created) {
			return out[a].Created.Before(out[b].Created)
		}
		return out[a].ID < out[b].ID
	})
	return out
}
