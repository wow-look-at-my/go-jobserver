// Schedules add a task on a fixed interval.

package jobserver

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wow-look-at-my/go-jobserver/priority"
)

// ScheduleTick is how often the server looks for due schedules.
const ScheduleTick = time.Second

// Schedule adds one task every Every.
type Schedule struct {
	Every time.Duration
	// Add adds the task of one tick and returns it. A nil task with a nil error is a tick that Add declined.
	Add func() (*Task, error)
	// OnSkip hears each tick the schedule skipped because previous has not ended. It must not block.
	OnSkip func(name string, previous *Task)
}

// ScheduleInfo is one schedule as Schedules reports it.
type ScheduleInfo struct {
	Name  string        `json:"name"`
	Every time.Duration `json:"every"`
	Next  time.Time     `json:"next"`
	// LastTask is the task the schedule added last, and LastAdded is when.
	LastTask  string    `json:"last_task,omitempty"`
	LastAdded time.Time `json:"last_added,omitzero"`
	Added     int       `json:"added"`
	// Skipped counts the ticks skipped because LastTask had not ended.
	Skipped     int       `json:"skipped"`
	LastSkipped time.Time `json:"last_skipped,omitzero"`
	// LastError is the error of the last Add call that failed, until an Add succeeds.
	LastError string `json:"last_error,omitempty"`
}

type scheduleEntry struct {
	sc   Schedule
	info ScheduleInfo
	last *Task
}

// scheduleSet holds the schedules of one server.
type scheduleSet struct {
	mu      sync.Mutex
	now     func() time.Time
	logf    func(format string, args ...any)
	entries map[string]*scheduleEntry
}

func newScheduleSet(logf func(string, ...any)) *scheduleSet {
	return &scheduleSet{now: time.Now, logf: logf, entries: map[string]*scheduleEntry{}}
}

// SetSchedule adds or replaces the schedule under name. An unchanged Every keeps the next tick. A changed Every makes
// it due at once. An Every of zero or less removes the schedule. A tick skips while the task the schedule added last
// has not ended. After a long pause the schedule adds one task, not one per missed interval.
func (s *Server) SetSchedule(name string, sc Schedule) error {
	if sc.Every <= 0 {
		s.RemoveSchedule(name)
		return nil
	}
	if sc.Add == nil {
		return fmt.Errorf("%w: schedule %s has no Add", ErrBadState, name)
	}
	ss := s.every
	now := ss.now()
	ss.mu.Lock()
	defer ss.mu.Unlock()
	e := ss.entries[name]
	if e == nil {
		e = &scheduleEntry{info: ScheduleInfo{Name: name}}
		ss.entries[name] = e
	}
	if e.info.Every != sc.Every {
		e.info.Every, e.info.Next = sc.Every, now
	}
	e.sc = sc
	return nil
}

// RemoveSchedule removes the schedule under name. The task it added last goes on. It reports whether one was there.
func (s *Server) RemoveSchedule(name string) bool {
	ss := s.every
	ss.mu.Lock()
	defer ss.mu.Unlock()
	_, ok := ss.entries[name]
	delete(ss.entries, name)
	return ok
}

// Schedules reports every schedule, sorted by name.
func (s *Server) Schedules() []ScheduleInfo {
	ss := s.every
	ss.mu.Lock()
	defer ss.mu.Unlock()
	out := make([]ScheduleInfo, 0, len(ss.entries))
	for _, e := range ss.entries {
		out = append(out, e.info)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Name < out[k].Name })
	return out
}

// scheduleLoop adds the tasks of due schedules until the server closes.
func (s *Server) scheduleLoop() {
	defer s.wg.Done()
	t := time.NewTicker(ScheduleTick)
	defer t.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-t.C:
			s.every.fireDue()
		}
	}
}

// fireDue runs each due schedule once, in name order, and moves its next tick one interval past now.
func (ss *scheduleSet) fireDue() {
	now := ss.now()
	type due struct {
		name string
		e    *scheduleEntry
		sc   Schedule
		last *Task
	}
	var list []due
	ss.mu.Lock()
	for name, e := range ss.entries {
		if now.Before(e.info.Next) {
			continue
		}
		e.info.Next = now.Add(e.info.Every)
		list = append(list, due{name: name, e: e, sc: e.sc, last: e.last})
	}
	ss.mu.Unlock()
	sort.Slice(list, func(i, k int) bool { return list[i].name < list[k].name })
	for _, d := range list {
		if d.last != nil && unfinished(d.last.State()) {
			ss.mu.Lock()
			d.e.info.Skipped++
			d.e.info.LastSkipped = now
			ss.mu.Unlock()
			ss.logf("go-jobserver: schedule %s: skipped; task %s has not ended", d.name, d.last.ID)
			if d.sc.OnSkip != nil {
				d.sc.OnSkip(d.name, d.last)
			}
			continue
		}
		t, err := d.sc.Add()
		ss.mu.Lock()
		switch {
		case err != nil:
			d.e.info.LastError = err.Error()
		case t != nil:
			d.e.last = t
			d.e.info.LastTask, d.e.info.LastAdded, d.e.info.LastError = t.ID, now, ""
			d.e.info.Added++
		}
		ss.mu.Unlock()
		if err != nil {
			ss.logf("go-jobserver: ERROR schedule %s: add: %v", d.name, err)
		}
	}
}

// lastOf names the schedule whose last task is id, or "".
func (ss *scheduleSet) lastOf(id string) string {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for name, e := range ss.entries {
		if e.info.LastTask == id {
			return name
		}
	}
	return ""
}

func unfinished(st TaskState) bool { return st == Queued || st == Running }

// errClientSchedule is returned by Every on an inbox that talks to a remote server.
var errClientSchedule = errors.New("a schedule runs in the server process")

// Every adds a task to the inbox every interval, under name. build makes the spec and its priority. When build
// returns false the tick adds nothing. The rules of SetSchedule apply. Server.Schedules reports it, and
// Server.RemoveSchedule removes it.
func (ib *Inbox[T, R]) Every(name string, every time.Duration, build func() (T, priority.MultiPriority, bool)) error {
	var s *Server
	switch b := ib.b.(type) {
	case localNamed:
		s = b.Server
	case localDefault:
		s = b.Server
	default:
		return fmt.Errorf("%w: inbox %s: %w", ErrBadInbox, ib.name, errClientSchedule)
	}
	return s.SetSchedule(name, Schedule{Every: every, Add: func() (*Task, error) {
		spec, p, ok := build()
		if !ok {
			return nil, nil
		}
		return ib.AddTask(spec, p)
	}})
}
