// Changing the priority of a task before it is admitted.

package jobserver

import (
	"fmt"

	"github.com/wow-look-at-my/go-jobserver/priority"
)

// SetPriority replaces the priority of a task that is not admitted yet. A
// task that waits for admission goes back in line at its new place.
func (ib *Inbox[T, R]) SetPriority(t *Task, p priority.MultiPriority) error {
	return ib.b.setPriority(t.ID, ib.keys.Values(p))
}

// setPriority writes the new values and moves the task's waiting lease to its new place.
func (c *inboxCore) setPriority(id string, values map[string]int) error {
	prio, err := c.keys.FromValues(values)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	j, ok := c.s.store.Job(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if j.Inbox != c.name && !(c.isDefault && j.Inbox == "") {
		return fmt.Errorf("%w: task %s is not in inbox %s", ErrNotFound, id, c.name)
	}
	if j.State.Terminal() || j.State == StateRunning || c.admitted[j.ID] != nil || c.out[j.ID] != nil {
		return fmt.Errorf("%w: task %s is %s and was admitted already", ErrBadState, j.ID, j.State)
	}
	if err := c.s.store.SetPriority(j.ID, values); err != nil {
		return err
	}
	cancel, ok := c.leasing[j.ID]
	if !ok || c.s.sched.reorder(j.ID, prio) {
		return nil
	}
	// The lease is not in the queue yet, so its order is still the one. Offer the task again.
	delete(c.leasing, j.ID)
	cancel()
	c.s.Wake()
	return nil
}

// reorder gives a waiting lease a new priority and plans again. It is false when no lease with the ID waits.
func (s *Scheduler) reorder(id string, p priority.MultiPriority) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range s.waiting {
		if l.req.ID == id {
			l.order.prio = p
			s.planLocked()
			return true
		}
	}
	return false
}

// SetPriority replaces the priority values of a task that has not ended.
func (s *Store) SetPriority(id string, values map[string]int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.liveJob(id)
	if err != nil {
		return err
	}
	if values == nil {
		values = map[string]int{}
	}
	return s.noteDetail(j, detailRec{Priority: &values})
}

func (b localNamed) setPriority(id string, values map[string]int) error {
	return b.c.setPriority(id, values)
}

func (b localDefault) setPriority(id string, values map[string]int) error {
	return b.defCore.setPriority(id, values)
}

func (b clientInbox) setPriority(id string, values map[string]int) error {
	_, err := b.call(Request{Op: OpInboxPriority, ID: id, TaskRequest: TaskRequest{Priority: values}})
	return err
}
