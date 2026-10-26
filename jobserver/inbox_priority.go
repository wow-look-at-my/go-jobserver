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

// setPriority writes the new values and asks for admission again with them.
func (c *inboxCore) setPriority(id string, values map[string]int) error {
	if _, err := c.keys.FromValues(values); err != nil {
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
	if cancel, ok := c.leasing[j.ID]; ok {
		delete(c.leasing, j.ID)
		cancel()
	}
	c.s.Wake()
	return nil
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
