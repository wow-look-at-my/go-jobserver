// Pause, Resume and Interrupt: the server controls that stop work.

package jobserver

import (
	"fmt"
	"sort"
	"time"
)

// Pause stops scheduling new jobs. Running jobs finish.
func (s *Server) Pause() error {
	if err := s.store.SetPaused(true); err != nil {
		return err
	}
	s.withdrawLeases()
	return nil
}

// Resume allows scheduling again.
func (s *Server) Resume() error {
	if err := s.store.SetPaused(false); err != nil {
		return err
	}
	s.Wake()
	return nil
}

// Paused reports whether scheduling is paused.
func (s *Server) Paused() bool { return s.store.Paused() }

// Interrupt stops one running job and returns once it has stopped, so the
// job's state is settled by the time the caller looks at it.
func (s *Server) Interrupt(id string) error {
	id = CanonicalID(id)
	s.mu.Lock()
	h, ok := s.running[id]
	s.mu.Unlock()
	if !ok {
		j, found := s.store.Job(id)
		if !found {
			return fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		if j.Inbox != "" && !j.State.Terminal() {
			return s.cancel(id)
		}
		if j.State.Terminal() {
			return fmt.Errorf("%w: %s is %s", ErrBadState, id, j.State)
		}
		// Not running yet: cancel it outright so the scheduler skips it.
		s.defCore.forget(id)
		return s.store.Finish(id, StateCancelled, -1, "interrupted", nil, j.LogBytes)
	}
	h.cancel()
	select {
	case <-h.done:
		return nil
	case <-time.After(interruptTimeout):
		return fmt.Errorf("go-jobserver: %s did not stop within %s", id, interruptTimeout)
	}
}

// InterruptAll cancels every running job.
func (s *Server) InterruptAll() int {
	s.mu.Lock()
	ids := make([]string, 0, len(s.running))
	for id := range s.running {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	sort.Strings(ids)
	for _, id := range ids {
		if err := s.Interrupt(id); err != nil {
			s.cfg.Logf("go-jobserver: interrupt %s: %v", id, err)
		}
	}
	return len(ids)
}
