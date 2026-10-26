// How a job ends: an interrupt, and the host's hook that hears every end.

package jobserver

import (
	"fmt"
	"time"
)

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
		if j.State.Terminal() {
			return fmt.Errorf("%w: %s is %s", ErrBadState, id, j.State)
		}
		// Not running yet: cancel it outright so the scheduler skips it.
		if err := s.store.Finish(id, StateCancelled, -1, "interrupted", nil, j.LogBytes); err != nil {
			return err
		}
		s.ended(id)
		return nil
	}
	h.cancel()
	select {
	case <-h.done:
		return nil
	case <-time.After(interruptTimeout):
		return fmt.Errorf("go-jobserver: %s did not stop within %s", id, interruptTimeout)
	}
}

// ended tells the host that a job reached its end, by whatever path: it ran,
// it was blocked.
func (s *Server) ended(id string) {
	if s.cfg.OnEnd == nil {
		return
	}
	if j, ok := s.store.Job(id); ok && j.State.Terminal() {
		s.cfg.OnEnd(j)
	}
}
