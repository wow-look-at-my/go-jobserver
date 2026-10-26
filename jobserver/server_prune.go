// The server's retention loop for ended tasks.

package jobserver

import "time"

// The retention defaults.
const (
	DefaultRetention     = time.Hour
	DefaultRetainCount   = 1000
	DefaultPruneInterval = time.Minute
)

// pruneLoop drops ended tasks on a fixed cadence until the server closes.
func (s *Server) pruneLoop() {
	defer s.wg.Done()
	s.prune()
	t := time.NewTicker(s.cfg.PruneInterval)
	defer t.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-t.C:
			s.prune()
		}
	}
}

// endOrphans cancels each task of a named inbox that this process did not open within the retention period.
// No consumer can reach such a task, so without this it stays forever. The next prunes drop it as an ended task.
func (s *Server) endOrphans(now time.Time) {
	if now.Sub(s.start) < s.cfg.Retention {
		return
	}
	ended := false
	// s.mu stays held, so an inbox cannot open while its tasks end.
	s.mu.Lock()
	for _, key := range s.store.liveInboxes() {
		if key == "" || s.inboxes[key] != nil {
			continue
		}
		for _, j := range s.store.liveIn(key) {
			s.finishIfLive(j.ID, StateCancelled, msgInboxGone)
			ended = true
		}
	}
	s.mu.Unlock()
	if ended {
		s.Wake()
	}
}

// prune drops ended tasks past the retention, and forgets what this process held for them.
func (s *Server) prune() {
	now := time.Now()
	s.endOrphans(now)
	dropped, err := s.store.Prune(now, Retention{Age: s.cfg.Retention, Count: s.cfg.RetainCount,
		CacheAge: s.cfg.CacheRetention, CacheCount: s.cfg.CacheCount})
	if err != nil {
		s.cfg.Logf("go-jobserver: ERROR: prune: %v", err)
	}
	if len(dropped) == 0 {
		return
	}
	s.mu.Lock()
	for _, id := range dropped {
		delete(s.errs, id)
	}
	s.mu.Unlock()
}
