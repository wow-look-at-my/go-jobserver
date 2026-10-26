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

// prune drops ended tasks past the retention, and forgets what this process held for them.
func (s *Server) prune() {
	dropped, err := s.store.Prune(time.Now(), Retention{Age: s.cfg.Retention, Count: s.cfg.RetainCount,
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
