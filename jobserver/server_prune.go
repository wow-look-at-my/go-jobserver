// The retention pass.

package jobserver

import "time"

// The retention pass runs this often, and rewrites the journal once this many jobs left it and they outnumber the
// jobs that stay.
const (
	pruneInterval = time.Minute
	compactAfter  = 1000
)

// prune forgets the finished jobs past the retention, and rewrites the journal when enough of them left.
func (s *Server) prune(now time.Time) {
	if s.cfg.Retention <= 0 {
		return
	}
	if _, err := s.store.Forget(Expired(s.store.Jobs(), now.Add(-s.cfg.Retention))); err != nil {
		s.cfg.Logf("go-jobserver: ERROR: retention: %v", err)
	}
	if n := s.store.Forgotten(); n >= compactAfter && n > len(s.store.Order()) {
		if err := s.store.Compact(); err != nil {
			s.cfg.Logf("go-jobserver: ERROR: journal rewrite: %v", err)
		}
	}
}
