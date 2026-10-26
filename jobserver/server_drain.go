// Drain: stop starting jobs, and let the running ones finish.

package jobserver

// Drain stops the server from starting jobs. The running jobs go on, and a
// job that waits stays in the journal for the next start.
func (s *Server) Drain() {
	s.draining.Store(true)
}

// Draining reports whether Drain was called.
func (s *Server) Draining() bool { return s.draining.Load() }
