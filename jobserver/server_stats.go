// Stats summarizes the job table for the stats call and the dashboard.

package jobserver

import (
	"os"
	"time"
)

// Stats summarizes the job table.
type Stats struct {
	Dir      string         `json:"dir"`
	Paused   bool           `json:"paused"`
	Uptime   string         `json:"uptime"`
	Running  int            `json:"running"`
	Total    int            `json:"total"`
	ByState  map[string]int `json:"by_state"`
	PID      int            `json:"pid"`
	Started  time.Time      `json:"started"`
	Revision string         `json:"version"`
	// CPU is what the daemon last measured, and what it did about it.
	CPU CPUReport `json:"cpu"`
	// Controls lists the process controls in effect right now.
	Controls []Control `json:"controls,omitempty"`
}

// Stats reports the server's current shape.
func (s *Server) Stats() Stats {
	jobs := s.store.Jobs()
	st := Stats{
		Dir:      s.cfg.Dir,
		Paused:   s.store.Paused(),
		Uptime:   time.Since(s.start).Round(time.Second).String(),
		Total:    len(jobs),
		ByState:  map[string]int{},
		PID:      os.Getpid(),
		Started:  s.start,
		Revision: Revision,
	}
	for _, j := range jobs {
		st.ByState[string(j.State)]++
	}
	s.mu.Lock()
	st.Running = len(s.running)
	s.mu.Unlock()
	if s.cfg.DisableCPU {
		st.CPU = CPUReport{Budget: s.sched.Budget(), CPUs: s.cfg.cpuCount(), Note: "cpu sampling is off"}
		return st
	}
	st.CPU = s.gov.report()
	st.Controls = s.gov.controls()
	return st
}
