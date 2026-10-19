package jobserver

import (
	"fmt"
	"time"
)

// watchBlindLocked tracks whether the governor can read the host's own CPU
// counters. It returns the error line to log now, and the line that says the
// counters can be read again. A negative HostBusyHigh turns the host check
// off, so the host's load is not needed then.
func (g *governor) watchBlindLocked(r Reading, now time.Time) (errLine, infoLine string) {
	if g.sched.cfg.HostBusyHigh < 0 {
		return "", ""
	}
	if r.HostErr == "" {
		if g.blindSince.IsZero() {
			return "", ""
		}
		g.hostErr, g.blindSince, g.blindLoggedAt = "", time.Time{}, time.Time{}
		return "", "go-jobserver: the host's CPU counters can be read again, so the host's load is known"
	}
	g.hostErr = r.HostErr
	if g.blindSince.IsZero() {
		g.blindSince = now
	}
	every := g.sched.cfg.HostBlindEvery
	if !g.blindLoggedAt.IsZero() && now.Sub(g.blindLoggedAt) < every {
		return "", ""
	}
	g.blindLoggedAt = now
	return blindLine(r.HostErr, now.Sub(g.blindSince), every), ""
}

// blindLine is the error for a host whose load go-jobserver cannot read.
func blindLine(why string, blind, every time.Duration) string {
	line := fmt.Sprintf("go-jobserver: CONFIGURATION ERROR: the host's CPU load is unknown (%s). "+
		"go-jobserver cannot see work outside its own jobs, and cannot tell when the host is saturated. "+
		"Under load it keeps starting work, and it can destabilize the system. "+
		"It needs the host's own CPU counters.", why)
	if blind > 0 {
		line += fmt.Sprintf(" The load has been unknown for %s.", blind.Round(time.Second))
	}
	return line + fmt.Sprintf(" This error repeats every %s until the counters can be read.", every)
}
