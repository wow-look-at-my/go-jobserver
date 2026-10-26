// hostLoadErrorEvery is how often the error about an unreadable host load repeats.

package jobserver

import (
	"fmt"
	"time"
)

// hostLoadErrorEvery is how often the error about an unreadable host load repeats.
const hostLoadErrorEvery = 5 * time.Minute

// watchHostLoadLocked tracks whether the governor can read the host's own CPU
// counters. It returns the error line to log now, and the line that says the
// counters can be read again.
func (g *governor) watchHostLoadLocked(r Reading, now time.Time) (errLine, okLine string) {
	if r.HostErr == "" {
		if g.unreadableSince.IsZero() {
			return "", ""
		}
		g.hostErr, g.unreadableSince, g.errorLoggedAt = "", time.Time{}, time.Time{}
		return "", "go-jobserver: the host's CPU counters can be read again, so the host's load is known"
	}
	g.hostErr = r.HostErr
	if g.unreadableSince.IsZero() {
		g.unreadableSince = now
	}
	if !g.errorLoggedAt.IsZero() && now.Sub(g.errorLoggedAt) < hostLoadErrorEvery {
		return "", ""
	}
	g.errorLoggedAt = now
	return hostLoadError(r.HostErr, now.Sub(g.unreadableSince)), ""
}

// hostLoadError is the error for a host whose load go-jobserver cannot read.
func hostLoadError(why string, unknown time.Duration) string {
	line := fmt.Sprintf("go-jobserver: CONFIGURATION ERROR: the host's CPU load is unknown (%s). "+
		"go-jobserver cannot see work outside its own jobs, and it cannot see how many tasks want a CPU. "+
		"Under load it keeps starting work, and it can destabilize the system. "+
		"It needs the host's own CPU counters.", why)
	if unknown > 0 {
		line += fmt.Sprintf(" The load has been unknown for %s.", unknown.Round(time.Second))
	}
	return line + fmt.Sprintf(" This error repeats every %s until the counters can be read.", hostLoadErrorEvery)
}
