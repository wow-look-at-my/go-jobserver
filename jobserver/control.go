package jobserver

import (
	"errors"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Outcome is what one attempt to control one process did.
type Outcome string

const (
	// OutcomeApplied means the mechanism took effect.
	OutcomeApplied Outcome = "applied"
	// OutcomeReverted means a mechanism was undone, or was already gone.
	OutcomeReverted Outcome = "reverted"
	// OutcomeNotApplicable means this host has no such control at all.
	OutcomeNotApplicable Outcome = "not-applicable"
	// OutcomeRefused means the host has the control and declined this request.
	OutcomeRefused Outcome = "refused"
)

// A Control records one mechanism applied, or attempted, on one process of a
// job.
type Control struct {
	Job       string    `json:"job"`
	Mechanism Mechanism `json:"mechanism"`
	PID       int       `json:"pid"`
	Name      string    `json:"name,omitempty"`
	Outcome   Outcome   `json:"outcome"`
	Detail    string    `json:"detail,omitempty"`
	// Active reports whether the mechanism is in effect right now.
	Active bool      `json:"active"`
	At     time.Time `json:"at"`
}

// applyMechanism applies one mechanism to one process of a job.
func applyMechanism(m Mechanism, pid int, policy JobPolicy, cpus int) (Outcome, string) {
	switch m {
	case MechAffinity:
		return applyAffinity(pid, affinityCPUs(policy.Affinity, cpus))
	case MechPriority:
		return applyNice(pid, policy.Priority.Nice)
	case MechFreeze:
		return applyFreeze(pid)
	}
	return OutcomeNotApplicable, "unknown mechanism " + string(m)
}

// revertMechanism undoes one mechanism on one process.
func revertMechanism(m Mechanism, pid, cpus int) (Outcome, string) {
	switch m {
	case MechAffinity:
		return revertAffinity(pid, cpus)
	case MechPriority:
		return revertNice(pid)
	case MechFreeze:
		return thaw(pid)
	}
	return OutcomeNotApplicable, "unknown mechanism " + string(m)
}

// applyAffinity pins a process to a CPU set. Linux has a real per-process CPU
// mask. Darwin's kernel keeps no such mask, so its CPU placement control is
// the background policy, which moves a process's work onto the efficiency
// cores.
func applyAffinity(pid int, cpus []int) (Outcome, string) {
	if hostOS() == "darwin" {
		out, detail := setDarwinBackground(pid)
		if out == OutcomeApplied {
			return out, detail + " (cpus " + joinInts(cpus) + ")"
		}
		return out, detail
	}
	return controlCall(func() error { return setAffinitySyscall(pid, cpus) },
		OutcomeApplied, "cpus "+joinInts(cpus))
}

// revertAffinity restores a process's full CPU set.
func revertAffinity(pid, cpus int) (Outcome, string) {
	if hostOS() == "darwin" {
		return clearDarwinBackground(pid)
	}
	return controlCall(func() error { return clearAffinitySyscall(pid, cpus) },
		OutcomeReverted, "every CPU")
}

// controlCall runs one process-control call.
func controlCall(call func() error, applied Outcome, detail string) (Outcome, string) {
	if err := call(); err != nil {
		return callFailure(err)
	}
	return applied, detail
}

// applyNice raises a process's nice value.
func applyNice(pid, nice int) (Outcome, string) {
	out, detail := setNice(pid, nice)
	return outcomeFor(hostOS(), out, detail)
}

// revertNice restores a process's nice value.
func revertNice(pid int) (Outcome, string) {
	out, detail := clearNice(pid)
	return outcomeFor(hostOS(), out, detail)
}

// callFailure classifies a failed process-control call.
func callFailure(err error) (Outcome, string) {
	out, detail := refused(err)
	return outcomeFor(hostOS(), out, detail)
}

// outcomeFor reports what a call did.
func outcomeFor(host string, out Outcome, detail string) (Outcome, string) {
	if out == OutcomeRefused && !schedulable(host) {
		return OutcomeNotApplicable, detail
	}
	return out, detail
}

// schedulable reports whether a host's syscall layer is the one these calls
// are spelled for.
func schedulable(host string) bool {
	switch host {
	case "linux", "darwin", "freebsd", "netbsd", "openbsd":
		return true
	}
	return false
}

// applyFreeze suspends a process.
func applyFreeze(pid int) (Outcome, string) {
	if hostOS() == "windows" {
		return OutcomeNotApplicable, "the Windows syscall layer turns this signal into termination"
	}
	return freeze(pid)
}

// refused classifies a failed control call. A process that has already exited
// is not a refusal: there is nothing left to control.
func refused(err error) (Outcome, string) {
	if errors.Is(err, syscall.ESRCH) {
		return OutcomeReverted, "the process is gone"
	}
	return OutcomeRefused, err.Error()
}

// joinInts renders a CPU list.
func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, strconv.Itoa(v))
	}
	return strings.Join(parts, ",")
}
