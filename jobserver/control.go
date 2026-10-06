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
// cores. Anywhere else the call is reported as not applicable rather than
// attempted and refused.
func applyAffinity(pid int, cpus []int) (Outcome, string) {
	switch hostOS() {
	case "linux":
		if err := setAffinitySyscall(pid, cpus); err != nil {
			return refused(err)
		}
		return OutcomeApplied, "cpus " + joinInts(cpus)
	case "darwin":
		out, detail := setDarwinBackground(pid)
		if out == OutcomeApplied {
			return out, detail + " (cpus " + joinInts(cpus) + ")"
		}
		return out, detail
	default:
		return OutcomeNotApplicable, "this binary's " + hostOS() + " syscall layer has no CPU-affinity call"
	}
}

// revertAffinity restores a process's full CPU set.
func revertAffinity(pid, cpus int) (Outcome, string) {
	switch hostOS() {
	case "linux":
		if err := clearAffinitySyscall(pid, cpus); err != nil {
			return refused(err)
		}
		return OutcomeReverted, "every CPU"
	case "darwin":
		return clearDarwinBackground(pid)
	default:
		return OutcomeNotApplicable, "this binary's " + hostOS() + " syscall layer has no CPU-affinity call"
	}
}

// applyNice raises a process's nice value.
func applyNice(pid, nice int) (Outcome, string) {
	if hostOS() == "windows" {
		return OutcomeNotApplicable, "this binary's Windows syscall layer has no setpriority call"
	}
	return setNice(pid, nice)
}

// revertNice restores a process's nice value.
func revertNice(pid int) (Outcome, string) {
	if hostOS() == "windows" {
		return OutcomeNotApplicable, "this binary's Windows syscall layer has no setpriority call"
	}
	return clearNice(pid)
}

// applyFreeze suspends a process.
func applyFreeze(pid int) (Outcome, string) {
	if hostOS() == "windows" {
		return OutcomeNotApplicable, "this binary's Windows syscall layer has no process-suspend call"
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
