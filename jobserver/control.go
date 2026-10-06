package jobserver

import (
	"errors"
	"os"
	"os/exec"
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

// A hostFacts is what the daemon has learned about process control here.
type hostFacts struct {
	CPUs int
	// CanFreeze is whether the stop signal suspends a process here.
	CanFreeze bool
	// FreezeDetail says how that was found out.
	FreezeDetail string
}

// applyMechanism applies one mechanism to one process of a job.
func applyMechanism(m Mechanism, pid int, policy JobPolicy, host hostFacts) (Outcome, string) {
	switch m {
	case MechAffinity:
		return applyAffinity(pid, affinityCPUs(policy.Affinity, host.CPUs))
	case MechPriority:
		return applyNice(pid, policy.Priority.Nice)
	case MechFreeze:
		return applyFreeze(pid, host)
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

// applyFreeze suspends a process, on a host whose stop signal is known to
// suspend one.
func applyFreeze(pid int, host hostFacts) (Outcome, string) {
	if !host.CanFreeze {
		return OutcomeNotApplicable, host.FreezeDetail
	}
	return freeze(pid)
}

// How long a probe child is given to come up, to answer the stop, and to be
// released again.
const (
	suspendProbeUp      = 5 * time.Second
	suspendProbeSettle  = 100 * time.Millisecond
	suspendProbeRelease = 5 * time.Second
)

// probeSuspend asks this host what its stop signal does to a process. It asks
// with a process of its own. That process is running this binary in the mode
// that waits for input. A host that ends a process when asked to stop it ends
// nothing.
//
// The answer cannot come from the signal alone: a host whose stop signal ends
// a process reports success for it. The same as one that suspends it. What
// separates them is what happens after the resume. This is because a process
// that was only stopped is still there to run and one that was ended is not.
func probeSuspend() (bool, string) {
	exe, err := os.Executable()
	if err != nil {
		return false, "this program cannot be found: " + err.Error()
	}
	cmd := exec.Command(exe, "-hold")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return false, "no pipe for the probe process: " + err.Error()
	}
	if err := cmd.Start(); err != nil {
		return false, "the probe process would not start: " + err.Error()
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stop := func() {
		_ = cmd.Process.Kill()
		_ = stdin.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}

	// Asking a process that had already ended would answer a different
	// question, so the child has to be waiting first.
	if !aliveFor(cmd.Process.Pid, suspendProbeUp) {
		stop()
		return false, "the probe process did not stay up"
	}
	if err := syscall.Kill(cmd.Process.Pid, syscall.SIGSTOP); err != nil {
		stop()
		return false, "the stop signal was refused: " + err.Error()
	}
	time.Sleep(suspendProbeSettle)
	stopped := syscall.Kill(cmd.Process.Pid, 0) == nil
	_ = syscall.Kill(cmd.Process.Pid, syscall.SIGCONT)

	// Releasing the child is what tells the hosts apart.
	_ = stdin.Close()
	select {
	case err := <-done:
		if endedBySignal(err) {
			return false, "the stop signal ended the process"
		}
		if err != nil || !stopped {
			return false, "the probe process left on its own: " + describeExit(err)
		}
		return true, "the stop signal suspended the process"
	case <-time.After(suspendProbeRelease):
	}
	stop()
	return false, "the probe process never came back from the stop"
}

// aliveFor waits for a process to exist, and reports whether it did.
func aliveFor(pid int, wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for {
		if syscall.Kill(pid, 0) == nil {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// endedBySignal reports whether a process died by a signal rather than leaving
// on its own.
func endedBySignal(err error) bool {
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return false
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	return ok && status.Signaled()
}

// describeExit names how a process ended.
func describeExit(err error) string {
	if err == nil {
		return "it left with no error"
	}
	return err.Error()
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
