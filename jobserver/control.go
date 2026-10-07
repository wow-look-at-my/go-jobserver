package jobserver

import (
	"errors"
	"io"
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
	Active bool `json:"active"`
	// Prior is the state this control found and replaced, in the mechanism's own spelling: the nice value for priority.
	Prior []int `json:"prior,omitempty"`
	// HadPrior reports whether Prior is that state.
	HadPrior bool      `json:"had_prior,omitempty"`
	At       time.Time `json:"at"`
}

// A controlResult is what one mechanism did to one process, with the state it
// replaced, so the same mechanism can put that back.
type controlResult struct {
	outcome  Outcome
	detail   string
	prior    []int
	hadPrior bool
}

// applied is a result that changed nothing it has to put back.
func appliedResult(out Outcome, detail string) controlResult {
	return controlResult{outcome: out, detail: detail}
}

// A hostFacts is what the daemon has learned about process control here.
type hostFacts struct {
	CPUs int
	// CanFreeze is whether the stop signal suspends a process here.
	CanFreeze bool
	// FreezeDetail says how that was found out.
	FreezeDetail string
	// NiceSpelling is how this host's getpriority answers, which decides what a revert writes.
	NiceSpelling niceSpelling
	// NiceDetail says how that was found out.
	NiceDetail string
}

// applyMechanism applies one mechanism to one process of a job.
func applyMechanism(m Mechanism, pid int, policy JobPolicy, host hostFacts) controlResult {
	switch m {
	case MechAffinity:
		return applyAffinity(pid, affinityCPUs(policy.Affinity, host.CPUs))
	case MechPriority:
		return applyNice(pid, policy.Priority.Nice, host)
	case MechFreeze:
		return applyFreeze(pid, host)
	}
	return appliedResult(OutcomeNotApplicable, "unknown mechanism "+string(m))
}

// revertMechanism undoes one mechanism on one process, putting back the state
// the control found where it can.
func revertMechanism(c Control, cpus int) controlResult {
	switch c.Mechanism {
	case MechAffinity:
		return revertAffinity(c, cpus)
	case MechPriority:
		return revertNice(c)
	case MechFreeze:
		out, detail := thaw(c.PID)
		return appliedResult(out, detail)
	}
	return appliedResult(OutcomeNotApplicable, "unknown mechanism "+string(c.Mechanism))
}

// applyAffinity pins a process to a CPU set. Linux has a real per-process CPU
// mask. Darwin's kernel keeps no such mask, so its CPU placement control is
// the background policy, which moves a process's work onto the efficiency
// cores.
func applyAffinity(pid int, cpus []int) controlResult {
	if hostOS() == "windows" {
		// The syscall layer serves this over SetProcessAffinityMask once the toolchain's runtime carries the call.
		prior, had := affinityPrior(pid)
		if err := setAffinitySyscall(pid, cpus); err == nil {
			return controlResult{outcome: OutcomeApplied, detail: "cpus " + joinInts(cpus),
				prior: prior, hadPrior: had}
		} else if !missingCall(err) {
			return controlResult{outcome: OutcomeRefused, detail: err.Error()}
		}
		return windowsAffinity(pid, cpus)
	}
	if hostOS() == "darwin" {
		// Darwin's getpriority does not report the policy. The read is held against the change itself: one that moves with it is the state to put back.
		was, _ := darwinPolicy(pid)
		out, detail := setDarwinBackground(pid)
		if out == OutcomeApplied {
			detail += " (cpus " + joinInts(cpus) + ")"
		}
		res := controlResult{outcome: out, detail: detail}
		if out == OutcomeApplied {
			if now, err := darwinPolicy(pid); err == nil && now != was {
				res.prior, res.hadPrior = []int{was}, true
			}
		}
		return res
	}
	prior, had := affinityPrior(pid)
	return controlCall(func() error { return setAffinitySyscall(pid, cpus) },
		OutcomeApplied, "cpus "+joinInts(cpus), prior, had)
}

// revertAffinity puts back the CPU set the process was on, or every CPU where
// none was read.
func revertAffinity(c Control, cpus int) controlResult {
	if hostOS() == "windows" {
		if c.HadPrior && len(c.Prior) > 0 {
			if err := setAffinitySyscall(c.PID, c.Prior); err == nil {
				return appliedResult(OutcomeReverted, "cpus "+joinInts(c.Prior))
			} else if !missingCall(err) {
				return appliedResult(OutcomeRefused, err.Error())
			}
		}
		return windowsAffinityRevert(c)
	}
	if hostOS() == "darwin" {
		// The state to put back is the policy it was under: one that was
		// already in the background keeps it.
		if c.HadPrior && len(c.Prior) > 0 && c.Prior[0] != 0 {
			// It was already in the background, so putting that back leaves it there.
			out, detail := setDarwinBackground(c.PID)
			return appliedResult(relabel(out), detail)
		}
		out, detail := clearDarwinBackground(c.PID)
		return appliedResult(out, detail)
	}
	if c.HadPrior && len(c.Prior) > 0 {
		return controlCall(func() error { return setAffinitySyscall(c.PID, c.Prior) },
			OutcomeReverted, "cpus "+joinInts(c.Prior), nil, false)
	}
	return controlCall(func() error { return clearAffinitySyscall(c.PID, cpus) },
		OutcomeReverted, "every CPU", nil, false)
}

// controlCall runs one process-control call, and remembers either what the
// call replaced or what its caller hands it.
func controlCall(call func() error, applied Outcome, detail string, prior []int, hadPrior bool) controlResult {
	if err := call(); err != nil {
		out, why := callFailure(err)
		return controlResult{outcome: out, detail: why}
	}
	return controlResult{outcome: applied, detail: detail, prior: prior, hadPrior: hadPrior}
}

// applyNice raises a process's nice value, remembering the value it had in the
// spelling the revert has to write.
func applyNice(pid, nice int, host hostFacts) controlResult {
	if hostOS() == "windows" {
		// The syscall layer serves this over SetPriorityClass once the toolchain's runtime carries the call.
		var prior []int
		if was, err := processNice(pid); err == nil {
			if value, ok := niceToWrite(host.NiceSpelling, was); ok {
				prior = []int{value}
			}
		}
		if err := setNiceTo(pid, nice); err == nil {
			return controlResult{outcome: OutcomeApplied, detail: "nice " + strconv.Itoa(nice),
				prior: prior, hadPrior: prior != nil}
		} else if !missingCall(err) {
			return controlResult{outcome: OutcomeRefused, detail: err.Error()}
		}
		return windowsPriority(pid, nice)
	}
	var prior []int
	if was, err := processNice(pid); err == nil {
		if value, ok := niceToWrite(host.NiceSpelling, was); ok {
			prior = []int{value}
		}
	}
	res := controlCall(func() error { return setNiceTo(pid, nice) },
		OutcomeApplied, "nice "+strconv.Itoa(nice), nil, false)
	res.prior, res.hadPrior = prior, prior != nil
	return res
}

// niceSpelling is how this host's getpriority answers: with the nice value itself, or with its complement.
type niceSpelling int

const (
	niceUnknown niceSpelling = iota
	niceItself
	niceComplement
)

// prioMax is the top of the range a complemented answer counts down from.
const prioMax = 20

// clueNice is a value whose spellings differ.
const clueNice = 7

// niceToWrite turns a value this host's getpriority answered with into the
// value this host's setpriority takes.
func niceToWrite(spelling niceSpelling, read int) (int, bool) {
	switch spelling {
	case niceItself:
		return read, true
	case niceComplement:
		return prioMax - read, true
	}
	return 0, false
}

// probeNiceSpelling learns which spelling a host answers with, on a process of
// the daemon's own.
func probeNiceSpelling(pid int) (niceSpelling, string) {
	if err := setNiceTo(pid, clueNice); err != nil {
		return niceUnknown, "no priority call here: " + err.Error()
	}
	got, err := processNice(pid)
	if err != nil {
		return niceUnknown, "no priority read here: " + err.Error()
	}
	switch got {
	case clueNice:
		return niceItself, "getpriority answers with the nice value"
	case prioMax - clueNice:
		return niceComplement, "getpriority answers with the value counted down from " + strconv.Itoa(prioMax)
	}
	return niceUnknown, "getpriority answered " + strconv.Itoa(got) + " for " + strconv.Itoa(clueNice)
}

// affinityPrior reads the CPU set a process is on, on a host whose syscall
// layer serves that read.
func affinityPrior(pid int) ([]int, bool) {
	cpus, err := getAffinitySyscall(pid)
	if err != nil || len(cpus) == 0 {
		return nil, false
	}
	return cpus, true
}

// relabel reports what a revert did: a call that worked put the state it found
// back, which is what a revert means.
func relabel(out Outcome) Outcome {
	if out == OutcomeApplied {
		return OutcomeReverted
	}
	return out
}

// revertNice puts back the nice value the process had, where it was read. A
// host that would not say what that value was goes back to the default, and
// the reason travels with the control.
func revertNice(c Control) controlResult {
	if hostOS() == "windows" {
		if c.HadPrior && len(c.Prior) > 0 {
			if err := setNiceTo(c.PID, c.Prior[0]); err == nil {
				return appliedResult(OutcomeReverted, "nice "+strconv.Itoa(c.Prior[0])+" back")
			} else if !missingCall(err) {
				return appliedResult(OutcomeRefused, err.Error())
			}
		}
		return windowsPriorityRevert(c)
	}
	if c.HadPrior && len(c.Prior) > 0 {
		return controlCall(func() error { return setNiceTo(c.PID, c.Prior[0]) },
			OutcomeReverted, "nice "+strconv.Itoa(c.Prior[0])+" back", nil, false)
	}
	return controlCall(func() error { return setNiceTo(c.PID, 0) },
		OutcomeReverted, "nice 0, with no value to go back to", nil, false)
}

// missingCall reports whether a host's syscall layer carries no such call,
// which is where a route built on that call hands over to another.
func missingCall(err error) bool {
	return errors.Is(err, syscall.ENOSYS)
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
func applyFreeze(pid int, host hostFacts) controlResult {
	if !host.CanFreeze {
		return appliedResult(OutcomeNotApplicable, host.FreezeDetail)
	}
	out, detail := freeze(pid)
	return appliedResult(out, detail)
}

// How long a probe child is given to come up, to answer the stop, and to be
// released again.
const (
	suspendProbeUp      = 5 * time.Second
	suspendProbeSettle  = 100 * time.Millisecond
	suspendProbeRelease = 5 * time.Second
)

// A hostProbe is what one host answered about process control, asked on a
// process of the daemon's own.
type hostProbe struct {
	known     bool
	canFreeze bool
	freezeWhy string
	spelling  niceSpelling
	niceWhy   string
}

// probeHost asks this host what its mechanisms cannot read off the platform.
// What its stop signal does to a process, and which spelling its getpriority
// answers with. It asks with a process of its own. That process is running
// this binary in the mode that waits for input. A host that ends a process
// when asked to stop it ends nothing.
//
// The stop answer cannot come from the signal alone: a host whose stop signal
// ends a process reports success for it. The same as one that suspends it.
// What separates them is what happens after the resume. This is because a
// process that was only stopped is still there to run and one that was ended
// is not.
func probeHost() hostProbe {
	var out hostProbe
	out.known = true
	exe, err := os.Executable()
	if err != nil {
		out.freezeWhy = "this program cannot be found: " + err.Error()
		out.niceWhy = out.freezeWhy
		return out
	}
	cmd := exec.Command(exe, "-hold")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		out.freezeWhy = "no pipe for the probe process: " + err.Error()
		out.niceWhy = out.freezeWhy
		return out
	}
	if err := cmd.Start(); err != nil {
		out.freezeWhy = "the probe process would not start: " + err.Error()
		out.niceWhy = out.freezeWhy
		return out
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
		out.freezeWhy = "the probe process did not stay up"
		out.niceWhy = out.freezeWhy
		return out
	}
	// The priority question goes first, because the stop half ends the child.
	out.spelling, out.niceWhy = probeNiceSpelling(cmd.Process.Pid)
	out.canFreeze, out.freezeWhy = stopTest(cmd.Process.Pid, stdin, done)
	stop()
	return out
}

// stopTest asks what a host's stop signal does to a process, and releases it
// to find out. Releasing is what tells the hosts apart. The child reads its
// input from that pipe, so a process that was suspended leaves as soon as the
// pipe closes.
func stopTest(pid int, stdin io.Closer, done <-chan error) (bool, string) {
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		return false, "the stop signal was refused: " + err.Error()
	}
	time.Sleep(suspendProbeSettle)
	stopped := syscall.Kill(pid, 0) == nil
	_ = syscall.Kill(pid, syscall.SIGCONT)
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
