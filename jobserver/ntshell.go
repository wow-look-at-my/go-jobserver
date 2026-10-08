package jobserver

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// An NT host has no syscall this binary can reach for a process's CPU set or its priority class. The daemon asks the host's own shell for them.

// ntShell is the program that answers those questions.
const ntShell = "powershell.exe"

// hostShellWait bounds one question, so a wedged shell cannot hold a sample.
const hostShellWait = 30 * time.Second

// runHostTool runs a program of the host's own and reports what it said, or
// why it could not be run at all.
func runHostTool(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), hostShellWait)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	said := strings.TrimSpace(string(out))
	if err != nil && said == "" {
		said = err.Error()
	}
	return said, err
}

// shellOutcome classifies a failed call to the host's shell.
func shellOutcome(err error) Outcome {
	if errors.Is(err, exec.ErrNotFound) {
		return OutcomeNotApplicable
	}
	return OutcomeRefused
}

// ntShellArgs wraps a statement in the shell's own invocation.
func ntShellArgs(statement string) []string {
	return []string{"-NoProfile", "-NonInteractive", "-Command", statement}
}

// ntAffinityRead asks for a process's CPU set as a number.
func ntAffinityRead(pid int) string {
	return fmt.Sprintf("[Int64](Get-Process -Id %d -ErrorAction Stop).ProcessorAffinity", pid)
}

// ntAffinityWrite asks for a process's CPU set to become mask.
func ntAffinityWrite(pid int, mask uint64) string {
	return fmt.Sprintf("(Get-Process -Id %d -ErrorAction Stop).ProcessorAffinity = [IntPtr]%d", pid, mask)
}

// ntPriorityRead asks for a process's priority class by name.
func ntPriorityRead(pid int) string {
	return fmt.Sprintf("(Get-Process -Id %d -ErrorAction Stop).PriorityClass", pid)
}

// ntPriorityWrite asks for a process's priority class to become class.
func ntPriorityWrite(pid int, class string) string {
	return fmt.Sprintf("(Get-Process -Id %d -ErrorAction Stop).PriorityClass = '%s'", pid, class)
}

// cpuMask packs a CPU list into the mask a Windows host takes.
func cpuMask(cpus []int) uint64 {
	var mask uint64
	for _, c := range cpus {
		if c >= 0 && c < 64 {
			mask |= 1 << uint(c)
		}
	}
	return mask
}

// ntClassName maps a nice value onto the name of a priority class, the same
// tiers cosmo libc's NT setpriority uses.
func ntClassName(nice int) string {
	switch {
	case nice <= -15:
		return "RealTime"
	case nice <= -9:
		return "High"
	case nice <= -3:
		return "AboveNormal"
	case nice <= 3:
		return "Normal"
	case nice <= 12:
		return "BelowNormal"
	}
	return "Idle"
}

// ntNiceForClassName reads a priority class name back as the nice value it
// stands for.
func ntNiceForClassName(name string) (int, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "realtime":
		return -16, true
	case "high":
		return -10, true
	case "abovenormal":
		return -5, true
	case "normal":
		return 0, true
	case "belownormal":
		return 5, true
	case "idle":
		return 15, true
	}
	return 0, false
}

// windowsAffinity pins a process to a CPU set through the host's shell,
// remembering the set it was on.
func windowsAffinity(pid int, cpus []int) controlResult {
	var prior []int
	if said, err := runHostTool(ntShell, ntShellArgs(ntAffinityRead(pid))...); err == nil {
		if was, convErr := strconv.Atoi(said); convErr == nil && was > 0 {
			prior = []int{was}
		}
	}
	said, err := runHostTool(ntShell, ntShellArgs(ntAffinityWrite(pid, cpuMask(cpus)))...)
	if err != nil {
		return appliedResult(shellOutcome(err), said)
	}
	return controlResult{
		outcome:  OutcomeApplied,
		detail:   "cpus " + joinInts(cpus) + " through " + ntShell,
		prior:    prior,
		hadPrior: prior != nil,
	}
}

// windowsPriority sets a process's priority class through the host's shell,
// remembering the class it was in.
func windowsPriority(pid, nice int) controlResult {
	var prior []int
	if said, err := runHostTool(ntShell, ntShellArgs(ntPriorityRead(pid))...); err == nil {
		if was, found := ntNiceForClassName(said); found {
			prior = []int{was}
		}
	}
	said, err := runHostTool(ntShell, ntShellArgs(ntPriorityWrite(pid, ntClassName(nice)))...)
	if err != nil {
		return appliedResult(shellOutcome(err), said)
	}
	return controlResult{
		outcome:  OutcomeApplied,
		detail:   ntClassName(nice) + " through " + ntShell,
		prior:    prior,
		hadPrior: prior != nil,
	}
}

// windowsAffinityRevert puts back the CPU set the process was on.
func windowsAffinityRevert(c Control) controlResult {
	if !c.HadPrior || len(c.Prior) == 0 {
		return appliedResult(OutcomeRefused, "this host would not say which CPUs the process was on")
	}
	said, err := runHostTool(ntShell, ntShellArgs(ntAffinityWrite(c.PID, uint64(c.Prior[0])))...)
	if err != nil {
		return controlResult{outcome: shellOutcome(err), detail: said}
	}
	return appliedResult(OutcomeReverted, strconv.Itoa(c.Prior[0])+" back through "+ntShell)
}

// windowsPriorityRevert puts back the priority class the process was in.
func windowsPriorityRevert(c Control) controlResult {
	if !c.HadPrior || len(c.Prior) == 0 {
		return appliedResult(OutcomeRefused, "this host would not say which priority the process had")
	}
	said, err := runHostTool(ntShell, ntShellArgs(ntPriorityWrite(c.PID, ntClassName(c.Prior[0])))...)
	if err != nil {
		return controlResult{outcome: shellOutcome(err), detail: said}
	}
	return appliedResult(OutcomeReverted, ntClassName(c.Prior[0])+" back through "+ntShell)
}
