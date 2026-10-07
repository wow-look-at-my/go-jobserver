//go:build unix

package jobserver

import "syscall"

// The which and value arguments the priority call takes.
const (
	// prioProcess selects a process by id for setpriority(2).
	prioProcess = 0
	// prioDarwinProcess selects a named process for Apple's background policy.
	prioDarwinProcess = 4
	// prioDarwinBG is Apple's "background" value: it lowers the process's CPU, disk and network priority.
	prioDarwinBG = 0x1000
	// prioDarwinNormal revokes the background policy.
	prioDarwinNormal = 0
)

// setNiceTo applies a nice value to one process.
func setNiceTo(pid, nice int) error {
	return syscall.Setpriority(prioProcess, pid, nice)
}

// processNice reads a process's nice value.
func processNice(pid int) (int, error) {
	return syscall.Getpriority(prioProcess, pid)
}

// darwinPolicy reads a process's background policy.
func darwinPolicy(pid int) (int, error) {
	return syscall.Getpriority(prioDarwinProcess, pid)
}

// setDarwinBackground puts a process into Apple's background state.
func setDarwinBackground(pid int) (Outcome, string) {
	if err := syscall.Setpriority(prioDarwinProcess, pid, prioDarwinBG); err != nil {
		return refused(err)
	}
	return OutcomeApplied, "background policy"
}

// clearDarwinBackground revokes Apple's background state.
func clearDarwinBackground(pid int) (Outcome, string) {
	if err := syscall.Setpriority(prioDarwinProcess, pid, prioDarwinNormal); err != nil {
		return refused(err)
	}
	return OutcomeReverted, "standard policy"
}

// freeze stops a process where it stands.
func freeze(pid int) (Outcome, string) {
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		return refused(err)
	}
	return OutcomeApplied, "SIGSTOP"
}

// thaw lets a stopped process run again.
func thaw(pid int) (Outcome, string) {
	if err := syscall.Kill(pid, syscall.SIGCONT); err != nil {
		return refused(err)
	}
	return OutcomeReverted, "SIGCONT"
}
