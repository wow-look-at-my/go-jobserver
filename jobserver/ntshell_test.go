package jobserver

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The NT route asks the host's own shell, so what it asks for is what decides
// whether a process is pinned to the right CPUs. It put in the right class.
func TestTheNtStatementsNameTheProcessAndTheState(t *testing.T) {
	assert.Contains(t, ntAffinityWrite(4321, cpuMask([]int{0, 2})), "-Id 4321")
	assert.Contains(t, ntAffinityWrite(4321, cpuMask([]int{0, 2})), "[IntPtr]5")
	assert.Contains(t, ntAffinityRead(4321), "-Id 4321")
	assert.Contains(t, ntPriorityWrite(4321, ntClassName(5)), "= 'BelowNormal'")
	assert.Contains(t, ntPriorityRead(4321), "PriorityClass")
	assert.Contains(t, ntShellArgs("x"), "x")
}

func TestCpuMaskPacksTheListedCpus(t *testing.T) {
	assert.Equal(t, uint64(0), cpuMask(nil))
	assert.Equal(t, uint64(1), cpuMask([]int{0}))
	assert.Equal(t, uint64(5), cpuMask([]int{0, 2}))
	assert.Equal(t, uint64(1)<<63, cpuMask([]int{63}))
	// A CPU a Windows mask cannot hold is left out rather than shifting past the word.
	assert.Equal(t, uint64(0), cpuMask([]int{64, -1}))
}

func TestNtPriorityClassNameRoundTrips(t *testing.T) {
	for _, nice := range []int{-20, -16, -15, -10, -9, -5, -3, 0, 3, 5, 12, 15, 20} {
		name := ntClassName(nice)
		back, found := ntNiceForClassName(name)
		require.True(t, found, "class %q for nice %d", name, nice)
		assert.Equal(t, name, ntClassName(back), "nice %d through %q", nice, name)
	}
	_, found := ntNiceForClassName("UnheardOf")
	assert.False(t, found, "a class this host does not name is not one")
}

// The shell plumbing is the part of the NT route this host can drive: what a
// program says travels with the outcome. A program that is not there is a
// host without the facility rather than a refusal.
func TestRunHostToolReportsWhatTheProgramSaid(t *testing.T) {
	said, err := runHostTool("sh", "-c", "echo heard; exit 3")
	require.Error(t, err)
	assert.Equal(t, "heard", said, "a program's own words travel with the failure")

	said, err = runHostTool("sh", "-c", "echo fine")
	require.NoError(t, err)
	assert.Equal(t, "fine", said)

	_, err = runHostTool("sh", "-c", "exit 3")
	require.Error(t, err)
	assert.Equal(t, OutcomeRefused, shellOutcome(err), "a program that ran and refused is a refusal")

	_, err = runHostTool("a-program-this-host-does-not-have")
	require.Error(t, err)
	assert.True(t, errors.Is(err, exec.ErrNotFound), "a program that is not there: %v", err)
	assert.Equal(t, OutcomeNotApplicable, shellOutcome(err),
		"a host without the program has no such facility")
}

// On a host without the shell the mechanism reaches, the daemon says so rather
// than claiming to have set anything. This host has no powershell, so it
// exercises the NT route's first step for real.
func TestTheNtRouteSaysSoWhereTheShellIsMissing(t *testing.T) {
	if hostOS() == "windows" {
		t.Skip("this host is the one the shell is for")
	}
	res := windowsAffinity(os.Getpid(), []int{0})
	assert.Equal(t, OutcomeNotApplicable, res.outcome, res.detail)
	assert.NotEmpty(t, res.detail)
}
