package jobserver

import (
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain lets this test binary stand in for the daemon as the process the
// suspend probe runs, which starts os.Executable() with -hold.
func TestMain(m *testing.M) {
	for _, arg := range os.Args[1:] {
		if arg == "-hold" {
			_, _ = io.Copy(io.Discard, os.Stdin)
			os.Exit(0)
		}
	}
	os.Exit(m.Run())
}

// freezable is the host this suite runs its mechanism tests against: the CPUs
// it was told about, with freeze known to work.
func freezable(cpus int) hostFacts {
	return hostFacts{CPUs: cpus, CanFreeze: true, FreezeDetail: "the stop signal suspended the process"}
}

func TestSuspendProbeAsksWithAProcessOfItsOwn(t *testing.T) {
	supported, detail := probeSuspend()
	switch hostOS() {
	case "linux", "darwin", "freebsd", "netbsd", "openbsd":
		assert.True(t, supported, "this host's stop signal suspends a process: %s", detail)
	default:
		// Either answer is possible outside that family. What matters is that
		// the mechanism then does what the answer says it can.
		out, why := applyMechanism(MechFreeze, os.Getpid(), JobPolicy{},
			hostFacts{CPUs: 1, CanFreeze: supported, FreezeDetail: detail})
		if supported {
			assert.Equal(t, OutcomeApplied, out, why)
			return
		}
		assert.Equal(t, OutcomeNotApplicable, out, why)
		assert.Equal(t, detail, why)
	}
}

func TestASignalDeathIsNotAnExit(t *testing.T) {
	killed := exec.Command("sleep", "30")
	require.NoError(t, killed.Start())
	require.NoError(t, killed.Process.Kill())
	waitErr := killed.Wait()
	assert.True(t, endedBySignal(waitErr), "a killed process ends by signal (err=%v)", waitErr)

	left := exec.Command("sh", "-c", "exit 3")
	assert.False(t, endedBySignal(left.Run()), "a process that leaves on its own is not a signal death")

	left = exec.Command("sh", "-c", "exit 0")
	assert.False(t, endedBySignal(left.Run()), "a process that succeeds is not a signal death")
}
