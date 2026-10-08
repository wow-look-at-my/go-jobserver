package jobserver

import (
	"io"
	"os"
	"os/exec"
	"runtime"
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

// hostHere is what this host answers, asked the way the daemon asks: on a
// process of the probe's own.
func hostHere(t *testing.T) hostFacts {
	t.Helper()
	host := probeHost()
	require.True(t, host.known, "the probe has to come back with an answer")
	return hostFacts{
		CPUs:         runtime.NumCPU(),
		CanFreeze:    host.canFreeze,
		FreezeDetail: host.freezeWhy,
		NiceSpelling: host.spelling,
		NiceDetail:   host.niceWhy,
	}
}

func TestTheHostProbeAsksWithAProcessOfItsOwn(t *testing.T) {
	host := probeHost()
	require.True(t, host.known, "the probe has to come back with an answer")
	switch hostOS() {
	case "linux", "darwin", "freebsd", "netbsd", "openbsd":
		assert.True(t, host.canFreeze, "this host's stop signal suspends a process: %s", host.freezeWhy)
		assert.NotEqual(t, niceUnknown, host.spelling,
			"this host's getpriority has a spelling: %s", host.niceWhy)
	default:
		// Either answer is possible outside that family. What matters is that
		// each mechanism then does what the answer says it can.
		res := applyMechanism(MechFreeze, os.Getpid(), JobPolicy{},
			hostFacts{CPUs: 1, CanFreeze: host.canFreeze, FreezeDetail: host.freezeWhy})
		if host.canFreeze {
			assert.Equal(t, OutcomeApplied, res.outcome, res.detail)
		} else {
			assert.Equal(t, OutcomeNotApplicable, res.outcome, res.detail)
			assert.Equal(t, host.freezeWhy, res.detail)
		}
	}
	assert.NotEmpty(t, host.niceWhy, "the priority answer carries its reason too")
}

// The value a revert writes depends on which spelling the host answers with,
// and both are read and written differently.
func TestNiceToWriteFollowsTheHostsSpelling(t *testing.T) {
	for _, tc := range []struct {
		spelling niceSpelling
		read     int
		want     int
		ok       bool
	}{
		{niceItself, 0, 0, true},
		{niceItself, 7, 7, true},
		{niceItself, 19, 19, true},
		{niceComplement, 20, 0, true},
		{niceComplement, 13, 7, true},
		{niceComplement, 1, 19, true},
		{niceUnknown, 7, 0, false},
	} {
		got, ok := niceToWrite(tc.spelling, tc.read)
		assert.Equal(t, tc.ok, ok, "spelling %d reading %d", tc.spelling, tc.read)
		assert.Equal(t, tc.want, got, "spelling %d reading %d", tc.spelling, tc.read)
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
