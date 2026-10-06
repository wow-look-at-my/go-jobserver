package jobserver

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spawnSleep starts a long-lived child the test can control.
func spawnSleep(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("sleep", "30")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	return cmd.Process.Pid
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return info.Size()
}

func TestFreezeStopsAProcessAndThawResumesIt(t *testing.T) {
	if hostOS() == "windows" {
		out, detail := applyMechanism(MechFreeze, os.Getpid(), JobPolicy{}, 1)
		assert.Equal(t, OutcomeNotApplicable, out, detail)
		return
	}
	marker := filepath.Join(t.TempDir(), "ticks")
	cmd := exec.Command("sh", "-c", "i=0; while [ $i -lt 400 ]; do echo tick >> "+marker+"; sleep 0.02; i=$((i+1)); done")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	require.Eventually(t, func() bool { return fileSize(marker) > 20 }, 10*time.Second, 10*time.Millisecond,
		"the child never started writing")

	outcome, detail := applyMechanism(MechFreeze, cmd.Process.Pid, JobPolicy{}, 1)
	require.Equal(t, OutcomeApplied, outcome, detail)

	time.Sleep(200 * time.Millisecond)
	stopped := fileSize(marker)
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, stopped, fileSize(marker), "a frozen process must not make progress")

	outcome, detail = revertMechanism(MechFreeze, cmd.Process.Pid, 1)
	require.Equal(t, OutcomeReverted, outcome, detail)
	require.Eventually(t, func() bool { return fileSize(marker) > stopped }, 10*time.Second, 10*time.Millisecond,
		"a thawed process must run again")
}

func TestPriorityChangesANiceValueAndRevertsIt(t *testing.T) {
	if hostOS() == "windows" {
		out, detail := applyMechanism(MechPriority, os.Getpid(), JobPolicy{}, 1)
		assert.Equal(t, OutcomeNotApplicable, out, detail)
		return
	}
	pid := spawnSleep(t)
	before, err := processNice(pid)
	require.NoError(t, err)

	policy := JobPolicy{Priority: PrioritySettings{Enabled: true, Nice: 7}}
	outcome, detail := applyMechanism(MechPriority, pid, policy, 1)
	require.Equal(t, OutcomeApplied, outcome, detail)
	after, err := processNice(pid)
	require.NoError(t, err)
	assert.NotEqual(t, before, after, "the nice value must change")

	// Putting the nice value back means lowering it, which POSIX lets only a privileged process do.
	outcome, detail = revertMechanism(MechPriority, pid, 1)
	if outcome == OutcomeRefused {
		assert.Contains(t, detail, "permission")
		return
	}
	require.Equal(t, OutcomeReverted, outcome, detail)
	back, err := processNice(pid)
	require.NoError(t, err)
	assert.NotEqual(t, after, back, "the revert must undo the applied value")
}

func TestAffinityIsAppliedOrHonestlyUnavailable(t *testing.T) {
	pid := spawnSleep(t)
	policy := JobPolicy{Affinity: AffinitySettings{Enabled: true, CPUs: []int{0}}}
	outcome, detail := applyMechanism(MechAffinity, pid, policy, runtime.NumCPU())
	switch hostOS() {
	case "linux", "darwin":
		assert.Equal(t, OutcomeApplied, outcome, detail)
	default:
		assert.Equal(t, OutcomeNotApplicable, outcome, detail)
	}
	if outcome != OutcomeApplied {
		return
	}
	outcome, detail = revertMechanism(MechAffinity, pid, runtime.NumCPU())
	assert.Equal(t, OutcomeReverted, outcome, detail)
}

// TestAffinityPinsTheLinuxCPUMask reads the CPU set back from the kernel, so
// it only says anything where sched_getaffinity exists.
func TestAffinityPinsTheLinuxCPUMask(t *testing.T) {
	if hostOS() != "linux" {
		t.Skip("sched_setaffinity is a Linux call")
	}
	pid := spawnSleep(t)
	allowed, err := getAffinitySyscall(pid)
	require.NoError(t, err)
	require.NotEmpty(t, allowed)
	want := []int{allowed[len(allowed)-1]}

	outcome, detail := applyMechanism(MechAffinity, pid,
		JobPolicy{Affinity: AffinitySettings{Enabled: true, CPUs: want}}, runtime.NumCPU())
	require.Equal(t, OutcomeApplied, outcome, detail)

	got, err := getAffinitySyscall(pid)
	require.NoError(t, err)
	assert.Equal(t, want, got, "the kernel must report the CPU set the daemon asked for")

	outcome, detail = revertMechanism(MechAffinity, pid, runtime.NumCPU())
	require.Equal(t, OutcomeReverted, outcome, detail)
}

func TestACallAMissingHostCarriesIsNotARefusal(t *testing.T) {
	out, detail := outcomeFor("windows", OutcomeRefused, "function not implemented")
	assert.Equal(t, OutcomeNotApplicable, out, "%s on a host without the call", detail)

	out, detail = outcomeFor("linux", OutcomeRefused, "operation not permitted")
	assert.Equal(t, OutcomeRefused, out, "a call the host has and declined stays a refusal")

	out, _ = outcomeFor("windows", OutcomeApplied, "cpus 0")
	assert.Equal(t, OutcomeApplied, out, "a call that worked is reported as applied")
}

func TestControllingAProcessThatIsGoneIsNotARefusal(t *testing.T) {
	if hostOS() == "windows" {
		return
	}
	cmd := exec.Command("sleep", "30")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	require.NoError(t, cmd.Process.Kill())
	_, _ = cmd.Process.Wait()

	outcome, detail := applyMechanism(MechFreeze, pid, JobPolicy{}, 1)
	require.Equal(t, OutcomeReverted, outcome, detail)
}
