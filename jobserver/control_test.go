package jobserver

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
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
	if host := probeHost(); !host.canFreeze {
		// A host whose stop signal ends a process has no freeze to offer, and
		// saying so is the whole of what it can do.
		res := applyMechanism(MechFreeze, os.Getpid(), JobPolicy{},
			hostFacts{CPUs: 1, FreezeDetail: host.freezeWhy})
		assert.Equal(t, OutcomeNotApplicable, res.outcome, res.detail)
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

	res := applyMechanism(MechFreeze, cmd.Process.Pid, JobPolicy{}, hostHere(t))
	require.Equal(t, OutcomeApplied, res.outcome, res.detail)

	time.Sleep(200 * time.Millisecond)
	stopped := fileSize(marker)
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, stopped, fileSize(marker), "a frozen process must not make progress")

	back := revertMechanism(Control{Mechanism: MechFreeze, PID: cmd.Process.Pid}, 1)
	require.Equal(t, OutcomeReverted, back.outcome, back.detail)
	require.Eventually(t, func() bool { return fileSize(marker) > stopped }, 10*time.Second, 10*time.Millisecond,
		"a thawed process must run again")
}

func TestPriorityChangesANiceValueAndRevertsIt(t *testing.T) {
	pid := spawnSleep(t)
	before, err := processNice(pid)
	require.NoError(t, err)

	policy := JobPolicy{Priority: PrioritySettings{Enabled: true, Nice: 7}}
	res := applyMechanism(MechPriority, pid, policy, hostHere(t))
	if res.outcome == OutcomeNotApplicable {
		// A host whose syscall layer has no priority call says so itself.
		return
	}
	require.Equal(t, OutcomeApplied, res.outcome, res.detail)
	require.True(t, res.hadPrior, "the value it found has to travel with the control")
	// The value it carries is the one a revert writes.
	wantPrior, ok := niceToWrite(hostHere(t).NiceSpelling, before)
	require.True(t, ok)
	require.Equal(t, []int{wantPrior}, res.prior)

	raised, err := processNice(pid)
	require.NoError(t, err)
	assert.NotEqual(t, before, raised, "the nice value must change")

	// Putting a nice value back means lowering it, which POSIX reserves for a
	// privileged process.
	back := revertMechanism(Control{
		Mechanism: MechPriority, PID: pid, Prior: res.prior, HadPrior: res.hadPrior,
	}, 1)
	got, err := processNice(pid)
	require.NoError(t, err)
	if back.outcome == OutcomeRefused {
		assert.Contains(t, back.detail, "permission")
		assert.Equal(t, raised, got, "a refused revert changes nothing")
		return
	}
	require.Equal(t, OutcomeReverted, back.outcome, back.detail)
	assert.Equal(t, before, got, "the revert must put back the value it found")
}

func TestAffinityIsAppliedOrHonestlyUnavailable(t *testing.T) {
	pid := spawnSleep(t)
	policy := JobPolicy{Affinity: AffinitySettings{Enabled: true, CPUs: []int{0}}}
	res := applyMechanism(MechAffinity, pid, policy, hostHere(t))
	switch hostOS() {
	case "linux", "darwin":
		assert.Equal(t, OutcomeApplied, res.outcome, res.detail)
	default:
		assert.Equal(t, OutcomeNotApplicable, res.outcome, res.detail)
	}
	if res.outcome != OutcomeApplied {
		return
	}
	back := revertMechanism(Control{
		Mechanism: MechAffinity, PID: pid, Prior: res.prior, HadPrior: res.hadPrior,
	}, runtime.NumCPU())
	assert.Equal(t, OutcomeReverted, back.outcome, back.detail)
	// Where the host lets the state be read the control carries it, and where
	// it does not the revert clears the state and says so.
	if !res.hadPrior {
		assert.NotEmpty(t, back.detail, "a revert without a prior says what it did")
	}
}

// TestDarwinAffinityIsTheBackgroundPolicyAndComesBack covers macOS's affinity
// mechanism. macOS reports the background policy of the process that asks for
// it. A job's policy cannot be read back and the assertions are what the
// kernel answered the daemon's calls.
func TestDarwinAffinityIsTheBackgroundPolicyAndComesBack(t *testing.T) {
	if hostOS() != "darwin" {
		t.Skip("the background policy is a macOS facility")
	}
	pid := spawnSleep(t)
	res := applyMechanism(MechAffinity, pid,
		JobPolicy{Affinity: AffinitySettings{Enabled: true}}, hostHere(t))
	require.Equal(t, OutcomeApplied, res.outcome, res.detail)
	assert.Contains(t, res.detail, "background policy")
	assert.False(t, res.hadPrior, "macOS does not report the policy of another process")

	back := revertMechanism(Control{
		Mechanism: MechAffinity, PID: pid, Prior: res.prior, HadPrior: res.hadPrior,
	}, runtime.NumCPU())
	assert.Equal(t, OutcomeReverted, back.outcome, back.detail)
	assert.Contains(t, back.detail, "standard policy")
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

	res := applyMechanism(MechAffinity, pid,
		JobPolicy{Affinity: AffinitySettings{Enabled: true, CPUs: want}}, hostHere(t))
	require.Equal(t, OutcomeApplied, res.outcome, res.detail)
	require.Equal(t, allowed, res.prior, "the mask it found has to travel with the control")

	got, err := getAffinitySyscall(pid)
	require.NoError(t, err)
	assert.Equal(t, want, got, "the kernel must report the CPU set the daemon asked for")

	back := revertMechanism(Control{
		Mechanism: MechAffinity, PID: pid, Prior: res.prior, HadPrior: res.hadPrior,
	}, runtime.NumCPU())
	require.Equal(t, OutcomeReverted, back.outcome, back.detail)

	restored, err := getAffinitySyscall(pid)
	require.NoError(t, err)
	assert.Equal(t, allowed, restored, "the revert must put back the mask it found")
}

// TestAMissingCallHandsOverToTheNextRoute pins what the Windows routes rest
// on. The daemon's own answer for a host that has no such call reads as a
// missing call. The syscall route hands over to the host's shell.
func TestAMissingCallHandsOverToTheNextRoute(t *testing.T) {
	assert.True(t, missingCall(errNoAffinity), "the daemon's own answer must hand over")
	assert.True(t, missingCall(syscall.ENOSYS), "a syscall layer without the call must hand over")
	assert.False(t, missingCall(syscall.EPERM), "a refusal the host made is not a missing call")
}

// TestACallTheSyscallLayerLacksIsNotARefusal covers what the status surface
// says about a control the host has no call for.
func TestACallTheSyscallLayerLacksIsNotARefusal(t *testing.T) {
	lacks := controlCall(func() error { return errNoAffinity }, OutcomeApplied, "cpus 0", nil, false)
	assert.Equal(t, OutcomeNotApplicable, lacks.outcome, lacks.detail)

	absent := controlCall(func() error { return syscall.ENOSYS }, OutcomeApplied, "cpus 0", nil, false)
	assert.Equal(t, OutcomeNotApplicable, absent.outcome, absent.detail)

	denied := controlCall(func() error { return syscall.EPERM }, OutcomeApplied, "cpus 0", nil, false)
	assert.Equal(t, OutcomeRefused, denied.outcome, denied.detail)
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
	cmd := exec.Command("sleep", "30")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	require.NoError(t, cmd.Process.Kill())
	_, _ = cmd.Process.Wait()

	// Letting a process that has already ended run again is not a refusal: there is nothing left to control.
	back := revertMechanism(Control{Mechanism: MechFreeze, PID: pid}, 1)
	require.Equal(t, OutcomeReverted, back.outcome, back.detail)
}
