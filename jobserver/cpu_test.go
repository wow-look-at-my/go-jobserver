package jobserver

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedulerHoldsAJobThatWouldOversubscribeTheBudget(t *testing.T) {
	hold := make(chan struct{})
	runner := &fakeRunner{hold: hold}
	srv := newTestServer(t, func(c *Config) {
		c.Runner = runner
		c.CPUBudget = 2
		c.DefaultCost = 2
		c.MaxConcurrent = 4
	})
	first, _, err := srv.Enqueue(Spec{Command: []string{"first"}})
	require.NoError(t, err)
	waitState(t, srv, first.ID, StateRunning)

	second, _, err := srv.Enqueue(Spec{Command: []string{"second"}})
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	waiting, err := srv.Get(second.ID)
	require.NoError(t, err)
	assert.Equal(t, StateActive, waiting.State, "a job that does not fit must stay runnable")
	assert.False(t, runner.ran(second.ID), "the budget must hold the second job back")

	close(hold)
	waitState(t, srv, second.ID, StateCompleted)
}

func TestSchedulerRunsAJobLargerThanTheWholeBudget(t *testing.T) {
	runner := &fakeRunner{}
	srv := newTestServer(t, func(c *Config) {
		c.Runner = runner
		c.CPUBudget = 1
		c.DefaultCost = 4
	})
	j, _, err := srv.Enqueue(Spec{Command: []string{"big"}})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateCompleted)
}

func TestStatsCarryTheCPUReport(t *testing.T) {
	srv := newTestServer(t, func(c *Config) {
		c.CPUBudget = 3
		c.SampleInterval = 20 * time.Millisecond
	})
	st := srv.Stats()
	assert.InDelta(t, 3, st.CPU.Budget, 1e-9)
	assert.Equal(t, runtime.NumCPU(), st.CPU.CPUs)
	assert.False(t, st.CPU.OverBudget)
	assert.Empty(t, st.Controls)

	off := newTestServer(t, func(c *Config) { c.DisableCPU = true })
	assert.Contains(t, off.Stats().CPU.Note, "cpu sampling is off")
}

func TestGovernorThrottlesAnOversubscribedJobAndSparesAnExemptProcess(t *testing.T) {
	srv := newTestServer(t, func(c *Config) {
		c.CPUBudget = 0.001
		c.SampleInterval = 20 * time.Millisecond
	})
	j, _, err := srv.Enqueue(Spec{
		Command: []string{"sh", "-c", "sleep 30 & while :; do :; done"},
		Policy: JobPolicy{
			Cost:   1,
			Freeze: FreezeSettings{Enabled: true, Exempt: ParseSelector("sleep")},
		},
	})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	t.Cleanup(func() { srv.InterruptAll() })

	var controls []Control
	require.Eventually(t, func() bool {
		controls = srv.Controls()
		return len(controls) > 0
	}, 20*time.Second, 20*time.Millisecond, "the governor never applied a control")

	for _, c := range controls {
		assert.Equal(t, j.ID, c.Job)
		assert.Equal(t, MechFreeze, c.Mechanism)
		assert.Equal(t, OutcomeApplied, c.Outcome, c.Detail)
		assert.NotEqual(t, "sleep", filepath.Base(c.Name), "an exempt process must be left alone")
	}
	assert.NotEmpty(t, srv.Stats().Controls)
}

func TestGovernorGivesAControlBackOnceTheJobIsUnderBudgetAgain(t *testing.T) {
	// The host's answers are asked for before the server starts.
	host := hostHere(t)
	srv := newTestServer(t, func(c *Config) {
		c.CPUBudget = 0.001
		c.SampleInterval = 20 * time.Millisecond
	})
	srv.gov.probe = hostProbe{known: true, canFreeze: host.CanFreeze,
		freezeWhy: host.FreezeDetail, spelling: host.NiceSpelling, niceWhy: host.NiceDetail}
	j, _, err := srv.Enqueue(Spec{
		Command: []string{"sh", "-c", "head -c 4000000000 /dev/zero > /dev/null; sleep 30"},
		Policy: JobPolicy{
			Cost:     1,
			Priority: PrioritySettings{Enabled: true, Nice: 10},
		},
	})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	t.Cleanup(func() { srv.InterruptAll() })

	require.Eventually(t, func() bool { return len(srv.Controls()) > 0 },
		20*time.Second, 20*time.Millisecond, "the governor never applied a control")

	// The heavy command finishes and the job goes quiet, which is where the
	// daemon hands its processes back.
	require.Eventually(t, func() bool {
		for _, c := range srv.Events() {
			if c.Job == j.ID && !c.Active {
				return true
			}
		}
		return false
	}, 20*time.Second, 20*time.Millisecond, "the governor never released the control")
	assert.Empty(t, srv.Controls(), "nothing may still be in effect once the job is under budget")
}

func TestGovernorLeavesAJobAloneWhereFreezeCannotSuspend(t *testing.T) {
	srv := newTestServer(t, func(c *Config) {
		c.CPUBudget = 0.001
		c.SampleInterval = 20 * time.Millisecond
	})
	// A host whose stop signal ends a process cannot be asked to stop a job.
	srv.gov.mu.Lock()
	srv.gov.probe = hostProbe{known: true, canFreeze: false, freezeWhy: "the stop signal ended the process"}
	srv.gov.mu.Unlock()

	j, _, err := srv.Enqueue(Spec{
		Command: []string{"sh", "-c", "while :; do :; done"},
		Policy:  JobPolicy{Cost: 1, Freeze: FreezeSettings{Enabled: true}},
	})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	t.Cleanup(func() { srv.InterruptAll() })

	var told bool
	require.Eventually(t, func() bool {
		for _, c := range srv.Events() {
			if c.Job == j.ID && c.Mechanism == MechFreeze {
				told = true
				assert.Equal(t, OutcomeNotApplicable, c.Outcome)
				assert.Equal(t, "the stop signal ended the process", c.Detail)
			}
		}
		return told
	}, 20*time.Second, 20*time.Millisecond, "the governor never said why it did not stop the job")

	assert.Empty(t, srv.Controls(), "nothing may be in effect where freeze cannot suspend")
	running, err := srv.Get(j.ID)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, running.State, "the job must keep running")
}

func TestServerCountsCPUThatAnIndirectChildBurns(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.SampleInterval = 10 * time.Millisecond })
	// The daemon's own child waits on a sleep while a grandchild burns.
	j, _, err := srv.Enqueue(Spec{Command: []string{"sh", "-c", "while :; do :; done & sleep 30"}})
	require.NoError(t, err)
	waitState(t, srv, j.ID, StateRunning)
	t.Cleanup(func() { srv.InterruptAll() })

	require.Eventually(t, func() bool { return srv.Stats().CPU.Jobs[j.ID] > 0 },
		30*time.Second, 20*time.Millisecond,
		"the daemon must count the CPU an indirect child burns")

	srv.InterruptAll()
	require.Eventually(t, func() bool {
		done, err := srv.Get(j.ID)
		return err == nil && done.State.Terminal()
	}, 30*time.Second, 20*time.Millisecond, "the job must stop when it is interrupted")

	done, err := srv.Get(j.ID)
	require.NoError(t, err)
	assert.Greater(t, done.CPUSeconds, 0.0, "the finished job carries what its tree burned")
}

func TestServerRecordsTheCPUAJobUsed(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.SampleInterval = 10 * time.Millisecond })
	j, _, err := srv.Enqueue(Spec{Command: []string{"sh", "-c", "head -c 4000000000 /dev/zero > /dev/null"}})
	require.NoError(t, err)
	done := waitState(t, srv, j.ID, StateCompleted)
	assert.Greater(t, done.CPUSeconds, 0.0, "the daemon must measure what the job's tree consumed")
}

func TestServerReplacesAJobPolicyAndReportsIt(t *testing.T) {
	srv := newTestServer(t, nil)
	j, _, err := srv.Enqueue(Spec{Command: []string{"sh", "-c", "sleep 30"}, Draft: true})
	require.NoError(t, err)

	updated, err := srv.SetPolicy(j.ID, JobPolicy{
		Cost:     2,
		Freeze:   FreezeSettings{Enabled: true},
		Priority: PrioritySettings{Enabled: true, Nice: 5},
	})
	require.NoError(t, err)
	assert.InDelta(t, 2, updated.Policy.Cost, 1e-9)
	assert.Equal(t, []Mechanism{MechPriority, MechFreeze}, updated.Policy.Enabled())

	stored, err := srv.Get(j.ID)
	require.NoError(t, err)
	assert.True(t, stored.Policy.equal(updated.Policy))
}
