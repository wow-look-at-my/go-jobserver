package jobserver

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectorMatchesANameAPathAndAPID(t *testing.T) {
	sel := ParseSelector("1234, ffmpeg")
	assert.Equal(t, []int{1234}, sel.PIDs)
	assert.Equal(t, []string{"ffmpeg"}, sel.Names)
	assert.True(t, sel.Matches(Proc{PID: 1234, Name: "sleep"}))
	assert.True(t, sel.Matches(Proc{PID: 7, Name: "ffmpeg"}))
	assert.True(t, sel.Matches(Proc{PID: 8, Name: "/usr/local/bin/ffmpeg"}))
	assert.False(t, sel.Matches(Proc{PID: 9, Name: "ffprobe"}))
	assert.Equal(t, "1234,ffmpeg", sel.String())
}

func TestSelectorThatNamesNothingMatchesNothing(t *testing.T) {
	var sel Selector
	assert.True(t, sel.Empty())
	assert.False(t, sel.Matches(Proc{PID: 1, Name: "sh"}))
}

func TestPolicyNormalizeFillsDefaults(t *testing.T) {
	got := JobPolicy{
		Cost:     2,
		Priority: PrioritySettings{Enabled: true},
		Affinity: AffinitySettings{Enabled: true, CPUs: []int{3, 1, 3, -1}},
	}.normalize()
	assert.Equal(t, defaultNice, got.Priority.Nice)
	assert.Equal(t, []int{1, 3}, got.Affinity.CPUs)
	assert.Equal(t, []Mechanism{MechAffinity, MechPriority}, got.Enabled())
	assert.False(t, got.Zero())
}

func TestPolicySelectorsApplyPerMechanism(t *testing.T) {
	policy := JobPolicy{
		Freeze:   FreezeSettings{Enabled: true, Exempt: ParseSelector("sleep")},
		Priority: PrioritySettings{Enabled: true, Only: ParseSelector("worker")},
	}
	procs := []Proc{{PID: 1, Name: "sh"}, {PID: 2, Name: "sleep"}, {PID: 3, Name: "worker"}}

	frozen := policy.settings(MechFreeze).selected(procs)
	require.Len(t, frozen, 2)
	assert.Equal(t, 1, frozen[0].PID)
	assert.Equal(t, 3, frozen[1].PID)

	reniced := policy.settings(MechPriority).selected(procs)
	require.Len(t, reniced, 1)
	assert.Equal(t, 3, reniced[0].PID)
}

func TestParseCPUsReadsListsAndKeywords(t *testing.T) {
	assert.Equal(t, []int{0, 2}, ParseCPUs("0,2,0"))
	assert.Nil(t, ParseCPUs("last"))
	assert.Len(t, ParseCPUs("all"), runtime.NumCPU())
}

func TestPolicySurvivesAReopen(t *testing.T) {
	dir := t.TempDir()
	want := JobPolicy{
		Cost: 2.5,
		Affinity: AffinitySettings{
			Enabled: true, CPUs: []int{2, 3},
			Exempt: ParseSelector("ffmpeg"), Only: ParseSelector("1234"),
		},
		Priority: PrioritySettings{Enabled: true, Nice: 7, Exempt: ParseSelector("sleep")},
		Freeze:   FreezeSettings{Enabled: true, Only: ParseSelector("worker")},
	}
	st, err := OpenStore(dir)
	require.NoError(t, err)
	created, err := st.Create(&Spec{Command: []string{"make"}, Policy: want})
	require.NoError(t, err)
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	got, ok := st.Job(created.ID)
	require.True(t, ok)
	assert.True(t, want.normalize().equal(got.Policy), "policy %+v, want %+v", got.Policy, want.normalize())
	assert.InDelta(t, 2.5, got.Policy.Cost, 1e-9)
}

func TestStoreReplaysAJournalFromBeforePoliciesExisted(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, logsDirName), 0o755))
	old := record{
		kind: recCreate, id: "j-old", name: "old",
		command: []string{"make"}, created: 1, state: StateActive,
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, journalName), frame(old.encode()), 0o644))

	st, err := OpenStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	j, ok := st.Job("j-old")
	require.True(t, ok, "a journal written before this change must still load")
	assert.True(t, j.Policy.Zero())
	assert.Empty(t, j.Policy.Enabled())
	assert.Zero(t, j.CPUSeconds)
}

func TestSetPolicyRecordsTheNewSettings(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	created, err := st.Create(&Spec{Command: []string{"make"}})
	require.NoError(t, err)
	require.NoError(t, st.SetPolicy(created.ID, JobPolicy{
		Cost:   4,
		Freeze: FreezeSettings{Enabled: true},
	}))
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	got, ok := st.Job(created.ID)
	require.True(t, ok)
	assert.InDelta(t, 4, got.Policy.Cost, 1e-9)
	assert.Equal(t, []Mechanism{MechFreeze}, got.Policy.Enabled())
}

func TestJobExpectedCostPrefersWhatItDeclaredThenWhatItMeasured(t *testing.T) {
	declared := &Job{Policy: JobPolicy{Cost: 3}, CPUSeconds: 10}
	assert.InDelta(t, 3, declared.ExpectedCost(1), 1e-9)

	measured := &Job{
		Started:    timeAt(0),
		Finished:   timeAt(4),
		CPUSeconds: 6,
	}
	assert.InDelta(t, 1.5, measured.ExpectedCost(1), 1e-9)

	unknown := &Job{}
	assert.InDelta(t, 1, unknown.ExpectedCost(1), 1e-9)
}

// timeAt is a fixed instant offset by whole seconds.
func timeAt(seconds int) time.Time {
	return time.Unix(0, 0).Add(time.Duration(seconds) * time.Second)
}
