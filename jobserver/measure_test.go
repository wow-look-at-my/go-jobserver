package jobserver

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCPUTimeReadsEveryFormPSPrints(t *testing.T) {
	cases := map[string]float64{
		"0:00.50":    0.5,
		"12:34.50":   754.5,
		"1:02:03":    3723,
		"2-01:02:03": 176523,
	}
	for text, want := range cases {
		got, err := parseCPUTime(text)
		require.NoError(t, err, text)
		assert.InDelta(t, want, got.Seconds(), 1e-6, text)
	}
	_, err := parseCPUTime("nonsense")
	assert.Error(t, err)
}

func TestParseProcStatReadsANameHoldingSpacesAndParens(t *testing.T) {
	line := "4242 (my (odd) name) S 100 4242 4242 0 -1 4194560 1 0 0 0 17 25 0 0 20 0 1 0 12345 0 0"
	p, ok := parseProcStat(line)
	require.True(t, ok)
	assert.Equal(t, 4242, p.PID)
	assert.Equal(t, "my (odd) name", p.Name)
	assert.Equal(t, 100, p.PPID)
	assert.Equal(t, 4242, p.PGID)
	assert.Equal(t, 42*clockTick, p.CPU)
}

func TestParsePSReadsTheTable(t *testing.T) {
	text := "    1     0     1 358:18.22 /sbin/launchd\n" +
		"  340     1   340   9:24.03 /usr/libexec/logd\n" +
		"not a process line\n"
	procs, err := parsePS(text)
	require.NoError(t, err)
	require.Len(t, procs, 2)

	want, err := parseCPUTime("358:18.22")
	require.NoError(t, err)
	assert.Equal(t, Proc{PID: 1, PPID: 0, PGID: 1, Name: "/sbin/launchd", CPU: want}, procs[0])
	assert.Equal(t, 340, procs[1].PID)
}

func TestProcIndexTreeCoversDescendantsAndTheProcessGroup(t *testing.T) {
	index := newProcIndex([]Proc{
		{PID: 10, PPID: 1, PGID: 10, Name: "worker"},
		{PID: 11, PPID: 10, PGID: 10, Name: "child"},
		{PID: 12, PPID: 11, PGID: 12, Name: "grandchild"},
		{PID: 13, PPID: 1, PGID: 10, Name: "reparented"},
		{PID: 14, PPID: 1, PGID: 14, Name: "stranger"},
	})
	var ids []int
	for _, p := range index.tree(10) {
		ids = append(ids, p.PID)
	}
	assert.Equal(t, []int{10, 11, 12, 13}, ids)
}

// scriptedSource serves a fixed list of process tables in order.
type scriptedSource struct {
	snapshots [][]Proc
	next      int
}

func (s *scriptedSource) procs() ([]Proc, error) {
	if s.next >= len(s.snapshots) {
		return s.snapshots[len(s.snapshots)-1], nil
	}
	out := s.snapshots[s.next]
	s.next++
	return out, nil
}

// failingSource stands in for a host with no process source.
type failingSource struct{}

func (failingSource) procs() ([]Proc, error) { return nil, errors.New("no process source here") }

func TestSamplerTurnsTwoSnapshotsIntoRates(t *testing.T) {
	base := time.Unix(0, 0)
	s := &sampler{
		src: &scriptedSource{snapshots: [][]Proc{
			{
				{PID: 100, PPID: 1, PGID: 100, Name: "worker", CPU: time.Second},
				{PID: 101, PPID: 100, PGID: 100, Name: "child", CPU: 500 * time.Millisecond},
			},
			{
				{PID: 100, PPID: 1, PGID: 100, Name: "worker", CPU: 3 * time.Second},
				{PID: 101, PPID: 100, PGID: 100, Name: "child", CPU: time.Second},
			},
		}},
	}
	times := []time.Time{base, base.Add(time.Second), base.Add(2 * time.Second)}
	tick := 0
	s.now = func() time.Time {
		at := times[len(times)-1]
		if tick < len(times) {
			at = times[tick]
		}
		tick++
		return at
	}
	roots := map[string]int{"j-1": 100}

	first := s.sample(roots)
	assert.False(t, first.OK, "the first sample only establishes a baseline")
	assert.NotEmpty(t, first.Note)

	second := s.sample(roots)
	require.True(t, second.OK)
	assert.Equal(t, time.Second, second.Window)
	assert.InDelta(t, 2.5, second.HostBusy, 1e-9)
	assert.InDelta(t, 2.5, second.Jobs["j-1"], 1e-9)
	require.Len(t, second.Table, 2)

	third := s.sample(roots)
	require.True(t, third.OK)
	assert.InDelta(t, 0, third.HostBusy, 1e-9)
	assert.InDelta(t, 0, third.Jobs["j-1"], 1e-9)
}

func TestSamplerReportsWhyItHasNoRates(t *testing.T) {
	s := &sampler{src: failingSource{}}
	r := s.sample(nil)
	assert.False(t, r.OK)
	assert.Contains(t, r.Note, "no process source here")
}

func TestSamplerReadsThisHost(t *testing.T) {
	procs, err := newProcSource().procs()
	require.NoError(t, err, "the host this suite runs on must offer a process source")
	assert.NotEmpty(t, procs)
	var self bool
	for _, p := range procs {
		if p.PID == os.Getpid() {
			self = true
			assert.NotEmpty(t, p.Name)
		}
	}
	assert.True(t, self, "the host's own process table must include this process")
}
