package jobserver

import (
	"math"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedHost serves host counters that move by a set busy share at each read.
type scriptedHost struct {
	mu      sync.Mutex
	cpus    int
	share   float64
	running int
	err     error
	busy    uint64
	total   uint64
}

func (h *scriptedHost) hostStat() (hostStat, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return hostStat{}, h.err
	}
	const step = 1000
	h.total += step
	h.busy += uint64(math.Round(h.share * step))
	return hostStat{busy: h.busy, total: h.total, cpus: h.cpus, running: h.running + 1}, nil
}

func (h *scriptedHost) set(share float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.share = share
}

// setRunning sets how many tasks want a CPU, not counting the reader.
func (h *scriptedHost) setRunning(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.running = n
}

// snapshotHost serves fixed host reads in order, then the last one again.
type snapshotHost struct {
	reads []hostStat
	next  int
}

func (h *snapshotHost) hostStat() (hostStat, error) {
	st := h.reads[min(h.next, len(h.reads)-1)]
	h.next++
	return st, nil
}

func TestParseHostStatReadsTheAggregateLineAndCountsTheCPUs(t *testing.T) {
	text := "cpu  100 20 30 400 50 6 7 8 90 10\n" +
		"cpu0 50 10 15 200 25 3 3 4 45 5\n" +
		"cpu1 50 10 15 200 25 3 4 4 45 5\n" +
		"intr 12345 0 0\n" +
		"ctxt 999\n" +
		"btime 1700000000\n" +
		"procs_running 3\n"
	st, err := parseHostStat(text)
	require.NoError(t, err)
	assert.Equal(t, 2, st.cpus)
	assert.Equal(t, uint64(100+20+30+6+7+8), st.busy)
	assert.Equal(t, uint64(100+20+30+6+7+8+400+50), st.total)
	assert.Equal(t, 3, st.running)
}

func TestParseHostStatNeedsTheCountOfTasksThatWantACPU(t *testing.T) {
	_, err := parseHostStat("cpu 1 2 3 4\ncpu0 1 2 3 4\n")
	assert.ErrorContains(t, err, "procs_running", "a text with no such line is not a usable /proc/stat")
	_, err = parseHostStat("cpu 1 2 3 4\ncpu0 1 2 3 4\nprocs_running many\n")
	assert.ErrorContains(t, err, "procs_running")
}

func TestParseHostStatReadsAnOldKernelsShortLine(t *testing.T) {
	st, err := parseHostStat("cpu 100 20 30 400\ncpu0 100 20 30 400\nprocs_running 1\n")
	require.NoError(t, err)
	assert.Equal(t, uint64(150), st.busy)
	assert.Equal(t, uint64(550), st.total)
	assert.Equal(t, 1, st.cpus)
}

func TestParseHostStatRefusesTextThatIsNotProcStat(t *testing.T) {
	for _, text := range []string{
		"",
		"cpu0 1 2 3 4\n",
		"cpu 1 2 3 4\n",
		"cpu 1 2 3\ncpu0 1 2 3\n",
		"cpu 1 2 x 4\ncpu0 1 2 3 4\n",
	} {
		_, err := parseHostStat(text)
		assert.Error(t, err, "%q", text)
	}
}

func TestTheHostSourceReadsThisHost(t *testing.T) {
	_, err := procStatSource{path: filepath.Join(t.TempDir(), "stat")}.hostStat()
	assert.Error(t, err, "a missing file is an error, not an idle host")

	src := newHostSource()
	if src == nil {
		assert.NotEqual(t, "linux", hostOS(), "a Linux host has /proc/stat")
		return
	}
	st, err := src.hostStat()
	require.NoError(t, err)
	assert.Positive(t, st.cpus)
	assert.GreaterOrEqual(t, st.total, st.busy)
}

func TestHostBusyComesFromTheChangeBetweenTwoReads(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prev, next hostStat
		ok         bool
		busy       float64
	}{
		{"a quarter busy", hostStat{100, 400, 4, 1}, hostStat{200, 800, 4, 1}, true, 1},
		{"iowait went back, so busy is above the total", hostStat{100, 400, 2, 1}, hostStat{300, 500, 2, 1}, true, 2},
		{"no tick between the reads", hostStat{100, 400, 2, 1}, hostStat{100, 400, 2, 1}, false, 0},
		{"busy went back", hostStat{300, 400, 2, 1}, hostStat{200, 900, 2, 1}, false, 0},
	} {
		s := &sampler{host: &snapshotHost{reads: []hostStat{tc.prev, tc.next}}}
		var first, second Reading
		require.True(t, s.readHost(&first), tc.name)
		require.True(t, s.readHost(&second), tc.name)
		assert.False(t, first.HostOK, "%s: the first read is only a baseline", tc.name)
		assert.Equal(t, tc.ok, second.HostOK, tc.name)
		assert.InDelta(t, tc.busy, second.HostBusy, 1e-9, tc.name)
		assert.Equal(t, tc.next.cpus, second.HostCPUs, tc.name)
	}
}
