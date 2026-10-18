package jobserver

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// A hostStat is one read of the host's own CPU counters, in clock ticks.
type hostStat struct {
	busy  uint64
	total uint64
	// cpus is how many per-CPU lines the read had.
	cpus int
}

// A hostSource reads the host's own CPU counters. They count every process
// and kernel thread, whatever PID namespace the reader is in.
type hostSource interface {
	hostStat() (hostStat, error)
}

// procStatSource reads /proc/stat. It is what a Linux host offers.
type procStatSource struct{ path string }

func (s procStatSource) hostStat() (hostStat, error) {
	body, err := os.ReadFile(s.path)
	if err != nil {
		return hostStat{}, err
	}
	return parseHostStat(string(body))
}

// newHostSource picks the host's own CPU counters. A host without /proc/stat
// has none, and the sampler sums the process table there.
func newHostSource() hostSource {
	if _, err := os.Stat("/proc/stat"); err != nil {
		return nil
	}
	return procStatSource{path: "/proc/stat"}
}

// parseHostStat reads the aggregate cpu line of /proc/stat and counts the
// per-CPU lines. The columns are user, nice, system, idle, iowait, irq,
// softirq, steal, guest and guest_nice. An old kernel stops early.
func parseHostStat(text string) (hostStat, error) {
	var st hostStat
	found := false
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.HasPrefix(fields[0], "cpu") {
			continue
		}
		if fields[0] != "cpu" {
			if isDigits(strings.TrimPrefix(fields[0], "cpu")) {
				st.cpus++
			}
			continue
		}
		var col [8]uint64
		n := min(len(fields)-1, len(col))
		if n < 4 {
			return hostStat{}, fmt.Errorf("jobserver: the cpu line %q is too short", line)
		}
		for i := range n {
			v, err := strconv.ParseUint(fields[i+1], 10, 64)
			if err != nil {
				return hostStat{}, fmt.Errorf("jobserver: the cpu line %q: %w", line, err)
			}
			col[i] = v
		}
		user, nice, system, idle, iowait, irq, softirq, steal := col[0], col[1], col[2], col[3], col[4], col[5], col[6], col[7]
		// Guest time is already inside user and nice, so the guest columns are not added.
		st.busy = user + nice + system + irq + softirq + steal
		st.total = st.busy + idle + iowait
		found = true
	}
	if !found {
		return hostStat{}, errors.New("jobserver: no aggregate cpu line")
	}
	if st.cpus == 0 {
		return hostStat{}, errors.New("jobserver: no per-CPU lines")
	}
	return st, nil
}
