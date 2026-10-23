package jobserver

import "slices"

// hostLoadWindow is how many samples the host-load gate reads. procs_running is one instant.
const hostLoadWindow = 5

// freeCost is the highest cost the host-load gate ignores.
const freeCost = 0.05

// hostLoad is the host's CPU count and the last samples of how many tasks
// wanted a CPU. cpus is zero when the load is unknown.
type hostLoad struct {
	cpus    int
	samples []int
}

// add records one sample and keeps the last hostLoadWindow.
func (h *hostLoad) add(cpus, runnable int) {
	h.cpus = cpus
	h.samples = append(h.samples, runnable)
	if n := len(h.samples); n > hostLoadWindow {
		h.samples = slices.Clone(h.samples[n-hostLoadWindow:])
	}
}

// runnable is the lower median of the recent samples.
func (h hostLoad) runnable() int {
	if len(h.samples) == 0 {
		return 0
	}
	s := slices.Clone(h.samples)
	slices.Sort(s)
	return s[(len(s)-1)/2]
}
