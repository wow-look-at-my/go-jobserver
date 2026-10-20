package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

// printCPU reports the daemon's CPU measurement.
func printCPU(cpu jobserver.CPUReport) {
	fmt.Printf("cpus     %d\n", cpu.CPUs)
	fmt.Printf("budget   %.2f\n", cpu.Budget)
	fmt.Printf("measured %.2f\n", cpu.Measured)
	if cpu.HostError != "" {
		fmt.Printf("host     unknown\n")
		fmt.Printf("error    configuration error: the host's CPU load is unknown (%s)\n", cpu.HostError)
		fmt.Printf("         the daemon cannot see work outside its own jobs or how many tasks want a CPU, and can destabilize the system under load\n")
	} else {
		fmt.Printf("host     %.2f\n", cpu.HostBusy)
		fmt.Printf("runnable %d\n", cpu.HostRunnable)
	}
	fmt.Printf("over     %v\n", cpu.OverBudget)
	if cpu.Note != "" {
		fmt.Printf("note     %s\n", cpu.Note)
	}
}

// printStats reports the daemon's counters.
func printStats(st *jobserver.Stats) {
	fmt.Printf("dir      %s\n", st.Dir)
	fmt.Printf("version  %s\n", st.Revision)
	fmt.Printf("pid      %d\n", st.PID)
	fmt.Printf("uptime   %s\n", st.Uptime)
	fmt.Printf("paused   %v\n", st.Paused)
	fmt.Printf("running  %d\n", st.Running)
	fmt.Printf("total    %d\n", st.Total)
	printCPU(st.CPU)
	for _, c := range st.Controls {
		fmt.Printf("control  %s %s pid %d %s %s\n", c.Job, c.Mechanism, c.PID, c.Outcome, c.Detail)
	}
	states := make([]string, 0, len(st.ByState))
	for state := range st.ByState {
		states = append(states, state)
	}
	sortStrings(states)
	for _, state := range states {
		fmt.Printf("%-10s %d\n", state, st.ByState[state])
	}
}

// printJSON writes a value as indented JSON.
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// sortStrings sorts a small slice in place.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for k := i; k > 0 && s[k] < s[k-1]; k-- {
			s[k], s[k-1] = s[k-1], s[k]
		}
	}
}
