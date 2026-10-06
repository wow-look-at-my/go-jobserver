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
	fmt.Printf("host     %.2f\n", cpu.HostBusy)
	fmt.Printf("over     %v\n", cpu.OverBudget)
	if cpu.Note != "" {
		fmt.Printf("note     %s\n", cpu.Note)
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
