package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func TestStatsPrintsAnUnreadableHostLoadAsAnError(t *testing.T) {
	out, err := capture(t, func() error {
		printCPU(jobserver.CPUReport{CPUs: 4, HostBusy: 1.5, HostError: "this host has no /proc/stat"})
		return nil
	})
	require.NoError(t, err)
	assert.Contains(t, out, "host     unknown")
	assert.Contains(t, out, "configuration error: the host's CPU load is unknown (this host has no /proc/stat)")
	assert.Contains(t, out, "destabilize the system under load")
	assert.NotContains(t, out, "1.50", "an estimate is not shown as the host's load")
}

func TestStatsPrintsAReadableHostLoadAsANumber(t *testing.T) {
	out, err := capture(t, func() error {
		printCPU(jobserver.CPUReport{CPUs: 4, HostBusy: 1.5})
		return nil
	})
	require.NoError(t, err)
	assert.Contains(t, out, "host     1.50")
	assert.NotContains(t, out, "error")
}
