package jobserver

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tasks that one pass offers together reach the scheduler in the order of the offers. A free slot goes to the oldest.
func TestTheFirstOfferedTaskTakesTheFreeSlot(t *testing.T) {
	for round := range 20 {
		runner := &fakeRunner{hold: make(chan struct{})}
		srv := newTestServer(t, func(c *Config) { c.Runner = runner; c.MaxConcurrent = 1 })
		ids := make([]string, 0, 3)
		for i := range 3 {
			j, _, err := srv.Enqueue(Spec{Command: []string{"x", strconv.Itoa(i)}})
			require.NoError(t, err)
			ids = append(ids, j.ID)
		}
		assert.Eventually(t, func() bool {
			st, _ := srv.store.stateOf(ids[0])
			return st == StateRunning
		}, 5*time.Second, 5*time.Millisecond, "round %d: the oldest task runs first", round)
		close(runner.hold)
		require.NoError(t, srv.Close())
	}
}
