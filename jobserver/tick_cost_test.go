package jobserver

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The scheduler ticks at least once a second. Its cost must not grow with the ended tasks the retention keeps.
func TestTickCostDoesNotGrowWithEndedTasks(t *testing.T) {
	srv := newTestServer(t, nil)
	require.NoError(t, srv.store.SetPaused(true))
	spec, err := json.Marshal(map[string]string{"title": "t", "body": strings.Repeat("x", 16<<10)})
	require.NoError(t, err)
	for range 5000 {
		j, err := srv.store.create(&Spec{Name: "t"}, &taskMeta{Inbox: "x", Spec: spec})
		require.NoError(t, err)
		require.NoError(t, srv.store.Finish(j.ID, StateCompleted, 0, "", nil, 0))
	}
	addQueued(t, srv.store, "x")
	require.NoError(t, srv.store.SetPaused(false))

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const ticks = 100
	for range ticks {
		srv.tick()
	}
	runtime.ReadMemStats(&after)
	perTick := (after.TotalAlloc - before.TotalAlloc) / ticks
	t.Logf("a tick over 5000 ended 16 KiB tasks allocates %d bytes", perTick)
	assert.Less(t, perTick, uint64(64<<10), "a tick reads the open tasks, not the retained history")
}
