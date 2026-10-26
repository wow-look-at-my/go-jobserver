package jobserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A drained server starts nothing, lets the running job finish, and journals no pause.
func TestDrainStopsStartsButNotTheRunningJob(t *testing.T) {
	hold := make(chan struct{})
	runner := &fakeRunner{hold: hold}
	srv := newTestServer(t, func(c *Config) { c.Runner = runner })
	busy, _, err := srv.Enqueue(Spec{ID: "busy", Command: []string{"long"}})
	require.NoError(t, err)
	waitState(t, srv, busy.ID, StateRunning)

	srv.Drain()
	assert.True(t, srv.Draining())
	queued, _, err := srv.Enqueue(Spec{ID: "queued", Command: []string{"x"}})
	require.NoError(t, err)
	srv.tick()
	time.Sleep(20 * time.Millisecond)
	j, err := srv.Get(queued.ID)
	require.NoError(t, err)
	assert.Equal(t, StateActive, j.State, "a drained server starts nothing")
	assert.False(t, srv.Paused(), "a drain is not a pause, so the next start schedules again")

	close(hold)
	waitState(t, srv, busy.ID, StateCompleted)
}
