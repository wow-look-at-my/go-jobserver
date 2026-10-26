package jobserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// OnEnd hears a job that never ran: one blocked behind a failed dependency, and one cancelled before it started.
func TestOnEndHearsJobsThatNeverRan(t *testing.T) {
	ends := make(chan *Job, 8)
	srv := newTestServer(t, func(c *Config) { c.OnEnd = func(j *Job) { ends <- j } })
	failing, _, err := srv.Enqueue(Spec{ID: "failing", Command: []string{"false"}, Draft: true})
	require.NoError(t, err)
	behind, _, err := srv.Enqueue(Spec{ID: "behind", Command: []string{"x"}, Deps: []string{failing.ID}})
	require.NoError(t, err)
	draft, _, err := srv.Enqueue(Spec{ID: "draft", Command: []string{"x"}, Draft: true})
	require.NoError(t, err)

	require.NoError(t, srv.Interrupt(draft.ID))
	require.NoError(t, srv.Interrupt(failing.ID))
	seen := map[string]State{}
	deadline := time.After(10 * time.Second)
	for len(seen) < 3 {
		select {
		case j := <-ends:
			seen[j.ID] = j.State
		case <-deadline:
			t.Fatalf("OnEnd heard only %v", seen)
		}
	}
	assert.Equal(t, StateCancelled, seen[draft.ID])
	assert.Equal(t, StateCancelled, seen[failing.ID])
	assert.Equal(t, StateBlocked, seen[behind.ID])
}

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
