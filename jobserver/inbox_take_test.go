package jobserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// takeAll takes n tasks and settles each at once.
func takeAll(t *testing.T, in *queueInbox, c Consumer, n int) []int {
	t.Helper()
	var order []int
	for range n {
		task, spec, ok, err := in.Take(context.Background(), c, 5*time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		order = append(order, spec.N)
		require.NoError(t, in.Done(c, task, 0, nil))
	}
	return order
}

func TestTakeOrdersABurstByPriorityThenArrival(t *testing.T) {
	for _, limit := range []int{1, 16} {
		srv := newTestServer(t, func(c *Config) { c.MaxConcurrent = limit })
		in, err := NewInbox[queueInbox](srv, "q")
		require.NoError(t, err)
		c := in.Bind()
		urgency := []int{10, 5, 10, 1, 5, 10, 1}
		for n, urgent := range urgency {
			p := in.NewPriority()
			p.Set(&in.Urgent, urgent)
			_, err := in.AddTask(work(n), p)
			require.NoError(t, err)
		}
		// Take runs at once, while admission of the burst is still under way.
		assert.Equal(t, []int{3, 6, 1, 4, 0, 2, 5}, takeAll(t, in, c, len(urgency)), "a cap of %d", limit)
	}
}

func TestTakeWaitsForTheBestTaskWhenALowerOneHoldsTheSlot(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.MaxConcurrent = 1 })
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	core := in.b.(localNamed).c
	c := in.Bind()
	_, err = in.AddTask(work(1))
	require.NoError(t, err)
	// The lower task takes the only slot before the better one arrives.
	require.Eventually(t, func() bool {
		core.mu.Lock()
		defer core.mu.Unlock()
		return len(core.admitted) == 1
	}, 5*time.Second, 5*time.Millisecond)
	p := in.NewPriority()
	p.Set(&in.Urgent, 1)
	_, err = in.AddTask(work(2), p)
	require.NoError(t, err)
	assert.Equal(t, []int{2, 1}, takeAll(t, in, c, 2), "the better task goes first, and the lower one gives back its slot")
}

func TestTakeWithNoWaitReturnsNothingWhileTheBestTaskWaits(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.MaxConcurrent = 1 })
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	hold, err := srv.Scheduler().Acquire(context.Background(), LeaseRequest{ID: "hold"}, nil)
	require.NoError(t, err)
	c := in.Bind()
	_, err = in.AddTask(work(1))
	require.NoError(t, err)
	_, _, ok, err := in.Take(context.Background(), c, 0)
	require.NoError(t, err)
	assert.False(t, ok)
	hold.Release()
	assert.Equal(t, []int{1}, takeAll(t, in, c, 1))
}

func TestLookupFindsATaskOfItsOwnInbox(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Runner = &fakeRunner{} })
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	other, err := NewInbox[queueInbox](srv, "other")
	require.NoError(t, err)
	task, err := in.AddTask(work(1))
	require.NoError(t, err)

	got, err := in.Lookup(task.ID)
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, Queued, got.State())

	_, err = other.Lookup(task.ID)
	assert.ErrorIs(t, err, ErrNotFound, "a task of another inbox is not found")
	_, err = in.Lookup("j-nope")
	assert.ErrorIs(t, err, ErrNotFound)

	def, err := srv.DefaultInbox().AddTask(&CommandSpec{Command: []string{"x"}, Draft: true})
	require.NoError(t, err)
	_, err = srv.DefaultInbox().Lookup(def.ID)
	require.NoError(t, err)
	_, err = in.Lookup(def.ID)
	assert.ErrorIs(t, err, ErrNotFound)
}
