package jobserver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBroke = errors.New("broke")

// A held task keeps reading after a prune drops it from the store.
func TestHeldTaskReadsAfterPrune(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Retention = time.Nanosecond })
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	ok1, err := in.AddTask(work(1))
	require.NoError(t, err)
	bad, err := in.AddTask(work(2))
	require.NoError(t, err)
	c := in.Bind("worker")
	got, _, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, in.SetTitle(c, got, "renamed"))
	require.NoError(t, in.Done(c, got, 7, nil))
	got, _, ok, err = in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, in.Done(c, got, 0, errBroke))

	time.Sleep(time.Millisecond)
	srv.prune()
	_, kept := srv.store.Job(ok1.ID)
	assert.False(t, kept, "the prune drops the task from the store")
	_, err = srv.Task(ok1.ID)
	require.ErrorIs(t, err, ErrNotFound, "a lookup by ID no longer finds it")
	page, err := in.Tasks(0)
	require.NoError(t, err)
	assert.Empty(t, page.Tasks, "the listing no longer shows it")

	assert.Equal(t, Succeeded, ok1.State())
	d, err := in.Details(ok1)
	require.NoError(t, err)
	assert.Equal(t, "renamed", d.Title)
	assert.Equal(t, "worker", d.ConsumerName)
	assert.Equal(t, 1, in.Spec(ok1).N)
	r, te, done := in.TryResult(ok1)
	require.True(t, done)
	require.Nil(t, te)
	assert.Equal(t, 7, r)
	r, te = in.Wait(context.Background(), ok1)
	require.Nil(t, te)
	assert.Equal(t, 7, r)

	assert.Equal(t, Failed, bad.State())
	te = bad.Wait(context.Background())
	require.NotNil(t, te)
	assert.ErrorIs(t, te, errBroke, "the held task keeps the error it failed with")
}

// A task that runs while a reader reads it through its record must not race.
func TestTaskRecordReadsAreRaceFree(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	task, err := in.AddTask(work(1))
	require.NoError(t, err)
	c := in.Bind()
	got, _, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = in.Details(task)
			_ = task.State()
		}
	})
	for i := range 50 {
		step, err := in.Step(c, got, 0, "step")
		require.NoError(t, err)
		require.NoError(t, in.EndStep(c, got, step, StepSucceeded, ""))
		require.NoError(t, in.SetTitle(c, got, "title"+string(rune('a'+i%26))))
	}
	require.NoError(t, in.Done(c, got, 1, nil))
	close(stop)
	wg.Wait()
	r, te := in.Wait(context.Background(), task)
	require.Nil(t, te)
	assert.Equal(t, 1, r)
}
