package jobserver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A wait carries its kind, key, bound and attrs. UpdateWait changes them and keeps the ID and the start.
func TestStructuredWaitsOpenAndUpdate(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "w")
	require.NoError(t, err)
	_, err = in.AddTask(work(1))
	require.NoError(t, err)
	c := in.Bind()
	got, _, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	until := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	w, err := in.OpenWaitInfo(c, got, WaitInfo{Reason: "lock k", Kind: "lock", Key: "k", Until: until,
		Attrs: map[string]string{"holder_run_id": "r1"}})
	require.NoError(t, err)
	d, err := in.Details(got)
	require.NoError(t, err)
	require.Len(t, d.Waits, 1)
	first := d.Waits[0]
	assert.Equal(t, "lock", first.Kind)
	assert.Equal(t, "k", first.Key)
	assert.True(t, until.Equal(first.Until))
	assert.Equal(t, map[string]string{"holder_run_id": "r1"}, first.Attrs)
	assert.False(t, first.Queue)

	require.NoError(t, in.UpdateWait(c, got, w, WaitInfo{Reason: "lock k", Kind: "lock", Key: "k", Until: until,
		Attrs: map[string]string{"holder_run_id": "r2"}}))
	d, err = in.Details(got)
	require.NoError(t, err)
	require.Len(t, d.Waits, 1, "an update changes the wait, and opens none")
	assert.Equal(t, w, d.Waits[0].ID)
	assert.True(t, first.Start.Equal(d.Waits[0].Start), "an update keeps the start")
	assert.Equal(t, "r2", d.Waits[0].Attrs["holder_run_id"])

	require.ErrorIs(t, in.UpdateWait("c-stale", got, w, WaitInfo{}), ErrNotConsumer)
	require.ErrorIs(t, in.UpdateWait(c, got, 99, WaitInfo{}), ErrNoStep)
	too := map[string]string{}
	for i := range maxWaitAttrs + 1 {
		too[strings.Repeat("k", i+1)] = "v"
	}
	require.ErrorIs(t, in.UpdateWait(c, got, w, WaitInfo{Attrs: too}), ErrBadState)
	_, err = in.OpenWaitInfo(c, got, WaitInfo{Attrs: map[string]string{"": "v"}})
	require.ErrorIs(t, err, ErrBadState)
	require.NoError(t, in.UpdateWait(c, got, w, WaitInfo{Attrs: map[string]string{"long": strings.Repeat("x", 500)}}))
	d, err = in.Details(got)
	require.NoError(t, err)
	assert.Len(t, d.Waits[0].Attrs["long"], maxTaskTitleLen, "a long attr value is clipped")

	require.NoError(t, in.CloseWait(c, got, w))
	require.ErrorIs(t, in.UpdateWait(c, got, w, WaitInfo{Reason: "late"}), ErrNoStep, "a closed wait does not change")
}

// The journal keeps every field of a wait, and a change to it, across a restart.
func TestStructuredWaitsSurviveARestart(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	id := addQueued(t, st, "n")
	require.NoError(t, st.Start(id))
	until := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	w, err := st.OpenWaitInfo(id, WaitInfo{Reason: "sleep", Kind: "wait", Until: until})
	require.NoError(t, err)
	require.NoError(t, st.UpdateWait(id, w, WaitInfo{Reason: "sleep more", Kind: "wait", Key: "k", Until: until,
		Attrs: map[string]string{"a": "b"}}))
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	j, _ := st.Job(id)
	require.Len(t, j.Waits, 1)
	got := j.Waits[0]
	assert.Equal(t, "sleep more", got.Reason)
	assert.Equal(t, "wait", got.Kind)
	assert.Equal(t, "k", got.Key)
	assert.True(t, until.Equal(got.Until))
	assert.Equal(t, map[string]string{"a": "b"}, got.Attrs)
	assert.True(t, got.End.IsZero())
}

// QueueWait goes on while the kind and the key stay, starts a new wait when they change, and ends at a zero state.
func TestQueueWaitFollowsTheQueueState(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	id := addQueued(t, st, "n")

	require.NoError(t, st.QueueWait(id, QueueState{Kind: WaitGroup, Key: "g", Reason: "group g: 1 of 1 running"}))
	require.NoError(t, st.QueueWait(id, QueueState{Kind: WaitGroup, Key: "g", Position: 2, Reason: "group g: 1 of 1 running"}))
	require.NoError(t, st.QueueWait(id, QueueState{Kind: WaitGroup, Key: "g", Reason: "group g: 2 of 2 running"}))
	j, _ := st.Job(id)
	require.Len(t, j.Waits, 1, "one wait while the kind and the key stay")
	assert.Equal(t, "group g: 2 of 2 running", j.Waits[0].Reason)
	assert.True(t, j.Waits[0].Queue)

	require.NoError(t, st.QueueWait(id, QueueState{Kind: WaitCap, Key: WaitCap, Reason: "run cap"}))
	j, _ = st.Job(id)
	require.Len(t, j.Waits, 2)
	assert.False(t, j.Waits[0].End.IsZero(), "a new kind ends the wait")
	assert.Equal(t, WaitCap, j.Waits[1].Kind)
	assert.True(t, j.Waits[1].End.IsZero())
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	j, _ = st.Job(id)
	require.Len(t, j.Waits, 2)
	assert.True(t, j.Waits[1].End.IsZero(), "a queued task still shows its wait after a restart")
	require.NoError(t, st.QueueWait(id, QueueState{Kind: WaitCap, Key: WaitCap, Reason: "run cap"}))
	j, _ = st.Job(id)
	assert.Len(t, j.Waits, 2, "the restarted scheduler goes on with the same wait")

	require.NoError(t, st.QueueWait(id, QueueState{}))
	j, _ = st.Job(id)
	require.Len(t, j.Waits, 2)
	assert.False(t, j.Waits[1].End.IsZero(), "admission ends the wait")
	require.NoError(t, st.QueueWait(id, QueueState{}), "a zero state with no open wait does nothing")
}

// A task that waits for its group shows the wait in its details, and the wait ends when the task is admitted.
func TestTheSchedulerRecordsQueueWaits(t *testing.T) {
	srv := newTestServer(t, nil)
	srv.Scheduler().SetGroups(map[string]int{"g": 1})
	in, err := NewInbox[groupInbox](srv, "grouped")
	require.NoError(t, err)
	in.hold, in.heard = make(chan struct{}), map[string][]QueueState{}

	first, err := in.AddTask(work(1))
	require.NoError(t, err)
	waitState(t, srv, first.ID, StateRunning)
	second, err := in.AddTask(work(2))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, err := in.Details(second)
		return err == nil && len(d.Waits) == 1
	}, 5*time.Second, 5*time.Millisecond, "the queued task holds a wait")
	d, err := in.Details(second)
	require.NoError(t, err)
	w := d.Waits[0]
	assert.True(t, w.Queue)
	assert.Equal(t, WaitGroup, w.Kind)
	assert.Equal(t, "g", w.Key)
	assert.Equal(t, "group g: 1 of 1 running", w.Reason)
	assert.True(t, w.End.IsZero())
	require.NotNil(t, d.Queue)
	assert.Equal(t, []string{first.ID}, d.Queue.Holders)
	d, err = in.Details(first)
	require.NoError(t, err)
	assert.Empty(t, d.Waits, "a task admitted at once never waited")

	close(in.hold)
	require.Nil(t, waitTask(t, second))
	d, err = in.Details(second)
	require.NoError(t, err)
	require.Len(t, d.Waits, 1)
	assert.False(t, d.Waits[0].End.IsZero(), "admission ends the queue wait")
}
