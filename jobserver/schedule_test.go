package jobserver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/priority"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// scheduleServer is a started server whose schedules read clk.
func scheduleServer(t *testing.T) (*Server, *fakeClock, *queueInbox) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	srv := newTunedServer(t, nil, func(s *Server) { s.every.now = clk.now })
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	return srv, clk, in
}

func scheduleInfo(t *testing.T, srv *Server, name string) ScheduleInfo {
	t.Helper()
	for _, s := range srv.Schedules() {
		if s.Name == name {
			return s
		}
	}
	require.FailNow(t, "no schedule", name)
	return ScheduleInfo{}
}

// finish takes and settles every queued task of the inbox.
func finish(t *testing.T, in *queueInbox) {
	t.Helper()
	c := in.Bind()
	for {
		n, err := in.Counts()
		require.NoError(t, err)
		if n.Queued == 0 {
			return
		}
		got, _, ok, err := in.Take(context.Background(), c, 5*time.Second)
		require.NoError(t, err)
		require.True(t, ok, "a queued task was never handed out")
		require.NoError(t, in.Done(c, got, 0, nil))
	}
}

func everyWork(in *queueInbox, name string, every time.Duration) error {
	return in.Every(name, every, func() (*workSpec, priority.MultiPriority, bool) {
		return work(1), in.NewPriority(), true
	})
}

func TestScheduleAddsAtOnceThenEveryInterval(t *testing.T) {
	srv, clk, in := scheduleServer(t)
	require.NoError(t, everyWork(in, "sweep", 5*time.Minute))
	srv.every.fireDue()
	info := scheduleInfo(t, srv, "sweep")
	assert.Equal(t, 1, info.Added, "a new schedule is due at once")
	require.NotEmpty(t, info.LastTask)
	finish(t, in)

	clk.advance(4 * time.Minute)
	srv.every.fireDue()
	assert.Equal(t, 1, scheduleInfo(t, srv, "sweep").Added, "not before the interval")
	clk.advance(time.Minute)
	srv.every.fireDue()
	assert.Equal(t, 2, scheduleInfo(t, srv, "sweep").Added)
}

func TestScheduleSkipsWhileTheLastTaskRuns(t *testing.T) {
	srv, clk, in := scheduleServer(t)
	var skipped []string
	var mu sync.Mutex
	add := func() (*Task, error) { return in.AddTask(work(1)) }
	require.NoError(t, srv.SetSchedule("sweep", Schedule{Every: time.Minute, Add: add, OnSkip: func(name string, prev *Task) {
		mu.Lock()
		defer mu.Unlock()
		skipped = append(skipped, name+" "+prev.ID)
	}}))
	srv.every.fireDue()
	first := scheduleInfo(t, srv, "sweep").LastTask

	clk.advance(time.Minute)
	srv.every.fireDue()
	info := scheduleInfo(t, srv, "sweep")
	assert.Equal(t, 1, info.Added)
	assert.Equal(t, 1, info.Skipped)
	assert.False(t, info.LastSkipped.IsZero())
	mu.Lock()
	assert.Equal(t, []string{"sweep " + first}, skipped)
	mu.Unlock()

	finish(t, in)
	clk.advance(time.Minute)
	srv.every.fireDue()
	assert.Equal(t, 2, scheduleInfo(t, srv, "sweep").Added, "an ended task lets the next tick add")
}

func TestScheduleUpdateKeepsOrResetsTheNextTick(t *testing.T) {
	srv, clk, in := scheduleServer(t)
	require.NoError(t, everyWork(in, "sweep", 5*time.Minute))
	srv.every.fireDue()
	finish(t, in)
	clk.advance(2 * time.Minute)
	require.NoError(t, everyWork(in, "sweep", 5*time.Minute))
	srv.every.fireDue()
	assert.Equal(t, 1, scheduleInfo(t, srv, "sweep").Added, "the same interval keeps the next tick")

	require.NoError(t, everyWork(in, "sweep", 10*time.Minute))
	srv.every.fireDue()
	info := scheduleInfo(t, srv, "sweep")
	assert.Equal(t, 2, info.Added, "a new interval is due at once")
	assert.Equal(t, 10*time.Minute, info.Every)
}

func TestScheduleRemoveAndNonPositiveInterval(t *testing.T) {
	srv, clk, in := scheduleServer(t)
	require.NoError(t, everyWork(in, "a", time.Minute))
	require.NoError(t, everyWork(in, "b", time.Minute))
	srv.every.fireDue()
	assert.True(t, srv.RemoveSchedule("a"))
	assert.False(t, srv.RemoveSchedule("a"))
	require.NoError(t, everyWork(in, "b", 0))
	assert.Empty(t, srv.Schedules())
	finish(t, in)
	clk.advance(time.Hour)
	srv.every.fireDue()
	c, err := in.Counts()
	require.NoError(t, err)
	assert.Zero(t, c.Queued, "a removed schedule adds nothing")
}

func TestScheduleAddsOnceAfterALongPause(t *testing.T) {
	srv, clk, in := scheduleServer(t)
	require.NoError(t, everyWork(in, "sweep", time.Minute))
	srv.every.fireDue()
	finish(t, in)
	clk.advance(time.Hour)
	srv.every.fireDue()
	assert.Equal(t, 2, scheduleInfo(t, srv, "sweep").Added)
}

func TestScheduleDeclineAndError(t *testing.T) {
	srv, _, in := scheduleServer(t)
	require.NoError(t, in.Every("never", time.Minute, func() (*workSpec, priority.MultiPriority, bool) {
		return nil, priority.MultiPriority{}, false
	}))
	require.NoError(t, srv.SetSchedule("broken", Schedule{Every: time.Minute, Add: func() (*Task, error) {
		return nil, errors.New("no hook")
	}}))
	require.Error(t, srv.SetSchedule("empty", Schedule{Every: time.Minute}), "a schedule needs Add")
	srv.every.fireDue()
	never := scheduleInfo(t, srv, "never")
	assert.Zero(t, never.Added)
	assert.Empty(t, never.LastTask)
	assert.Equal(t, "no hook", scheduleInfo(t, srv, "broken").LastError)
}

func TestScheduleShowsInDetailsAndStats(t *testing.T) {
	srv, _, in := scheduleServer(t)
	require.NoError(t, everyWork(in, "sweep", time.Minute))
	srv.every.fireDue()
	id := scheduleInfo(t, srv, "sweep").LastTask
	task, err := in.Lookup(id)
	require.NoError(t, err)
	d, err := in.Details(task)
	require.NoError(t, err)
	assert.Equal(t, "sweep", d.Schedule)
	st := srv.Stats()
	require.Len(t, st.Schedules, 1)
	assert.Equal(t, id, st.Schedules[0].LastTask)
}
