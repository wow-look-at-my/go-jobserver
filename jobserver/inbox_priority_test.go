package jobserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/priority"
)

// rankedInbox runs one task at a time through group "g" and records the order the tasks ran in.
type rankedInbox struct {
	Inbox[*workSpec, int]
	Urgency priority.MultiPriorityKey `priority:"urgency,default=5"`
	hold    chan struct{}
	mu      sync.Mutex
	ran     []int
}

func (i *rankedInbox) Run(ctx context.Context, s *workSpec) (int, error) {
	i.mu.Lock()
	i.ran = append(i.ran, s.N)
	i.mu.Unlock()
	if s.N == 0 {
		<-i.hold
	}
	return s.N, nil
}

func (i *rankedInbox) LeaseFor(s *workSpec) LeaseRequest { return LeaseRequest{Group: "g"} }

func (i *rankedInbox) order() []int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]int(nil), i.ran...)
}

// A task that waits for admission goes back in line at its new priority. A task that waits on its trigger gets the
// new priority before it is offered. A task that runs keeps its own and the call fails.
func TestSetPriorityMovesATaskInLine(t *testing.T) {
	srv := newTestServer(t, nil)
	srv.Scheduler().SetGroups(map[string]int{"g": 1})
	in, err := NewInbox[rankedInbox](srv, "ranked")
	require.NoError(t, err)
	in.hold = make(chan struct{})

	first, err := in.AddTask(work(0))
	require.NoError(t, err)
	waitState(t, srv, first.ID, StateRunning)
	b, err := in.AddTask(work(1))
	require.NoError(t, err)
	c, err := in.AddTask(work(2))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ws, _ := srv.Why(c.ID)
		return len(ws) == 1 && ws[0].Kind == WaitGroup
	}, 5*time.Second, 5*time.Millisecond)

	p := in.NewPriority()
	p.Set(&in.Urgency, 0)
	require.NoError(t, in.SetPriority(c, p))
	j, err := srv.Get(c.ID)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"urgency": 0}, j.Priority)
	assert.ErrorIs(t, in.SetPriority(first, p), ErrBadState, "a running task keeps its priority")

	close(in.hold)
	require.Nil(t, waitTask(t, b))
	require.Nil(t, waitTask(t, c))
	assert.Equal(t, []int{0, 2, 1}, in.order(), "the task moved up runs before the one added ahead of it")
}

// The new priority is in the journal, so a restart keeps it.
func TestSetPriorityIsJournaled(t *testing.T) {
	dir := shortDir(t)
	srv := newTestServer(t, func(c *Config) { c.Dir = dir })
	gate, err := NewInbox[rankedInbox](srv, "ranked")
	require.NoError(t, err)
	gate.hold = make(chan struct{})
	close(gate.hold)
	trigger, err := srv.DefaultInbox().AddTask(&CommandSpec{TaskSpec: TaskSpec{Title: "never"}, Command: []string{"false"}, Draft: true})
	require.NoError(t, err)
	task, err := OnSuccess(trigger, &gate.Inbox, work(3))
	require.NoError(t, err)
	p := gate.NewPriority()
	p.Set(&gate.Urgency, 1)
	require.NoError(t, gate.SetPriority(task, p))
	require.NoError(t, srv.Close())

	again := newTestServer(t, func(c *Config) { c.Dir = dir })
	j, err := again.Get(task.ID)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"urgency": 1}, j.Priority)
}
