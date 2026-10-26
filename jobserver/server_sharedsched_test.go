package jobserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A scheduler the host passes in admits the server's tasks beside the host's own leases, and the server's Close
// leaves it open for its owner.
func TestASharedSchedulerAdmitsTasksBesideTheHostsLeases(t *testing.T) {
	sched := NewScheduler(SchedulerConfig{DisableCPU: true, Groups: map[string]int{"g": 1}})
	t.Cleanup(sched.Close)
	host, err := sched.Acquire(context.Background(), LeaseRequest{ID: "host", Group: "g"}, nil)
	require.NoError(t, err)

	srv := newTestServer(t, func(c *Config) { c.Scheduler = sched })
	assert.Same(t, sched, srv.Scheduler())
	in, err := NewInbox[groupInbox](srv, "grouped")
	require.NoError(t, err)
	in.hold, in.heard = make(chan struct{}), map[string][]QueueState{}
	close(in.hold)

	task, err := in.AddTask(work(1))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		q, _ := in.last(task.ID)
		return q.Kind == WaitGroup
	}, 5*time.Second, 5*time.Millisecond, "the task waits for the group the host's lease holds")
	q, _ := in.last(task.ID)
	assert.Equal(t, []string{"host"}, q.Holders)

	host.Release()
	require.Nil(t, waitTask(t, task))

	require.NoError(t, srv.Close())
	l, ok, err := sched.TryAcquire(LeaseRequest{ID: "after", Group: "g"})
	require.NoError(t, err)
	require.True(t, ok, "the scheduler still admits after the server closed")
	l.Release()
}

// leaseInbox answers the ID of the lease its Run sees.
type leaseInbox struct {
	Inbox[*workSpec, string]
}

func (i *leaseInbox) Run(ctx context.Context, s *workSpec) (string, error) {
	if l := LeaseFromContext(ctx); l != nil {
		return l.ID(), nil
	}
	return "", nil
}

// A Run method reaches the lease it runs under, and the lease ID is the task ID.
func TestARunReachesItsLease(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[leaseInbox](srv, "leased")
	require.NoError(t, err)
	task, err := in.AddTask(work(1))
	require.NoError(t, err)
	id, terr := in.Wait(context.Background(), task)
	require.Nil(t, terr)
	assert.Equal(t, task.ID, id)
	assert.Nil(t, LeaseFromContext(context.Background()))
}
