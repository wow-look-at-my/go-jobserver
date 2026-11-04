package jobserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Work that books the CPUs full holds back the next ordinary lease. An urgent lease books past that limit by
// urgentHeadroom, and no further.
func TestAnUrgentLeaseBooksPastFullCPUsWithinItsHeadroom(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 4, DefaultCost: 1})
	_, ok, err := s.TryAcquire(LeaseRequest{ID: "full", Policy: JobPolicy{Cost: bookable(4)}})
	require.NoError(t, err)
	require.True(t, ok)

	cases := []struct {
		name   string
		req    LeaseRequest
		starts bool
	}{
		{"an ordinary lease waits on the full CPUs", LeaseRequest{Policy: JobPolicy{Cost: 1}}, false},
		{"an urgent lease starts in the headroom", LeaseRequest{Urgent: true, Policy: JobPolicy{Cost: 1}}, true},
		{"an urgent lease past the headroom waits", LeaseRequest{Urgent: true, Policy: JobPolicy{Cost: urgentHeadroom(4) + 1}}, false},
	}
	for _, c := range cases {
		a, err := s.Admits(c.req)
		require.NoError(t, err, c.name)
		assert.Equal(t, c.starts, a.Starts, c.name)
		if !c.starts {
			require.NotNil(t, a.Wait, c.name)
			assert.Equal(t, WaitCPU, a.Wait.Kind, c.name)
		}
	}
}

// An urgent lease goes ahead of every queued lease that is not urgent, whatever their priority keys say.
func TestAnUrgentLeaseGoesAheadOfTheQueue(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1, PriorityKeys: testKeys})
	first, err := s.Acquire(context.Background(), LeaseRequest{ID: "first"}, nil)
	require.NoError(t, err)

	ordinary := acquireAsync(s, context.Background(), LeaseRequest{ID: "ordinary", Priority: map[string]float64{"urgency": -100}}, nil)
	waitQueued(t, s, 1)
	urgent := acquireAsync(s, context.Background(), LeaseRequest{ID: "urgent", Urgent: true, Priority: map[string]float64{"urgency": 100}}, nil)
	waitQueued(t, s, 2)
	assert.True(t, s.Status().Waiting[0].Urgent, "the status lists the urgent lease first")

	first.Release()
	got := <-urgent
	require.NotNil(t, got)
	assert.Equal(t, "urgent", got.ID())
	got.Release()
	next := <-ordinary
	require.NotNil(t, next)
	next.Release()
}
