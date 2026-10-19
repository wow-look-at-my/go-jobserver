package jobserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdmitsAnswersWhatTryAcquireDoes(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 3, CPUBudget: 4, DefaultCost: 1})
	s.SetGroups(map[string]int{"one": 1, "wide": 5})
	_, ok, err := s.TryAcquire(LeaseRequest{ID: "a", Group: "one", Policy: JobPolicy{Cost: 3}})
	require.NoError(t, err)
	require.True(t, ok)

	cases := []struct {
		name string
		req  LeaseRequest
		kind string
	}{
		{"its group is full", LeaseRequest{Group: "one"}, WaitGroup},
		{"it fits", LeaseRequest{Group: "wide"}, ""},
		{"it is over the booking limit", LeaseRequest{Group: "wide", Policy: JobPolicy{Cost: 4}}, WaitCPU},
		{"a group-only lease skips the CPU gates", LeaseRequest{Group: "wide", GroupOnly: true, Policy: JobPolicy{Cost: 40}}, ""},
	}
	for _, c := range cases {
		a, err := s.Admits(c.req)
		require.NoError(t, err, c.name)
		if c.kind == "" {
			assert.True(t, a.Starts, c.name)
			assert.Nil(t, a.Wait, c.name)
		} else {
			require.False(t, a.Starts, c.name)
			assert.Equal(t, c.kind, a.Wait.Kind, c.name)
		}
		l, got, err := s.TryAcquire(c.req)
		require.NoError(t, err, c.name)
		assert.Equal(t, a.Starts, got, "%s: Admits and TryAcquire must agree", c.name)
		if got {
			// Hand the slot back so the next case sees the same holders.
			l.Release()
		}
	}
}

func TestAdmitsReportsTheCostItWouldAdmitAt(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{CPUBudget: 8, DefaultCost: 0.25})
	a, err := s.Admits(LeaseRequest{Class: "never-ran"})
	require.NoError(t, err)
	assert.Equal(t, 0.25, a.Cost, "no declared cost and no history: the default")
	a, err = s.Admits(LeaseRequest{Class: "never-ran", Policy: JobPolicy{Cost: 2}})
	require.NoError(t, err)
	assert.Equal(t, 2.0, a.Cost, "a declared cost wins")
}

func TestAdmitsRefusesAnUnknownGroup(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{})
	_, err := s.Admits(LeaseRequest{Group: "nope"})
	assert.ErrorIs(t, err, ErrUnknownGroup)
}

func TestAdmitsChangesNothing(t *testing.T) {
	s := newTestScheduler(t, SchedulerConfig{MaxConcurrent: 1})
	_, err := s.Admits(LeaseRequest{ID: "x"})
	require.NoError(t, err)
	st := s.Status()
	assert.Empty(t, st.Holding)
	assert.Empty(t, st.Waiting)
}
