package jobserver

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fullQueue(b *testing.B) (*Scheduler, *scriptedHost) { return fullQueueOf(b, 0) }

// fullQueueOf fills the queue with waiters spread over batches; zero batches leaves batch unset.
func fullQueueOf(b *testing.B, batches int) (*Scheduler, *scriptedHost) {
	keys := []PriorityKey{{Name: "infra-priority", Default: 3}, {Name: "hook-priority"}, {Name: KeyOpenBatch, Default: 1}}
	s := NewScheduler(SchedulerConfig{CPUBudget: 16, DisableCPU: true, PriorityKeys: keys})
	b.Cleanup(s.Close)
	clock := &testClock{at: time.Now()}
	s.now = clock.now
	host := &scriptedHost{cpus: 16}
	s.gov.sampler.Host = hostOf(host)
	host.setRunning(10)
	s.gov.tick()
	for i := range 16 {
		_, ok, err := s.TryAcquire(LeaseRequest{ID: fmt.Sprintf("h%d", i), Policy: JobPolicy{Cost: 0.25}})
		require.NoError(b, err)
		require.True(b, ok, "holder %d: %+v", i, s.Status().Waiting)

	}
	clock.add(hostLoadWindow * time.Second)
	host.setRunning(18)
	for range hostLoadWindow {
		s.gov.tick()
	}
	for i := range 2800 {
		req := LeaseRequest{ID: fmt.Sprintf("w%d", i), Policy: JobPolicy{Cost: 0.25}}
		if batches > 0 {
			req.Batch = fmt.Sprintf("b%d", i%batches)
		}
		go func() { _, _ = s.Acquire(b.Context(), req, func(QueueState) {}) }()
	}
	for {
		s.mu.Lock()
		n := len(s.waiting)
		s.mu.Unlock()
		if n == 2800 {
			return s, host
		}
	}
}

func BenchmarkPlanPassOverAFullQueue(b *testing.B) {
	s, _ := fullQueue(b)
	b.ResetTimer()
	for b.Loop() {
		s.mu.Lock()
		s.planLocked()
		s.mu.Unlock()
	}
}

func BenchmarkPlanPassOverAFullBatchedQueue(b *testing.B) {
	s, _ := fullQueueOf(b, 100)
	b.ResetTimer()
	for b.Loop() {
		s.mu.Lock()
		s.planLocked()
		s.mu.Unlock()
	}
}

func BenchmarkStatusOfAFullQueue(b *testing.B) {
	s, _ := fullQueue(b)
	b.ResetTimer()
	for b.Loop() {
		_ = s.Status()
	}
}

func BenchmarkHostSampleOverAFullQueue(b *testing.B) {
	s, host := fullQueue(b)
	n := 18
	b.ResetTimer()
	for b.Loop() {
		n = 18 + (n-17)%3
		host.setRunning(n)
		s.gov.tick()
	}
}
