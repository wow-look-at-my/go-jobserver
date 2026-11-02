package jobserver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// One host sample on a full queue: the scheduler hands every waiter a reason with the new host count.
func BenchmarkHostSampleQueueWaitsOfAFullQueue(b *testing.B) {
	st, err := OpenStore(b.TempDir())
	require.NoError(b, err)
	b.Cleanup(func() { st.Close() })
	ids := make([]string, 2800)
	for i := range ids {
		j, err := st.create(&Spec{Name: "q"}, &taskMeta{Inbox: "deliveries", Spec: []byte(`{"title":"q"}`)})
		require.NoError(b, err)
		ids[i] = j.ID
	}
	sample := func(n int) {
		reason := fmt.Sprintf("host load: %d tasks want a CPU on 16 CPUs, this run needs 0.25", n)
		for _, id := range ids {
			require.NoError(b, st.QueueWait(id, QueueState{Kind: WaitCPU, Key: WaitCPU, Reason: reason}))
		}
	}
	sample(14)
	n := 14
	b.ResetTimer()
	for b.Loop() {
		n = 14 + (n-13)%4
		sample(n)
	}
}
