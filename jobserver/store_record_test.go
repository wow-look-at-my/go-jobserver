package jobserver

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// findJob returns the job with the ID from a list. The ID of a record is fixed, so it reads with no lock.
func findJob(jobs []*Job, id string) *Job {
	for _, j := range jobs {
		if j.ID == id {
			return j
		}
	}
	return nil
}

func TestEveryAccessorReturnsTheOneRecordOfATask(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	task, err := in.AddTask(work(1))
	require.NoError(t, err)
	require.NotNil(t, task.rec)

	j, ok := srv.store.Job(task.ID)
	require.True(t, ok)
	assert.Same(t, task.rec, j, "Store.Job and the task handle")
	assert.Same(t, j, findJob(srv.store.InboxJobs("q"), task.ID), "InboxJobs")
	assert.Same(t, j, findJob(srv.List(), task.ID), "Server.List")
	got, err := srv.Get(task.ID)
	require.NoError(t, err)
	assert.Same(t, j, got, "Server.Get")
	looked, err := in.Lookup(task.ID)
	require.NoError(t, err)
	assert.Same(t, j, looked.rec, "Inbox.Lookup")

	c := in.Bind()
	taken, _, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Same(t, j, taken.rec, "Take")
	require.NoError(t, in.Done(c, taken, 7, nil))

	srv.store.mu.RLock()
	_, kept := srv.store.jobs[task.ID]
	srv.store.mu.RUnlock()
	assert.False(t, kept, "memory keeps no strong pointer to an ended task")
	ended, ok := srv.store.Job(task.ID)
	require.True(t, ok)
	assert.Same(t, task.rec, ended, "a read of an ended task that a handle holds returns that record")
	assert.Same(t, task.rec, findJob(srv.store.InboxJobs("q"), task.ID), "InboxJobs of an ended task")
	assert.Same(t, task.rec, findJob(srv.store.Jobs(), task.ID), "Jobs of an ended task")
	assert.Equal(t, Succeeded, task.State())
	r, te := in.Wait(context.Background(), task)
	require.Nil(t, te)
	assert.Equal(t, 7, r)
}

// bigSpec is a spec of about size bytes.
func bigSpec(size int) json.RawMessage {
	b, err := json.Marshal(struct {
		Title string `json:"title"`
		Pad   string `json:"pad"`
	}{"t", strings.Repeat("x", size)})
	if err != nil {
		panic(err)
	}
	return b
}

func TestAReadOfTheTasksCopiesNoSpec(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	const n, size = 1000, 64 << 10
	spec := bigSpec(size)
	for range n {
		_, err := st.create(&Spec{Name: "t"}, &taskMeta{Inbox: "q", Spec: spec})
		require.NoError(t, err)
	}
	require.Len(t, st.Jobs(), n)

	allocs := testing.AllocsPerRun(5, func() { st.Jobs() })
	assert.Less(t, allocs, float64(16*n), "a read allocates a few values per task and copies no task")

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	jobs := st.Jobs()
	runtime.ReadMemStats(&after)
	assert.Len(t, jobs, n)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(n*size/16), "the bytes a read allocates do not grow with the specs")
	for _, id := range []string{jobs[0].ID, jobs[n-1].ID} {
		again, _ := st.Job(id)
		assert.Same(t, findJob(jobs, id), again)
	}
}

func TestMemoryLetsGoOfEndedTasks(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	const n, size = 2000, 64 << 10
	spec := bigSpec(size)
	add := func() {
		for range n {
			j, err := st.create(&Spec{Name: "t"}, &taskMeta{Inbox: "q", Spec: spec})
			require.NoError(t, err)
			require.NoError(t, st.Finish(j.ID, StateCompleted, 0, "", nil, 0))
		}
	}
	add()
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	assert.Less(t, m.HeapAlloc, uint64(n*size/4), "the ended tasks are in the database, not on the heap")
	assert.Eventually(t, func() bool {
		runtime.GC()
		st.weakMu.Lock()
		defer st.weakMu.Unlock()
		return len(st.weak) < n/10
	}, 10*time.Second, 50*time.Millisecond, "a cleanup removes the weak entry of each freed record")
	assert.Equal(t, n, st.Counts("q").Ended)
	jobs := st.InboxJobs("q")
	require.Len(t, jobs, n)
	st.Read(func() { assert.JSONEq(t, string(spec), string(jobs[0].Spec), "an ended task reads from the database") })
}

func TestARestartKeepsTheEndedTasksOfTheRetention(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.NoError(t, err)
	const n = 200
	for i := range n {
		j, err := st.create(&Spec{Name: fmt.Sprintf("t%d", i)}, &taskMeta{Inbox: "q", Spec: []byte(`{"title":"t"}`)})
		require.NoError(t, err)
		require.NoError(t, st.SetResult(j.ID, []byte(fmt.Sprint(i))))
		require.NoError(t, st.Finish(j.ID, StateCompleted, 0, "", nil, 0))
	}
	require.NoError(t, st.Close())

	st, err = OpenStore(dir)
	require.NoError(t, err)
	defer st.Close()
	retention := Retention{Age: 48 * time.Hour, Count: 100000}
	dropped, err := st.Prune(time.Now().Add(47*time.Hour), retention)
	require.NoError(t, err)
	assert.Empty(t, dropped, "a task that ended less than 48 hours ago stays")
	jobs := st.InboxJobs("q")
	require.Len(t, jobs, n)
	st.Read(func() {
		for i, j := range jobs {
			assert.Equal(t, fmt.Sprintf("t%d", i), j.Name)
			assert.Equal(t, StateCompleted, j.State)
			assert.Equal(t, fmt.Sprint(i), string(j.Result))
		}
	})
	dropped, err = st.Prune(time.Now().Add(49*time.Hour), retention)
	require.NoError(t, err)
	assert.Len(t, dropped, n, "a task that ended more than 48 hours ago goes")
	assert.Empty(t, st.InboxJobs("q"))
	assert.Equal(t, 0, st.Counts("q").Ended)
}

// Writers change tasks while readers read them through every view. The toolchain has no race detector, so this test
// proves that the views neither panic nor deadlock, and a hand review covers the data races.
func TestReadsDuringChangesNeitherPanicNorDeadlock(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Retention = time.Hour })
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	c := in.Bind("worker")
	stop := make(chan struct{})
	var tasks sync.Map
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			task, err := in.AddTask(work(i))
			if !assert.NoError(t, err) {
				return
			}
			tasks.Store(task.ID, task)
			got, _, ok, err := in.Take(context.Background(), c, time.Second)
			if !assert.NoError(t, err) || !ok {
				continue
			}
			step, err := in.Step(c, got, 0, "step")
			assert.NoError(t, err)
			w, err := in.OpenWaitInfo(c, got, WaitInfo{Reason: "r", Attrs: map[string]string{"k": "v"}})
			assert.NoError(t, err)
			assert.NoError(t, in.SetTitle(c, got, fmt.Sprintf("title %d", i)))
			assert.NoError(t, in.CloseWait(c, got, w))
			assert.NoError(t, in.EndStep(c, got, step, StepSucceeded, ""))
			assert.NoError(t, in.Done(c, got, i, nil))
		}
	})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				jobs := srv.List()
				srv.Read(func() {
					for _, j := range jobs {
						_ = j.State
						_ = len(j.Steps) + len(j.Waits)
						_ = j.Name
					}
				})
				_ = srv.Graph()
				_ = srv.MonitorViews()
				_ = srv.Explain()
				_ = srv.Stats()
				_, _ = in.Tasks(0)
				tasks.Range(func(_, v any) bool {
					task := v.(*Task)
					_ = task.State()
					_ = in.Spec(task)
					_, _ = in.Details(task)
					_, _, _ = in.TryResult(task)
					return true
				})
			}
		})
	}
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()
	n := 0
	tasks.Range(func(_, v any) bool {
		n++
		r, te, done := in.TryResult(v.(*Task))
		if done && te == nil {
			assert.Equal(t, in.Spec(v.(*Task)).N, r)
		}
		return true
	})
	assert.Positive(t, n)
}
