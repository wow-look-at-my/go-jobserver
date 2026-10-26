package jobserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestThePruneOfACachedJobKeepsTheCache(t *testing.T) {
	dir := t.TempDir()
	work := t.TempDir()
	runner := &fakeRunner{writes: map[string]string{"out.bin": "payload"}}
	open := func() *Server {
		cfg := DefaultConfig(dir)
		cfg.HTTPAddr, cfg.UnixSocket, cfg.IPC = "", "", false
		cfg.Runner = runner
		srv, err := New(cfg)
		require.NoError(t, err)
		require.NoError(t, srv.Start())
		return srv
	}
	srv := open()
	spec := Spec{Command: []string{"build"}, Dir: work, Outputs: []string{"out.bin"}}
	first, _, err := srv.Enqueue(spec)
	require.NoError(t, err)
	done := waitState(t, srv, first.ID, StateCompleted)
	require.Len(t, done.Artifacts, 1)

	dropped, err := srv.store.Prune(time.Now().Add(2*time.Hour), keep(time.Hour, 1000))
	require.NoError(t, err)
	require.Equal(t, []string{first.ID}, dropped)
	_, err = srv.Get(first.ID)
	require.ErrorIs(t, err, ErrNotFound)

	second, _, err := srv.Enqueue(spec)
	require.NoError(t, err)
	cached := waitState(t, srv, second.ID, StateCached)
	assert.Equal(t, first.ID, cached.CacheOf)
	assert.Equal(t, done.Artifacts, cached.Artifacts, "the cache keeps the artifacts of the pruned job")
	assert.Equal(t, 1, runner.count(), "the identical job does not run")

	_, err = srv.store.Prune(time.Now().Add(4*time.Hour), keep(time.Hour, 1000))
	require.NoError(t, err)
	require.NoError(t, srv.Close())

	srv = open()
	t.Cleanup(func() { srv.Close() })
	third, _, err := srv.Enqueue(spec)
	require.NoError(t, err)
	waitState(t, srv, third.ID, StateCached)
	assert.Equal(t, 1, runner.count(), "the compacted journal keeps the cache across a restart")
}

func TestTheCacheHasItsOwnBound(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	var ids []string
	for _, identity := range []string{"x", "y", "z"} {
		j, err := st.Create(&Spec{Command: []string{"true"}})
		require.NoError(t, err)
		require.NoError(t, st.SetIdentity(j.ID, identity, ""))
		require.NoError(t, st.Finish(j.ID, StateCompleted, 0, "", nil, 0))
		ids = append(ids, j.ID)
		time.Sleep(time.Millisecond)
	}
	_, err = st.Prune(time.Now(), Retention{Age: time.Hour, Count: 1000, CacheCount: 2})
	require.NoError(t, err)
	assert.Equal(t, IdentityIndex{"y": ids[1], "z": ids[2]}, st.IdentityIndex(), "the oldest entry goes")
	_, err = st.Prune(time.Now().Add(2*time.Hour), Retention{Age: time.Hour, Count: 1000, CacheAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, st.IdentityIndex())
	assert.Empty(t, st.Order())
}
