package jobserver

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// takeAs binds a consumer under name, takes one task and returns it.
func takeAs(t *testing.T, in *queueInbox, name string) (Consumer, *Task) {
	t.Helper()
	c := in.Bind(name)
	require.NotEmpty(t, c)
	got, _, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	return c, got
}

func consumerNameOf(t *testing.T, in *queueInbox, task *Task) string {
	t.Helper()
	d, err := in.Details(task)
	require.NoError(t, err)
	return d.ConsumerName
}

// nameTasks checks that each task keeps the name of the consumer that took it.
func nameTasks(t *testing.T, in *queueInbox) {
	t.Helper()
	first, err := in.AddTask(work(1))
	require.NoError(t, err)
	second, err := in.AddTask(work(2))
	require.NoError(t, err)
	third, err := in.AddTask(work(3))
	require.NoError(t, err)

	alpha, got := takeAs(t, in, "alpha")
	assert.Equal(t, first.ID, got.ID)
	assert.Equal(t, "alpha", consumerNameOf(t, in, got))
	require.NoError(t, in.Done(alpha, got, 10, nil))
	require.NoError(t, in.Unbind(alpha))
	assert.Equal(t, "alpha", consumerNameOf(t, in, first), "the name stays after the consumer is gone")

	_, got = takeAs(t, in, "beta")
	assert.Equal(t, second.ID, got.ID)
	assert.Equal(t, "beta", consumerNameOf(t, in, second))
	assert.Equal(t, "alpha", consumerNameOf(t, in, first))

	_, got = takeAs(t, in, "")
	assert.Equal(t, third.ID, got.ID)
	assert.Empty(t, consumerNameOf(t, in, third), "a bind with no name gives the task no name")
	assert.Equal(t, "beta", consumerNameOf(t, in, second), "a new bind keeps the name of a task the old consumer lost")
	assert.Equal(t, Failed, second.State())

	page, err := in.Tasks(0)
	require.NoError(t, err)
	names := map[string]string{}
	for _, d := range page.Tasks {
		names[d.ID] = d.ConsumerName
	}
	assert.Equal(t, map[string]string{first.ID: "alpha", second.ID: "beta", third.ID: ""}, names)
}

func TestConsumerNameLocally(t *testing.T) {
	srv := newTestServer(t, nil)
	in, err := NewInbox[queueInbox](srv, "q")
	require.NoError(t, err)
	nameTasks(t, in)
}

func TestConsumerNameOverTheClient(t *testing.T) {
	dir := shortDir(t)
	srv := newTestServer(t, func(c *Config) { c.UnixSocket = filepath.Join(dir, "s.sock") })
	client, err := Dial(context.Background(), srv.Config())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	in, err := ClientInbox[queueInbox](client, "remote")
	require.NoError(t, err)
	nameTasks(t, in)
}

func TestConsumerNameSurvivesARestartAndACompaction(t *testing.T) {
	data := t.TempDir()
	sock := filepath.Join(shortDir(t), "s.sock")
	open := func() (*Server, *Client) {
		cfg := DefaultConfig(data)
		cfg.HTTPAddr, cfg.UnixSocket, cfg.IPC = "", sock, false
		srv, err := New(cfg)
		require.NoError(t, err)
		require.NoError(t, srv.Start())
		client, err := Dial(context.Background(), srv.Config())
		require.NoError(t, err)
		return srv, client
	}

	srv, client := open()
	in, err := ClientInbox[queueInbox](client, "q")
	require.NoError(t, err)
	done, err := in.AddTask(work(1))
	require.NoError(t, err)
	held, err := in.AddTask(work(2))
	require.NoError(t, err)
	c, got := takeAs(t, in, "alpha")
	require.Equal(t, done.ID, got.ID)
	require.NoError(t, in.Done(c, got, 10, nil))
	_, got = takeAs(t, in, "beta")
	require.Equal(t, held.ID, got.ID)
	require.NoError(t, client.Close())
	require.NoError(t, srv.Close())

	srv, client = open()
	again, err := ClientInbox[queueInbox](client, "q")
	require.NoError(t, err)
	assert.Equal(t, "alpha", consumerNameOf(t, again, done), "the journal keeps the name across a restart")
	assert.Equal(t, "beta", consumerNameOf(t, again, held), "a task the restart failed keeps its name")
	srv.prune()
	require.NoError(t, client.Close())
	require.NoError(t, srv.Close())

	srv, client = open()
	t.Cleanup(func() { client.Close(); srv.Close() })
	last, err := ClientInbox[queueInbox](client, "q")
	require.NoError(t, err)
	assert.Equal(t, "alpha", consumerNameOf(t, last, done), "a compaction keeps the name")
	assert.Equal(t, "beta", consumerNameOf(t, last, held))
}
