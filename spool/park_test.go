package spool

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func TestParkRoundTrip(t *testing.T) {
	srv := newServer(t)
	in, err := jobserver.NewInbox[deliveryInbox](srv, "hooks")
	require.NoError(t, err)
	dir := t.TempDir()
	s := open(t, dir, in)
	name, err := s.Park(&deliverySpec{
		TaskSpec: jobserver.TaskSpec{Title: "t"},
		Hook:     "pr-minder",
		Headers:  map[string][]string{"X-Github-Event": {"pull_request"}},
		Body:     []byte(`{"action":"opened"}`),
	})
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(name, ".json"))

	c := in.Bind()
	task, spec, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "t", spec.Title)
	assert.Equal(t, "pr-minder", spec.Hook)
	assert.Equal(t, []byte(`{"action":"opened"}`), spec.Body)
	assert.Equal(t, []string{"pull_request"}, spec.Headers["X-Github-Event"],
		"headers survive, so the task cannot tell a replay from the live delivery")
	require.NoError(t, in.Done(c, task, "ok", nil))
	eventuallyExists(t, filepath.Join(dir, DoneDir, name))
}

func TestParkAfterCloseIsReplayedByTheNextOpen(t *testing.T) {
	srv := newServer(t)
	in, err := jobserver.NewInbox[deliveryInbox](srv, "hooks")
	require.NoError(t, err)
	dir := t.TempDir()
	s := open(t, dir, in)
	require.NoError(t, s.Close())
	name, err := s.Park(deliverySpec{Hook: "late"})
	require.NoError(t, err, "a spool takes a park during shutdown, and a value of the spec type is accepted")
	n, err := s.Count()
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	open(t, dir, in)
	c := in.Bind()
	task, spec, ok, err := in.Take(context.Background(), c, 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "late", spec.Hook)
	require.NoError(t, in.Done(c, task, "ok", nil))
	eventuallyExists(t, filepath.Join(dir, DoneDir, name))
}

func TestParkNamesSortByArrival(t *testing.T) {
	dir := t.TempDir()
	s := idle(t, dir, &stoppedInbox{}, &lineLog{})
	var names []string
	for range 5 {
		name, err := s.Park(&jobserver.CommandSpec{Command: []string{"true"}})
		require.NoError(t, err)
		names = append(names, name)
	}
	list, err := s.entries()
	require.NoError(t, err)
	var got []string
	for _, e := range list {
		got = append(got, e.name)
	}
	assert.Equal(t, names, got, "the replay order is the park order")
}

func TestParkRefusesAnotherSpecType(t *testing.T) {
	s := idle(t, t.TempDir(), &stoppedInbox{}, &lineLog{})
	_, err := s.Park("echo hi")
	assert.Error(t, err)
	_, err = s.Park(&deliverySpec{})
	assert.Error(t, err)
}

func TestParkRefusesPastTheEntryBound(t *testing.T) {
	s := idle(t, t.TempDir(), &stoppedInbox{}, &lineLog{})
	s.SetBounds(2, 0)
	for range 2 {
		_, err := s.Park(&jobserver.CommandSpec{Command: []string{"true"}})
		require.NoError(t, err)
	}
	_, err := s.Park(&jobserver.CommandSpec{Command: []string{"true"}})
	assert.ErrorIs(t, err, ErrFull, "a full spool says so, so the caller can answer honestly")
	n, err := s.Count()
	require.NoError(t, err)
	assert.Equal(t, 2, n, "the refused spec wrote nothing")
}

func TestParkRefusesPastTheByteBound(t *testing.T) {
	s := idle(t, t.TempDir(), &stoppedInbox{}, &lineLog{})
	s.SetBounds(0, 32)
	_, err := s.Park(&jobserver.CommandSpec{Command: []string{strings.Repeat("x", 64)}})
	assert.ErrorIs(t, err, ErrFull)
}

func TestWithBoundsAndSetBoundsKeepABoundOfZero(t *testing.T) {
	s := open(t, t.TempDir(), &stoppedInbox{}, WithBounds(3, 0))
	assert.Equal(t, 3, s.maxEntries)
	assert.Equal(t, int64(DefaultMaxBytes), s.maxBytes)
	s.SetBounds(0, 99)
	assert.Equal(t, 3, s.maxEntries)
	assert.Equal(t, int64(99), s.maxBytes)
}
