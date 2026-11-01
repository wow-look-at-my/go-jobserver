package backlog

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func newStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = filepath.Join(t.TempDir(), "queues")
	}
	s, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	return s
}

// The core contract: FIFO, push is a set union, take removes.
func TestPushTakeFIFO(t *testing.T) {
	s := newStore(t, Config{})

	res, err := s.Push("h", "backlog", []string{"a", "b", "c"})
	require.NoError(t, err)
	require.Equal(t, PushResult{Queued: 3, Depth: 3}, res)

	items, depth, err := s.Take("h", "backlog", 2)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, items, "head first")
	require.Equal(t, 1, depth)

	items, depth, err = s.Take("h", "backlog", 10)
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, items, "take never blocks on a short queue")
	require.Equal(t, 0, depth)

	items, depth, err = s.Take("h", "backlog", 5)
	require.NoError(t, err)
	require.Empty(t, items, "an empty queue is not an error, it is nothing to do")
	require.Equal(t, 0, depth)

	items, depth, err = s.Take("h", "backlog", 0)
	require.NoError(t, err)
	require.Empty(t, items)
	require.Equal(t, 0, depth)
}

// A push of the whole backlog every tick is the intended use. A duplicate keeps
// its original position, so the tail cannot starve behind items pushed again.
func TestPushDedupeKeepsOriginalPosition(t *testing.T) {
	s := newStore(t, Config{})

	_, err := s.Push("h", "backlog", []string{"a", "b", "c"})
	require.NoError(t, err)

	res, err := s.Push("h", "backlog", []string{"a", "b", "c", "d"})
	require.NoError(t, err)
	require.Equal(t, PushResult{Queued: 1, Duplicates: 3, Depth: 4}, res)

	items, _, err := s.Take("h", "backlog", 4)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d"}, items, "the push again must not move 'a' behind 'd'")

	// Dedupe is per queue, not per namespace.
	_, err = s.Push("h", "other", []string{"a"})
	require.NoError(t, err)
	require.Equal(t, 1, s.Depth("h", "other"))
}

// An item that a take removed is new again, and goes to the tail.
func TestTakenItemPushedAgainGoesToTheTail(t *testing.T) {
	s := newStore(t, Config{})
	_, err := s.Push("h", "q", []string{"a", "b", "c"})
	require.NoError(t, err)
	items, _, err := s.Take("h", "q", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, items)

	res, err := s.Push("h", "q", []string{"a", "b"})
	require.NoError(t, err)
	require.Equal(t, PushResult{Queued: 1, Duplicates: 1, Depth: 3}, res)
	items, _, err = s.Take("h", "q", 3)
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c", "a"}, items)
}

// A queue outliving the process that filled it is the whole point.
func TestQueueSurvivesReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queues")
	s := newStore(t, Config{Dir: dir})
	_, err := s.Push("h", "backlog", []string{"a", "b"})
	require.NoError(t, err)

	reopened := newStore(t, Config{Dir: dir})
	items, depth, err := reopened.Take("h", "backlog", 5)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, items)
	require.Equal(t, 0, depth)

	// A fully drained queue leaves nothing behind to reload.
	again := newStore(t, Config{Dir: dir})
	require.Equal(t, 0, again.Depth("h", "backlog"))
	require.Empty(t, again.List("h"))
}

// The file format is a JSON object of queue name to an array of items, indented
// with spaces. A file written in that format loads, and a write gives the same
// bytes.
func TestOnDiskFormat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queues")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	old := map[string][]string{"backlog": {"c", "a", "b"}, "sweep": {"x"}}
	want, err := json.MarshalIndent(old, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "h.json"), want, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{not json"), 0o600))

	s := newStore(t, Config{Dir: dir})
	require.Equal(t, []Stat{{Name: "backlog", Depth: 3}, {Name: "sweep", Depth: 1}}, s.List("h"))
	require.Empty(t, s.List("broken"), "a file that does not parse is skipped")

	res, err := s.Push("h", "backlog", []string{"a"})
	require.NoError(t, err)
	require.Equal(t, PushResult{Duplicates: 1, Depth: 3}, res)
	got, err := os.ReadFile(filepath.Join(dir, "h.json"))
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))

	items, _, err := s.Take("h", "backlog", 2)
	require.NoError(t, err)
	require.Equal(t, []string{"c", "a"}, items, "the loaded order is the file's order")
}

// The depth cap never silently swallows work: the overflow is counted and
// reported, because a full backlog means the consumer is not keeping up.
func TestDepthCapReportsDropped(t *testing.T) {
	s := newStore(t, Config{MaxDepth: 3})

	res, err := s.Push("h", "backlog", []string{"a", "b", "c", "d", "e"})
	require.NoError(t, err)
	require.Equal(t, PushResult{Queued: 3, Dropped: 2, Depth: 3}, res)

	items, _, err := s.Take("h", "backlog", 5)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, items, "the head is kept, the overflow is refused")
}

func TestListReportsDepthsSorted(t *testing.T) {
	s := newStore(t, Config{})
	_, err := s.Push("h", "sweep", []string{"x"})
	require.NoError(t, err)
	_, err = s.Push("h", "minded", []string{"a", "b"})
	require.NoError(t, err)

	require.Equal(t, []Stat{{Name: "minded", Depth: 2}, {Name: "sweep", Depth: 1}}, s.List("h"))
	require.Empty(t, s.List("other-hook"), "namespaces are isolated")
}

func TestValidationRejectsBadInput(t *testing.T) {
	s := newStore(t, Config{MaxItemBytes: 8, MaxQueues: 1, MaxNamespaces: 1})

	_, err := s.Push("Bad NS", "q", []string{"a"})
	require.ErrorIs(t, err, ErrBadNamespace)
	_, err = s.Push("h", "Bad Name", []string{"a"})
	require.ErrorIs(t, err, ErrBadName)
	_, err = s.Push("h", "q", []string{""})
	require.ErrorIs(t, err, ErrEmptyItem)
	_, err = s.Push("h", "q", []string{"waaaaaaaaay too long"})
	require.ErrorIs(t, err, ErrItemTooLarge)
	_, _, err = s.Take("Bad NS", "q", 1)
	require.ErrorIs(t, err, ErrBadNamespace)
	_, _, err = s.Take("h", "Bad Name", 1)
	require.ErrorIs(t, err, ErrBadName)
	require.False(t, ValidName("Bad Name"))
	require.True(t, ValidName("ok-name"))

	// A rejected push changes nothing, not even the namespace it would have made.
	require.Empty(t, s.List("h"))

	_, err = s.Push("h", "q", []string{"ok"})
	require.NoError(t, err)
	_, err = s.Push("h", "second", []string{"ok"})
	require.ErrorIs(t, err, ErrTooManyQueues)
	require.Equal(t, []Stat{{Name: "q", Depth: 1}}, s.List("h"), "the refused queue was never created")
	_, err = s.Push("other", "q", []string{"ok"})
	require.ErrorIs(t, err, ErrTooManyNS)
}

// A write that fails leaves memory as the disk holds it.
func TestFailedPersistRollsBack(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queues")
	s := newStore(t, Config{Dir: dir})
	_, err := s.Push("h", "q", []string{"a", "b"})
	require.NoError(t, err)

	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if f, err := os.CreateTemp(dir, "probe"); err == nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		t.Skip("the directory stays writable, so a write cannot fail here")
	}

	_, err = s.Push("h", "q", []string{"c"})
	require.Error(t, err)
	_, err = s.Push("h", "new", []string{"x"})
	require.Error(t, err)
	_, _, err = s.Take("h", "q", 1)
	require.Error(t, err)
	require.Equal(t, []Stat{{Name: "q", Depth: 2}}, s.List("h"))

	require.NoError(t, os.Chmod(dir, 0o700))
	items, _, err := s.Take("h", "q", 5)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, items)
}

func TestNewNeedsADir(t *testing.T) {
	_, err := New(Config{}, nil)
	require.Error(t, err)
}
