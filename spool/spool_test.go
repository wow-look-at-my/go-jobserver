package spool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func open(t *testing.T, opt Options) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), opt)
	require.NoError(t, err)
	return s
}

func TestPutAndProcessInAgeOrder(t *testing.T) {
	s := open(t, Options{})
	base := time.Unix(1000, 0)
	require.NoError(t, s.Put(Name(base.Add(time.Second), "b", ".json"), []byte("b")))
	require.NoError(t, s.Put(Name(base, "a", ".json"), []byte("a")))
	// Both are written in the same instant, so the name orders them: the name sorts by receipt.
	now := time.Now()
	for _, n := range []string{Name(base, "a", ".json"), Name(base.Add(time.Second), "b", ".json")} {
		require.NoError(t, os.Chtimes(filepath.Join(s.Dir(), n), now, now))
	}
	n, err := s.Count()
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	var seen []string
	r, err := s.Process(func(e Entry) error {
		seen = append(seen, string(e.Body))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, seen)
	assert.Equal(t, Result{Done: 2}, r)
	n, _ = s.Count()
	assert.Zero(t, n, "a done entry is deleted without KeepDone")
}

func TestATempFileIsNeverAnEntry(t *testing.T) {
	s := open(t, Options{})
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), ".spool-123.tmp"), []byte("half"), 0o600))
	n, err := s.Count()
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestBoundsRefuseInsteadOfDropping(t *testing.T) {
	s := open(t, Options{MaxEntries: 1})
	require.NoError(t, s.Put("one", []byte("x")))
	assert.ErrorIs(t, s.Put("two", []byte("y")), ErrFull)
	b := open(t, Options{MaxBytes: 3})
	assert.ErrorIs(t, b.Put("big", []byte("four")), ErrFull)
	assert.Error(t, s.Put("../escape", nil))
	assert.Error(t, s.Put(".hidden", nil))
}

// Keep leaves the entry for a later pass. Any other error moves it to failed/ with the reason. KeepDone keeps the
// processed ones under done/.
func TestKeepFailAndDone(t *testing.T) {
	s := open(t, Options{KeepDone: true})
	require.NoError(t, s.Put("keep", []byte("k")))
	require.NoError(t, s.Put("fail", []byte("f")))
	require.NoError(t, s.Put("ok", []byte("o")))
	r, err := s.Process(func(e Entry) error {
		switch e.Name {
		case "keep":
			return Keep(errors.New("the target is not loaded yet"))
		case "fail":
			return errors.New("not a spec")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, Result{Done: 1, Kept: 1, Failed: 1}, r)
	assert.FileExists(t, filepath.Join(s.Dir(), "keep"))
	assert.FileExists(t, filepath.Join(s.Dir(), DoneDir, "ok"))
	reason, err := os.ReadFile(filepath.Join(s.Dir(), FailedDir, "fail.error"))
	require.NoError(t, err)
	assert.Equal(t, "not a spec\n", string(reason))
}

// An entry that cannot be moved is skipped until it changes, so it is processed once and not once per pass.
func TestAnEntryThatCannotMoveIsSkippedUntilItChanges(t *testing.T) {
	s := open(t, Options{KeepDone: true})
	require.NoError(t, s.Put("stuck", []byte("s")))
	require.NoError(t, os.Remove(filepath.Join(s.Dir(), DoneDir)))
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), DoneDir), nil, 0o600))
	calls := 0
	fn := func(Entry) error { calls++; return nil }
	_, err := s.Process(fn)
	require.NoError(t, err)
	r, err := s.Process(fn)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 1, r.Skipped)

	later := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(s.Dir(), "stuck"), later, later))
	_, err = s.Process(fn)
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "a changed entry is processed again")
}

func TestCountOfAMissingDirectoryIsAnError(t *testing.T) {
	s := open(t, Options{})
	require.NoError(t, os.RemoveAll(s.Dir()))
	_, err := s.Count()
	assert.Error(t, err)
	_, err = s.Process(func(Entry) error { return nil })
	assert.Error(t, err)
}
