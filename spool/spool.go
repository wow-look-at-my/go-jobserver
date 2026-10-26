// Package spool is a directory of entries that a process takes in age order. A writer commits each entry with an
// atomic rename, so a reader never sees half of one.
package spool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// The subdirectories a processed entry moves to.
const (
	DoneDir   = "done"
	FailedDir = "failed"
)

// ErrFull is a Put past the bounds. The caller answers it; nothing is dropped.
var ErrFull = errors.New("spool: full")

// keepErr marks a handler error that leaves the entry in the spool for a later pass.
type keepErr struct{ err error }

func (k keepErr) Error() string { return k.err.Error() }
func (k keepErr) Unwrap() error { return k.err }

// Keep wraps a handler error so the entry stays in the spool, untouched, for the next pass.
func Keep(err error) error { return keepErr{err} }

// Options bound the spool and say what a processed entry becomes.
type Options struct {
	// MaxEntries and MaxBytes bound Put. Zero is no bound.
	MaxEntries int
	MaxBytes   int64
	// KeepDone moves a processed entry to done/. Otherwise it is deleted.
	KeepDone bool
	// Logf receives every entry that failed, stayed, or could not be moved. Nil is silent.
	Logf func(format string, args ...any)
	// DirMode is the mode of the directories Open makes. A drop directory for other users needs more.
	DirMode os.FileMode
	// ByName orders entries by name only. Use it when the writer encodes the order in the name, as Name does.
	ByName bool
}

// Store is one spool directory.
type Store struct {
	dir string
	opt Options

	mu sync.Mutex
	// stuck holds the mtime of each entry that could not be moved. It is skipped until it changes.
	stuck map[string]int64
}

// Entry is one committed entry.
type Entry struct {
	Name    string
	Body    []byte
	ModTime time.Time
}

// Result counts what one pass did.
type Result struct {
	Done, Kept, Failed, Skipped int
}

// Open makes the directory and its done/ and failed/ subdirectories.
func Open(dir string, opt Options) (*Store, error) {
	s := &Store{dir: dir, opt: opt, stuck: map[string]int64{}}
	for _, sub := range []string{"", DoneDir, FailedDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), s.dirMode()); err != nil {
			return nil, fmt.Errorf("spool: %w", err)
		}
	}
	return s, nil
}

func (s *Store) dirMode() os.FileMode {
	if s.opt.DirMode == 0 {
		return 0o700
	}
	return s.opt.DirMode
}

// Dir is the spool directory.
func (s *Store) Dir() string { return s.dir }

func (s *Store) logf(format string, args ...any) {
	if s.opt.Logf != nil {
		s.opt.Logf(format, args...)
	}
}

// Name is an entry name that sorts by the given instant, so a listing is already in age order.
func Name(at time.Time, id, ext string) string {
	return fmt.Sprintf("%020d-%s%s", at.UTC().UnixNano(), id, ext)
}

// Put commits body under name with a temp file and a rename. It answers ErrFull past the bounds.
func (s *Store) Put(name string, body []byte) error {
	if strings.ContainsAny(name, `/\`) || name == "" || strings.HasPrefix(name, ".") {
		return fmt.Errorf("spool: bad entry name %q", name)
	}
	entries, err := s.list()
	if err != nil {
		return err
	}
	var total int64
	for _, e := range entries {
		total += e.size
	}
	if (s.opt.MaxEntries > 0 && len(entries) >= s.opt.MaxEntries) || (s.opt.MaxBytes > 0 && total+int64(len(body)) > s.opt.MaxBytes) {
		return ErrFull
	}
	tmp, err := os.CreateTemp(s.dir, ".spool-*.tmp")
	if err != nil {
		return fmt.Errorf("spool: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("spool: write %s: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("spool: close %s: %w", name, err)
	}
	if err := os.Rename(tmpName, filepath.Join(s.dir, name)); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("spool: commit %s: %w", name, err)
	}
	return nil
}

type listed struct {
	name  string
	size  int64
	mtime time.Time
}

// list is every committed entry, oldest first: by mtime, then by name, or by name alone with ByName. A temp file or a
// directory is not an entry.
func (s *Store) list() ([]listed, error) {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("spool: read %s: %w", s.dir, err)
	}
	out := make([]listed, 0, len(ents))
	for _, e := range ents {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") || e.Name() == DoneDir || e.Name() == FailedDir {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, listed{name: e.Name(), size: info.Size(), mtime: info.ModTime()})
	}
	sort.Slice(out, func(a, b int) bool {
		if !s.opt.ByName && !out[a].mtime.Equal(out[b].mtime) {
			return out[a].mtime.Before(out[b].mtime)
		}
		return out[a].name < out[b].name
	})
	return out, nil
}

// Count is how many entries wait. An unreadable directory is an error, never zero.
func (s *Store) Count() (int, error) {
	l, err := s.list()
	return len(l), err
}

// Process hands each entry to fn, oldest first. A nil answer is done: the entry is deleted, or moved to done/. A
// Keep error leaves it in place. Any other error moves it to failed/ with the error beside it. An entry that cannot
// be moved is skipped until it changes, so one bad file makes one error and not one per pass.
func (s *Store) Process(fn func(Entry) error) (Result, error) {
	var r Result
	entries, err := s.list()
	if err != nil {
		return r, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	waiting := set.New[string]()
	for _, e := range entries {
		waiting.Add(e.name)
		mtime := e.mtime.UnixNano()
		if stuck, ok := s.stuck[e.name]; ok && stuck == mtime {
			r.Skipped++
			continue
		}
		body, err := os.ReadFile(filepath.Join(s.dir, e.name))
		if err != nil {
			s.logf("spool: %s cannot be read, left in place: %v", e.name, err)
			r.Kept++
			continue
		}
		ferr := fn(Entry{Name: e.name, Body: body, ModTime: e.mtime})
		var keep keepErr
		switch {
		case ferr == nil:
			if err := s.settle(e.name, ferr); err != nil {
				s.stuck[e.name] = mtime
				s.logf("spool: %s was processed and cannot be moved, so it is skipped until it changes: %v", e.name, err)
			}
			r.Done++
		case errors.As(ferr, &keep):
			s.logf("spool: %s stays for a later pass: %v", e.name, keep.err)
			r.Kept++
		default:
			s.logf("spool: %s failed and moves to %s/: %v", e.name, FailedDir, ferr)
			if err := s.settle(e.name, ferr); err != nil {
				s.stuck[e.name] = mtime
				s.logf("spool: %s cannot be moved to %s/, so it is skipped until it changes: %v", e.name, FailedDir, err)
			}
			r.Failed++
		}
	}
	for name := range s.stuck {
		if !waiting.Contains(name) {
			delete(s.stuck, name)
		}
	}
	return r, nil
}

// settle takes a processed entry out of the spool. A failed one goes to failed/, and its reason is written first, so
// a file never sits there without the reason beside it.
func (s *Store) settle(name string, cause error) error {
	src := filepath.Join(s.dir, name)
	if cause == nil && !s.opt.KeepDone {
		return os.Remove(src)
	}
	dir := DoneDir
	if cause != nil {
		dir = FailedDir
	}
	// The directory can be gone: an operator who empties done/ or failed/ removes it.
	if err := os.MkdirAll(filepath.Join(s.dir, dir), s.dirMode()); err != nil {
		return err
	}
	dst := filepath.Join(s.dir, dir, name)
	reason := dst + ".error"
	if cause != nil {
		if err := os.WriteFile(reason, []byte(cause.Error()+"\n"), 0o600); err != nil {
			return err
		}
	}
	if err := os.Rename(src, dst); err != nil {
		if cause != nil {
			os.Remove(reason)
		}
		return err
	}
	return nil
}
