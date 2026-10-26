// Package spool turns task spec files in a directory into tasks of one inbox.

package spool

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

// The subdirectories that hold the files of ended tasks.
const (
	DoneDir   = "done"
	FailedDir = "failed"
)

// The suffixes of the files the spool writes beside a file it set aside.
const (
	corruptSuffix = ".corrupt"
	errorSuffix   = ".error"
)

// The bounds and the scan interval that Open uses when no option sets them.
const (
	DefaultMaxEntries = 1000
	DefaultMaxBytes   = 64 << 20
	DefaultInterval   = 250 * time.Millisecond
)

var (
	// ErrFull is returned by Park when the spool is at a bound. The caller must answer it, not drop the spec.
	ErrFull = errors.New("spool: the spool is full")
	// ErrBadInbox is returned by Open for an inbox with no AddTask method of the right shape.
	ErrBadInbox = errors.New("spool: the inbox has no AddTask(spec) (*jobserver.Task, error) method")
)

// Option changes how Open sets up a spool.
type Option func(*Spool)

// WithInterval sets how often the directory is scanned.
func WithInterval(d time.Duration) Option {
	return func(s *Spool) {
		if d > 0 {
			s.interval = d
		}
	}
}

// WithLogf sets the function that gets the status lines.
func WithLogf(logf func(format string, args ...any)) Option {
	return func(s *Spool) {
		if logf != nil {
			s.logf = logf
		}
	}
}

// WithBounds sets how many entries and bytes Park allows. A value of zero or less keeps that bound.
func WithBounds(maxEntries int, maxBytes int64) Option {
	return func(s *Spool) { s.SetBounds(maxEntries, maxBytes) }
}

// Spool watches a directory and adds each spec file in it to one inbox as a task.
type Spool struct {
	dir      string
	add      reflect.Value
	specType reflect.Type
	interval time.Duration
	logf     func(format string, args ...any)

	mu         sync.Mutex
	maxEntries int
	maxBytes   int64
	// active holds the files whose tasks did not end yet.
	active map[string]bool
	// stuck maps each file the spool must skip to the modification time it had.
	stuck map[string]int64

	scanMu sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

var (
	taskType  = reflect.TypeFor[*jobserver.Task]()
	errorType = reflect.TypeFor[error]()
)

// Open finds the AddTask method of inbox, adds each file that waits in dir in age order, and then watches dir.
func Open(dir string, inbox any, opts ...Option) (*Spool, error) {
	if inbox == nil {
		return nil, fmt.Errorf("%w: the inbox is nil", ErrBadInbox)
	}
	add := reflect.ValueOf(inbox).MethodByName("AddTask")
	if !add.IsValid() {
		return nil, fmt.Errorf("%w: %T has no AddTask", ErrBadInbox, inbox)
	}
	mt := add.Type()
	if mt.NumIn() < 1 || mt.NumOut() != 2 || mt.Out(0) != taskType || mt.Out(1) != errorType ||
		(mt.IsVariadic() && mt.NumIn() == 1) {
		return nil, fmt.Errorf("%w: %T.AddTask is %v", ErrBadInbox, inbox, mt)
	}
	spec := mt.In(0)
	if base := spec; (base.Kind() == reflect.Pointer && base.Elem().Kind() != reflect.Struct) ||
		(base.Kind() != reflect.Pointer && base.Kind() != reflect.Struct) {
		return nil, fmt.Errorf("%w: %T.AddTask takes %v, not a struct", ErrBadInbox, inbox, spec)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Spool{
		dir:        dir,
		add:        add,
		specType:   spec,
		interval:   DefaultInterval,
		logf:       log.Printf,
		maxEntries: DefaultMaxEntries,
		maxBytes:   DefaultMaxBytes,
		active:     make(map[string]bool),
		stuck:      make(map[string]int64),
		ctx:        ctx,
		cancel:     cancel,
	}
	for _, opt := range opts {
		opt(s)
	}
	for _, sub := range []string{"", DoneDir, FailedDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			cancel()
			return nil, fmt.Errorf("spool: %w", err)
		}
	}
	s.logf("spool: %s", dir)
	s.scan()
	s.wg.Add(1)
	go s.watch()
	return s, nil
}

// Dir is the directory the spool watches.
func (s *Spool) Dir() string { return s.dir }

// SetBounds sets how many entries and bytes Park allows. A value of zero or less keeps that bound.
func (s *Spool) SetBounds(maxEntries int, maxBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maxEntries > 0 {
		s.maxEntries = maxEntries
	}
	if maxBytes > 0 {
		s.maxBytes = maxBytes
	}
}

// Close stops the watch and the waits. A file whose task did not end stays, and the next Open adds it again.
func (s *Spool) Close() error {
	s.once.Do(func() {
		s.cancel()
		s.wg.Wait()
	})
	return nil
}

// watch scans the directory until Close.
func (s *Spool) watch() {
	defer s.wg.Done()
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.scan()
		}
	}
}

// entry is one file that waits in the spool.
type entry struct {
	name  string
	mtime time.Time
	size  int64
}

// entries lists the files that wait, oldest first. Temporary files, set-aside files and reasons are not entries.
func (s *Spool) entries() ([]entry, error) {
	list, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	var out []entry
	for _, de := range list {
		name := de.Name()
		if de.IsDir() || strings.HasPrefix(name, ".") || strings.HasSuffix(name, corruptSuffix) || strings.HasSuffix(name, errorSuffix) {
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue
		}
		out = append(out, entry{name: name, mtime: info.ModTime(), size: info.Size()})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].mtime.Equal(out[j].mtime) {
			return out[i].mtime.Before(out[j].mtime)
		}
		return out[i].name < out[j].name
	})
	return out, nil
}

// Count is how many files wait in the spool, with the files whose tasks did not end.
func (s *Spool) Count() (int, error) {
	list, err := s.entries()
	if err != nil {
		return 0, err
	}
	return len(list), nil
}

// scan adds each file that waits and is not active or stuck. A file that left the directory is no longer stuck.
func (s *Spool) scan() {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	list, err := s.entries()
	if err != nil {
		s.logf("%v", err)
		return
	}
	waiting := set.New[string]()
	for _, e := range list {
		waiting.Add(e.name)
		if s.ctx.Err() != nil {
			return
		}
		s.mu.Lock()
		stuck, isStuck := s.stuck[e.name]
		skip := s.active[e.name] || (isStuck && stuck == e.mtime.UnixNano())
		s.mu.Unlock()
		if !skip {
			s.take(e)
		}
	}
	s.mu.Lock()
	for name := range s.stuck {
		if !waiting.Contains(name) {
			delete(s.stuck, name)
		}
	}
	s.mu.Unlock()
}

// take decodes one file and adds it to the inbox. A wait then moves the file when its task ends.
func (s *Spool) take(e entry) {
	body, err := os.ReadFile(filepath.Join(s.dir, e.name))
	if err != nil {
		s.logf("spool: %s: %v", e.name, err)
		return
	}
	spec, err := s.decode(e.name, body)
	if err != nil {
		s.quarantine(e, err)
		return
	}
	task, err := s.call(spec)
	if errors.Is(err, jobserver.ErrStopped) {
		s.markStuck(e, fmt.Sprintf("spool: %s: %v; the file stays for the next start", e.name, err))
		return
	}
	if err != nil {
		s.move(e, FailedDir, err)
		return
	}
	s.logf("spool: %s is task %s", e.name, task.ID)
	s.mu.Lock()
	s.active[e.name] = true
	s.mu.Unlock()
	s.wg.Add(1)
	go s.wait(e, task)
}

// call adds spec through the inbox's AddTask. Each parameter after the spec gets its zero value.
func (s *Spool) call(spec reflect.Value) (task *jobserver.Task, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("spool: AddTask panicked: %v", r)
		}
	}()
	mt := s.add.Type()
	args := []reflect.Value{spec}
	for i := 1; i < mt.NumIn(); i++ {
		if mt.IsVariadic() && i == mt.NumIn()-1 {
			break
		}
		args = append(args, reflect.Zero(mt.In(i)))
	}
	out := s.add.Call(args)
	if e, _ := out[1].Interface().(error); e != nil {
		return nil, e
	}
	task, _ = out[0].Interface().(*jobserver.Task)
	if task == nil {
		return nil, errors.New("spool: AddTask returned no task and no error")
	}
	return task, nil
}

// wait moves the file to done/ or failed/ when its task ends. After Close the file stays where it is.
func (s *Spool) wait(e entry, task *jobserver.Task) {
	defer s.wg.Done()
	terr := task.Wait(s.ctx)
	defer func() {
		s.mu.Lock()
		delete(s.active, e.name)
		s.mu.Unlock()
	}()
	if s.ctx.Err() != nil {
		return
	}
	if terr != nil {
		s.move(e, FailedDir, terr)
		return
	}
	s.move(e, DoneDir, nil)
}

// decode reads a file into a new value of the spec type. A body that is not JSON goes to the type's UnmarshalText, when it has one.
func (s *Spool) decode(name string, body []byte) (reflect.Value, error) {
	text := bytes.TrimSpace(body)
	if len(text) == 0 {
		return reflect.Value{}, errors.New("the file is empty")
	}
	ptr := s.specType
	if ptr.Kind() != reflect.Pointer {
		ptr = reflect.PointerTo(ptr)
	}
	v := reflect.New(ptr.Elem())
	tu, isText := v.Interface().(encoding.TextUnmarshaler)
	var err error
	switch {
	case text[0] == '{' || text[0] == '[' || text[0] == '"' || !isText:
		err = json.Unmarshal(text, v.Interface())
	default:
		err = tu.UnmarshalText(text)
	}
	if err != nil {
		return reflect.Value{}, err
	}
	setTitle(v.Elem(), strings.TrimSuffix(name, filepath.Ext(name)))
	if s.specType.Kind() != reflect.Pointer {
		return v.Elem(), nil
	}
	return v, nil
}

// setTitle gives a spec that embeds jobserver.TaskSpec the title when it has none.
func setTitle(v reflect.Value, title string) {
	f := v.FieldByName("TaskSpec")
	if !f.IsValid() || f.Type() != reflect.TypeFor[jobserver.TaskSpec]() {
		return
	}
	if t := f.FieldByName("Title"); t.String() == "" {
		t.SetString(title)
	}
}

// quarantine renames a file that does not decode to name.corrupt, with the reason in name.corrupt.error.
func (s *Spool) quarantine(e entry, cause error) {
	path := filepath.Join(s.dir, e.name)
	bad := path + corruptSuffix
	reason := bad + errorSuffix
	if err := os.WriteFile(reason, []byte(cause.Error()+"\n"), 0o644); err != nil {
		s.markStuck(e, fmt.Sprintf("spool: %s does not decode (%v), and the reason cannot be written: %v; the file stays and is skipped until it changes", e.name, cause, err))
		return
	}
	if err := os.Rename(path, bad); err != nil {
		os.Remove(reason)
		s.markStuck(e, fmt.Sprintf("spool: %s does not decode (%v), and the quarantine failed: %v; the file stays and is skipped until it changes", e.name, cause, err))
		return
	}
	s.logf("spool: %s does not decode: %v; it is now %s", e.name, cause, filepath.Base(bad))
}

// move moves a file into dir. With a cause, the reason is written first, so a failed file never appears without it.
func (s *Spool) move(e entry, dir string, cause error) {
	if err := s.rename(e.name, dir, cause); err != nil {
		s.markStuck(e, fmt.Sprintf("spool: %s: %v; the file stays and is skipped until it changes", e.name, err))
		return
	}
	if cause != nil {
		s.logf("spool: %s failed: %v", e.name, cause)
	}
}

// rename makes the destination directory and moves the file into it.
func (s *Spool) rename(name, dir string, cause error) error {
	dstDir := filepath.Join(s.dir, dir)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	dst := filepath.Join(dstDir, name)
	reason := dst + errorSuffix
	if cause != nil {
		if err := os.WriteFile(reason, []byte(cause.Error()+"\n"), 0o644); err != nil {
			return err
		}
	}
	if err := os.Rename(filepath.Join(s.dir, name), dst); err != nil {
		if cause != nil {
			os.Remove(reason)
		}
		return err
	}
	return nil
}

// markStuck logs why a file stays and skips that version of it until it changes.
func (s *Spool) markStuck(e entry, msg string) {
	s.mu.Lock()
	s.stuck[e.name] = e.mtime.UnixNano()
	s.mu.Unlock()
	s.logf("%s", msg)
}
