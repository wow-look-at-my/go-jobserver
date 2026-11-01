// Package backlog is a durable batch backlog: a named list of opaque item ids
// per namespace, which runs drain a slice at a time. It holds the work that is
// left for a run that can only afford part of it.
//
// Push is a set union that keeps an existing item's original position. A
// caller can then push its whole candidate set again every tick, and the tail
// does not starve. Take removes a bounded slice with no lease or ack.
// Consumers must be idempotent: their next push restores what a dying run
// drops. Do not replace this with a cursor, because a cursor changes meaning
// when the list changes. The store is on disk, so that a restart does not
// restart a fleet walk.
package backlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/wow-look-at-my/go-containers/orderedset"
)

// Config bounds the store. A zero bound takes the default New applies.
type Config struct {
	// Dir holds one <namespace>.json file per namespace.
	Dir string
	// MaxDepth bounds the items of a queue.
	MaxDepth int
	// MaxItemBytes bounds the size of an item.
	MaxItemBytes int
	// MaxQueues bounds the queues of a namespace.
	MaxQueues int
	// MaxNamespaces bounds the namespaces.
	MaxNamespaces int
}

// Typed errors a caller maps onto status codes.
var (
	ErrBadNamespace  = errors.New("backlog: invalid namespace")
	ErrBadName       = errors.New("backlog: invalid queue name")
	ErrItemTooLarge  = errors.New("backlog: item exceeds max size")
	ErrEmptyItem     = errors.New("backlog: empty item")
	ErrTooManyQueues = errors.New("backlog: queue limit reached for this namespace")
	ErrTooManyNS     = errors.New("backlog: namespace limit reached")
)

// namePattern is the alphabet of a namespace and of a queue name: lowercase kebab-case.
var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

func validNamespace(ns string) bool {
	return ns != "" && len(ns) <= 128 && namePattern.MatchString(ns)
}

func validName(name string) bool {
	return name != "" && len(name) <= 128 && namePattern.MatchString(name)
}

// ValidName reports whether name is a valid queue name, so a router can refuse a bad name for every verb.
func ValidName(name string) bool { return validName(name) }

// Stat is the observable state of a queue: its name and how much work waits.
type Stat struct {
	Name  string `json:"name"`
	Depth int    `json:"depth"`
}

// PushResult reports what a push did.
type PushResult struct {
	Queued     int `json:"queued"`
	Duplicates int `json:"duplicates"`
	Dropped    int `json:"dropped"`
	Depth      int `json:"depth"`
}

// queue is one first-in-first-out list. It is a JSON array of items on disk.
type queue = orderedset.OrderedSet[string]

// Store holds every namespace in memory and writes a namespace's file on each change.
type Store struct {
	mu  sync.Mutex
	ns  map[string]map[string]*queue
	cfg Config
	log *slog.Logger
}

// New makes a Store. It creates Dir when it is missing, and loads every
// namespace file in it. It logs and skips a namespace file that does not
// parse, so one bad file does not stop the start.
func New(cfg Config, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Dir == "" {
		return nil, errors.New("backlog: Config.Dir is required")
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 10000
	}
	if cfg.MaxItemBytes <= 0 {
		cfg.MaxItemBytes = 512
	}
	if cfg.MaxQueues <= 0 {
		cfg.MaxQueues = 64
	}
	if cfg.MaxNamespaces <= 0 {
		cfg.MaxNamespaces = 256
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("backlog: create dir: %w", err)
	}
	s := &Store{ns: make(map[string]map[string]*queue), cfg: cfg, log: log}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	des, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		return fmt.Errorf("backlog: read dir: %w", err)
	}
	for _, de := range des {
		if de.IsDir() || !strings.HasSuffix(de.Name(), ".json") {
			continue
		}
		ns := strings.TrimSuffix(de.Name(), ".json")
		if !validNamespace(ns) {
			s.log.Warn("backlog: skipping file with invalid namespace name", "file", de.Name())
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.cfg.Dir, de.Name()))
		if err != nil {
			s.log.Error("backlog: read namespace file", "file", de.Name(), "err", err)
			continue
		}
		var m map[string]*queue
		if err := json.Unmarshal(data, &m); err != nil {
			s.log.Error("backlog: parse namespace file (skipping)", "file", de.Name(), "err", err)
			continue
		}
		for name, q := range m {
			if q == nil {
				m[name] = &queue{} // a JSON null is an empty queue
			}
		}
		s.ns[ns] = m
	}
	return nil
}

// Push appends the items that are not already queued, in the given order, and
// returns what it did. A duplicate keeps its original position, because a
// push of the same backlog every tick is the intended use.
func (s *Store) Push(ns, name string, items []string) (PushResult, error) {
	if !validNamespace(ns) {
		return PushResult{}, ErrBadNamespace
	}
	if !validName(name) {
		return PushResult{}, ErrBadName
	}
	for _, it := range items {
		if it == "" {
			return PushResult{}, ErrEmptyItem
		}
		if len(it) > s.cfg.MaxItemBytes {
			return PushResult{}, ErrItemTooLarge
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	qs, nsExisted := s.ns[ns]
	if !nsExisted {
		if len(s.ns) >= s.cfg.MaxNamespaces {
			return PushResult{}, ErrTooManyNS
		}
		qs = make(map[string]*queue)
		s.ns[ns] = qs
	}
	q, qExisted := qs[name]
	if !qExisted {
		if len(qs) >= s.cfg.MaxQueues {
			if !nsExisted {
				delete(s.ns, ns)
			}
			return PushResult{}, ErrTooManyQueues
		}
		q = &queue{}
		qs[name] = q
	}

	res := PushResult{}
	var added []string
	for _, it := range items {
		if q.Contains(it) {
			res.Duplicates++
			continue
		}
		if q.Len() >= s.cfg.MaxDepth {
			res.Dropped++
			continue
		}
		q.Add(it)
		added = append(added, it)
		res.Queued++
	}
	res.Depth = q.Len()

	if err := s.persist(ns); err != nil {
		// Memory goes back to what the disk holds. A queue only memory holds would empty itself at the next restart.
		q.Remove(added...)
		if !qExisted {
			delete(qs, name)
		}
		if !nsExisted {
			delete(s.ns, ns)
		}
		return PushResult{}, err
	}
	if res.Dropped > 0 {
		s.log.Warn("backlog: depth cap reached, items dropped", "ns", ns, "queue", name, "dropped", res.Dropped, "depth", res.Depth)
	}
	return res, nil
}

// Take removes and returns up to count items from the head, and the depth
// left. A missing queue is an empty result and not an error, because that is
// what "nothing to do" means to a caller that drains a backlog.
func (s *Store) Take(ns, name string, count int) ([]string, int, error) {
	if !validNamespace(ns) {
		return nil, 0, ErrBadNamespace
	}
	if !validName(name) {
		return nil, 0, ErrBadName
	}
	if count <= 0 {
		return []string{}, s.Depth(ns, name), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	qs := s.ns[ns]
	if qs == nil {
		return []string{}, 0, nil
	}
	q := qs[name]
	if q == nil || q.IsEmpty() {
		return []string{}, 0, nil
	}
	taken := q.TakeFront(count)

	if err := s.persist(ns); err != nil {
		// The take never happened: the taken items go back to the head.
		back := orderedset.Of(taken...)
		back.AddSet(*q)
		qs[name] = &back
		return nil, 0, err
	}
	left := q.Len()
	if left == 0 {
		delete(qs, name)
		if len(qs) == 0 {
			delete(s.ns, ns)
		}
		// The file now describes an empty namespace. A failure here leaves the drained queue on disk as an empty list.
		if err := s.persist(ns); err != nil {
			s.log.Error("backlog: persist after drain", "ns", ns, "err", err)
		}
	}
	return taken, left, nil
}

// Depth reports how much work waits on a queue.
func (s *Store) Depth(ns, name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q := s.ns[ns][name]; q != nil {
		return q.Len()
	}
	return 0
}

// List reports every queue of a namespace that holds work, sorted by name. It
// gives depths only, because the items belong to the namespace's owner.
func (s *Store) List(ns string) []Stat {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Stat, 0, len(s.ns[ns]))
	for name, q := range s.ns[ns] {
		out = append(out, Stat{Name: name, Depth: q.Len()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// persist writes a namespace's file again: a temporary file, synced, then
// renamed over the one. The caller holds the mutex. A crash in the middle
// never leaves a torn file, because a rename on one filesystem is atomic.
func (s *Store) persist(ns string) error {
	data, err := json.MarshalIndent(s.ns[ns], "", "  ")
	if err != nil {
		return fmt.Errorf("backlog: marshal namespace %q: %w", ns, err)
	}
	tmp, err := os.CreateTemp(s.cfg.Dir, "."+ns+".json.tmp-*")
	if err != nil {
		return fmt.Errorf("backlog: temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("backlog: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("backlog: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("backlog: close temp: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(s.cfg.Dir, ns+".json")); err != nil {
		return fmt.Errorf("backlog: rename: %w", err)
	}
	return nil
}
