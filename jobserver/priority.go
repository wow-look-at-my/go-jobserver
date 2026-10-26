// PriorityKey is one row of the admission order.

package jobserver

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
)

// PriorityKey is one row of the admission order.
type PriorityKey struct {
	// Name is hyphenated lowercase words, such as "infra-priority".
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Default     float64 `json:"default"`
}

// KeyOpenBatch is the key the scheduler fills itself. A key list that leaves it out gets it at the end.
const KeyOpenBatch = "open-batch"

var (
	// ErrUnknownPriorityKey is returned for a lease that sets a key the scheduler does not have.
	ErrUnknownPriorityKey = errors.New("jobserver: unknown priority key")
	// ErrBadPriorityValue is returned for a lease value that is not a finite number.
	ErrBadPriorityValue = errors.New("jobserver: priority value is not finite")
)

var priorityKeyName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// priorityTable is the validated key list.
type priorityTable struct {
	keys  []PriorityKey
	index map[string]int
	batch int
}

// newPriorityTable checks the key list and adds KeyOpenBatch when it is missing.
func newPriorityTable(keys []PriorityKey) (priorityTable, error) {
	t := priorityTable{index: map[string]int{}, batch: -1}
	for _, k := range keys {
		if !priorityKeyName.MatchString(k.Name) {
			return t, fmt.Errorf("jobserver: priority key %q is not hyphenated lowercase words", k.Name)
		}
		if _, dup := t.index[k.Name]; dup {
			return t, fmt.Errorf("jobserver: priority key %q is listed twice", k.Name)
		}
		if math.IsNaN(k.Default) || math.IsInf(k.Default, 0) {
			return t, fmt.Errorf("jobserver: priority key %q: %w", k.Name, ErrBadPriorityValue)
		}
		t.index[k.Name] = len(t.keys)
		t.keys = append(t.keys, k)
	}
	if at, ok := t.index[KeyOpenBatch]; ok {
		t.batch = at
	} else {
		t.batch = len(t.keys)
		t.index[KeyOpenBatch] = t.batch
		t.keys = append(t.keys, PriorityKey{Name: KeyOpenBatch, Description: "0 while a member of the lease's batch holds a slot, else 1", Default: 1})
	}
	return t, nil
}

// rank resolves a lease's values into one value per key, in key order. The open-batch row is filled at plan time.
func (t priorityTable) rank(values map[string]float64) ([]float64, error) {
	out := make([]float64, len(t.keys))
	for idx, k := range t.keys {
		out[idx] = k.Default
	}
	for name, v := range values {
		at, ok := t.index[name]
		if !ok || at == t.batch {
			return nil, fmt.Errorf("%w: %q", ErrUnknownPriorityKey, name)
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("priority key %q: %w", name, ErrBadPriorityValue)
		}
		out[at] = v
	}
	return out, nil
}

// values names each value of a rank by its key.
func (t priorityTable) values(rank []float64) map[string]float64 {
	out := make(map[string]float64, len(t.keys))
	for idx, k := range t.keys {
		out[k.Name] = rank[idx]
	}
	return out
}

// PriorityTable is a validated key list, for a queue of the caller's own that orders the way the scheduler does.
type PriorityTable struct{ t priorityTable }

// NewPriorityTable checks a key list the way NewScheduler does, and adds KeyOpenBatch when it is missing.
func NewPriorityTable(keys []PriorityKey) (PriorityTable, error) {
	t, err := newPriorityTable(keys)
	return PriorityTable{t}, err
}

// Rank resolves values into one value per key, in key order, with defaults filled in.
func (p PriorityTable) Rank(values map[string]float64) ([]float64, error) { return p.t.rank(values) }

// CompareRanks orders ranks the way the scheduler does: the first key that differs decides, lower first.
func CompareRanks(a, b []float64) int { return slices.Compare(a, b) }

// PriorityKeys returns the scheduler's key list, in comparison order.
func (s *Scheduler) PriorityKeys() []PriorityKey {
	return append([]PriorityKey(nil), s.priority.keys...)
}
