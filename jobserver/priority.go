// PriorityKey is one key of the lease priority order.

package jobserver

import (
	"errors"
	"fmt"
	"math"
	"regexp"

	"github.com/wow-look-at-my/go-jobserver/priority"
)

// PriorityKey is one key of the lease priority order. Its default must be a whole number.
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
	// ErrBadPriorityValue is returned for a lease value that is not a whole number.
	ErrBadPriorityValue = errors.New("jobserver: priority value is not a whole number")
)

var priorityKeyName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// leaseKeys is the key list of the leases that come with no inbox. The default inbox uses it too.
type leaseKeys struct {
	described []PriorityKey
	keys      *priority.Keys
}

// newLeaseKeys checks the key list and adds KeyOpenBatch when it is missing.
func newLeaseKeys(list []PriorityKey) (leaseKeys, error) {
	var out leaseKeys
	var keys []*priority.MultiPriorityKey
	hasBatch := false
	for _, k := range list {
		if !priorityKeyName.MatchString(k.Name) {
			return out, fmt.Errorf("jobserver: priority key %q is not hyphenated lowercase words", k.Name)
		}
		def, ok := wholeNumber(k.Default)
		if !ok {
			return out, fmt.Errorf("jobserver: priority key %q: %w", k.Name, ErrBadPriorityValue)
		}
		hasBatch = hasBatch || k.Name == KeyOpenBatch
		keys = append(keys, &priority.MultiPriorityKey{Name: k.Name, DefaultPriority: def})
		out.described = append(out.described, k)
	}
	if !hasBatch {
		keys = append(keys, &priority.MultiPriorityKey{Name: KeyOpenBatch, DefaultPriority: 1})
		out.described = append(out.described, PriorityKey{Name: KeyOpenBatch, Description: "0 while a member of the lease's batch holds a slot, else 1", Default: 1})
	}
	pk, err := priority.NewKeys(keys...)
	if err != nil {
		return out, err
	}
	out.keys = pk
	return out, nil
}

// wholeNumber converts v to an int when it is a whole number in range.
func wholeNumber(v float64) (int, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) || v != math.Trunc(v) || math.Abs(v) > 1<<53 {
		return 0, false
	}
	return int(v), true
}

// priorityOf resolves a lease's named values against the key list. The scheduler fills the open-batch key at plan time.
func (t leaseKeys) priorityOf(values map[string]float64) (priority.MultiPriority, error) {
	var p priority.MultiPriority
	for name, v := range values {
		k := t.keys.Lookup(name)
		if k == nil || name == KeyOpenBatch {
			return p, fmt.Errorf("%w: %q", ErrUnknownPriorityKey, name)
		}
		n, ok := wholeNumber(v)
		if !ok {
			return p, fmt.Errorf("priority key %q: %w", name, ErrBadPriorityValue)
		}
		p.Set(k, n)
	}
	return p, nil
}

// PriorityKeys returns the key list of leases with no inbox, in comparison order.
func (s *Scheduler) PriorityKeys() []PriorityKey {
	return append([]PriorityKey(nil), s.priority.described...)
}
