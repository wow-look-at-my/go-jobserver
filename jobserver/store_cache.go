// The dedup cache: the identity of each piece of work that succeeded, kept apart from task retention.

package jobserver

import (
	"encoding/json"
	"sort"
	"time"
)

// cacheEntry is the last task that succeeded with an identity, and the artifacts it left.
type cacheEntry struct {
	Identity  string     `json:"identity"`
	ID        string     `json:"id"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
	At        time.Time  `json:"at"`
}

// remember records a task that succeeded with an identity in the cache. Caller holds s.mu.
func (s *Store) remember(j *Job) {
	if j.Identity == "" || !j.State.succeeded() {
		return
	}
	if old, ok := s.cache[j.Identity]; ok && old.ID != j.ID && old.At.After(endedAt(j)) {
		return
	}
	s.cache[j.Identity] =cacheEntry{Identity: j.Identity, ID: j.ID, Artifacts: append([]Artifact(nil), j.Artifacts...), At: endedAt(j)}
}

// applyCache folds a cache record into the cache. A blob that does not decode is left out.
func (s *Store) applyCache(r record) {
	var e cacheEntry
	if json.Unmarshal([]byte(r.blob), &e) == nil && e.Identity != "" {
		s.cache[e.Identity] = e
	}
}

// cacheEntry returns the cache entry of an identity.
func (s *Store) cacheEntry(identity string) (cacheEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.cache[identity]
	e.Artifacts = append([]Artifact(nil), e.Artifacts...)
	return e, ok
}

// pruneCache drops cache entries older than age, and past count, oldest first. A zero bound is no bound.
// It reports whether it dropped any. Caller holds s.mu.
func (s *Store) pruneCache(now time.Time, age time.Duration, count int) bool {
	entries := make([]cacheEntry, 0, len(s.cache))
	for _, e := range s.cache {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(a, b int) bool {
		if !entries[a].At.Equal(entries[b].At) {
			return entries[a].At.Before(entries[b].At)
		}
		return entries[a].Identity < entries[b].Identity
	})
	left := len(entries)
	dropped := false
	for _, e := range entries {
		if (count > 0 && left > count) || (age > 0 && now.Sub(e.At) > age) {
			delete(s.cache, e.Identity)
			left--
			dropped = true
		}
	}
	return dropped
}

// cacheRecords are the records that rebuild the cache, oldest first. Caller holds s.mu.
func (s *Store) cacheRecords() ([]record, error) {
	entries := make([]cacheEntry, 0, len(s.cache))
	for _, e := range s.cache {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(a, b int) bool { return entries[a].Identity < entries[b].Identity })
	recs := make([]record, 0, len(entries))
	for _, e := range entries {
		blob, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		recs = append(recs, record{kind: recCache, blob: string(blob)})
	}
	return recs, nil
}
