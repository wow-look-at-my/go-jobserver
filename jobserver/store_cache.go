// The dedup cache: the identity of each piece of work that succeeded, kept apart from task retention.

package jobserver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// cacheEntry is the last task that succeeded with an identity, and the artifacts it left.
type cacheEntry struct {
	Identity  string     `json:"identity"`
	ID        string     `json:"id"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
	At        time.Time  `json:"at"`
}

// putCache writes one cache entry.
func putCache(tx execer, e cacheEntry) error {
	blob, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT OR REPLACE INTO cache (identity, id, at, data) VALUES (?, ?, ?, ?)`, e.Identity, e.ID, e.At.UnixNano(), string(blob))
	return err
}

// remember records a task that succeeded with an identity in the cache. A newer entry of another task stays.
// Caller holds s.mu for writing.
func remember(tx *sql.Tx, j *Job) error {
	if j.Identity == "" || !j.State.succeeded() {
		return nil
	}
	var id string
	var at int64
	err := tx.QueryRow(`SELECT id, at FROM cache WHERE identity = ?`, j.Identity).Scan(&id, &at)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return err
	case id != j.ID && at > endedAt(j).UnixNano():
		return nil
	}
	return putCache(tx, cacheEntry{Identity: j.Identity, ID: j.ID, Artifacts: j.Artifacts, At: endedAt(j)})
}

// cacheEntry returns the cache entry of an identity.
func (s *Store) cacheEntry(identity string) (cacheEntry, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return cacheEntry{}, false, errors.New("jobserver: store is closed")
	}
	var blob string
	err := s.db.QueryRow(`SELECT data FROM cache WHERE identity = ?`, identity).Scan(&blob)
	if errors.Is(err, sql.ErrNoRows) {
		return cacheEntry{}, false, nil
	}
	if err != nil {
		return cacheEntry{}, false, err
	}
	var e cacheEntry
	if err := json.Unmarshal([]byte(blob), &e); err != nil {
		return cacheEntry{}, false, err
	}
	return e, true, nil
}

// pruneCache drops cache entries older than age, and past count, oldest first. A zero bound is no bound.
func pruneCache(tx execer, now time.Time, age time.Duration, count int) error {
	if age > 0 {
		if _, err := tx.Exec(`DELETE FROM cache WHERE at < ?`, now.Add(-age).UnixNano()); err != nil {
			return err
		}
	}
	if count > 0 {
		if _, err := tx.Exec(`DELETE FROM cache WHERE identity IN (SELECT identity FROM cache ORDER BY at DESC, identity DESC LIMIT -1 OFFSET ?)`, count); err != nil {
			return err
		}
	}
	return nil
}
