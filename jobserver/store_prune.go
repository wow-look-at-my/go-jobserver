// Retention deletes ended tasks from the database.

package jobserver

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"
)

// hold marks a caller that blocks on a task, so no prune drops it and memory keeps it. It fails for a task the store
// does not hold.
func (s *Store) hold(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.rec(id)
	if err != nil {
		return err
	}
	s.waiters[j.ID]++
	s.jobs[j.ID] = j
	return nil
}

// release ends a hold.
func (s *Store) release(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id = CanonicalID(id)
	if s.waiters[id] <= 1 {
		delete(s.waiters, id)
	} else {
		s.waiters[id]--
	}
	s.unpin(id)
}

// endedAt is when a job ended. A job that ended with no run has only its creation time.
func endedAt(j *Job) time.Time {
	if j.Finished.IsZero() {
		return j.Created
	}
	return j.Finished
}

// Retention bounds what a prune keeps.
type Retention struct {
	// Age and Count bound the ended tasks of each inbox.
	Age   time.Duration
	Count int
	// CacheAge and CacheCount bound the dedup cache. Zero is no bound.
	CacheAge   time.Duration
	CacheCount int
}

// Prune drops each ended task that ended more than r.Age ago, or that its inbox holds past r.Count, oldest first.
// It keeps a task that a task which has not ended depends on, and a task a caller waits on.
// It bounds the dedup cache on its own, by r.CacheAge and r.CacheCount.
// It deletes them from the database, and returns the IDs of the tasks it dropped. A caller that holds the record of a
// dropped task still reads it.
func (s *Store) Prune(now time.Time, r Retention) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil
	}
	drop, err := s.pruneList(now, r.Age, r.Count)
	if err != nil {
		return nil, err
	}
	err = s.inTx(func(tx *sql.Tx) error {
		for _, id := range drop {
			if err := deleteTask(tx, id); err != nil {
				return err
			}
		}
		return pruneCache(tx, now, r.CacheAge, r.CacheCount)
	})
	if err != nil {
		return nil, fmt.Errorf("jobserver: prune: %w", err)
	}
	if len(drop) == 0 {
		return nil, nil
	}
	s.prunes++
	var errs []error
	for _, id := range drop {
		delete(s.jobs, id)
		s.dropWeak(id)
		if err := s.closeLog(id); err != nil {
			errs = append(errs, err)
		}
		if err := os.Remove(s.LogPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	s.bump()
	return drop, errors.Join(errs...)
}

// pruneList reads the ended tasks of each inbox, oldest first, and returns the ones past age or past count. A task an
// open task depends on, or that a caller waits on, stays and counts toward count. Caller holds s.mu.
func (s *Store) pruneList(now time.Time, age time.Duration, count int) ([]string, error) {
	type ended struct {
		id string
		at int64
	}
	byInbox := map[string][]ended{}
	rows, err := s.db.Query(`SELECT id, inbox, ended FROM tasks WHERE ended IS NOT NULL ORDER BY inbox, ended, id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var e ended
		var inbox string
		if err := rows.Scan(&e.id, &inbox, &e.at); err != nil {
			rows.Close()
			return nil, err
		}
		byInbox[inbox] = append(byInbox[inbox], e)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var drop []string
	for _, list := range byInbox {
		left := len(list)
		for _, e := range list {
			if s.depRefs[e.id] > 0 || s.waiters[e.id] > 0 {
				continue
			}
			if left > count || now.Sub(time.Unix(0, e.at)) > age {
				drop = append(drop, e.id)
				left--
			}
		}
	}
	return drop, nil
}
