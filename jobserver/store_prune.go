// Retention drops ended tasks from memory and compacts them out of the journal.

package jobserver

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/wow-look-at-my/go-containers/set"
	"os"
	"path/filepath"
	"sort"
	"time"
)


func (s *Store) compactPath() string { return filepath.Join(s.dir, journalName+".compact") }

// hold marks a caller that blocks on a task, so no prune drops it. It fails for a task the store does not hold.
func (s *Store) hold(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id = CanonicalID(id)
	if _, ok := s.jobs[id]; !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	s.waiters[id]++
	return nil
}

// release ends a hold.
func (s *Store) release(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id = CanonicalID(id)
	if s.waiters[id] <= 1 {
		delete(s.waiters, id)
		return
	}
	s.waiters[id]--
}

// endedAt is when a job ended. A job that ended with no run has only its creation time.
func endedAt(j *Job) time.Time {
	if j.Finished.IsZero() {
		return j.Created
	}
	return j.Finished
}

// Prune drops each ended task that ended more than age ago, or that its inbox holds past count, oldest first.
// It keeps a task that a task which has not ended depends on, and a task a caller waits on.
// It compacts the journal when it drops a task, and returns the IDs it dropped.
func (s *Store) Prune(now time.Time, age time.Duration, count int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil
	}
	needed := set.New[string]()
	for _, ids := range s.idx.live {
		for id := range ids.All() {
			for _, d := range s.jobs[id].Deps {
				needed.Add(d)
			}
		}
	}
	var drop []string
	for _, ids := range s.idx.members {
		ended := make([]*Job, 0, ids.Len())
		for id := range ids.All() {
			if j := s.jobs[id]; j.State.Terminal() {
				ended = append(ended, j)
			}
		}
		sort.Slice(ended, func(a, b int) bool {
			ta, tb := endedAt(ended[a]), endedAt(ended[b])
			if !ta.Equal(tb) {
				return ta.Before(tb)
			}
			return ended[a].ID < ended[b].ID
		})
		left := len(ended)
		for _, j := range ended {
			if needed.Contains(j.ID) || s.waiters[j.ID] > 0 {
				continue
			}
			if left > count || now.Sub(endedAt(j)) > age {
				drop = append(drop, j.ID)
				left--
			}
		}
	}
	if len(drop) == 0 {
		return nil, nil
	}
	s.prunes++
	gone := set.New[string]()
	var errs []error
	for _, id := range drop {
		gone.Add(id)
		delete(s.jobs, id)
		s.idx.remove(id)
		if err := s.closeLog(id); err != nil {
			errs = append(errs, err)
		}
		if err := os.Remove(s.LogPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	order := s.order[:0]
	for _, id := range s.order {
		if !gone.Contains(id) {
			order = append(order, id)
		}
	}
	clear(s.order[len(order):])
	s.order = order
	if err := s.compactLocked(); err != nil {
		errs = append(errs, fmt.Errorf("compact: %w", err))
	}
	s.bump()
	return drop, errors.Join(errs...)
}

// compactLocked writes the journal again from the table: a new file, synced,
// then renamed over the one. A crash before the rename leaves the journal
// whole. Caller holds s.mu.
func (s *Store) compactLocked() (err error) {
	if s.journal == nil {
		return errors.New("jobserver: store is closed")
	}
	tmp := s.compactPath()
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()
	w := bufio.NewWriter(f)
	if s.paused {
		if _, err := w.Write(frame(record{kind: recControl, paused: true, created: time.Now().UnixNano()}.encode())); err != nil {
			return err
		}
	}
	for _, id := range s.order {
		recs, err := snapshot(s.jobs[id])
		if err != nil {
			return err
		}
		for _, r := range recs {
			if _, err := w.Write(frame(r.encode())); err != nil {
				return err
			}
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if s.compactHook != nil {
		if err := s.compactHook("written"); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, s.journalPath()); err != nil {
		return err
	}
	// The new file is the journal now. A failure past here must not remove it.
	if d, derr := os.Open(s.dir); derr == nil {
		_ = d.Sync()
		d.Close()
	}
	old := s.journal
	s.journal = f
	return old.Close()
}

// snapshot is the records that rebuild one job as the table holds it.
func snapshot(j *Job) ([]record, error) {
	created := StateActive
	if j.State == StateDraft {
		created = StateDraft
	}
	recs := []record{{
		kind: recCreate, id: j.ID, name: j.Name, command: j.Command, deps: j.Deps,
		inputs: j.Inputs, outputs: j.Outputs, env: j.Env, dir: j.Dir, key: j.Key,
		force: j.Force, created: j.Created.UnixNano(), state: created,
	}}
	if !j.Policy.Zero() {
		recs = append(recs, record{kind: recPolicy, id: j.ID, policy: j.Policy})
	}
	meta := taskMeta{Inbox: j.Inbox, Trigger: j.Trigger, Description: j.Description, Timeout: j.Timeout, Priority: j.Priority, Spec: j.Spec}
	if meta.Inbox != "" || meta.Trigger != "" || meta.Description != "" || meta.Timeout != 0 || len(meta.Priority) > 0 || len(meta.Spec) > 0 {
		blob, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		recs = append(recs, record{kind: recTask, id: j.ID, blob: string(blob)})
	}
	if len(j.Steps) > 0 || len(j.Waits) > 0 {
		blob, err := json.Marshal(detailRec{Steps: j.Steps, Waits: j.Waits})
		if err != nil {
			return nil, err
		}
		recs = append(recs, record{kind: recDetail, id: j.ID, blob: string(blob)})
	}
	if j.Attempts > 0 || !j.Started.IsZero() {
		recs = append(recs, record{kind: recStart, id: j.ID, attempt: j.Attempts, started: j.Started.UnixNano()})
	}
	switch {
	case j.State.Terminal() && !j.Finished.IsZero():
		recs = append(recs, record{kind: recFinish, id: j.ID, state: j.State, exitCode: j.ExitCode,
			finished: j.Finished.UnixNano(), errMsg: j.Error, logBytes: j.LogBytes, artifacts: j.Artifacts})
	case j.State != created || j.Error != "":
		recs = append(recs, record{kind: recState, id: j.ID, state: j.State, errMsg: j.Error})
	}
	if j.Identity != "" || j.CacheOf != "" {
		recs = append(recs, record{kind: recIdentity, id: j.ID, identity: j.Identity, cacheOf: j.CacheOf})
	}
	if len(j.Result) > 0 {
		recs = append(recs, record{kind: recResult, id: j.ID, blob: string(j.Result)})
	}
	if j.CPUSeconds != 0 {
		recs = append(recs, record{kind: recCPU, id: j.ID, cpuSeconds: j.CPUSeconds})
	}
	return recs, nil
}
