// Retention: a finished job leaves the table, and the journal is rewritten without it.

package jobserver

import (
	"fmt"
	"os"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// recForget removes a job from the table.
const recForget byte = 12

// Forget removes finished jobs and their logs. A job that is not finished stays, and so does a job an unfinished job
// depends on. It answers the IDs it removed.
func (s *Store) Forget(ids []string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	needed := set.New[string]()
	for _, id := range s.order {
		if j := s.jobs[id]; !j.State.Terminal() {
			needed.AddRange(j.Deps...)
		}
	}
	var gone []string
	for _, id := range ids {
		j, ok := s.jobs[id]
		if !ok || !j.State.Terminal() || needed.Contains(id) {
			continue
		}
		if err := s.append(record{kind: recForget, id: id}); err != nil {
			return gone, err
		}
		s.apply(record{kind: recForget, id: id})
		gone = append(gone, id)
	}
	if len(gone) == 0 {
		return nil, nil
	}
	if err := s.sync(); err != nil {
		return gone, err
	}
	for _, id := range gone {
		if w, ok := s.logs[id]; ok {
			_ = w.Close()
			delete(s.logs, id)
		}
		if err := os.Remove(s.LogPath(id)); err != nil && !os.IsNotExist(err) {
			return gone, err
		}
	}
	return gone, nil
}

// Expired answers the finished jobs that ended before cutoff. A blocked job has no finish time, so its creation
// time counts.
func Expired(jobs []*Job, cutoff time.Time) []string {
	var out []string
	for _, j := range jobs {
		if !j.State.Terminal() {
			continue
		}
		at := j.Finished
		if at.IsZero() {
			at = j.Created
		}
		if at.Before(cutoff) {
			out = append(out, j.ID)
		}
	}
	return out
}

// Forgotten is how many jobs left the table since the journal was last rewritten.
func (s *Store) Forgotten() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.forgotten
}

// Compact rewrites the journal from the jobs in the table. The new file
// replaces the one with a rename, so a crash leaves one of those whole.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.journal == nil {
		return fmt.Errorf("jobserver: store is closed")
	}
	tmp, err := os.CreateTemp(s.dir, ".journal-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	fail := func(err error) error {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if s.paused {
		if _, err := tmp.Write(frame(record{kind: recControl, paused: true, created: time.Now().UnixNano()}.encode())); err != nil {
			return fail(err)
		}
	}
	for _, id := range s.order {
		for _, r := range jobRecords(s.jobs[id]) {
			if _, err := tmp.Write(frame(r.encode())); err != nil {
				return fail(err)
			}
		}
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, s.journalPath()); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := s.journal.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(s.journalPath(), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		s.journal = nil
		return err
	}
	s.journal = f
	s.forgotten = 0
	return nil
}

// jobRecords are the records that rebuild one job as it stands.
func jobRecords(j *Job) []record {
	recs := []record{{kind: recCreate, id: j.ID, name: j.Name, command: j.Command, deps: j.Deps, inputs: j.Inputs,
		outputs: j.Outputs, env: j.Env, dir: j.Dir, key: j.Key, force: j.Force, created: j.Created.UnixNano(), state: j.State}}
	if j.Monitor != nil {
		var deadline int64
		if !j.Monitor.Deadline.IsZero() {
			deadline = j.Monitor.Deadline.UnixNano()
		}
		recs = append(recs, record{kind: recMonitor, id: j.ID, monitorKind: j.Monitor.Kind, monitorArgs: j.Monitor.argList(), deadline: deadline})
	}
	if j.Task != nil {
		recs = append(recs, record{kind: recTask, id: j.ID, monitorKind: j.Task.Kind, errMsg: string(j.Task.Data)})
	}
	recs = append(recs, record{kind: recPolicy, id: j.ID, policy: j.Policy})
	if j.Attempts > 0 && !j.Started.IsZero() {
		recs = append(recs, record{kind: recStart, id: j.ID, attempt: j.Attempts, started: j.Started.UnixNano()})
	}
	if !j.Finished.IsZero() {
		recs = append(recs, record{kind: recFinish, id: j.ID, state: j.State, exitCode: j.ExitCode, finished: j.Finished.UnixNano(),
			errMsg: j.Error, logBytes: j.LogBytes, artifacts: j.Artifacts})
	}
	recs = append(recs, record{kind: recState, id: j.ID, state: j.State, errMsg: j.Error})
	if j.Identity != "" || j.CacheOf != "" {
		recs = append(recs, record{kind: recIdentity, id: j.ID, identity: j.Identity, cacheOf: j.CacheOf})
	}
	if j.CPUSeconds != 0 {
		recs = append(recs, record{kind: recCPU, id: j.ID, cpuSeconds: j.CPUSeconds})
	}
	return recs
}
