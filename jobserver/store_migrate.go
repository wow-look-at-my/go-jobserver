// The move from the journal file to the database: a journal that a release before the database wrote is read once.

package jobserver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// journalName is the journal a release before the database wrote. migratedName is what the move renames it to.
const (
	journalName  = "journal.log"
	migratedName = journalName + ".migrated"
)

// journalTable is the task table a journal replays to.
type journalTable struct {
	jobs   map[string]*Job
	order  []string
	paused bool
	cache  map[string]cacheEntry
	seq    uint64
}

// readJournal replays a journal file. A torn or corrupt frame ends the replay: a crash during an append costs that one
// record and nothing else.
func readJournal(path string) (*journalTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	t := &journalTable{jobs: map[string]*Job{}, cache: map[string]cacheEntry{}}
	for {
		payload, err := readFrame(f)
		if errors.Is(err, io.EOF) || err != nil {
			break
		}
		rec, err := decodeRecord(payload)
		if err != nil {
			break
		}
		t.apply(rec)
	}
	return t, nil
}

// apply folds one journal record into the table.
func (t *journalTable) apply(r record) {
	switch r.kind {
	case recCreate:
		j := &Job{ID: r.id, Name: r.name, Command: r.command, Deps: r.deps, Inputs: r.inputs, Outputs: r.outputs,
			Env: r.env, Dir: r.dir, Key: r.key, Force: r.force, State: r.state, Created: time.Unix(0, r.created)}
		if j.State == "" {
			j.State = StateActive
		}
		if _, ok := t.jobs[j.ID]; !ok {
			t.order = append(t.order, j.ID)
		}
		t.jobs[j.ID] = j
	case recControl:
		t.paused = r.paused
	case recCache:
		var e cacheEntry
		if json.Unmarshal([]byte(r.blob), &e) == nil && e.Identity != "" {
			t.cache[e.Identity] = e
		}
	}
	j, ok := t.jobs[r.id]
	if !ok {
		return
	}
	switch r.kind {
	case recStart:
		j.State, j.Attempts, j.Started = StateRunning, r.attempt, time.Unix(0, r.started)
	case recFinish:
		j.State, j.ExitCode, j.Finished = r.state, r.exitCode, time.Unix(0, r.finished)
		j.Error, j.LogBytes, j.Artifacts = r.errMsg, r.logBytes, r.artifacts
		closeOpen(j, j.Finished)
	case recIdentity:
		j.Identity, j.CacheOf = r.identity, r.cacheOf
	case recState:
		j.State, j.Error = r.state, r.errMsg
	case recDeps:
		j.Deps = r.replaceDeps
	case recPolicy:
		j.Policy = r.policy
	case recCPU:
		j.CPUSeconds = r.cpuSeconds
	case recResult:
		j.Result = json.RawMessage(r.blob)
	case recTask:
		var m taskMeta
		if json.Unmarshal([]byte(r.blob), &m) == nil {
			m.applyTo(j)
		}
	case recDetail:
		var d detailRec
		if json.Unmarshal([]byte(r.blob), &d) == nil {
			d.merge(j)
		}
	}
	t.seq++
	j.Seq = t.seq
	if j.Identity != "" && j.State.succeeded() {
		if old, ok := t.cache[j.Identity]; !ok || old.ID == j.ID || !old.At.After(endedAt(j)) {
			t.cache[j.Identity] = cacheEntry{Identity: j.Identity, ID: j.ID, Artifacts: j.Artifacts, At: endedAt(j)}
		}
	}
}

// migrate moves a journal into the database in one transaction, then renames the journal. A crash before the rename
// leaves the journal, and the next open moves it again from the start.
func (s *Store) migrate() error {
	path := filepath.Join(s.dir, journalName)
	t, err := readJournal(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	err = s.inTx(func(tx *sql.Tx) error {
		for _, table := range []string{"tasks", "task_specs", "task_results", "task_steps", "task_waits", "cache", "meta"} {
			if _, err := tx.Exec(`DELETE FROM ` + table); err != nil {
				return err
			}
		}
		for _, id := range t.order {
			if err := insertTask(tx, t.jobs[id]); err != nil {
				return err
			}
		}
		for _, e := range t.cache {
			if err := putCache(tx, e); err != nil {
				return err
			}
		}
		_, err := tx.Exec(`INSERT INTO meta (key, value) VALUES ('paused', ?)`, strconv.FormatBool(t.paused))
		return err
	})
	if err != nil {
		return err
	}
	return os.Rename(path, filepath.Join(s.dir, migratedName))
}
