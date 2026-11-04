// The store's SQLite database: the schema, the writes of each change and the reads of the tasks memory does not hold.

package jobserver

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// dbName is the database file under a server directory.
const dbName = "tasks.sqlite"

// schema is every table and index of the database. A task is one row of tasks. Its spec, its result, its steps and
// its waits each have their own table. A change of one step writes that step and not the task's spec.
//
//go:embed store_schema.sql
var schema string

// openDB opens the database of a server directory and makes its tables. WAL mode lets readers run while one writer commits.
func openDB(dir string) (*sql.DB, error) {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, dbName)+"?"+q.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("jobserver: database schema: %w", err)
	}
	return db, nil
}

// taskData is the part of a task the data column holds. The other columns hold the ID, the inbox, the state, the
// creation time and the change count. MonitorStatus and err live only in memory.
type taskData struct {
	Name         string         `json:"name,omitempty"`
	Command      []string       `json:"command,omitempty"`
	Deps         []string       `json:"deps,omitempty"`
	Inputs       []string       `json:"inputs,omitempty"`
	Outputs      []string       `json:"outputs,omitempty"`
	Env          []string       `json:"env,omitempty"`
	Dir          string         `json:"dir,omitempty"`
	Key          string         `json:"key,omitempty"`
	Force        bool           `json:"force,omitempty"`
	Started      int64          `json:"started,omitempty"`
	Finished     int64          `json:"finished,omitempty"`
	Attempts     int            `json:"attempts,omitempty"`
	ExitCode     int            `json:"exit_code,omitempty"`
	Error        string         `json:"error,omitempty"`
	Identity     string         `json:"identity,omitempty"`
	CacheOf      string         `json:"cache_of,omitempty"`
	Artifacts    []Artifact     `json:"artifacts,omitempty"`
	LogBytes     int64          `json:"log_bytes,omitempty"`
	Policy       *JobPolicy     `json:"policy,omitempty"`
	CPUSeconds   float64        `json:"cpu_seconds,omitempty"`
	Trigger      string         `json:"trigger,omitempty"`
	Description  string         `json:"description,omitempty"`
	Timeout      time.Duration  `json:"timeout,omitempty"`
	Priority     map[string]int `json:"priority,omitempty"`
	Consumer     string         `json:"consumer,omitempty"`
	ConsumerName string         `json:"consumer_name,omitempty"`
	StepsDropped int            `json:"steps_dropped,omitempty"`
	WaitsDropped int            `json:"waits_dropped,omitempty"`
	Outcome      string         `json:"outcome,omitempty"`
}

// unixNano is the column value of a time. The zero time is zero.
func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// fromUnixNano reads a column value back. Zero is the zero time.
func fromUnixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

func dataOf(j *Job) taskData {
	d := taskData{Name: j.Name, Command: j.Command, Deps: j.Deps, Inputs: j.Inputs, Outputs: j.Outputs, Env: j.Env,
		Dir: j.Dir, Key: j.Key, Force: j.Force, Started: unixNano(j.Started), Finished: unixNano(j.Finished),
		Attempts: j.Attempts, ExitCode: j.ExitCode, Error: j.Error, Identity: j.Identity, CacheOf: j.CacheOf,
		Artifacts: j.Artifacts, LogBytes: j.LogBytes, CPUSeconds: j.CPUSeconds, Trigger: j.Trigger,
		Description: j.Description, Timeout: j.Timeout, Priority: j.Priority, Consumer: j.Consumer,
		ConsumerName: j.ConsumerName, StepsDropped: j.StepsDropped, WaitsDropped: j.WaitsDropped, Outcome: j.Outcome}
	if !j.Policy.Zero() {
		p := j.Policy
		d.Policy = &p
	}
	return d
}

func (d taskData) applyTo(j *Job) {
	j.Name, j.Command, j.Deps, j.Inputs, j.Outputs, j.Env = d.Name, d.Command, d.Deps, d.Inputs, d.Outputs, d.Env
	j.Dir, j.Key, j.Force = d.Dir, d.Key, d.Force
	j.Started, j.Finished = fromUnixNano(d.Started), fromUnixNano(d.Finished)
	j.Attempts, j.ExitCode, j.Error, j.Identity, j.CacheOf = d.Attempts, d.ExitCode, d.Error, d.Identity, d.CacheOf
	j.Artifacts, j.LogBytes, j.CPUSeconds = d.Artifacts, d.LogBytes, d.CPUSeconds
	j.Trigger, j.Description, j.Timeout, j.Priority = d.Trigger, d.Description, d.Timeout, d.Priority
	j.Consumer, j.ConsumerName, j.StepsDropped, j.WaitsDropped, j.Outcome = d.Consumer, d.ConsumerName, d.StepsDropped, d.WaitsDropped, d.Outcome
	if d.Policy != nil {
		j.Policy = d.Policy.normalize()
	}
}

// endedCol is the ended column of a task: when it ended, or NULL while it has not.
func endedCol(j *Job) any {
	if !j.State.Terminal() {
		return nil
	}
	return endedAt(j).UnixNano()
}

// execer is a transaction or the database.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// insertTask writes every part of a new task. A task with the same ID is an error.
func insertTask(tx execer, j *Job) error {
	data, err := json.Marshal(dataOf(j))
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO tasks (id, inbox, state, created, ended, seq, data) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Inbox, string(j.State), unixNano(j.Created), endedCol(j), int64(j.Seq), string(data)); err != nil {
		return err
	}
	if len(j.Spec) > 0 {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO task_specs (id, spec) VALUES (?, ?)`, j.ID, []byte(j.Spec)); err != nil {
			return err
		}
	}
	if len(j.Result) > 0 {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO task_results (id, result) VALUES (?, ?)`, j.ID, []byte(j.Result)); err != nil {
			return err
		}
	}
	return writeDetail(tx, j.ID, &detailRec{Steps: j.Steps, Waits: j.Waits})
}

// updateTask writes the row of a task, with no spec, result, step or wait.
func updateTask(tx execer, j *Job) error {
	data, err := json.Marshal(dataOf(j))
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE tasks SET state = ?, ended = ?, seq = ?, data = ? WHERE id = ?`,
		string(j.State), endedCol(j), int64(j.Seq), string(data), j.ID)
	return err
}

// writeDetail writes the steps and the waits a detail change sets, and deletes the ones it drops.
func writeDetail(tx execer, id string, d *detailRec) error {
	if d == nil {
		return nil
	}
	for _, st := range d.Steps {
		b, err := json.Marshal(st)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO task_steps (id, step, data) VALUES (?, ?, ?)`, id, st.ID, string(b)); err != nil {
			return err
		}
	}
	for _, w := range d.Waits {
		b, err := json.Marshal(w)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO task_waits (id, wait, data) VALUES (?, ?, ?)`, id, w.ID, string(b)); err != nil {
			return err
		}
	}
	for _, n := range d.DropSteps {
		if _, err := tx.Exec(`DELETE FROM task_steps WHERE id = ? AND step = ?`, id, n); err != nil {
			return err
		}
	}
	for _, n := range d.DropWaits {
		if _, err := tx.Exec(`DELETE FROM task_waits WHERE id = ? AND wait = ?`, id, n); err != nil {
			return err
		}
	}
	return nil
}

// deleteTask removes every part of a task.
func deleteTask(tx execer, id string) error {
	for _, table := range []string{"tasks", "task_specs", "task_results", "task_steps", "task_waits"} {
		if _, err := tx.Exec(`DELETE FROM `+table+` WHERE id = ?`, id); err != nil {
			return err
		}
	}
	return nil
}

// inTx runs fn in one transaction and commits it.
func (s *Store) inTx(fn func(tx *sql.Tx) error) error {
	if s.db == nil {
		return errors.New("jobserver: store is closed")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// loadWhere returns the tasks that match where, in the order that order names. A task memory holds is that record.
// Each other one is read from the database once and registered, so every reader gets the same record. Caller holds
// s.mu, for reading or writing.
func (s *Store) loadWhere(where, order string, args ...any) ([]*Job, error) {
	if s.db == nil {
		return nil, errors.New("jobserver: store is closed")
	}
	rows, err := s.db.Query(`SELECT t.id, t.inbox, t.state, t.created, t.seq, t.data FROM tasks t WHERE `+where+` ORDER BY `+order, args...)
	if err != nil {
		return nil, err
	}
	var out []*Job
	fresh := map[string]*Job{}
	for rows.Next() {
		var (
			id, inbox, state string
			created, seq     int64
			data             sql.RawBytes
		)
		if err := rows.Scan(&id, &inbox, &state, &created, &seq, &data); err != nil {
			rows.Close()
			return nil, err
		}
		if j := s.held(id); j != nil {
			out = append(out, j)
			continue
		}
		var d taskData
		if err := json.Unmarshal(data, &d); err != nil {
			rows.Close()
			return nil, fmt.Errorf("jobserver: task %s: %w", id, err)
		}
		j := &Job{ID: id, Inbox: inbox, State: State(state), Created: fromUnixNano(created), Seq: uint64(seq)}
		d.applyTo(j)
		fresh[id] = j
		out = append(out, j)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(fresh) == 0 {
		return out, nil
	}
	if err := s.loadParts(fresh, where, args); err != nil {
		return nil, err
	}
	for i, j := range out {
		if fresh[j.ID] == j {
			j.Monitor = monitorOf(j)
			out[i] = s.register(j)
		}
	}
	return out, nil
}

// taskPart is a table that holds one part of a task, and how one of its rows fills a record.
type taskPart struct {
	table, col, order string
	fill              func(j *Job, b []byte) error
}

var taskParts = []taskPart{
	{"task_specs", "spec", "x.id", func(j *Job, b []byte) error { j.Spec = append(json.RawMessage(nil), b...); return nil }},
	{"task_results", "result", "x.id", func(j *Job, b []byte) error { j.Result = append(json.RawMessage(nil), b...); return nil }},
	{"task_steps", "data", "x.id, x.step", func(j *Job, b []byte) error {
		var st TaskStep
		if err := json.Unmarshal(b, &st); err != nil {
			return err
		}
		j.Steps = append(j.Steps, st)
		return nil
	}},
	{"task_waits", "data", "x.id, x.wait", func(j *Job, b []byte) error {
		var w TaskWait
		if err := json.Unmarshal(b, &w); err != nil {
			return err
		}
		if w.End.IsZero() {
			j.openWaits++
		}
		j.Waits = append(j.Waits, w)
		return nil
	}},
}

// maxListedIDs is the most IDs a part query names one by one. Past it, the query selects the tasks by where again.
const maxListedIDs = 500

// loadParts reads the spec, the result, the steps and the waits of the fresh tasks, which match where. It reads the
// parts of only the fresh tasks when they are few.
func (s *Store) loadParts(fresh map[string]*Job, where string, args []any) error {
	in, inArgs := "SELECT t.id FROM tasks t WHERE "+where, args
	if len(fresh) <= maxListedIDs {
		in, inArgs = "", make([]any, 0, len(fresh))
		for id := range fresh {
			if in != "" {
				in += ", "
			}
			in += "?"
			inArgs = append(inArgs, id)
		}
	}
	for _, p := range taskParts {
		rows, err := s.db.Query(`SELECT x.id, x.`+p.col+` FROM `+p.table+` x WHERE x.id IN (`+in+`) ORDER BY `+p.order, inArgs...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			var b sql.RawBytes
			if err := rows.Scan(&id, &b); err != nil {
				rows.Close()
				return err
			}
			if j := fresh[id]; j != nil {
				if err := p.fill(j, b); err != nil {
					rows.Close()
					return fmt.Errorf("jobserver: task %s: %s: %w", id, p.table, err)
				}
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	return nil
}
