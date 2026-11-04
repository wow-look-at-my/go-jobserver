// A writer of the journal format that releases before the database wrote, so a test can make a journal to move.

package jobserver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// legacyRecords is the records that rebuild one job, as the journal of an older release holds them.
func legacyRecords(j *Job) ([]record, error) {
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
	if len(j.Steps) > 0 || len(j.Waits) > 0 || j.StepsDropped > 0 || j.WaitsDropped > 0 || j.Outcome != "" || j.ConsumerName != "" {
		stepsDropped, waitsDropped, outcome, consumerName := j.StepsDropped, j.WaitsDropped, j.Outcome, j.ConsumerName
		blob, err := json.Marshal(detailRec{Steps: j.Steps, Waits: j.Waits, StepsDropped: &stepsDropped, WaitsDropped: &waitsDropped,
			Outcome: &outcome, ConsumerName: &consumerName})
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

// writeLegacyJournal writes the records of jobs, and then extra, as the journal of an older release in dir.
func writeLegacyJournal(t testing.TB, dir string, jobs []*Job, extra ...record) {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, journalName))
	require.NoError(t, err)
	defer f.Close()
	for _, j := range jobs {
		recs, err := legacyRecords(j)
		require.NoError(t, err)
		for _, r := range recs {
			_, err := f.Write(frame(r.encode()))
			require.NoError(t, err)
		}
	}
	for _, r := range extra {
		_, err := f.Write(frame(r.encode()))
		require.NoError(t, err)
	}
}
