package jobserver

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordRoundTrip(t *testing.T) {
	recs := []record{
		{
			kind: recCreate, id: "j-1", name: "build", command: []string{"make", "-j4"},
			deps: []string{"j-0"}, inputs: []string{"src"}, outputs: []string{"bin"},
			env: []string{"CGO=0"}, dir: "/work", key: "k", force: true,
			created: 12345, state: StateDraft,
		},
		{kind: recStart, id: "j-1", attempt: 3, started: 999},
		{
			kind: recFinish, id: "j-1", state: StateCompleted, exitCode: 0,
			finished: 1000, errMsg: "ok", logBytes: 42,
			artifacts: []Artifact{{Path: "bin/app", Size: 10, SHA256: "sha256:aa"}, {Path: "bin/lib", Size: 20, SHA256: "sha256:bb"}},
		},
		{kind: recIdentity, id: "j-1", identity: "sha256:cc", cacheOf: "j-0"},
		{kind: recState, id: "j-1", state: StateFailed, errMsg: "boom"},
		{kind: recControl, paused: true, created: 7},
		{kind: recDeps, id: "j-1", replaceDeps: []string{"j-0", "j-2"}},
		{kind: recCreate, id: "j-2", command: []string{"true"}, state: StateActive},
	}
	for _, want := range recs {
		payload := want.encode()
		got, err := decodeRecord(payload)
		require.Nil(t, err)

		want.state = stateOrDefault(want.state, want.kind)
		assert.Equal(t, want, got)
	}
}

// stateOrDefault fills the state a record kind implies, which the create and
// start records leave empty in the table above.
func stateOrDefault(state State, kind byte) State {
	if state == "" && kind == recCreate {
		return StateActive
	}
	return state
}

func TestDecodeRecordRejectsGarbage(t *testing.T) {
	cases := map[string][]byte{
		"empty":         {},
		"unknown kind":  {200},
		"short string":  {recState, 10, 'a'},
		"trailing data": append(record{kind: recControl}.encode(), 0xff),
	}
	for _, payload := range cases {
		_, err := decodeRecord(payload)
		assert.NotNil(t, err)

	}
}

func TestFrameDetectsCorruption(t *testing.T) {
	f := frame(record{kind: recControl}.encode())
	_, err := readFrame(bytes.NewReader(f))
	require.NoError(t, err)

	f[len(f)-1] ^= 0xff
	_, err = readFrame(bytes.NewReader(f))
	require.ErrorIs(t, err, errTornFrame)
}

func TestReadFrameReportsTornTail(t *testing.T) {
	f := frame(record{kind: recControl}.encode())
	for _, cut := range []int{1, frameHeaderSize - 1, len(f) - 1} {
		_, err := readFrame(bytes.NewReader(f[:cut]))
		assert.ErrorIs(t, err, errTornFrame, "a frame cut at %d of %d bytes was accepted", cut, len(f))
	}
	_, err := readFrame(bytes.NewReader(nil))
	assert.ErrorIs(t, err, io.EOF)
}

func TestReadFrameRejectsAnOversizedLength(t *testing.T) {
	f := frame(record{kind: recControl}.encode())
	f[0], f[1], f[2], f[3] = 0xff, 0xff, 0xff, 0xff
	_, err := readFrame(bytes.NewReader(f))
	assert.ErrorIs(t, err, errTornFrame)
}

func TestStoreOpenOnAnEmptyDirectory(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	assert.Empty(t, st.Jobs())
	assert.NoFileExists(t, st.LogPath("nobody"))
}

func TestOpenStoreCutsATornTail(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.Nil(t, err)

	j, err := st.Create(&Spec{Command: []string{"true"}, ID: "j-1"})
	require.Nil(t, err)

	require.NoError(t, st.Finish(j.ID, StateCompleted, 0, "", nil, 0))

	path := st.journalPath()
	require.NoError(t, st.Close())

	// The daemon dies in the middle of an append.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.Nil(t, err)

	_, err = f.Write(frame(record{kind: recState, id: "j-1", state: StateRunning}.encode())[:6])
	require.Nil(t, err)

	f.Close()

	st2, err := OpenStore(dir)
	require.Nil(t, err)

	defer st2.Close()
	got, ok := st2.Job("j-1")
	require.True(t, ok)

	require.Equal(t, StateCompleted, got.State)

	info, err := os.Stat(path)
	require.Nil(t, err)

	require.Greater(t, info.Size(), int64(0))

	require.NoError(t, st2.SetPaused(true))

	require.NoError(t, st2.Close())

	st3, err := OpenStore(dir)
	require.Nil(t, err)

	defer st3.Close()
	require.True(t, st3.Paused())
}

func TestOpenStoreKeepsJobs(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.Nil(t, err)

	j, err := st.Create(&Spec{
		ID: "j-a", Name: "build", Command: []string{"make"},
		Deps: []string{}, Inputs: []string{"in"}, Outputs: []string{"out"},
		Env: []string{"A=1"}, Dir: "/work", Key: "k", Force: true, Draft: true,
	})
	require.Nil(t, err)

	require.Equal(t, StateDraft, j.State)

	require.NoError(t, st.Activate("j-a"))

	require.NoError(t, st.Start("j-a"))

	arts := []Artifact{{Path: "out", Size: 3, SHA256: "sha256:x"}}
	require.NoError(t, st.Finish("j-a", StateCompleted, 0, "", arts, 12))

	require.NoError(t, st.SetIdentity("j-a", "sha256:id", "j-0"))

	require.NoError(t, st.Close())

	st2, err := OpenStore(dir)
	require.Nil(t, err)

	defer st2.Close()
	got, ok := st2.Job("j-a")
	require.True(t, ok)

	require.False(t, got.Name != "build" || got.Key != "k" || !got.Force || got.Dir != "/work")

	require.False(t, len(got.Outputs) != 1 || got.Outputs[0] != "out")

	require.False(t, got.State != StateCompleted || got.Attempts != 1)

	require.False(t, len(got.Artifacts) != 1 || got.Artifacts[0].SHA256 != "sha256:x")

	require.False(t, got.Identity != "sha256:id" || got.CacheOf != "j-0")

	require.Equal(t, int64(12), got.LogBytes)

}

func TestOpenStoreFailsAJobThatWasRunning(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.Nil(t, err)

	_, err = st.Create(&Spec{ID: "j-r", Command: []string{"sleep", "100"}})
	require.Nil(t, err)

	require.NoError(t, st.Start("j-r"))

	w, err := st.LogWriter("j-r")
	require.Nil(t, err)

	_, err = w.Write([]byte("half a line"))
	require.Nil(t, err)

	w.Sync()
	require.NoError(t, st.Close())

	st2, err := OpenStore(dir)
	require.Nil(t, err)

	defer st2.Close()
	got, _ := st2.Job("j-r")
	require.Equal(t, StateFailed, got.State)

	require.NotEqual(t, "", got.Error)

	out, err := st2.ReadLog("j-r", 0, 1024)
	require.Nil(t, err)

	require.Equal(t, "half a line", string(out))

}

func TestOpenStoreTruncatesALogPastTheJournal(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.Nil(t, err)

	_, err = st.Create(&Spec{ID: "j-l", Command: []string{"true"}})
	require.Nil(t, err)

	require.NoError(t, st.Finish("j-l", StateCompleted, 0, "", nil, 5))

	w, err := st.LogWriter("j-l")
	require.Nil(t, err)

	_, err = w.Write([]byte("0123456789"))
	require.Nil(t, err)

	require.NoError(t, st.Close())

	st2, err := OpenStore(dir)
	require.Nil(t, err)

	defer st2.Close()
	got, _ := st2.Job("j-l")
	require.Equal(t, int64(5), got.LogBytes)

	out, err := st2.ReadLog("j-l", 0, 1024)
	require.Nil(t, err)

	require.Equal(t, "01234", string(out))

}

func TestStoreRejectsBadInput(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.Nil(t, err)

	defer st.Close()
	_, err = st.Create(&Spec{ID: "j-1"})
	require.Equal(t, ErrNoCommand, err)

	_, err = st.Create(&Spec{ID: "j-1", Command: []string{"true"}})
	require.Nil(t, err)

	_, err = st.Create(&Spec{ID: "j-1", Command: []string{"true"}})
	require.NotNil(t, err)

	_, err = st.Create(&Spec{ID: "j-2", Command: []string{"true"}, Deps: []string{"nope"}})
	require.NotNil(t, err)

	err = st.AddDep("j-1", "j-1")
	require.NotNil(t, err)

	err = st.Activate("nope")
	require.NotNil(t, err)

}

func TestStoreRejectsCycles(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.Nil(t, err)

	defer st.Close()
	for _, id := range []string{"a", "b", "c"} {
		_, err = st.Create(&Spec{ID: id, Command: []string{"true"}, Draft: true})
		require.Nil(t, err)

	}
	require.NoError(t, st.AddDep("b", "a"))

	require.NoError(t, st.AddDep("c", "b"))

	err = st.AddDep("a", "c")
	require.NotNil(t, err)

	err = st.SetDeps("b", []string{"c"})
	require.NotNil(t, err)

	require.NoError(t, st.AddDep("b", "a"))

}

func TestStoreAddDepKeepsOrderAndRejectsFinished(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.Nil(t, err)

	defer st.Close()
	for _, id := range []string{"a", "b", "c"} {
		_, err = st.Create(&Spec{ID: id, Command: []string{"true"}, Draft: true})
		require.Nil(t, err)

	}
	require.NoError(t, st.AddDep("c", "b"))

	require.NoError(t, st.AddDep("c", "a"))

	j, _ := st.Job("c")
	got := j.Deps
	require.False(t, len(got) != 2 || got[0] != "a" || got[1] != "b")

	if err := st.Close(); err == nil {
	}
	st2, err := OpenStore(st.Dir())
	require.Nil(t, err)

	defer st2.Close()
	j2, _ := st2.Job("c")
	require.Equal(t, 2, len(j2.Deps))

	require.NoError(t, st2.Finish("c", StateCompleted, 0, "", nil, 0))

	err = st2.AddDep("c", "a")
	require.NotNil(t, err)

}

func TestStorePauseRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.Nil(t, err)

	require.False(t, st.Paused())

	require.NoError(t, st.SetPaused(true))

	require.NoError(t, st.Close())

	st2, err := OpenStore(dir)
	require.Nil(t, err)

	defer st2.Close()
	require.True(t, st2.Paused())

}

func TestIdentityIndexPrefersTheLatestSuccess(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.Nil(t, err)

	defer st.Close()
	for _, id := range []string{"j-1", "j-2", "j-3"} {
		_, err = st.Create(&Spec{ID: id, Command: []string{"true"}})
		require.Nil(t, err)

	}
	st.SetIdentity("j-1", "sha256:x", "")
	st.Finish("j-1", StateCompleted, 0, "", nil, 0)
	st.SetIdentity("j-2", "sha256:x", "")
	st.Finish("j-2", StateFailed, 1, "boom", nil, 0)
	st.SetIdentity("j-3", "sha256:y", "")
	st.Finish("j-3", StateCompleted, 0, "", nil, 0)
	idx := st.IdentityIndex()
	require.Equal(t, "j-1", idx["sha256:x"])

	require.Equal(t, "j-3", idx["sha256:y"])

}

func TestStoreJobsReturnsCopies(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.Nil(t, err)

	defer st.Close()
	_, err = st.Create(&Spec{ID: "j-1", Command: []string{"true"}})
	require.Nil(t, err)

	jobs := st.Jobs()
	jobs[0].Command[0] = "mutated"
	again, _ := st.Job("j-1")
	require.Equal(t, "true", again.Command[0])

}

func TestLogWriterAppendsAndSyncs(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.Nil(t, err)

	defer st.Close()
	w, err := st.LogWriter("j-1")
	require.Nil(t, err)

	_, err = w.Write([]byte("one\n"))
	require.Nil(t, err)

	_, err = w.Write([]byte("two\n"))
	require.Nil(t, err)

	require.NoError(t, w.Sync())

	require.Equal(t, int64(8), w.N())

	require.Equal(t, int64(8), st.LogBytes("j-1"))

	out, err := st.ReadLog("j-1", 4, 1024)
	require.Nil(t, err)

	require.Equal(t, "two\n", string(out))

	require.NoError(t, st.RemoveLog("j-1"))

	require.Equal(t, int64(0), st.LogBytes("j-1"))

}

func TestJournalRecordsSurviveManyJobs(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	require.Nil(t, err)

	const n = 50
	for i := 0; i < n; i++ {
		id := NewID(func(string) bool { return false })
		_, err = st.Create(&Spec{ID: id, Command: []string{"true"}, Inputs: []string{"a"}, Outputs: []string{"b"}})
		require.Nil(t, err)

		require.NoError(t, st.Start(id))

		require.NoError(t, st.Finish(id, StateCompleted, 0, "", []Artifact{{Path: "b", Size: 1, SHA256: "sha256:z"}}, 7))

	}
	require.NoError(t, st.Close())

	st2, err := OpenStore(dir)
	require.Nil(t, err)

	defer st2.Close()
	got := len(st2.Jobs())
	require.Equal(t, n, got)

	for _, j := range st2.Jobs() {
		require.False(t, j.State != StateCompleted || j.Attempts != 1 || j.LogBytes != 7)

	}
}

func TestApplyIgnoresRecordsForUnknownJobs(t *testing.T) {
	st := &Store{jobs: map[string]*Job{}, logs: map[string]*LogWriter{}}
	st.apply(record{kind: recStart, id: "ghost"})
	st.apply(record{kind: recFinish, id: "ghost", state: StateCompleted})
	st.apply(record{kind: recIdentity, id: "ghost"})
	st.apply(record{kind: recState, id: "ghost", state: StateFailed})
	st.apply(record{kind: recDeps, id: "ghost"})
	require.Equal(t, 0, len(st.jobs))

	st.apply(record{kind: recControl, paused: true})
	require.True(t, st.Paused())

}

func TestStoreOrderTracksCreation(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.Nil(t, err)

	defer st.Close()
	for _, id := range []string{"c", "a", "b"} {
		_, err = st.Create(&Spec{ID: id, Command: []string{"true"}})
		require.Nil(t, err)

	}
	got := st.Order()
	want := []string{"c", "a", "b"}
	require.Equal(t, want, got)
}
