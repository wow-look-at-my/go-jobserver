package jobserver

import (
	"bytes"
	"os"
	"reflect"
	"testing"
	"time"
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
		if err != nil {
			t.Fatalf("kind %d: decodeRecord: %v", want.kind, err)
		}
		want.state = stateOrDefault(want.state, want.kind)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("kind %d round trip:\n got %+v\nwant %+v", want.kind, got, want)
		}
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
	for name, payload := range cases {
		if _, err := decodeRecord(payload); err == nil {
			t.Errorf("%s: decodeRecord accepted a malformed record", name)
		}
	}
}

func TestFrameDetectsCorruption(t *testing.T) {
	f := frame(record{kind: recControl}.encode())
	if _, err := readFrame(bytes.NewReader(f)); err != nil {
		t.Fatalf("readFrame on an intact frame: %v", err)
	}
	f[len(f)-1] ^= 0xff
	if _, err := readFrame(bytes.NewReader(f)); err == nil {
		t.Fatal("readFrame accepted a frame with a flipped payload byte")
	}
}

func TestReadFrameReportsTornTail(t *testing.T) {
	f := frame(record{kind: recControl}.encode())
	for _, cut := range []int{1, frameHeaderSize - 1, len(f) - 1} {
		if _, err := readFrame(bytes.NewReader(f[:cut])); err == nil {
			t.Errorf("readFrame accepted a frame cut at %d of %d bytes", cut, len(f))
		}
	}
	if _, err := readFrame(bytes.NewReader(nil)); err != os.ErrClosed && err.Error() != "EOF" {
		t.Errorf("readFrame on an empty reader = %v, want EOF", err)
	}
}

func TestOpenStoreCutsATornTail(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	j, err := st.Create(&Spec{Command: []string{"true"}, ID: "j-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Finish(j.ID, StateCompleted, 0, "", nil, 0); err != nil {
		t.Fatal(err)
	}
	path := st.journalPath()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// The daemon dies in the middle of an append.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(frame(record{kind: recState, id: "j-1", state: StateRunning}.encode())[:6]); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("OpenStore after a torn tail: %v", err)
	}
	defer st2.Close()
	got, ok := st2.Job("j-1")
	if !ok {
		t.Fatal("the job did not survive the reopen")
	}
	if got.State != StateCompleted {
		t.Fatalf("state = %s, want %s", got.State, StateCompleted)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= 0 {
		t.Fatal("the journal is empty after the cut")
	}
	if err := st2.SetPaused(true); err != nil {
		t.Fatalf("appending after the cut: %v", err)
	}
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}
	st3, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("OpenStore after appending past the cut: %v", err)
	}
	defer st3.Close()
	if !st3.Paused() {
		t.Fatal("the record appended after the cut was lost")
	}
}

// journalRecordSize measures the records the torn-tail test writes before the
// cut, so a size assertion stays honest when the codec changes.
func journalRecordSize() int {
	create := record{kind: recCreate, id: "j-1", command: []string{"true"}, state: StateActive}
	finish := record{kind: recFinish, id: "j-1", state: StateCompleted}
	return len(create.encode()) + len(finish.encode())
}

func TestOpenStoreKeepsJobs(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	j, err := st.Create(&Spec{
		ID: "j-a", Name: "build", Command: []string{"make"},
		Deps: []string{}, Inputs: []string{"in"}, Outputs: []string{"out"},
		Env: []string{"A=1"}, Dir: "/work", Key: "k", Force: true, Draft: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if j.State != StateDraft {
		t.Fatalf("state = %s, want draft", j.State)
	}
	if err := st.Activate("j-a"); err != nil {
		t.Fatal(err)
	}
	if err := st.Start("j-a"); err != nil {
		t.Fatal(err)
	}
	arts := []Artifact{{Path: "out", Size: 3, SHA256: "sha256:x"}}
	if err := st.Finish("j-a", StateCompleted, 0, "", arts, 12); err != nil {
		t.Fatal(err)
	}
	if err := st.SetIdentity("j-a", "sha256:id", "j-0"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	got, ok := st2.Job("j-a")
	if !ok {
		t.Fatal("the job did not survive the reopen")
	}
	if got.Name != "build" || got.Key != "k" || !got.Force || got.Dir != "/work" {
		t.Fatalf("job fields did not survive: %+v", got)
	}
	if len(got.Outputs) != 1 || got.Outputs[0] != "out" {
		t.Fatalf("outputs = %v", got.Outputs)
	}
	if got.State != StateCompleted || got.Attempts != 1 {
		t.Fatalf("state = %s attempts = %d", got.State, got.Attempts)
	}
	if len(got.Artifacts) != 1 || got.Artifacts[0].SHA256 != "sha256:x" {
		t.Fatalf("artifacts = %+v", got.Artifacts)
	}
	if got.Identity != "sha256:id" || got.CacheOf != "j-0" {
		t.Fatalf("identity = %q cacheOf = %q", got.Identity, got.CacheOf)
	}
	if got.LogBytes != 12 {
		t.Fatalf("log bytes = %d, want 12", got.LogBytes)
	}
}

func TestOpenStoreFailsAJobThatWasRunning(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(&Spec{ID: "j-r", Command: []string{"sleep", "100"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Start("j-r"); err != nil {
		t.Fatal(err)
	}
	w, err := st.LogWriter("j-r")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("half a line")); err != nil {
		t.Fatal(err)
	}
	w.Sync()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	got, _ := st2.Job("j-r")
	if got.State != StateFailed {
		t.Fatalf("state = %s, want failed after a restart", got.State)
	}
	if got.Error == "" {
		t.Fatal("the failed job carries no reason")
	}
	out, err := st2.ReadLog("j-r", 0, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "half a line" {
		t.Fatalf("log = %q, want the bytes written before the restart", out)
	}
}

func TestOpenStoreTruncatesALogPastTheJournal(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(&Spec{ID: "j-l", Command: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Finish("j-l", StateCompleted, 0, "", nil, 5); err != nil {
		t.Fatal(err)
	}
	w, err := st.LogWriter("j-l")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	got, _ := st2.Job("j-l")
	if got.LogBytes != 5 {
		t.Fatalf("log bytes = %d, want the 5 the journal recorded", got.LogBytes)
	}
	out, err := st2.ReadLog("j-l", 0, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "01234" {
		t.Fatalf("log = %q, want it cut to the recorded length", out)
	}
}

func TestStoreRejectsBadInput(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Create(&Spec{ID: "j-1"}); err != ErrNoCommand {
		t.Fatalf("create with no command = %v, want %v", err, ErrNoCommand)
	}
	if _, err := st.Create(&Spec{ID: "j-1", Command: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(&Spec{ID: "j-1", Command: []string{"true"}}); err == nil {
		t.Fatal("create accepted a duplicate id")
	}
	if _, err := st.Create(&Spec{ID: "j-2", Command: []string{"true"}, Deps: []string{"nope"}}); err == nil {
		t.Fatal("create accepted a missing dependency")
	}
	if err := st.AddDep("j-1", "j-1"); err == nil {
		t.Fatal("AddDep accepted a self dependency")
	}
	if err := st.Activate("nope"); err == nil {
		t.Fatal("activate accepted an unknown id")
	}
}

func TestStoreRejectsCycles(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, id := range []string{"a", "b", "c"} {
		if _, err := st.Create(&Spec{ID: id, Command: []string{"true"}, Draft: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.AddDep("b", "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.AddDep("c", "b"); err != nil {
		t.Fatal(err)
	}
	if err := st.AddDep("a", "c"); err == nil {
		t.Fatal("AddDep accepted a cycle across three jobs")
	}
	if err := st.SetDeps("b", []string{"c"}); err == nil {
		t.Fatal("SetDeps accepted a cycle across three jobs")
	}
	if err := st.AddDep("b", "a"); err != nil {
		t.Fatalf("adding a dependency a job already has: %v", err)
	}
}

func TestStoreAddDepKeepsOrderAndRejectsFinished(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, id := range []string{"a", "b", "c"} {
		if _, err := st.Create(&Spec{ID: id, Command: []string{"true"}, Draft: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.AddDep("c", "b"); err != nil {
		t.Fatal(err)
	}
	if err := st.AddDep("c", "a"); err != nil {
		t.Fatal(err)
	}
	j, _ := st.Job("c")
	if got := j.Deps; len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("deps = %v, want them sorted", got)
	}
	if err := st.Close(); err == nil {
	}
	st2, err := OpenStore(st.Dir())
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	j2, _ := st2.Job("c")
	if len(j2.Deps) != 2 {
		t.Fatalf("deps did not survive a reopen: %v", j2.Deps)
	}
	if err := st2.Finish("c", StateCompleted, 0, "", nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := st2.AddDep("c", "a"); err == nil {
		t.Fatal("AddDep accepted a dependency on a finished job")
	}
}

func TestStorePauseRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if st.Paused() {
		t.Fatal("a fresh store reports paused")
	}
	if err := st.SetPaused(true); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if !st2.Paused() {
		t.Fatal("the paused flag did not survive a reopen")
	}
}

func TestIdentityIndexPrefersTheLatestSuccess(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, id := range []string{"j-1", "j-2", "j-3"} {
		if _, err := st.Create(&Spec{ID: id, Command: []string{"true"}}); err != nil {
			t.Fatal(err)
		}
	}
	st.SetIdentity("j-1", "sha256:x", "")
	st.Finish("j-1", StateCompleted, 0, "", nil, 0)
	st.SetIdentity("j-2", "sha256:x", "")
	st.Finish("j-2", StateFailed, 1, "boom", nil, 0)
	st.SetIdentity("j-3", "sha256:y", "")
	st.Finish("j-3", StateCompleted, 0, "", nil, 0)
	idx := st.IdentityIndex()
	if idx["sha256:x"] != "j-1" {
		t.Fatalf("sha256:x maps to %q, want the successful j-1", idx["sha256:x"])
	}
	if idx["sha256:y"] != "j-3" {
		t.Fatalf("sha256:y maps to %q, want j-3", idx["sha256:y"])
	}
}

func TestStoreJobsReturnsCopies(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Create(&Spec{ID: "j-1", Command: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	jobs := st.Jobs()
	jobs[0].Command[0] = "mutated"
	again, _ := st.Job("j-1")
	if again.Command[0] != "true" {
		t.Fatal("a caller mutated the stored job through the returned slice")
	}
}

func TestLogWriterAppendsAndSyncs(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w, err := st.LogWriter("j-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("one\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("two\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if w.N() != 8 {
		t.Fatalf("N() = %d, want 8", w.N())
	}
	if st.LogBytes("j-1") != 8 {
		t.Fatalf("LogBytes = %d, want 8", st.LogBytes("j-1"))
	}
	out, err := st.ReadLog("j-1", 4, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "two\n" {
		t.Fatalf("ReadLog at an offset = %q", out)
	}
	if err := st.RemoveLog("j-1"); err != nil {
		t.Fatal(err)
	}
	if st.LogBytes("j-1") != 0 {
		t.Fatal("RemoveLog left the file behind")
	}
}

func TestJournalRecordsSurviveManyJobs(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		id := NewID(func(string) bool { return false })
		if _, err := st.Create(&Spec{ID: id, Command: []string{"true"}, Inputs: []string{"a"}, Outputs: []string{"b"}}); err != nil {
			t.Fatal(err)
		}
		if err := st.Start(id); err != nil {
			t.Fatal(err)
		}
		if err := st.Finish(id, StateCompleted, 0, "", []Artifact{{Path: "b", Size: 1, SHA256: "sha256:z"}}, 7); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if got := len(st2.Jobs()); got != n {
		t.Fatalf("replayed %d jobs, want %d", got, n)
	}
	for _, j := range st2.Jobs() {
		if j.State != StateCompleted || j.Attempts != 1 || j.LogBytes != 7 {
			t.Fatalf("job %s replayed as %+v", j.ID, j)
		}
	}
}

func TestApplyIgnoresRecordsForUnknownJobs(t *testing.T) {
	st := &Store{jobs: map[string]*Job{}, logs: map[string]*LogWriter{}}
	st.apply(record{kind: recStart, id: "ghost"})
	st.apply(record{kind: recFinish, id: "ghost", state: StateCompleted})
	st.apply(record{kind: recIdentity, id: "ghost"})
	st.apply(record{kind: recState, id: "ghost", state: StateFailed})
	st.apply(record{kind: recDeps, id: "ghost"})
	if len(st.jobs) != 0 {
		t.Fatalf("a record for an unknown job created %d entries", len(st.jobs))
	}
	st.apply(record{kind: recControl, paused: true})
	if !st.Paused() {
		t.Fatal("the control record did not set the paused flag")
	}
}

func TestStoreOrderTracksCreation(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, id := range []string{"c", "a", "b"} {
		if _, err := st.Create(&Spec{ID: id, Command: []string{"true"}}); err != nil {
			t.Fatal(err)
		}
	}
	got := st.Order()
	want := []string{"c", "a", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Order = %v, want %v", got, want)
		}
	}
	if _, err := st.LogWriter("j-x"); err != nil {
		t.Fatal(err)
	}
	if got := time.Now(); got.IsZero() {
		t.Fatal("unreachable")
	}
}
