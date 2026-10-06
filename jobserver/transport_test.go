package jobserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIHandleAnswersEveryOperation(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Runner = &fakeRunner{} })
	resp := srv.Handle(Request{Op: OpEnqueue, Spec: &Spec{Command: []string{"x"}, Draft: true}})
	require.True(t, resp.OK, resp.Error)
	id := resp.Job.ID

	require.True(t, srv.Handle(Request{Op: OpGet, ID: id}).OK)
	require.True(t, srv.Handle(Request{Op: OpActivate, ID: id}).OK)
	waitState(t, srv, id, StateCompleted)
	require.True(t, srv.Handle(Request{Op: OpLogs, ID: id}).OK)
	require.True(t, srv.Handle(Request{Op: OpList}).OK)
	require.True(t, srv.Handle(Request{Op: OpStats}).OK)
	require.True(t, srv.Handle(Request{Op: OpPause}).OK)
	require.True(t, srv.Handle(Request{Op: OpResume}).OK)
	require.True(t, srv.Handle(Request{Op: OpInterruptAll}).OK)

	bad := srv.Handle(Request{Op: "nonsense"})
	assert.False(t, bad.OK)
	assert.Contains(t, bad.Error, "unknown op")
	noSpec := srv.Handle(Request{Op: OpEnqueue})
	assert.False(t, noSpec.OK)
	missing := srv.Handle(Request{Op: OpGet, ID: "ghost"})
	assert.False(t, missing.OK)
}

func TestHTTPTransport(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.Runner = &fakeRunner{} })
	front := httptest.NewServer(srv.Mux())
	defer front.Close()

	body, err := json.Marshal(Spec{Command: []string{"x"}, Draft: true})
	require.NoError(t, err)
	res, err := http.Post(front.URL+"/api/jobs", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	var created Response
	require.NoError(t, json.NewDecoder(res.Body).Decode(&created))
	res.Body.Close()
	require.True(t, created.OK, created.Error)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// A bare argv array is accepted too.
	res, err = http.Post(front.URL+"/api/jobs", "application/json", bytes.NewReader([]byte(`["y"]`)))
	require.NoError(t, err)
	var second Response
	require.NoError(t, json.NewDecoder(res.Body).Decode(&second))
	res.Body.Close()
	require.True(t, second.OK, second.Error)
	require.Equal(t, []string{"y"}, second.Job.Command)

	// The generic endpoint takes a request envelope.
	reqBody, err := json.Marshal(Request{Op: OpActivate, ID: created.Job.ID})
	require.NoError(t, err)
	res, err = http.Post(front.URL+"/api/activate", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	waitState(t, srv, created.Job.ID, StateCompleted)

	res, err = http.Get(front.URL + "/api/jobs")
	require.NoError(t, err)
	var listed Response
	require.NoError(t, json.NewDecoder(res.Body).Decode(&listed))
	res.Body.Close()
	assert.Len(t, listed.Jobs, 2)

	res, err = http.Get(front.URL + "/api/jobs/" + created.Job.ID + "/logs")
	require.NoError(t, err)
	log, _ := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Empty(t, log)

	res, err = http.Get(front.URL + "/api/stats")
	require.NoError(t, err)
	res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)

	res, err = http.Get(front.URL + "/api/jobs/ghost")
	require.NoError(t, err)
	res.Body.Close()
	assert.Equal(t, http.StatusNotFound, res.StatusCode)

	res, err = http.Get(front.URL + "/")
	require.NoError(t, err)
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Contains(t, string(page), "go-jobserver")
	assert.Contains(t, string(page), created.Job.ID)

	res, err = http.Get(front.URL + "/job/" + created.Job.ID)
	require.NoError(t, err)
	detail, _ := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Contains(t, string(detail), created.Job.ID)
}

func TestHTTPRejectsAnEmptyBody(t *testing.T) {
	srv := newTestServer(t, nil)
	front := httptest.NewServer(srv.Mux())
	defer front.Close()
	res, err := http.Post(front.URL+"/api/jobs", "application/json", bytes.NewReader(nil))
	require.NoError(t, err)
	defer res.Body.Close()
	assert.Equal(t, http.StatusBadRequest, res.StatusCode)
}

func TestClientOverTheUnixSocket(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, func(c *Config) {
		c.Dir = dir
		c.UnixSocket = filepath.Join(dir, "go-jobserver.sock")
		c.Runner = &fakeRunner{}
	})
	require.FileExists(t, filepath.Join(dir, "go-jobserver.sock"))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Config())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Ping(ctx))

	resp, err := c.Enqueue(ctx, Spec{Command: []string{"x"}, Draft: true})
	require.NoError(t, err)
	require.True(t, resp.OK, resp.Error)
	_, err = c.Do(ctx, Request{Op: OpActivate, ID: resp.Job.ID})
	require.NoError(t, err)
	done, err := c.Wait(ctx, resp.Job.ID, 10*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, StateCompleted, done.State)

	logs, err := c.Logs(ctx, resp.Job.ID, 0, 1<<20)
	require.NoError(t, err)
	assert.True(t, logs.OK)
}

func TestClientOverIPC(t *testing.T) {
	dir := t.TempDir()
	srv := newTestServer(t, func(c *Config) {
		c.Dir = dir
		c.IPC = true
		c.IPCName = IPCName(dir)
		c.Runner = &fakeRunner{}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Config())
	require.NoError(t, err)
	require.NotNil(t, c.ipc, "the client did not use the ipc service")
	defer c.Close()

	resp, err := c.Enqueue(ctx, Spec{Command: []string{"x"}})
	require.NoError(t, err)
	require.True(t, resp.OK, resp.Error)
	done, err := c.Wait(ctx, resp.Job.ID, 10*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, StateCompleted, done.State)

	stats, err := c.Do(ctx, Request{Op: OpStats})
	require.NoError(t, err)
	require.True(t, stats.OK)
	assert.Equal(t, 1, stats.Stats.Total)
}

func TestDialReportsAMissingDaemon(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := Dial(ctx, Config{Dir: dir, IPC: true, IPCName: "go-jobserver-no-such-daemon-" + nameSuffix(dir)})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoDaemon)
}

func TestSpoolTransport(t *testing.T) {
	spool := t.TempDir()
	runner := &fakeRunner{}
	srv := newTestServer(t, func(c *Config) {
		c.SpoolDir = spool
		c.Runner = runner
		c.SpoolInterval = 10 * time.Millisecond
	})
	require.NoError(t, os.WriteFile(filepath.Join(spool, "hello.cmd"), []byte("echo hello\n"), 0o644))
	waitForJobs(t, srv, 1)
	jobs := srv.List()
	require.Len(t, jobs, 1)
	assert.Equal(t, []string{"echo", "hello"}, jobs[0].Command)
	assert.Equal(t, "hello", jobs[0].Name)
	waitState(t, srv, jobs[0].ID, StateCompleted)
	require.FileExists(t, filepath.Join(spool, spoolDoneDir, "hello.cmd"))

	spec := Spec{Command: []string{"one", "two"}, Name: "spec-job", Draft: true}
	body, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(spool, "spec.json"), body, 0o644))
	waitForJobs(t, srv, 2)

	require.NoError(t, os.WriteFile(filepath.Join(spool, "bad.json"), []byte("{not json"), 0o644))
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(spool, spoolFailedDir, "bad.json"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.FileExists(t, filepath.Join(spool, spoolFailedDir, "bad.json.error"))
}

func waitForJobs(t *testing.T, srv *Server, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return len(srv.List()) >= want }, 5*time.Second, 10*time.Millisecond)
}

func TestSpoolAcceptsAnArgvArray(t *testing.T) {
	spool := t.TempDir()
	srv := newTestServer(t, func(c *Config) {
		c.SpoolDir = spool
		c.Runner = &fakeRunner{}
		c.SpoolInterval = 10 * time.Millisecond
	})
	require.NoError(t, os.WriteFile(filepath.Join(spool, "argv.json"), []byte(`["echo","hi"]`), 0o644))
	waitForJobs(t, srv, 1)
	assert.Equal(t, []string{"echo", "hi"}, srv.List()[0].Command)
}

func TestSpecFromFile(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		want    []string
		wantErr bool
	}{
		{"cmd", "echo hi\n", []string{"echo", "hi"}, false},
		{"json spec", `{"command":["make","-j4"]}`, []string{"make", "-j4"}, false},
		{"json array", `["ls","-l"]`, []string{"ls", "-l"}, false},
		{"json string", `"echo quoted"`, []string{"echo", "quoted"}, false},
		{"empty", "   ", nil, true},
		{"no command", `{"name":"x"}`, nil, true},
		{"bad json", `{"command":`, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec, err := specFromFile(c.name, []byte(c.body))
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.want, spec.Command)
		})
	}
}

func TestDecodeSpec(t *testing.T) {
	spec, err := decodeSpec([]byte(`{"command":["a"]}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, spec.Command)
	spec, err = decodeSpec([]byte(`["b"]`))
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, spec.Command)
	spec, err = decodeSpec([]byte("c d\n"))
	require.NoError(t, err)
	assert.Equal(t, []string{"c", "d"}, spec.Command)
	_, err = decodeSpec(nil)
	assert.Error(t, err)
	_, err = decodeSpec([]byte("   \n"))
	assert.Error(t, err)
}

func TestStatusForMapsErrors(t *testing.T) {
	assert.Equal(t, http.StatusOK, statusFor(Response{OK: true}))
	assert.Equal(t, http.StatusNotFound, statusFor(Failure(ErrNotFound)))
	assert.Equal(t, http.StatusBadRequest, statusFor(Failure(ErrNoCommand)))
	assert.Equal(t, http.StatusInternalServerError, statusFor(Failure(io.EOF)))
}

func TestPageHelpers(t *testing.T) {
	assert.Equal(t, "-", shortTime(time.Time{}))
	assert.NotEqual(t, "-", shortTime(time.Now()))
	assert.Equal(t, "-", shortDuration(0))
	assert.Equal(t, "5ms", shortDuration(5*time.Millisecond))
}
