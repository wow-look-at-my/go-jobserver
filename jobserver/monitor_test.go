package jobserver

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testEnv returns an env whose Sleep never waits, so a test runs every check back to back.
func testEnv(t *testing.T, doc string) *MonitorEnv {
	t.Helper()
	defs, err := ParseMonitorDefs([]byte(doc))
	require.NoError(t, err)
	return &MonitorEnv{Defs: defs, Sleep: func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }}
}

func TestPollRequestHoldsAfterNotYetAndAnError(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/status/db", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		switch hits.Add(1) {
		case 1:
			_, _ = w.Write([]byte(`{"state":"starting"}`))
		case 2:
			http.Error(w, "boom", http.StatusBadGateway)
		default:
			_, _ = w.Write([]byte(`{"state":"ok"}`))
		}
	}))
	defer srv.Close()
	env := testEnv(t, goodDefs)
	env.Vars = map[string]any{"base": srv.URL}
	env.Transports = map[string]http.RoundTripper{"api": http.DefaultTransport}

	var out bytes.Buffer
	var seen []MonitorStatus
	err := env.Watch(context.Background(), MonitorSpec{Kind: "status-ok", Args: map[string]string{"svc": "db"}}, &out, nil, func(s MonitorStatus) { seen = append(seen, s) })
	require.NoError(t, err)
	require.Len(t, seen, 3)
	assert.Equal(t, "state starting", seen[0].LastDetail)
	assert.Contains(t, seen[1].LastError, "answered 502", "a bad status is a check error, not a \"not yet\"")
	assert.Equal(t, 1, seen[1].ErrorsInARow)
	assert.Empty(t, seen[2].LastError, "a clean check clears the error")
	assert.Equal(t, "state ok", seen[2].LastDetail)
	assert.Contains(t, out.String(), "check 1: not yet: state starting")
	assert.Contains(t, out.String(), "check 2: ERROR:")
	assert.Contains(t, out.String(), "check 3: holds: state ok")
}

func TestPollRequestNeedsItsTransport(t *testing.T) {
	env := testEnv(t, goodDefs)
	ctx, cancel := context.WithCancel(context.Background())
	var first MonitorStatus
	err := env.Watch(ctx, MonitorSpec{Kind: "status-ok", Args: map[string]string{"svc": "db"}}, &bytes.Buffer{}, nil, func(s MonitorStatus) {
		first = s
		cancel()
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, first.LastError, `no transport named "api"`)
}

func TestPollArgvExitRule(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flag")
	env := testEnv(t, goodDefs)
	n := 0
	err := env.Watch(context.Background(), MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": path}}, &bytes.Buffer{}, func(int) {}, func(s MonitorStatus) {
		n++
		assert.Equal(t, "exit 1", s.LastDetail, "exit 1 means not yet")
		require.NoError(t, os.WriteFile(path, nil, 0o644))
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the report after the holding check is the last one")
}

func TestPollArgvOtherExitIsAnError(t *testing.T) {
	env := testEnv(t, `<monitors><monitor name="m"><poll><argv>sh</argv><argv>-c</argv><argv>exit 3</argv></poll></monitor></monitors>`)
	ctx, cancel := context.WithCancel(context.Background())
	var got MonitorStatus
	err := env.Watch(ctx, MonitorSpec{Kind: "m"}, &bytes.Buffer{}, nil, func(s MonitorStatus) {
		got = s
		if s.ErrorsInARow == 2 {
			cancel()
		}
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, got.LastError, "exited 3")
	assert.Equal(t, 2, got.Checks, "the error was retried")
}

func TestPollArgvMissingProgramIsAnError(t *testing.T) {
	env := testEnv(t, `<monitors><monitor name="m"><poll><argv>/no/such/program</argv></poll></monitor></monitors>`)
	ctx, cancel := context.WithCancel(context.Background())
	var got MonitorStatus
	_ = env.Watch(ctx, MonitorSpec{Kind: "m"}, &bytes.Buffer{}, nil, func(s MonitorStatus) { got = s; cancel() })
	assert.Contains(t, got.LastError, "start /no/such/program")
}

func TestPollArgvTimeoutIsAnError(t *testing.T) {
	env := testEnv(t, `<monitors><monitor name="m"><poll timeout="50ms"><argv>sleep</argv><argv>5</argv></poll></monitor></monitors>`)
	ctx, cancel := context.WithCancel(context.Background())
	var got MonitorStatus
	_ = env.Watch(ctx, MonitorSpec{Kind: "m"}, &bytes.Buffer{}, nil, func(s MonitorStatus) { got = s; cancel() })
	assert.Contains(t, got.LastError, "did not finish within 50ms")
}

func TestStreamHoldsOnAMatchingLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	require.NoError(t, os.WriteFile(path, []byte("one\ntwo\nREADY\nafter\n"), 0o644))
	env := testEnv(t, goodDefs)
	var pids []int
	var out bytes.Buffer
	var last MonitorStatus
	err := env.Watch(context.Background(), MonitorSpec{Kind: "line", Args: map[string]string{"path": path}}, &out, func(pid int) { pids = append(pids, pid) }, func(s MonitorStatus) { last = s })
	require.NoError(t, err)
	assert.Len(t, pids, 1, "the stream process is handed to the caller to govern")
	assert.Equal(t, 3, last.Checks, "every line up to the match was tested")
	assert.Contains(t, out.String(), "holds: READY")
}

// A stream that exits without a match is a check error, and it restarts.
func TestStreamRestartsAfterItExits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	require.NoError(t, os.WriteFile(path, []byte("one\n"), 0o644))
	env := testEnv(t, goodDefs)
	starts := 0
	err := env.Watch(context.Background(), MonitorSpec{Kind: "line", Args: map[string]string{"path": path}}, &bytes.Buffer{}, func(int) {
		starts++
		if starts == 2 {
			require.NoError(t, os.WriteFile(path, []byte("READY\n"), 0o644))
		}
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, starts)
}

func TestWatchEndsAtItsDeadline(t *testing.T) {
	env := testEnv(t, goodDefs)
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	env.Now = func() time.Time { return now }
	env.Sleep = func(context.Context, time.Duration) bool { now = now.Add(time.Second); return true }
	var out bytes.Buffer
	err := env.Watch(context.Background(), MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": "/no/such/file"}, Deadline: now.Add(3 * time.Second)}, &out, nil, nil)
	assert.ErrorIs(t, err, ErrMonitorDeadline)
	assert.Contains(t, out.String(), "deadline 2026-10-01T00:00:03Z passed after 3 checks")
}

func TestWatchRefusesABadSpec(t *testing.T) {
	env := testEnv(t, goodDefs)
	assert.ErrorIs(t, env.Watch(context.Background(), MonitorSpec{Kind: "nope"}, &bytes.Buffer{}, nil, nil), ErrUnknownMonitorKind)
}

func TestCappedSaysWhatItDropped(t *testing.T) {
	c := capped{max: 3}
	_, _ = c.Write([]byte("abcdef"))
	assert.Equal(t, "abc\n[3 more bytes dropped]", c.String())
}
