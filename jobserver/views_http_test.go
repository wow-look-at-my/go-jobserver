package jobserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getView(t *testing.T, base, path string, wantStatus int) Response {
	t.Helper()
	res, err := http.Get(base + path)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, wantStatus, res.StatusCode, path)
	var r Response
	require.NoError(t, json.NewDecoder(res.Body).Decode(&r))
	return r
}

// The HTTP view routes answer with the same monitor, wait and summary data as the CLI.
func TestViewRoutesOverHTTP(t *testing.T) {
	srv := newMonitorServer(t, &fakeRunner{})
	spec := MonitorSpec{Kind: "file-exists", Args: map[string]string{"path": filepath.Join(t.TempDir(), "never")}}
	mon, _, err := srv.Enqueue(Spec{ID: "mon", Monitor: &spec})
	require.NoError(t, err)
	waitState(t, srv, mon.ID, StateRunning)
	after, _, err := srv.Enqueue(Spec{ID: "after", Command: []string{"deploy"}, Deps: []string{mon.ID}})
	require.NoError(t, err)
	front := httptest.NewServer(srv.Mux())
	t.Cleanup(front.Close)

	g := getView(t, front.URL, "/api/graph", http.StatusOK)
	require.NotNil(t, g.Graph)
	assert.Equal(t, "0 running, 1 monitor watching; 1 waits on 1 monitor", g.Summary)
	assert.Len(t, g.Graph.Nodes, 2)
	assert.Len(t, g.Graph.Edges, 1)

	m := getView(t, front.URL, "/api/monitors", http.StatusOK)
	require.Len(t, m.Monitors, 1)
	assert.Equal(t, mon.ID, m.Monitors[0].ID)

	k := getView(t, front.URL, "/api/kinds", http.StatusOK)
	assert.NotEmpty(t, k.Kinds)

	w := getView(t, front.URL, "/api/why/"+after.ID, http.StatusOK)
	require.Len(t, w.Waits, 1)
	assert.Equal(t, WaitMonitor, w.Waits[0].Kind)
	assert.Equal(t, mon.ID, w.Waits[0].ID)

	getView(t, front.URL, "/api/why/no-such-job", http.StatusNotFound)

	s := getView(t, front.URL, "/api/scheduler", http.StatusOK)
	require.NotNil(t, s.Scheduler)
	assert.Len(t, s.Scheduler.Monitors, 1, "the monitor's lease is listed apart from the slots")
}
