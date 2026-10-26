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
	run, err := NewInbox[runInbox](srv, "run")
	require.NoError(t, err)
	mon, _, err := srv.Watch(fileMonitor(filepath.Join(t.TempDir(), "never")), "")
	require.NoError(t, err)
	waitState(t, srv, mon.ID, StateRunning)
	after, err := OnSuccess(mon, &run.Inbox, work(1))
	require.NoError(t, err)
	front := httptest.NewServer(srv.Mux())
	t.Cleanup(front.Close)

	g := getView(t, front.URL, "/api/graph", http.StatusOK)
	require.NotNil(t, g.Graph)
	assert.Equal(t, "0 running, 1 monitor watching; 1 waits on 1 monitor", g.Summary)
	require.Len(t, g.Graph.Nodes, 2)
	kinds := map[string]string{}
	for _, n := range g.Graph.Nodes {
		kinds[n.ID] = n.Kind
	}
	assert.Equal(t, map[string]string{mon.ID: "monitor", after.ID: "run"}, kinds)
	assert.Equal(t, []GraphEdge{{From: mon.ID, To: after.ID, Label: "gates"}}, g.Graph.Edges)

	m := getView(t, front.URL, "/api/monitors", http.StatusOK)
	require.Len(t, m.Monitors, 1)
	assert.Equal(t, mon.ID, m.Monitors[0].ID)
	assert.Equal(t, []string{after.ID}, m.Monitors[0].Waiting)

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
