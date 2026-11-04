package jobserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nodeIDs(g JobGraph) []string {
	out := make([]string, 0, len(g.Nodes))
	for _, n := range g.Nodes {
		out = append(out, n.ID)
	}
	return out
}

func TestOpenInReturnsTheOpenRecordsOfAnInbox(t *testing.T) {
	srv := newTestServer(t, nil)
	require.NoError(t, srv.store.SetPaused(true))
	addEnded(t, srv.store, "x")
	open := addQueued(t, srv.store, "x")
	addQueued(t, srv.store, "y")
	got := srv.store.OpenIn("x")
	require.Len(t, got, 1)
	rec, ok := srv.store.Job(open)
	require.True(t, ok)
	assert.Same(t, rec, got[0], "OpenIn returns the one record of the task")
}

func TestOpenGraphShowsOpenTasksAndTheirEndedDepsOnly(t *testing.T) {
	srv := newTestServer(t, nil)
	require.NoError(t, srv.store.SetPaused(true))
	gone := addEnded(t, srv.store, "x")
	dep := addEnded(t, srv.store, "x")
	open := addQueued(t, srv.store, "x", dep)

	assert.ElementsMatch(t, []string{gone, dep, open}, nodeIDs(srv.Graph()), "Graph reads every task, the ended ones from the database")
	g := srv.OpenGraph()
	assert.ElementsMatch(t, []string{dep, open}, nodeIDs(g), "an ended task that no open task needs stays in the database")
	assert.Equal(t, []GraphEdge{{From: dep, To: open, Label: "before"}}, g.Edges)
}
