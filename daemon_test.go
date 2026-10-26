package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func TestSpoolDirFollowsTheFlags(t *testing.T) {
	g := &globals{dir: "/srv/goj"}
	assert.Equal(t, filepath.Join("/srv/goj", "spool"), g.spoolDir(g.config()))
	g.spool = "/elsewhere"
	assert.Equal(t, "/elsewhere", g.spoolDir(g.config()))
	g.noSpool = true
	assert.Empty(t, g.spoolDir(g.config()))
}

func TestDaemonWithNoSpoolOpensNone(t *testing.T) {
	dir := shortDir(t)
	cfg := jobserver.DefaultConfig(dir)
	cfg.HTTPAddr, cfg.UnixSocket, cfg.IPC = "", "", false
	d, err := openDaemon(cfg, "", time.Second)
	require.NoError(t, err)
	assert.Nil(t, d.spool)
	require.NoError(t, d.Close())
	assert.NoDirExists(t, filepath.Join(dir, "spool"))
}
