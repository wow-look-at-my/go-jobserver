// daemon is a running server and the spool on its default inbox.

package main

import (
	"errors"
	"time"

	"github.com/wow-look-at-my/go-jobserver/jobserver"
	"github.com/wow-look-at-my/go-jobserver/spool"
)

// daemon is a running server and the spool on its default inbox.
type daemon struct {
	srv   *jobserver.Server
	spool *spool.Spool
}

// openDaemon starts a server and opens a spool in spoolDir on its default inbox. An empty spoolDir opens no spool.
func openDaemon(cfg jobserver.Config, spoolDir string, interval time.Duration) (*daemon, error) {
	srv, err := jobserver.New(cfg)
	if err != nil {
		return nil, err
	}
	if err := srv.Start(); err != nil {
		return nil, err
	}
	d := &daemon{srv: srv}
	if spoolDir == "" {
		return d, nil
	}
	sp, err := spool.Open(spoolDir, srv.DefaultInbox(), spool.WithLogf(cfg.Logf), spool.WithInterval(interval))
	if err != nil {
		return nil, errors.Join(err, srv.Close())
	}
	d.spool = sp
	return d, nil
}

// Close closes the spool before the server, so a task the server stops leaves its file for the next start.
func (d *daemon) Close() error {
	var err error
	if d.spool != nil {
		err = d.spool.Close()
	}
	return errors.Join(err, d.srv.Close())
}
