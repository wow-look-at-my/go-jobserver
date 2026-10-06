package jobserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wow-look-at-my/go-ipc"
)

// ErrNoDaemon is returned when no daemon answers for the given directory.
var ErrNoDaemon = errors.New("go-jobserver: no daemon is running")

// connectTimeout bounds how long a client waits for a go-ipc service.
const connectTimeout = 2 * time.Second

// A Client is a connection to a running daemon, over its file socket or over
// its go-ipc service.
type Client struct {
	http    *http.Client
	base    string
	ipc     *ipc.Client
	ipcName string
}

// Dial connects to the daemon for a directory.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Dir == "" {
		cfg.Dir = DefaultDir()
	}
	if cfg.UnixSocket == "" {
		cfg.UnixSocket = filepath.Join(cfg.Dir, "go-jobserver.sock")
	}
	if _, err := os.Stat(cfg.UnixSocket); err == nil {
		c := &Client{base: "http://go-jobserver"}
		c.http = &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", cfg.UnixSocket)
				},
			},
		}
		if err := c.Ping(ctx); err != nil {
			return nil, err
		}
		return c, nil
	}
	name := cfg.IPCName
	if name == "" {
		name = IPCName(cfg.Dir)
	}
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	c, err := ipc.Connect(connectCtx, name)
	if err != nil {
		return nil, fmt.Errorf("%w for %s: %v", ErrNoDaemon, cfg.Dir, err)
	}
	return &Client{ipc: c, ipcName: name}, nil
}

// Close ends the connection.
func (c *Client) Close() error {
	if c.ipc != nil {
		return c.ipc.Close()
	}
	if c.http != nil {
		c.http.CloseIdleConnections()
	}
	return nil
}

// Do sends one API call and returns its answer.
func (c *Client) Do(ctx context.Context, req Request) (Response, error) {
	if c.ipc != nil {
		payload, err := json.Marshal(req)
		if err != nil {
			return Response{}, err
		}
		typ, body, err := c.ipc.Call(ctx, ipcCallType, payload)
		if err != nil {
			return Response{}, err
		}
		if typ != ipcCallType {
			return Response{}, fmt.Errorf("go-jobserver: ipc reply type %d", typ)
		}
		var resp Response
		if err := json.Unmarshal(body, &resp); err != nil {
			return Response{}, err
		}
		return resp, nil
	}
	body, err := json.Marshal(req)
	if err != nil {
		return Response{}, err
	}
	url := c.base + "/api/" + req.Op
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	res, err := c.http.Do(httpReq)
	if err != nil {
		return Response{}, err
	}
	defer res.Body.Close()
	out, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return Response{}, err
	}
	var resp Response
	if err := json.Unmarshal(out, &resp); err != nil {
		return Response{}, fmt.Errorf("go-jobserver: %s: %w", strings.TrimSpace(string(out)), err)
	}
	if !resp.OK && resp.Error == "" {
		resp.Error = res.Status
	}
	return resp, nil
}

// Ping checks that a daemon answers.
func (c *Client) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := c.Do(ctx, Request{Op: OpStats})
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	return nil
}

// Enqueue adds a job.
func (c *Client) Enqueue(ctx context.Context, spec Spec) (Response, error) {
	return c.Do(ctx, Request{Op: OpEnqueue, Spec: &spec})
}

// Get returns one job.
func (c *Client) Get(ctx context.Context, id string) (Response, error) {
	return c.Do(ctx, Request{Op: OpGet, ID: id})
}

// List returns every job.
func (c *Client) List(ctx context.Context) (Response, error) {
	return c.Do(ctx, Request{Op: OpList})
}

// Logs returns a job's output.
func (c *Client) Logs(ctx context.Context, id string, offset int64, max int) (Response, error) {
	return c.Do(ctx, Request{Op: OpLogs, ID: id, Offset: offset, Max: max})
}

// Wait polls a job until it reaches a terminal state and returns it.
func (c *Client) Wait(ctx context.Context, id string, interval time.Duration) (*Job, error) {
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	for {
		resp, err := c.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if !resp.OK {
			return nil, errors.New(resp.Error)
		}
		if resp.Job != nil && resp.Job.State.Terminal() {
			return resp.Job, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}
