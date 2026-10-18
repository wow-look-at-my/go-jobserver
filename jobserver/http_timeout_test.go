package jobserver

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shortenHTTPTimeout makes one of the server timeouts short enough that a
// quiet client costs a test a fraction of a second.
func shortenHTTPTimeout(t *testing.T, timeout *time.Duration) {
	t.Helper()
	old := *timeout
	*timeout = 200 * time.Millisecond
	t.Cleanup(func() { *timeout = old })
}

// httpDialers starts a server on both HTTP transports and returns a dial
// function for each.
func httpDialers(t *testing.T) map[string]func() (net.Conn, error) {
	t.Helper()
	socket := filepath.Join(shortDir(t), "js.sock")
	var logs lineLog
	newTestServer(t, func(c *Config) {
		c.HTTPAddr = "127.0.0.1:0"
		c.UnixSocket = socket
		c.Logf = logs.logf
	})
	lines := logs.containing("dashboard on http://")
	require.Len(t, lines, 1)
	_, addr, found := strings.Cut(lines[0], "http://")
	require.True(t, found)
	return map[string]func() (net.Conn, error){
		"tcp":  func() (net.Conn, error) { return net.Dial("tcp", addr) },
		"unix": func() (net.Conn, error) { return net.Dial("unix", socket) },
	}
}

// assertServerCloses fails the test unless the server closes the connection
// well before the deadline. from is conn, or a reader that wraps it.
func assertServerCloses(t *testing.T, conn net.Conn, from io.Reader) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err := io.Copy(io.Discard, from)
	assert.NoError(t, err, "the server never closed the connection")
}

func TestHTTPDropsAClientThatSendsNothing(t *testing.T) {
	shortenHTTPTimeout(t, &httpReadHeaderTimeout)
	for name, dial := range httpDialers(t) {
		t.Run(name, func(t *testing.T) {
			conn, err := dial()
			require.NoError(t, err)
			defer conn.Close()

			assertServerCloses(t, conn, conn)
		})
	}
}

func TestHTTPDropsAnIdleKeepAliveClient(t *testing.T) {
	shortenHTTPTimeout(t, &httpIdleTimeout)
	for name, dial := range httpDialers(t) {
		t.Run(name, func(t *testing.T) {
			conn, err := dial()
			require.NoError(t, err)
			defer conn.Close()
			_, err = io.WriteString(conn, "GET /api/stats HTTP/1.1\r\nHost: jobserver\r\n\r\n")
			require.NoError(t, err)
			in := bufio.NewReader(conn)
			res, err := http.ReadResponse(in, nil)
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, res.Body)
			require.NoError(t, err)
			require.NoError(t, res.Body.Close())

			assertServerCloses(t, conn, in)
		})
	}
}

func TestHTTPDropsAClientThatStallsInABody(t *testing.T) {
	shortenHTTPTimeout(t, &httpReadTimeout)
	for name, dial := range httpDialers(t) {
		t.Run(name, func(t *testing.T) {
			conn, err := dial()
			require.NoError(t, err)
			defer conn.Close()
			_, err = io.WriteString(conn, "POST /api/jobs HTTP/1.1\r\nHost: jobserver\r\nContent-Length: 1000\r\n\r\n{")
			require.NoError(t, err)

			assertServerCloses(t, conn, conn)
		})
	}
}
