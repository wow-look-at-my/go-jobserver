// Monitors: a task kind that checks a declared condition until it holds. A task that must wait for the condition triggers on the monitor task.

package jobserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	apidsl "github.com/wow-look-at-my/api-dsl"
)

// MonitorSpec names a monitor kind and its args. It is the whole of a monitor job's work.
type MonitorSpec struct {
	Kind string            `json:"kind"`
	Args map[string]string `json:"args,omitempty"`
	// Deadline, when set, ends the monitor as failed once it passes.
	Deadline time.Time `json:"deadline,omitzero"`
}

func (m MonitorSpec) clone() MonitorSpec {
	out := m
	if m.Args != nil {
		out.Args = make(map[string]string, len(m.Args))
		for k, v := range m.Args {
			out.Args[k] = v
		}
	}
	return out
}

// Key is the monitor's dedup key: the kind and its args in a fixed order.
func (m MonitorSpec) Key() string {
	names := make([]string, 0, len(m.Args))
	for k := range m.Args {
		names = append(names, k)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("monitor:")
	b.WriteString(m.Kind)
	for _, k := range names {
		fmt.Fprintf(&b, " %s=%s", k, m.Args[k])
	}
	return b.String()
}

// MonitorStatus is what the last checks of a running monitor found.
type MonitorStatus struct {
	Checks        int       `json:"checks"`
	WatchingSince time.Time `json:"watching_since,omitzero"`
	LastCheck     time.Time `json:"last_check,omitzero"`
	// LastDetail is what the last check that ran saw, in words.
	LastDetail string `json:"last_detail,omitempty"`
	// LastError is the last check error. It clears when a check runs clean.
	LastError    string `json:"last_error,omitempty"`
	ErrorsInARow int    `json:"errors_in_a_row,omitempty"`
}

// ErrMonitorDeadline ends a monitor whose deadline passed.
var ErrMonitorDeadline = errors.New("jobserver: monitor deadline passed")

// Output bounds for one check.
const (
	maxCheckOutput = 64 << 10
	maxCheckBody   = 1 << 20
)

// MonitorEnv is what monitor checks run with.
type MonitorEnv struct {
	// Defs are the monitor kinds. SetDefs replaces them while monitors run.
	Defs *MonitorDefs
	mu   sync.RWMutex
	// Transports are the named HTTP transports a <request transport="..."> may use. They carry credentials, so the XML never does.
	Transports map[string]http.RoundTripper
	// Vars override the document's vars by name.
	Vars map[string]any
	// Now and Sleep are the clock. Sleep returns false when ctx ended first.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) bool
}

// SetDefs replaces the monitor kinds. A monitor already watching keeps the kind it started with.
func (e *MonitorEnv) SetDefs(d *MonitorDefs) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Defs = d
}

// Definitions returns the monitor kinds in use now.
func (e *MonitorEnv) Definitions() *MonitorDefs {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.Defs
}

func (e *MonitorEnv) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

func (e *MonitorEnv) sleep(ctx context.Context, d time.Duration) bool {
	if e.Sleep != nil {
		return e.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// checkResult is one check: the condition held or not, what it saw, or why it could not tell.
type checkResult struct {
	holds  bool
	detail string
	err    error
}

// Watch checks the monitor until its condition holds, which returns nil. A check error is logged and retried on
// the same cadence; it never reads as "not yet" and never ends the watch. It returns ctx.Err() when cancelled and
// ErrMonitorDeadline at the deadline. started receives each process it starts, so the caller can govern it.
func (e *MonitorEnv) Watch(ctx context.Context, spec MonitorSpec, out io.Writer, started func(pid int), report func(MonitorStatus)) error {
	defs := e.Definitions()
	if err := defs.Validate(spec); err != nil {
		return err
	}
	k, _ := defs.Kind(spec.Kind)
	st := MonitorStatus{WatchingSince: e.now()}
	data := map[string]any{"arg": stringMap(spec.Args), "var": apidsl.MergeVars(defs.vars, e.Vars)}
	for {
		if !spec.Deadline.IsZero() && !e.now().Before(spec.Deadline) {
			fmt.Fprintf(out, "%s deadline %s passed after %d checks\n", stamp(e.now()), spec.Deadline.UTC().Format(time.RFC3339), st.Checks)
			return fmt.Errorf("%w (%s)", ErrMonitorDeadline, spec.Deadline.UTC().Format(time.RFC3339))
		}
		// .now is the check's clock in Unix seconds, so a condition can compare against a time argument.
		data["now"] = e.now().Unix()
		var r checkResult
		if k.Shape == "stream" {
			r = e.stream(ctx, k, data, out, started, &st, report)
		} else {
			r = e.poll(ctx, k, data, started)
			st.Checks++
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		st.LastCheck = e.now()
		switch {
		case r.err != nil:
			st.LastError = r.err.Error()
			st.ErrorsInARow++
			fmt.Fprintf(out, "%s check %d: ERROR: %s\n", stamp(st.LastCheck), st.Checks, r.err)
		default:
			st.LastError, st.ErrorsInARow = "", 0
			st.LastDetail = r.detail
			verdict := "not yet"
			if r.holds {
				verdict = "holds"
			}
			fmt.Fprintf(out, "%s check %d: %s: %s\n", stamp(st.LastCheck), st.Checks, verdict, r.detail)
		}
		if report != nil {
			report(st)
		}
		if r.err == nil && r.holds {
			return nil
		}
		if !e.sleep(ctx, k.Every) {
			return ctx.Err()
		}
	}
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func stringMap(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// poll runs one check of a <poll>.
func (e *MonitorEnv) poll(ctx context.Context, k *MonitorKind, data map[string]any, started func(pid int)) checkResult {
	ctx, cancel := context.WithTimeout(ctx, k.Timeout)
	defer cancel()
	if len(k.requests) > 0 {
		return e.requests(ctx, k, data)
	}
	return e.argvCheck(ctx, k, data, started)
}

// requests sends each request in order. One request puts its result at .status, .response and .body. A named one
// also puts it at .req.<name>. Any failed request is the check's error.
func (e *MonitorEnv) requests(ctx context.Context, k *MonitorKind, data map[string]any) checkResult {
	named := map[string]any{}
	var last map[string]any
	for _, r := range k.requests {
		res, err := e.request(ctx, r, data)
		if err != nil {
			if r.name != "" {
				err = fmt.Errorf("request %q: %w", r.name, err)
			}
			return checkResult{err: err}
		}
		if r.name != "" {
			named[r.name] = res
		}
		last = res
	}
	extra := map[string]any{"req": named}
	if len(k.requests) == 1 {
		for key, v := range last {
			extra[key] = v
		}
	}
	return e.judge(k, withResult(data, extra), fmt.Sprintf("HTTP %v", last["status"]))
}

func (e *MonitorEnv) request(ctx context.Context, r *monitorRequest, data map[string]any) (map[string]any, error) {
	url, err := apidsl.RenderString(r.url, data)
	if err != nil {
		return nil, err
	}
	body, err := apidsl.RenderString(r.body, data)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, r.method, strings.TrimSpace(url), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	for _, h := range r.headers {
		v, err := apidsl.RenderString(h[1], data)
		if err != nil {
			return nil, err
		}
		req.Header.Set(h[0], v)
	}
	client := &http.Client{}
	if r.transport != "" {
		t, ok := e.Transports[r.transport]
		if !ok {
			return nil, fmt.Errorf("no transport named %q is registered", r.transport)
		}
		client.Transport = t
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, maxCheckBody))
	if err != nil {
		return nil, fmt.Errorf("read the response: %w", err)
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, fmt.Errorf("%s %s answered %d: %s", r.method, req.URL.Redacted(), res.StatusCode, clipN(string(raw), 200))
	}
	var parsed any = string(raw)
	var v any
	if json.Unmarshal(raw, &v) == nil {
		parsed = v
	}
	return map[string]any{"status": res.StatusCode, "response": parsed, "body": string(raw)}, nil
}

func (e *MonitorEnv) argvCheck(ctx context.Context, k *MonitorKind, data map[string]any, started func(pid int)) checkResult {
	argv, err := renderArgv(k.argv, data)
	if err != nil {
		return checkResult{err: err}
	}
	var stdout, stderr capped
	stdout.max, stderr.max = maxCheckOutput, maxCheckOutput
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	prepareProcess(cmd)
	cmd.WaitDelay = waitDelay
	if err := cmd.Start(); err != nil {
		return checkResult{err: fmt.Errorf("start %s: %w", argv[0], err)}
	}
	if started != nil {
		started(cmd.Process.Pid)
	}
	err = cmd.Wait()
	if ctx.Err() != nil {
		return checkResult{err: fmt.Errorf("%s did not finish within %s", argv[0], k.Timeout)}
	}
	code := 0
	var exitErr *exec.ExitError
	switch {
	case errors.As(err, &exitErr):
		code = exitErr.ExitCode()
	case err != nil && !errors.Is(err, exec.ErrWaitDelay):
		return checkResult{err: err}
	}
	d := withResult(data, map[string]any{"exit": code, "stdout": stdout.String(), "stderr": stderr.String()})
	if k.until != "" {
		return e.judge(k, d, fmt.Sprintf("exit %d", code))
	}
	switch code {
	case 0, 1:
		r := e.judge(k, d, fmt.Sprintf("exit %d", code))
		r.holds = code == 0
		return r
	}
	return checkResult{err: fmt.Errorf("%s exited %d, which is neither 0 (holds) nor 1 (not yet): %s", argv[0], code, clipN(strings.TrimSpace(stderr.String()), 200))}
}

// stream runs one <stream> process and tests each stdout line. It returns when a line holds, the process exits,
// or ctx ends. Each line counts as a check.
func (e *MonitorEnv) stream(ctx context.Context, k *MonitorKind, data map[string]any, out io.Writer, started func(pid int), st *MonitorStatus, report func(MonitorStatus)) checkResult {
	argv, err := renderArgv(k.argv, data)
	if err != nil {
		return checkResult{err: err}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var stderr capped
	stderr.max = maxCheckOutput
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stderr = &stderr
	prepareProcess(cmd)
	cmd.WaitDelay = waitDelay
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return checkResult{err: err}
	}
	if err := cmd.Start(); err != nil {
		return checkResult{err: fmt.Errorf("start %s: %w", argv[0], err)}
	}
	if started != nil {
		started(cmd.Process.Pid)
	}
	sc := bufio.NewScanner(pipe)
	sc.Buffer(make([]byte, 64<<10), maxCheckOutput)
	for sc.Scan() {
		line := sc.Text()
		st.Checks++
		r := e.judge(k, withResult(data, map[string]any{"line": line}), clipN(line, 200))
		if r.err != nil || r.holds {
			cancel()
			_ = cmd.Wait()
			return r
		}
		st.LastCheck, st.LastDetail = e.now(), r.detail
		if report != nil {
			report(*st)
		}
	}
	err = cmd.Wait()
	if ctx.Err() != nil {
		return checkResult{err: ctx.Err()}
	}
	if scanErr := sc.Err(); scanErr != nil {
		return checkResult{err: fmt.Errorf("read %s: %w", argv[0], scanErr)}
	}
	return checkResult{err: fmt.Errorf("%s exited (%v) before a line held; it restarts in %s: %s", argv[0], err, k.Every, clipN(strings.TrimSpace(stderr.String()), 200))}
}

// judge evaluates <until> and <describe> against a check's data.
func (e *MonitorEnv) judge(k *MonitorKind, data map[string]any, fallback string) checkResult {
	r := checkResult{detail: fallback}
	if k.describe != "" {
		d, err := apidsl.RenderString(k.describe, data)
		if err != nil {
			return checkResult{err: fmt.Errorf("<describe>: %w", err)}
		}
		r.detail = strings.TrimSpace(d)
	}
	if k.until != "" {
		v, err := apidsl.RenderString(k.until, data)
		if err != nil {
			return checkResult{err: fmt.Errorf("<until>: %w", err)}
		}
		r.holds = apidsl.IsTruthy(strings.TrimSpace(v))
	}
	return r
}

func renderArgv(tmpls []string, data map[string]any) ([]string, error) {
	out := make([]string, 0, len(tmpls))
	for _, t := range tmpls {
		v, err := apidsl.RenderString(t, data)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if len(out) == 0 || out[0] == "" {
		return nil, errors.New("the check's first <argv> rendered empty")
	}
	return out, nil
}

func withResult(base, extra map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// clipN cuts s to n bytes and marks the cut.
func clipN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// capped keeps the first max bytes written to it. String says how much it dropped.
type capped struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	max     int
	dropped int
}

func (c *capped) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	keep := min(len(p), max(c.max-c.buf.Len(), 0))
	c.buf.Write(p[:keep])
	c.dropped += len(p) - keep
	return len(p), nil
}

func (c *capped) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropped > 0 {
		return fmt.Sprintf("%s\n[%d more bytes dropped]", c.buf.String(), c.dropped)
	}
	return c.buf.String()
}
