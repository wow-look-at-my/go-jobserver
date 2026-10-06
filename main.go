// Command go-jobserver runs a dependency-ordered job queue as a daemon, and
// hands jobs to it from the command line.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

const usage = `go-jobserver runs a queue of jobs with dependencies.

usage:
  go-jobserver [global flags] <command> [args]

commands:
  run                  run the daemon in the foreground
  queue [flags] cmd... enqueue a job, then print its id
  list                 list jobs
  status <id>          show one job
  logs <id>            print a job's output
  activate <id>        move a draft job to active
  depend <id> <dep>... add dependencies to a draft job
  pause                stop starting new jobs
  resume               start scheduling again
  interrupt <id|all>   stop running jobs
  stats                show server counters
  version              print the version

global flags (also accepted after the command):
  -dir DIR       server directory (default ~/.local/state/go-jobserver)
  -http ADDR     dashboard address for the daemon (default 127.0.0.1:8059)
  -socket PATH   file socket for the daemon (default DIR/go-jobserver.sock)
  -spool DIR     spool directory to watch (default DIR/spool)
  -ipc BOOL      serve the go-ipc service (default true)
  -j N           how many jobs may run at once (default one per CPU)
  -no-http       do not serve the dashboard
  -no-socket     do not serve the file socket
  -no-spool      do not watch a spool directory

queue flags:
  -name NAME     a label for the job
  -dep ID        add a dependency (repeatable)
  -input PATH    a file whose content decides the job's cache identity (repeatable)
  -output PATH   a file the job must produce (repeatable)
  -env K=V       an extra environment entry (repeatable)
  -workdir DIR   the directory to run in
  -key KEY       dedup key: an unfinished job with the key is reused
  -draft         create the job as a draft instead of active
  -force         run even when an identical job already finished
  -wait          wait for the job and report its final state
  -shell         run the command through "sh -c"
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "go-jobserver:", err)
		os.Exit(1)
	}
}

// globals are the flags every command accepts.
type globals struct {
	dir      string
	http     string
	socket   string
	spool    string
	ipc      bool
	jobs     int
	noHTTP   bool
	noSocket bool
	noSpool  bool
}

// bind adds the global flags, defaulting each to the value it already holds so
// a subcommand can bind them again after the command. Line parsed them.
func (g *globals) bind(fs *flag.FlagSet) {
	fs.StringVar(&g.dir, "dir", g.dir, "server directory")
	fs.StringVar(&g.http, "http", g.http, "dashboard address")
	fs.StringVar(&g.socket, "socket", g.socket, "file socket path")
	fs.StringVar(&g.spool, "spool", g.spool, "spool directory")
	fs.BoolVar(&g.ipc, "ipc", g.ipc, "serve the go-ipc service")
	fs.IntVar(&g.jobs, "j", g.jobs, "max concurrent jobs")
	fs.BoolVar(&g.noHTTP, "no-http", g.noHTTP, "do not serve the dashboard")
	fs.BoolVar(&g.noSocket, "no-socket", g.noSocket, "do not serve the file socket")
	fs.BoolVar(&g.noSpool, "no-spool", g.noSpool, "do not watch a spool directory")
}

// config turns the flags into a server configuration.
func (g *globals) config() jobserver.Config {
	dir := g.dir
	if dir == "" {
		dir = jobserver.DefaultDir()
	}
	cfg := jobserver.DefaultConfig(dir)
	if g.http != "" {
		cfg.HTTPAddr = g.http
	}
	if g.noHTTP {
		cfg.HTTPAddr = ""
	}
	if g.socket != "" {
		cfg.UnixSocket = g.socket
	}
	if g.noSocket {
		cfg.UnixSocket = ""
	}
	if g.spool != "" {
		cfg.SpoolDir = g.spool
	}
	if g.noSpool {
		cfg.SpoolDir = ""
	}
	if g.jobs > 0 {
		cfg.MaxConcurrent = g.jobs
	}
	cfg.IPC = g.ipc
	return cfg
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	// Global flags may come before the command.
	var g globals
	if strings.HasPrefix(args[0], "-") && args[0] != "-" {
		fs := flag.NewFlagSet("go-jobserver", flag.ContinueOnError)
		g.bind(fs)
		if err := fs.Parse(args); err != nil {
			return err
		}
		args = fs.Args()
		if len(args) == 0 {
			fmt.Print(usage)
			return nil
		}
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "run", "daemon", "serve", "start":
		return cmdRun(&g, rest)
	case "queue", "enqueue", "add":
		return cmdQueue(&g, rest)
	case "list", "ls":
		return cmdList(&g, rest)
	case "status", "show":
		return cmdStatus(&g, rest)
	case "logs", "log":
		return cmdLogs(&g, rest)
	case "activate":
		return cmdSimple(&g, rest, jobserver.OpActivate)
	case "pause":
		return cmdSimple(&g, rest, jobserver.OpPause)
	case "resume":
		return cmdSimple(&g, rest, jobserver.OpResume)
	case "interrupt", "cancel":
		return cmdInterrupt(&g, rest)
	case "depend":
		return cmdDepend(&g, rest)
	case "stats":
		return cmdStats(&g, rest)
	case "version":
		fmt.Printf("go-jobserver %s\n", jobserver.Revision)
		return nil
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		fmt.Print(usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// cmdRun runs the daemon until it is interrupted.
func cmdRun(g *globals, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.Usage = func() { fmt.Print(usage) }
	g.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := g.config()
	cfg.Logf = func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) }
	srv, err := jobserver.New(cfg)
	if err != nil {
		return err
	}
	if err := srv.Start(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	fmt.Fprintln(os.Stderr, "go-jobserver: shutting down")
	return srv.Close()
}

// client dials the daemon for these flags.
func client(g *globals) (*jobserver.Client, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c, err := jobserver.Dial(ctx, g.config())
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	return c, ctx, cancel, nil
}

// listFlag collects a repeated string flag.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(v string) error {
	*l = append(*l, v)
	return nil
}

// cmdQueue enqueues one job.
func cmdQueue(g *globals, args []string) error {
	fs := flag.NewFlagSet("queue", flag.ContinueOnError)
	fs.Usage = func() { fmt.Print(usage) }
	g.bind(fs)
	var (
		name    = fs.String("name", "", "label for the job")
		deps    listFlag
		inputs  listFlag
		outputs listFlag
		envs    listFlag
		workdir = fs.String("workdir", "", "directory to run in")
		key     = fs.String("key", "", "dedup key")
		draft   = fs.Bool("draft", false, "create as a draft")
		force   = fs.Bool("force", false, "run even when an identical job finished")
		wait    = fs.Bool("wait", false, "wait for the job")
		shell   = fs.Bool("shell", false, "run through sh -c")
	)
	fs.Var(&deps, "dep", "add a dependency")
	fs.Var(&deps, "depends", "add a dependency")
	fs.Var(&inputs, "input", "a file whose content decides the cache identity")
	fs.Var(&outputs, "output", "a file the job must produce")
	fs.Var(&envs, "env", "an extra environment entry")
	if err := fs.Parse(args); err != nil {
		return err
	}
	argv := fs.Args()
	if len(argv) == 0 {
		line, err := readCommandLine(os.Stdin)
		if err != nil {
			return err
		}
		if line == "" {
			return errors.New("queue needs a command")
		}
		argv = splitLine(line)
	}
	if len(argv) == 0 {
		return errors.New("queue needs a command")
	}
	if *shell {
		argv = append([]string{"sh", "-c"}, strings.Join(argv, " "))
	}
	spec := jobserver.Spec{
		Name:    *name,
		Command: argv,
		Deps:    deps,
		Inputs:  inputs,
		Outputs: outputs,
		Env:     envs,
		Dir:     *workdir,
		Key:     *key,
		Draft:   *draft,
		Force:   *force,
	}
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	resp, err := c.Enqueue(ctx, spec)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	j := resp.Job
	if j == nil {
		return errors.New("the daemon returned no job")
	}
	fmt.Println(j.ID)
	if !*wait {
		return nil
	}
	done, err := c.Wait(ctx, j.ID, 50*time.Millisecond)
	if err != nil {
		return err
	}
	fmt.Printf("%s %s", done.ID, done.State)
	if done.ExitCode != 0 {
		fmt.Printf(" exit %d", done.ExitCode)
	}
	fmt.Println()
	if done.State != jobserver.StateCompleted && done.State != jobserver.StateCached {
		if out, lerr := c.Logs(ctx, done.ID, 0, 64<<10); lerr == nil && out.Output != "" {
			fmt.Print(out.Output)
		}
		return fmt.Errorf("job %s %s", done.ID, done.State)
	}
	return nil
}

// readCommandLine reads one command line from stdin.
func readCommandLine(f *os.File) (string, error) {
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeCharDevice != 0 {
		return "", nil
	}
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 512)
	for {
		n, err := f.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil || len(buf) > 1<<16 {
			break
		}
	}
	return strings.TrimSpace(string(buf)), nil
}

// splitLine splits a command line on whitespace.
func splitLine(line string) []string {
	return strings.Fields(line)
}

// cmdList prints every job.
func cmdList(g *globals, args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	g.bind(fs)
	var asJSON = fs.Bool("json", false, "print the raw API answer")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	resp, err := c.List(ctx)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if *asJSON {
		return printJSON(resp.Jobs)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATE\tDEPTH\tCOMMAND\tDEPS\tTOOK")
	byID := jobserver.Index(resp.Jobs)
	for _, j := range resp.Jobs {
		took := "-"
		if !j.Started.IsZero() && !j.Finished.IsZero() {
			took = j.Finished.Sub(j.Started).Round(time.Millisecond).String()
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\n", j.ID, j.State, depth(j, byID, 0),
			strings.Join(j.Command, " "), strings.Join(j.Deps, ","), took)
	}
	return w.Flush()
}

// depth is how many dependency levels deep a job sits.
func depth(j *jobserver.Job, byID map[string]*jobserver.Job, seen int) int {
	if seen > 64 || len(j.Deps) == 0 {
		return 0
	}
	best := 0
	for _, d := range j.Deps {
		dep, ok := byID[d]
		if !ok {
			continue
		}
		if got := depth(dep, byID, seen+1) + 1; got > best {
			best = got
		}
	}
	return best
}

// cmdStatus prints one job.
func cmdStatus(g *globals, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	g.bind(fs)
	var asJSON = fs.Bool("json", false, "print the raw API answer")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("status needs one job id")
	}
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	resp, err := c.Get(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if *asJSON {
		return printJSON(resp.Job)
	}
	j := resp.Job
	fmt.Printf("id        %s\n", j.ID)
	fmt.Printf("state     %s\n", j.State)
	if j.Name != "" {
		fmt.Printf("name      %s\n", j.Name)
	}
	fmt.Printf("command   %s\n", strings.Join(j.Command, " "))
	if j.Dir != "" {
		fmt.Printf("dir       %s\n", j.Dir)
	}
	if len(j.Deps) > 0 {
		fmt.Printf("deps      %s\n", strings.Join(j.Deps, " "))
	}
	if len(j.Inputs) > 0 {
		fmt.Printf("inputs    %s\n", strings.Join(j.Inputs, " "))
	}
	if len(j.Outputs) > 0 {
		fmt.Printf("outputs   %s\n", strings.Join(j.Outputs, " "))
	}
	if j.Identity != "" {
		fmt.Printf("identity  %s\n", j.Identity)
	}
	if j.CacheOf != "" {
		fmt.Printf("reused    %s\n", j.CacheOf)
	}
	fmt.Printf("attempts  %d\n", j.Attempts)
	if j.State.Terminal() {
		fmt.Printf("exit      %d\n", j.ExitCode)
	}
	if j.Error != "" {
		fmt.Printf("error     %s\n", j.Error)
	}
	fmt.Printf("log       %d bytes\n", j.LogBytes)
	for _, a := range j.Artifacts {
		fmt.Printf("artifact  %s %d %s\n", a.Path, a.Size, a.SHA256)
	}
	return nil
}

// cmdLogs prints a job's output, optionally following it.
func cmdLogs(g *globals, args []string) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	g.bind(fs)
	var follow = fs.Bool("f", false, "follow the output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("logs needs one job id")
	}
	id := fs.Arg(0)
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	var offset int64
	for {
		resp, err := c.Logs(ctx, id, offset, 256<<10)
		if err != nil {
			return err
		}
		if !resp.OK {
			return errors.New(resp.Error)
		}
		if resp.Output != "" {
			fmt.Print(resp.Output)
			offset += int64(len(resp.Output))
		}
		if !*follow {
			return nil
		}
		j, err := c.Get(ctx, id)
		if err != nil {
			return err
		}
		if j.Job != nil && j.Job.State.Terminal() && int64(len(resp.Output)) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// cmdSimple runs a command that only needs an operation name.
func cmdSimple(g *globals, args []string, op string) error {
	fs := flag.NewFlagSet(op, flag.ContinueOnError)
	g.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	req := jobserver.Request{Op: op}
	if fs.NArg() > 0 {
		req.ID = fs.Arg(0)
	}
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	resp, err := c.Do(ctx, req)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	switch op {
	case jobserver.OpPause:
		fmt.Println("paused")
	case jobserver.OpResume:
		fmt.Println("resumed")
	default:
		fmt.Println("ok")
	}
	return nil
}

// cmdInterrupt stops one job, or every running job with "all".
func cmdInterrupt(g *globals, args []string) error {
	fs := flag.NewFlagSet("interrupt", flag.ContinueOnError)
	g.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("interrupt needs a job id or \"all\"")
	}
	target := fs.Arg(0)
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	req := jobserver.Request{Op: jobserver.OpInterrupt, ID: target}
	if target == "all" {
		req = jobserver.Request{Op: jobserver.OpInterruptAll}
	}
	resp, err := c.Do(ctx, req)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if req.Op == jobserver.OpInterruptAll {
		fmt.Printf("interrupted %d job(s)\n", resp.Count)
		return nil
	}
	fmt.Printf("interrupted %s\n", target)
	return nil
}

// cmdDepend adds dependencies to a job.
func cmdDepend(g *globals, args []string) error {
	fs := flag.NewFlagSet("depend", flag.ContinueOnError)
	g.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return errors.New("depend needs a job id and at least one dependency")
	}
	id := fs.Arg(0)
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	var last jobserver.Response
	for _, dep := range fs.Args()[1:] {
		resp, err := c.Do(ctx, jobserver.Request{Op: jobserver.OpDepend, ID: id, Dep: dep})
		if err != nil {
			return err
		}
		if !resp.OK {
			return errors.New(resp.Error)
		}
		last = resp
	}
	fmt.Printf("%s deps %s\n", id, strings.Join(last.Job.Deps, " "))
	return nil
}

// cmdStats prints the server counters.
func cmdStats(g *globals, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	g.bind(fs)
	var asJSON = fs.Bool("json", false, "print the raw API answer")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	resp, err := c.Do(ctx, jobserver.Request{Op: jobserver.OpStats})
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if *asJSON {
		return printJSON(resp.Stats)
	}
	st := resp.Stats
	fmt.Printf("dir      %s\n", st.Dir)
	fmt.Printf("version  %s\n", st.Revision)
	fmt.Printf("pid      %d\n", st.PID)
	fmt.Printf("uptime   %s\n", st.Uptime)
	fmt.Printf("paused   %v\n", st.Paused)
	fmt.Printf("running  %d\n", st.Running)
	fmt.Printf("total    %d\n", st.Total)
	states := make([]string, 0, len(st.ByState))
	for state := range st.ByState {
		states = append(states, state)
	}
	sortStrings(states)
	for _, state := range states {
		fmt.Printf("%-9s%d\n", state, st.ByState[state])
	}
	return nil
}

// printJSON writes a value as indented JSON.
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// sortStrings sorts a small slice in place.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for k := i; k > 0 && s[k] < s[k-1]; k-- {
			s[k], s[k-1] = s[k-1], s[k]
		}
	}
}
