package main

import (
	"context"
	"errors"
	"time"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

// commands holds one constructor per subcommand. Each command file adds its own in init.
var commands []func(*globals) *cobra.Command

// register adds a subcommand constructor to the tree.
func register(f func(*globals) *cobra.Command) { commands = append(commands, f) }

// globals are the flags every command accepts.
type globals struct {
	dir      string
	http     string
	socket   string
	spool    string
	ipc      bool
	jobs     int
	budget   float64
	cost     float64
	sample   time.Duration
	noCPU    bool
	noHTTP   bool
	noSocket bool
	noSpool  bool
}

// newRootCmd builds the whole command tree.
func newRootCmd() *cobra.Command {
	g := &globals{}
	root := &cobra.Command{
		Use:   "go-jobserver",
		Short: "Run a queue of jobs with dependencies",
		Long: `go-jobserver runs a queue of jobs with dependencies.

"go-jobserver run" starts the daemon. The other commands talk to it over its
file socket, or over its go-ipc service when the socket is off.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.CompletionOptions.HiddenDefaultCmd = true
	def := jobserver.DefaultConfig(jobserver.DefaultDir())
	f := root.PersistentFlags()
	f.StringVar(&g.dir, "dir", def.Dir, "server directory")
	f.StringVar(&g.http, "http", def.HTTPAddr, "dashboard address for the daemon")
	f.StringVar(&g.socket, "socket", "", "file socket for the daemon (default DIR/go-jobserver.sock)")
	f.StringVar(&g.spool, "spool", "", "spool directory to watch (default DIR/spool)")
	f.BoolVar(&g.ipc, "ipc", true, "serve the go-ipc service")
	f.IntVarP(&g.jobs, "jobs", "j", def.MaxConcurrent, "how many jobs may run at once")
	f.Float64Var(&g.budget, "budget", def.CPUBudget, "how many CPUs the running jobs may use together")
	f.Float64Var(&g.cost, "cost", def.DefaultCost, "CPU cost assumed for a job that declares none")
	f.DurationVar(&g.sample, "sample", def.SampleInterval, "how often CPU use is sampled")
	f.BoolVar(&g.noCPU, "no-cpu", false, "do not sample CPU use or throttle anything")
	f.BoolVar(&g.noHTTP, "no-http", false, "do not serve the dashboard")
	f.BoolVar(&g.noSocket, "no-socket", false, "do not serve the file socket")
	f.BoolVar(&g.noSpool, "no-spool", false, "do not watch a spool directory")
	for _, newCmd := range commands {
		root.AddCommand(newCmd(g))
	}
	return root
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
	if g.budget > 0 {
		cfg.CPUBudget = g.budget
	}
	if g.cost > 0 {
		cfg.DefaultCost = g.cost
	}
	if g.sample > 0 {
		cfg.SampleInterval = g.sample
	}
	cfg.DisableCPU = g.noCPU
	cfg.IPC = g.ipc
	return cfg
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

// do sends one request to the daemon and fails on a refusal.
func do(g *globals, req jobserver.Request) (jobserver.Response, error) {
	c, ctx, cancel, err := client(g)
	if err != nil {
		return jobserver.Response{}, err
	}
	defer cancel()
	defer c.Close()
	resp, err := c.Do(ctx, req)
	if err != nil {
		return resp, err
	}
	if !resp.OK {
		return resp, errors.New(resp.Error)
	}
	return resp, nil
}
