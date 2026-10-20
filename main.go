// Command go-jobserver runs a dependency-ordered job queue as a daemon, and
// hands jobs to it from the command line.
package main

import (
	"fmt"
	"os"
)

<<<<<<< HEAD
=======
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
  policy [flags] <id>  replace a job's CPU cost and process-control settings
  stats                show server counters, CPU use and active controls
  version              print the version

global flags (also accepted after the command):
  -dir DIR       server directory (default ~/.local/state/go-jobserver)
  -http ADDR     dashboard address for the daemon (default 127.0.0.1:8059)
  -socket PATH   file socket for the daemon (default DIR/go-jobserver.sock)
  -spool DIR     spool directory to watch (default DIR/spool)
  -ipc BOOL      serve the go-ipc service (default true)
  -j N           how many jobs may run at once (default one per CPU)
  -budget N      how many CPUs the running jobs may use together (default one per CPU)
  -cost N        CPU cost assumed for a job that declares none (default 1)
  -sample DUR    how often CPU use is sampled (default 1s)
  -no-cpu        do not sample CPU use or throttle anything
  -stats-url URL read the host load from a simple-stats-api (default: this host's /proc/stat)
  -no-http       do not serve the dashboard
  -no-socket     do not serve the file socket
  -no-spool      do not watch a spool directory

queue and policy flags:
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

CPU policy flags, accepted by queue and by policy:
  -cpu N         CPUs the job is expected to use (default: its last run, then 1)
  -priority      raise the job's nice value while it is over the CPU budget
  -nice N        the nice value to apply (default 10)
  -freeze        suspend the job's processes while it is over the budget
  -affinity LIST pin the job to CPUs ("0,1", "all", or "last")
  -exempt SEL    processes every enabled mechanism leaves alone
  -only SEL      processes every enabled mechanism is restricted to
  -<mech>-exempt SEL, -<mech>-only SEL   the same, for one mechanism, where
                 <mech> is affinity, priority or freeze
  a selector is a comma-separated list of process ids and names, e.g. "1234,ffmpeg"
`

>>>>>>> origin/master
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "go-jobserver:", err)
		os.Exit(1)
	}
}

<<<<<<< HEAD
// run executes one command line on a fresh command tree, so flag values never
// carry over from an earlier call.
=======
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
	statsURL string
	hold     bool
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
	fs.Float64Var(&g.budget, "budget", g.budget, "CPUs the running jobs may use together")
	fs.Float64Var(&g.cost, "cost", g.cost, "CPU cost assumed for a job that declares none")
	fs.DurationVar(&g.sample, "sample", g.sample, "how often CPU use is sampled")
	fs.BoolVar(&g.noCPU, "no-cpu", g.noCPU, "do not sample CPU use")
	fs.StringVar(&g.statsURL, "stats-url", g.statsURL, "simple-stats-api to read the host load from")
	fs.BoolVar(&g.hold, "hold", g.hold, "internal: wait for input to end, then leave")
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
	cfg.StatsURL = g.statsURL
	cfg.IPC = g.ipc
	return cfg
}

// newGlobals returns the flag set every command starts from.
func newGlobals() *globals { return &globals{ipc: true} }

// run dispatches one command line.
>>>>>>> origin/master
func run(args []string) error {
	root := newRootCmd()
	root.SetArgs(args)
	return root.Execute()
}
