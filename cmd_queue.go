// queue enqueues one job and prints its id, then waits for it when asked.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newQueueCmd) }

func newQueueCmd(g *globals) *cobra.Command {
	var (
		spec  jobserver.Spec
		wait  bool
		shell bool
		pol   policyFlags
		mon   monitorFlags
	)
	cmd := &cobra.Command{
		Use:     "queue [flags] [--] command [args...]",
		Aliases: []string{"enqueue", "add"},
		Short:   "Enqueue a job, then print its id",
		Long: `Enqueue a job, then print its id.

With no command arguments, queue reads one command line from standard input.
Flags stop at the first argument that is not a flag, so "queue sh -c x" works.`,
		RunE: func(cmd *cobra.Command, argv []string) error {
			if mon.kind != "" {
				if len(argv) > 0 {
					return errors.New("a monitor task runs no command: drop the command, or drop --monitor")
				}
				m, err := mon.spec()
				if err != nil {
					return err
				}
				return watch(g, m, spec.Name, wait)
			}
			if len(argv) == 0 {
				line, err := readCommandLine(os.Stdin)
				if err != nil {
					return err
				}
				argv = strings.Fields(line)
			}
			if len(argv) == 0 {
				return errors.New("queue needs a command")
			}
			if shell {
				argv = []string{"sh", "-c", strings.Join(argv, " ")}
			}
			spec.Command = argv
			spec.Policy = pol.policy()
			return queue(g, spec, wait)
		},
	}
	f := cmd.Flags()
	f.SetInterspersed(false)
	f.StringVar(&spec.Name, "name", "", "a label for the job")
	f.Var((*listFlag)(&spec.Deps), "dep", "add a dependency (repeatable)")
	f.Var((*listFlag)(&spec.Deps), "depends", "add a dependency (repeatable)")
	f.Var((*listFlag)(&spec.Inputs), "input", "a file whose content decides the job's cache identity (repeatable)")
	f.Var((*listFlag)(&spec.Outputs), "output", "a file the job must produce (repeatable)")
	f.Var((*listFlag)(&spec.Env), "env", "an extra K=V environment entry (repeatable)")
	f.StringVar(&spec.Dir, "workdir", "", "the directory to run in")
	f.StringVar(&spec.Key, "key", "", "dedup key: an unfinished job with the key is reused")
	f.BoolVar(&spec.Draft, "draft", false, "create the job as a draft instead of active")
	f.BoolVar(&spec.Force, "force", false, "run even when an identical job already finished")
	f.BoolVar(&wait, "wait", false, "wait for the job and report its final state")
	f.BoolVar(&shell, "shell", false, `run the command through "sh -c"`)
	pol.bind(f)
	f.StringVar(&mon.kind, "monitor", "", "add a monitor task of this kind instead of a command (see go-jobserver kinds)")
	f.Var((*listFlag)(&mon.args), "arg", "a monitor arg as name=value (repeatable)")
	f.DurationVar(&mon.deadline, "deadline", 0, "end the monitor as failed after this long")
	return cmd
}

// monitorFlags are the queue flags that make a monitor task.
type monitorFlags struct {
	kind     string
	args     []string
	deadline time.Duration
}

func (m monitorFlags) spec() (jobserver.MonitorSpec, error) {
	out := jobserver.MonitorSpec{Kind: m.kind, Args: map[string]string{}}
	for _, kv := range m.args {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			return out, fmt.Errorf("--arg %q is not name=value", kv)
		}
		out.Args[k] = v
	}
	if m.deadline > 0 {
		out.Deadline = time.Now().Add(m.deadline)
	}
	return out, nil
}

// watch adds a monitor task and prints its id. A monitor that already watches the condition answers instead.
func watch(g *globals, m jobserver.MonitorSpec, title string, wait bool) error {
	resp, err := call(g, jobserver.Request{Op: jobserver.OpWatch, Monitor: &m, Title: title})
	if err != nil {
		return err
	}
	if resp.Job == nil {
		return errors.New("the daemon returned no task")
	}
	fmt.Println(resp.Job.ID)
	if resp.Existing {
		fmt.Fprintf(os.Stderr, "%s already watches this (%s); no new task was added\n", resp.Job.ID, resp.Job.State)
	}
	if !wait {
		return nil
	}
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	return report(ctx, c, resp.Job.ID)
}

// queue enqueues one job and prints its id, then waits for it when asked.
func queue(g *globals, spec jobserver.Spec, wait bool) error {
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
	if !wait {
		return nil
	}
	return report(ctx, c, j.ID)
}

// report waits for a task and reports its final state on stderr.
func report(ctx context.Context, c *jobserver.Client, id string) error {
	done, err := c.Wait(ctx, id, 50*time.Millisecond)
	if err != nil {
		return err
	}
	// The job id is what stdout carries.
	fmt.Fprintf(os.Stderr, "%s %s", done.ID, done.State)
	if done.ExitCode != 0 {
		fmt.Fprintf(os.Stderr, " exit %d", done.ExitCode)
	}
	fmt.Fprintln(os.Stderr)
	if done.State != jobserver.StateCompleted && done.State != jobserver.StateCached {
		if out, lerr := c.Logs(ctx, done.ID, 0, 64<<10); lerr == nil && out.Output != "" {
			fmt.Fprint(os.Stderr, out.Output)
		}
		return fmt.Errorf("job %s %s", done.ID, done.State)
	}
	return nil
}

// listFlag collects a repeated string flag.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(v string) error {
	*l = append(*l, v)
	return nil
}

func (l *listFlag) Type() string { return "string" }

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
