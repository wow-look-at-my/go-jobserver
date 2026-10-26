// monitors lists every monitor: what it watches, where it stands, and which jobs wait on it.

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newMonitorsCmd) }

func newMonitorsCmd(g *globals) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "monitors",
		Short: "List the monitors, what each one last saw, and the jobs that wait on it",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := call(g, jobserver.Request{Op: jobserver.OpMonitors})
			if err != nil {
				return err
			}
			if asJSON {
				return printJSON(resp.Monitors)
			}
			fmt.Println(resp.Summary)
			return printMonitors(resp.Monitors, time.Now())
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the raw API answer")
	return cmd
}

func printMonitors(ms []jobserver.MonitorView, now time.Time) error {
	if len(ms) == 0 {
		fmt.Println("no monitors")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATE\tWATCHES\tCHECKS\tLAST CHECK\tLAST RESULT\tWAITING JOBS")
	for _, m := range ms {
		checks, last, result := "-", "-", "-"
		if st := m.MonitorStatus; st != nil {
			checks = fmt.Sprint(st.Checks)
			if !st.LastCheck.IsZero() {
				last = now.Sub(st.LastCheck).Round(time.Second).String() + " ago"
			}
			result = st.LastDetail
			if st.LastError != "" {
				result = "ERROR: " + st.LastError
			}
		} else if m.State.Terminal() {
			result = m.Error
		}
		waiting := "-"
		if len(m.Waiting) > 0 {
			waiting = strings.Join(m.Waiting, ",")
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", m.ID, m.State, m.Monitor.Key(), checks, last, result, waiting)
	}
	return w.Flush()
}

// call sends one request to the daemon and turns a refusal into an error.
func call(g *globals, req jobserver.Request) (jobserver.Response, error) {
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
