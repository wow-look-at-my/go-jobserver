// printJobs writes the job table.

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

func init() { register(newListCmd) }

func newListCmd(g *globals) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List jobs",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
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
			if asJSON {
				return printJSON(resp.Jobs)
			}
			fmt.Println(resp.Summary)
			return printJobs(resp.Jobs, resp.JobWaits)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the raw API answer")
	return cmd
}

// printJobs writes the job table.
func printJobs(jobs []*jobserver.Job, waits map[string][]jobserver.Wait) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATE\tDEPTH\tCOMMAND\tDEPS\tTOOK\tWAITING ON")
	depths := jobserver.Depths(jobs)
	for _, j := range jobs {
		took := "-"
		if !j.Started.IsZero() && !j.Finished.IsZero() {
			took = j.Finished.Sub(j.Started).Round(time.Millisecond).String()
		}
		what := strings.Join(j.Command, " ")
		if j.Monitor != nil {
			what = "[" + j.Monitor.Key() + "]"
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\t%s\n", j.ID, j.State, depths[j.ID],
			what, strings.Join(j.Deps, ","), took, waitText(waits[j.ID]))
	}
	return w.Flush()
}

// waitText is a job's waits on one line, or a dash.
func waitText(ws []jobserver.Wait) string {
	if len(ws) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(ws))
	for _, wt := range ws {
		parts = append(parts, wt.Detail)
	}
	return strings.Join(parts, "; ")
}
