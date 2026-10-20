package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

func init() { register(newLogsCmd) }

func newLogsCmd(g *globals) *cobra.Command {
	var follow bool
	cmd := &cobra.Command{
		Use:     "logs <id>",
		Aliases: []string{"log"},
		Short:   "Print a job's output",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
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
				if !follow {
					return nil
				}
				j, err := c.Get(ctx, id)
				if err != nil {
					return err
				}
				if j.Job != nil && j.Job.State.Terminal() && resp.Output == "" {
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(200 * time.Millisecond):
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "follow the output until the job ends")
	return cmd
}
