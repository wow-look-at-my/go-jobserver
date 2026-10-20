package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newStatsCmd) }

func newStatsCmd(g *globals) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show server counters, CPU use and active controls",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := do(g, jobserver.Request{Op: jobserver.OpStats})
			if err != nil {
				return err
			}
			if asJSON {
				return printJSON(resp.Stats)
			}
			printStats(resp.Stats)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the raw API answer")
	return cmd
}
