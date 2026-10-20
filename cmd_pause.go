package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newPauseCmd) }

func newPauseCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "pause",
		Short: "Stop starting new jobs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := do(g, jobserver.Request{Op: jobserver.OpPause}); err != nil {
				return err
			}
			fmt.Println("paused")
			return nil
		},
	}
}
