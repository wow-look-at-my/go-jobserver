package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newResumeCmd) }

func newResumeCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "resume",
		Short: "Start scheduling again",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := do(g, jobserver.Request{Op: jobserver.OpResume}); err != nil {
				return err
			}
			fmt.Println("resumed")
			return nil
		},
	}
}
