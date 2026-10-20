package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newActivateCmd) }

func newActivateCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "activate <id>",
		Short: "Move a draft job to active",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := do(g, jobserver.Request{Op: jobserver.OpActivate, ID: args[0]}); err != nil {
				return err
			}
			fmt.Println("ok")
			return nil
		},
	}
}
