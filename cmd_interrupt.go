package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newInterruptCmd) }

func newInterruptCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:     "interrupt <id|all>",
		Aliases: []string{"cancel"},
		Short:   "Stop one running job, or every one with \"all\"",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := args[0]
			if target == "all" {
				resp, err := do(g, jobserver.Request{Op: jobserver.OpInterruptAll})
				if err != nil {
					return err
				}
				fmt.Printf("interrupted %d job(s)\n", resp.Count)
				return nil
			}
			if _, err := do(g, jobserver.Request{Op: jobserver.OpInterrupt, ID: target}); err != nil {
				return err
			}
			fmt.Printf("interrupted %s\n", target)
			return nil
		},
	}
}
