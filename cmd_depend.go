package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newDependCmd) }

func newDependCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "depend <id> <dep>...",
		Short: "Add dependencies to a draft job",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			var last jobserver.Response
			for _, dep := range args[1:] {
				resp, err := do(g, jobserver.Request{Op: jobserver.OpDepend, ID: id, Dep: dep})
				if err != nil {
					return err
				}
				last = resp
			}
			fmt.Printf("%s deps %s\n", id, strings.Join(last.Job.Deps, " "))
			return nil
		},
	}
}
