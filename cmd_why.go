// why says what one job waits on: an unfinished dependency, a monitor that has not seen its condition, or a scheduler gate.

package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newWhyCmd) }

func newWhyCmd(g *globals) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "why <id>",
		Short: "Say what a job waits on",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := call(g, jobserver.Request{Op: jobserver.OpWhy, ID: args[0]})
			if err != nil {
				return err
			}
			if asJSON {
				return printJSON(resp.Waits)
			}
			if len(resp.Waits) == 0 {
				fmt.Printf("%s waits on nothing: it runs, or it has ended\n", args[0])
				return nil
			}
			for _, w := range resp.Waits {
				// The run cap's key is "cap", the same as its kind. Print it once.
				if w.ID != "" && w.ID != w.Kind {
					fmt.Printf("%s %s: %s\n", w.Kind, w.ID, w.Detail)
					continue
				}
				fmt.Printf("%s: %s\n", w.Kind, w.Detail)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the raw API answer")
	return cmd
}
