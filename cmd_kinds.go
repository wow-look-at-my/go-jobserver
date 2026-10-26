// kinds lists the monitor kinds the daemon loads, with their args.

package main

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newKindsCmd) }

func newKindsCmd(g *globals) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "kinds",
		Short: "List the monitor kinds the daemon knows",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := call(g, jobserver.Request{Op: jobserver.OpKinds})
			if err != nil {
				return err
			}
			if asJSON {
				return printJSON(resp.Kinds)
			}
			if len(resp.Kinds) == 0 {
				fmt.Println("no monitor kinds: start the daemon with --monitors <file.xml>")
				return nil
			}
			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "KIND\tCHECK\tEVERY\tARGS\tDESCRIPTION")
			for _, k := range resp.Kinds {
				var args []string
				for _, a := range k.Args {
					name := a.Name
					if a.Required {
						name += "*"
					}
					args = append(args, name)
				}
				fmt.Fprintf(w, "%s\t%s %s\t%s\t%s\t%s\n", k.Name, k.Shape, k.Source, k.Every, strings.Join(args, " "), k.Description)
			}
			return w.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the raw API answer")
	return cmd
}
