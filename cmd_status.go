package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newStatusCmd) }

func newStatusCmd(g *globals) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:     "status <id>",
		Aliases: []string{"show"},
		Short:   "Show one job",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, ctx, cancel, err := client(g)
			if err != nil {
				return err
			}
			defer cancel()
			defer c.Close()
			resp, err := c.Get(ctx, args[0])
			if err != nil {
				return err
			}
			if !resp.OK {
				return errors.New(resp.Error)
			}
			if asJSON {
				return printJSON(resp.Job)
			}
			printJob(resp.Job)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the raw API answer")
	return cmd
}

// printJob writes one job's fields.
func printJob(j *jobserver.Job) {
	fmt.Printf("id        %s\n", j.ID)
	fmt.Printf("state     %s\n", j.State)
	if j.Name != "" {
		fmt.Printf("name      %s\n", j.Name)
	}
	fmt.Printf("command   %s\n", strings.Join(j.Command, " "))
	if j.Dir != "" {
		fmt.Printf("dir       %s\n", j.Dir)
	}
	if len(j.Deps) > 0 {
		fmt.Printf("deps      %s\n", strings.Join(j.Deps, " "))
	}
	if len(j.Inputs) > 0 {
		fmt.Printf("inputs    %s\n", strings.Join(j.Inputs, " "))
	}
	if len(j.Outputs) > 0 {
		fmt.Printf("outputs   %s\n", strings.Join(j.Outputs, " "))
	}
	if j.Identity != "" {
		fmt.Printf("identity  %s\n", j.Identity)
	}
	if j.CacheOf != "" {
		fmt.Printf("reused    %s\n", j.CacheOf)
	}
	fmt.Printf("attempts  %d\n", j.Attempts)
	if j.State.Terminal() {
		fmt.Printf("exit      %d\n", j.ExitCode)
	}
	if j.Error != "" {
		fmt.Printf("error     %s\n", j.Error)
	}
	fmt.Printf("log       %d bytes\n", j.LogBytes)
	for _, a := range j.Artifacts {
		fmt.Printf("artifact  %s %d %s\n", a.Path, a.Size, a.SHA256)
	}
}
