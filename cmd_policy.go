package main

import (
	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newPolicyCmd) }

func newPolicyCmd(g *globals) *cobra.Command {
	var pol policyFlags
	cmd := &cobra.Command{
		Use:     "policy [flags] <id>",
		Aliases: []string{"cpu"},
		Short:   "Replace a job's CPU cost and process-control settings",
		Long: `Replace a job's CPU cost and process-control settings.

The flags say what the policy is now, so a flag left out turns its mechanism off.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			policy := pol.policy()
			resp, err := do(g, jobserver.Request{Op: jobserver.OpPolicy, ID: args[0], Policy: &policy})
			if err != nil {
				return err
			}
			printPolicy(resp.Job.Policy)
			return nil
		},
	}
	pol.bind(cmd.Flags())
	return cmd
}
