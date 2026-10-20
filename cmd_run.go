package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

func init() { register(newRunCmd) }

func newRunCmd(g *globals) *cobra.Command {
	return &cobra.Command{
		Use:     "run",
		Aliases: []string{"daemon", "serve", "start"},
		Short:   "Run the daemon in the foreground",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := g.config()
			cfg.Logf = func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) }
			srv, err := jobserver.New(cfg)
			if err != nil {
				return err
			}
			if err := srv.Start(); err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			<-ctx.Done()
			fmt.Fprintln(os.Stderr, "go-jobserver: shutting down")
			return srv.Close()
		},
	}
}
