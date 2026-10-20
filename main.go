// Command go-jobserver runs a dependency-ordered job queue as a daemon, and
// hands jobs to it from the command line.
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "go-jobserver:", err)
		os.Exit(1)
	}
}

// run executes one command line on a fresh command tree, so flag values never
// carry over from an earlier call.
func run(args []string) error {
	root := newRootCmd()
	root.SetArgs(args)
	return root.Execute()
}
