//go:build !unix

package jobserver

import "os/exec"

// prepareProcess leaves cancellation at its default: killing the command itself. A process group is a Unix notion.
func prepareProcess(cmd *exec.Cmd) {}
