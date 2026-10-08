//go:build !cosmo

package jobserver

import "runtime"

// hostOS names the operating system this binary is running on.
func hostOS() string { return runtime.GOOS }
