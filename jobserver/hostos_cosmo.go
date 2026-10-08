//go:build cosmo

package jobserver

import (
	"runtime"
	"strings"
	"syscall"
)

// hostOS names the operating system this binary is running on. The toolchain
// builds one binary that runs on Linux, macOS and Windows, so a cosmo build
// asks the host with uname.
func hostOS() string {
	var u syscall.Utsname
	if err := syscall.Uname(&u); err != nil {
		return runtime.GOOS
	}
	name := utsField(u.Sysname[:])
	switch {
	case strings.Contains(name, "Windows"):
		return "windows"
	case strings.Contains(name, "Darwin"):
		return "darwin"
	case strings.Contains(name, "Linux"):
		return "linux"
	}
	return strings.ToLower(name)
}

// utsField reads a NUL-terminated utsname field.
func utsField(field []byte) string {
	n := 0
	for n < len(field) && field[n] != 0 {
		n++
	}
	return string(field[:n])
}
