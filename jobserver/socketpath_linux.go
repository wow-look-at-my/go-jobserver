//go:build linux

// unixPathLimit is the longest filesystem path a Unix socket can be bound to.

package jobserver

// unixPathLimit is the longest filesystem path a Unix socket can be bound to.
const unixPathLimit = 107
