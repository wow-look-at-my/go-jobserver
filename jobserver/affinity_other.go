//go:build !cosmo && !linux

package jobserver

import "errors"

// errNoAffinity is what a host without sched_setaffinity(2) reports.
var errNoAffinity = errors.New("jobserver: this host has no sched_setaffinity")

func setAffinitySyscall(int, []int) error { return errNoAffinity }

func clearAffinitySyscall(int, int) error { return errNoAffinity }

func getAffinitySyscall(int) ([]int, error) { return nil, errNoAffinity }
