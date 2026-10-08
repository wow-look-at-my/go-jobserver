//go:build !cosmo && !linux

package jobserver

// The CPU mask calls a host outside the Linux family does not offer.

func setAffinitySyscall(int, []int) error { return errNoAffinity }

func clearAffinitySyscall(int, int) error { return errNoAffinity }

func getAffinitySyscall(int) ([]int, error) { return nil, errNoAffinity }
