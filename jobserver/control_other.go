//go:build !unix

package jobserver

import "errors"

// The process controls a host outside the Unix family does not offer through
// this binary's syscall layer.

func setNiceTo(int, int) error {
	return errors.New("no setpriority call on this host")
}

func processNice(int) (int, error) {
	return 0, errors.New("no getpriority call on this host")
}

func darwinPolicy(int) (int, error) {
	return 0, errors.New("the background policy is macOS only")
}

func setDarwinBackground(int) (Outcome, string) {
	return OutcomeNotApplicable, "the background policy is macOS only"
}

func clearDarwinBackground(int) (Outcome, string) {
	return OutcomeNotApplicable, "the background policy is macOS only"
}

func freeze(int) (Outcome, string) {
	return OutcomeNotApplicable, "no process-suspend call on this host"
}

func thaw(int) (Outcome, string) {
	return OutcomeNotApplicable, "no process-suspend call on this host"
}
