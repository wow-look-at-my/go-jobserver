package jobserver

import "github.com/wow-look-at-my/simple-stats-api/cpusample"

// Reading is one sample of what the jobs and the host are using. simple-stats-api's cpusample takes it.
type Reading = cpusample.Reading

// Proc is one process as the host reported it.
type Proc = cpusample.Proc

// newSampler reads this host, or the host load from the stats-api at statsURL when it is set.
func newSampler(statsURL string) *cpusample.Sampler {
	if statsURL != "" {
		return cpusample.NewRemoteSampler(statsURL)
	}
	return cpusample.NewSampler()
}
