package jobserver

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnUnreadableHostLoadIsAnErrorThatRepeats(t *testing.T) {
	var errs, infos lineLog
	s := newTestScheduler(t, SchedulerConfig{ErrorLogf: errs.logf, Logf: infos.logf})
	clock := &testClock{at: time.Now()}
	s.now = clock.now

	s.gov.tick()
	require.Len(t, errs.lines, 1, "the first sample that cannot read the host's load is an error")
	line := errs.lines[0]
	assert.Contains(t, line, "CONFIGURATION ERROR")
	assert.Contains(t, line, "this host has no /proc/stat", "the error names why")
	assert.Contains(t, line, "destabilize the system", "the error names what it risks")
	assert.Contains(t, line, "repeats every 5m0s")
	assert.NotContains(t, line, "has been unknown", "the first line has no duration to report")
	assert.Equal(t, "this host has no /proc/stat", s.Readings().HostError)

	clock.add(DefaultHostBlindEvery - time.Second)
	s.gov.tick()
	assert.Len(t, errs.lines, 1, "the error waits for its interval")
	clock.add(time.Second)
	s.gov.tick()
	require.Len(t, errs.lines, 2, "the error comes again when the interval has passed")
	assert.Contains(t, errs.lines[1], "unknown for 5m0s")
	clock.add(DefaultHostBlindEvery)
	s.gov.tick()
	require.Len(t, errs.lines, 3)
	assert.Contains(t, errs.lines[2], "unknown for 10m0s")
	assert.Empty(t, infos.containing("CONFIGURATION ERROR"), "an error line goes to ErrorLogf only")
}

func TestTheErrorIntervalIsConfigurable(t *testing.T) {
	var errs lineLog
	s := newTestScheduler(t, SchedulerConfig{ErrorLogf: errs.logf, HostBlindEvery: time.Minute})
	clock := &testClock{at: time.Now()}
	s.now = clock.now

	s.gov.tick()
	clock.add(time.Minute)
	s.gov.tick()
	require.Len(t, errs.lines, 2)
	assert.Contains(t, errs.lines[1], "repeats every 1m0s")
}

func TestTheErrorGoesToLogfWhenThereIsNoErrorLogf(t *testing.T) {
	var all lineLog
	s := newTestScheduler(t, SchedulerConfig{Logf: all.logf})

	s.gov.tick()
	assert.Len(t, all.containing("CONFIGURATION ERROR"), 1)
}

func TestTheErrorEndsWhenTheCountersCanBeReadAgain(t *testing.T) {
	var errs, infos lineLog
	s := newTestScheduler(t, SchedulerConfig{ErrorLogf: errs.logf, Logf: infos.logf})
	host := &scriptedHost{cpus: 4, err: errors.New("open /proc/stat: permission denied")}
	s.gov.sampler.host = host

	s.gov.tick()
	require.Len(t, errs.lines, 1)
	assert.Contains(t, errs.lines[0], "permission denied")
	assert.Equal(t, "open /proc/stat: permission denied", s.Readings().HostError)

	host.mu.Lock()
	host.err = nil
	host.mu.Unlock()
	s.gov.tick()
	assert.Empty(t, s.Readings().HostError, "the first read after the failure is only a baseline, and it is no error")
	assert.Len(t, infos.containing("can be read again"), 1)
	s.gov.tick()
	assert.Len(t, infos.containing("can be read again"), 1, "the all-clear comes once")
	assert.Len(t, errs.lines, 1)

	host.mu.Lock()
	host.err = errors.New("open /proc/stat: too many open files")
	host.mu.Unlock()
	s.gov.tick()
	assert.Len(t, errs.lines, 2, "a new streak is an error at once, not after the interval")
}

func TestAReadableHostIsNoError(t *testing.T) {
	var errs lineLog
	s := newTestScheduler(t, SchedulerConfig{ErrorLogf: errs.logf})
	s.gov.sampler.host = &scriptedHost{cpus: 4}

	for range 3 {
		s.gov.tick()
	}
	assert.Empty(t, errs.lines, "a first read that only sets the baseline is no failure")
	assert.Empty(t, s.Readings().HostError)
}

func TestAHostCheckTurnedOffNeedsNoHostLoad(t *testing.T) {
	var errs lineLog
	s := newTestScheduler(t, SchedulerConfig{HostBusyHigh: -1, ErrorLogf: errs.logf})

	s.gov.tick()
	assert.Empty(t, errs.lines)
	assert.Empty(t, s.Readings().HostError)
}

func TestTheDashboardShowsTheUnreadableHostLoad(t *testing.T) {
	srv := newTunedServer(t, nil, func(srv *Server) { srv.gov.sampler.host = nil })
	srv.gov.tick()

	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	page := rec.Body.String()
	assert.Contains(t, page, "Configuration error:")
	assert.Contains(t, page, "this host has no /proc/stat")
	assert.Contains(t, page, "host load unknown")
}
