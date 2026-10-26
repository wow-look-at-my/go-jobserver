// The spool is a directory the daemon watches.

package jobserver

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/wow-look-at-my/go-jobserver/spool"
)

// The subdirectories a processed job file moves to.
const (
	spoolDoneDir   = spool.DoneDir
	spoolFailedDir = spool.FailedDir
)

// watchSpool takes the spool directory's job files until the server closes. Each moves to done/ once enqueued, or
// to failed/ with the reason.
func (s *Server) watchSpool() {
	defer s.wg.Done()
	sp, err := spool.Open(s.cfg.SpoolDir, spool.Options{
		KeepDone: true,
		DirMode:  0o755,
		Logf:     func(format string, args ...any) { s.cfg.Logf("go-jobserver: "+format, args...) },
	})
	if err != nil {
		s.cfg.Logf("go-jobserver: %v", err)
		return
	}
	s.spool = sp
	s.cfg.Logf("go-jobserver: spool %s", s.cfg.SpoolDir)
	s.scanSpool()
	t := time.NewTicker(s.cfg.SpoolInterval)
	defer t.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-t.C:
			s.scanSpool()
		}
	}
}

// scanSpool enqueues every job file that waits.
func (s *Server) scanSpool() {
	if _, err := s.spool.Process(func(e spool.Entry) error {
		spec, err := specFromFile(e.Name, e.Body)
		if err != nil {
			return err
		}
		if _, _, err := s.Enqueue(*spec); err != nil {
			return err
		}
		s.cfg.Logf("go-jobserver: spooled %s", e.Name)
		return nil
	}); err != nil {
		s.cfg.Logf("go-jobserver: spool: %v", err)
	}
}

// specFromFile parses a spool file. JSON is a Spec, an array or a string; any
// other content is a command line.
func specFromFile(name string, body []byte) (*Spec, error) {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return nil, fmt.Errorf("go-jobserver: %s is empty", name)
	}
	base := strings.TrimSuffix(name, filepath.Ext(name))
	if strings.HasPrefix(text, "{") {
		var spec Spec
		if err := json.Unmarshal(body, &spec); err != nil {
			return nil, fmt.Errorf("go-jobserver: %s: %w", name, err)
		}
		if len(spec.Command) == 0 {
			return nil, fmt.Errorf("go-jobserver: %s: spec has no command", name)
		}
		if spec.Name == "" {
			spec.Name = base
		}
		return &spec, nil
	}
	if strings.HasPrefix(text, "[") {
		var argv []string
		if err := json.Unmarshal(body, &argv); err != nil {
			return nil, fmt.Errorf("go-jobserver: %s: %w", name, err)
		}
		if len(argv) == 0 {
			return nil, fmt.Errorf("go-jobserver: %s: empty command", name)
		}
		return &Spec{Name: base, Command: argv}, nil
	}
	if strings.HasPrefix(text, "\"") {
		var line string
		if err := json.Unmarshal(body, &line); err != nil {
			return nil, fmt.Errorf("go-jobserver: %s: %w", name, err)
		}
		argv := splitCommand(line)
		if len(argv) == 0 {
			return nil, fmt.Errorf("go-jobserver: %s: empty command", name)
		}
		return &Spec{Name: base, Command: argv}, nil
	}
	argv := splitCommand(text)
	if len(argv) == 0 {
		return nil, fmt.Errorf("go-jobserver: %s: no command", name)
	}
	return &Spec{Name: base, Command: argv}, nil
}
