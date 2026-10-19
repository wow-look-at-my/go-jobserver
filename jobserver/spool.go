package jobserver

import (
	"encoding/json"
	"fmt"
	"github.com/wow-look-at-my/go-containers/set"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The spool is a directory the daemon watches.
const (
	spoolDoneDir   = "done"
	spoolFailedDir = "failed"
)

// watchSpool scans the spool directory until the server closes.
func (s *Server) watchSpool() {
	defer s.wg.Done()
	for _, sub := range []string{"", spoolDoneDir, spoolFailedDir} {
		if err := os.MkdirAll(filepath.Join(s.cfg.SpoolDir, sub), 0o755); err != nil {
			s.cfg.Logf("go-jobserver: spool: %v", err)
			return
		}
	}
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

// scanSpool processes every job file that is waiting. A file that left the
// directory is no longer stuck.
func (s *Server) scanSpool() {
	entries, err := os.ReadDir(s.cfg.SpoolDir)
	if err != nil {
		return
	}
	waiting := set.New[string]()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		waiting.Add(entry.Name())
		s.takeSpoolFile(entry.Name())
	}
	for name := range s.spoolStuck {
		if !waiting.Contains(name) {
			delete(s.spoolStuck, name)
		}
	}
}

// takeSpoolFile reads one spool file, enqueues what it describes, and moves
// the file to done/ or failed/. A file that cannot be moved stays in the
// spool. It is skipped until it changes, so it makes one job and not one per
// scan.
func (s *Server) takeSpoolFile(name string) {
	path := filepath.Join(s.cfg.SpoolDir, name)
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	mtime := info.ModTime().UnixNano()
	if stuck, ok := s.spoolStuck[name]; ok && stuck == mtime {
		return
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return
	}
	spec, err := specFromFile(name, body)
	if err != nil {
		s.moveSpool(name, mtime, spoolFailedDir, err)
		return
	}
	if _, _, err := s.Enqueue(*spec); err != nil {
		s.moveSpool(name, mtime, spoolFailedDir, err)
		return
	}
	s.cfg.Logf("go-jobserver: spooled %s", name)
	s.moveSpool(name, mtime, spoolDoneDir, nil)
}

// moveSpool moves a processed file aside. When the move fails, it logs the
// failure and marks that version of the file as stuck.
func (s *Server) moveSpool(name string, mtime int64, dir string, cause error) {
	if err := s.renameSpool(name, dir, cause); err != nil {
		s.spoolStuck[name] = mtime
		s.cfg.Logf("go-jobserver: spool %s: %v; the file stays and is skipped until it changes", name, err)
	}
}

// renameSpool makes the destination directory and moves the file into it,
// recording why when it failed. The reason is written before the move, so a
// file never appears under failed/ without the reason beside it.
func (s *Server) renameSpool(name, dir string, cause error) error {
	dstDir := filepath.Join(s.cfg.SpoolDir, dir)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	dst := filepath.Join(dstDir, name)
	reason := dst + ".error"
	if cause != nil {
		if err := os.WriteFile(reason, []byte(cause.Error()+"\n"), 0o644); err != nil {
			return err
		}
	}
	if err := os.Rename(filepath.Join(s.cfg.SpoolDir, name), dst); err != nil {
		if cause != nil {
			os.Remove(reason)
		}
		return err
	}
	return nil
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
