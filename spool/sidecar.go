// A task sidecar records the task a spool file became, so a restart does not add the file twice.

package spool

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

// sidecarBody is the content of a task sidecar. Mtime is the file's modification time when the task was added.
type sidecarBody struct {
	Task  string `json:"task"`
	Mtime int64  `json:"mtime"`
}

// sidecar is the path of the task sidecar of a spool file.
func (s *Spool) sidecar(name string) string { return filepath.Join(s.dir, name+taskSuffix) }

// record writes the task sidecar of a file through a temporary file and a rename.
func (s *Spool) record(e entry, task *jobserver.Task) error {
	data, err := json.Marshal(sidecarBody{Task: task.ID, Mtime: e.mtime.UnixNano()})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".task-*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), s.sidecar(e.name)); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return nil
}

// resume returns the task a file already became, when its sidecar names a task the server still holds.
// A sidecar for another version of the file, or for a task the server does not know, is removed, and known is false.
func (s *Spool) resume(e entry) (task *jobserver.Task, known bool, err error) {
	path := s.sidecar(e.name)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var body sidecarBody
	if err := json.Unmarshal(data, &body); err != nil || body.Task == "" || body.Mtime != e.mtime.UnixNano() {
		return nil, false, s.drop(path, "it does not name a task of this version of the file")
	}
	if !s.lookup.IsValid() {
		return nil, false, s.drop(path, "the inbox has no Lookup method to find its task")
	}
	out := s.lookup.Call([]reflect.Value{reflect.ValueOf(body.Task)})
	if lerr, _ := out[1].Interface().(error); lerr != nil {
		if errors.Is(lerr, jobserver.ErrNotFound) {
			return nil, false, s.drop(path, fmt.Sprintf("the server does not know task %s", body.Task))
		}
		return nil, false, lerr
	}
	task, _ = out[0].Interface().(*jobserver.Task)
	if task == nil {
		return nil, false, fmt.Errorf("Lookup(%s) returned no task and no error", body.Task)
	}
	return task, true, nil
}

// dropOrphans removes each sidecar whose file left the spool, so a new file with the same name and time is a new task.
func (s *Spool) dropOrphans() {
	list, err := os.ReadDir(s.dir)
	if err != nil {
		s.logf("spool: %v", err)
		return
	}
	for _, de := range list {
		name := de.Name()
		base, isSidecar := strings.CutSuffix(name, taskSuffix)
		if de.IsDir() || !isSidecar || strings.HasPrefix(name, ".") {
			continue
		}
		if _, err := os.Lstat(filepath.Join(s.dir, base)); !errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.logf("spool: %s: %v", name, err)
		}
	}
}

// drop removes a sidecar the spool cannot use, and logs why. The file is then added again.
func (s *Spool) drop(path, why string) error {
	s.logf("spool: %s: %s; the file is added again", filepath.Base(path), why)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
