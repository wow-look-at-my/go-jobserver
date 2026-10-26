// A LogWriter appends one job's output.

package jobserver

import (
	"errors"
	"io"
	"os"
)

// SyncLog flushes a job's log if the store holds it open. Unlike LogWriter, it
// never opens one, so a flush after the job ends leaves no file open.
func (s *Store) SyncLog(id string) error {
	s.mu.Lock()
	w, ok := s.logs[CanonicalID(id)]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	// Finish can close the file between the lookup and the flush. Close flushed it.
	if err := w.Sync(); !errors.Is(err, os.ErrClosed) {
		return err
	}
	return nil
}

func (s *Store) LogBytes(id string) int64 {
	info, err := os.Stat(s.LogPath(CanonicalID(id)))
	if err != nil {
		return 0
	}
	return info.Size()
}

// ReadLog returns up to max bytes of a job's output starting at offset.
func (s *Store) ReadLog(id string, offset int64, max int) ([]byte, error) {
	f, err := os.Open(s.LogPath(CanonicalID(id)))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	if max <= 0 {
		max = 64 << 10
	}
	return io.ReadAll(io.LimitReader(f, int64(max)))
}

// RemoveLog drops a job's output file. It is used when a job is removed.
func (s *Store) RemoveLog(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id = CanonicalID(id)
	// The file goes next, so a failed flush loses nothing.
	_ = s.closeLog(id)
	err := os.Remove(s.LogPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// A LogWriter appends one job's output.
type LogWriter struct {
	f *os.File
	n int64
}

// Write appends output.
func (w *LogWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.n += int64(n)
	return n, err
}

// N is how many bytes the file holds: those it held when opened, and those appended since.
func (w *LogWriter) N() int64 { return w.n }

// Sync flushes the file to stable storage.
func (w *LogWriter) Sync() error { return w.f.Sync() }

// Close syncs and closes the file.
func (w *LogWriter) Close() error {
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}
