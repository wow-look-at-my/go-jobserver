// Opening a store: the journal replay, then the log recovery that follows it.

package jobserver

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// replay reads the journal into the job table, cutting a torn tail.
func (s *Store) replay() error {
	path := s.journalPath()
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	var (
		offset int64
		good   int64
	)
frames:
	for {
		payload, err := readFrame(f)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A torn or corrupt frame ends the replay. Everything after it is unreadable, so the file is cut here.
			break
		}
		rec, err := decodeRecord(payload)
		switch {
		case errors.Is(err, errUnknownRecord):
			// A newer build wrote this frame. Cutting it would delete every record after it, so the replay skips it and says so.
			s.skipped = append(s.skipped, fmt.Sprintf("journal record kind %d at offset %d is unknown to this build and was skipped", rec.kind, good))
		case err != nil:
			// The frame is checksummed, so a decode error means a corrupt record.
			break frames
		default:
			s.apply(rec)
		}
		good += int64(frameHeaderSize + len(payload))
		offset = good
	}
	if err := f.Close(); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > offset {
		if err := os.Truncate(path, offset); err != nil {
			return err
		}
	}
	return nil
}

// recoverLogs settles each job's log against what the journal recorded, and
// fails a job that was running when the process died.
func (s *Store) recoverLogs() error {
	for _, id := range s.order {
		j := s.jobs[id]
		info, err := os.Stat(s.LogPath(id))
		switch {
		case err == nil && j.LogBytes > 0 && info.Size() > j.LogBytes:
			// A crash left bytes the journal never recorded.
			if err := os.Truncate(s.LogPath(id), j.LogBytes); err != nil {
				return err
			}
		case err == nil:
			j.LogBytes = info.Size()
		case errors.Is(err, os.ErrNotExist):
			// The journal is the record of how much output a finished job
			// wrote; a missing file is not evidence that it wrote none.
		default:
			return err
		}
		if j.State == StateRunning {
			j.State = StateFailed
			j.Error = "interrupted by daemon restart"
			j.Finished = time.Now()
			if err := s.append(record{kind: recFinish, id: id, state: StateFailed, exitCode: -1, finished: j.Finished.UnixNano(), errMsg: j.Error, logBytes: j.LogBytes}); err != nil {
				return err
			}
			if err := s.sync(); err != nil {
				return err
			}
		}
	}
	return nil
}
