// The store's journal writes: append, sync, commit and note.

package jobserver

import "errors"

// append writes one record and returns after the bytes are in the file.
func (s *Store) append(r record) error {
	if s.journal == nil {
		return errors.New("jobserver: store is closed")
	}
	_, err := s.journal.Write(frame(r.encode()))
	return err
}

// sync pushes every appended byte to stable storage.
func (s *Store) sync() error {
	if s.journal == nil {
		return nil
	}
	return s.journal.Sync()
}

// commit appends a record and syncs it.
func (s *Store) commit(r record) error {
	if err := s.append(r); err != nil {
		return err
	}
	if j, ok := s.jobs[r.id]; ok {
		s.touch(j)
	}
	s.bump()
	return s.sync()
}

// note appends a record with no sync. A crash can lose the last notes, and the next commit syncs them.
func (s *Store) note(r record) error {
	if err := s.append(r); err != nil {
		return err
	}
	if j, ok := s.jobs[r.id]; ok {
		s.touch(j)
	}
	s.bump()
	return nil
}
