// Park writes a spec into the spool, so the next scan or the next Open adds it.

package spool

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

// Park writes spec to the spool as a JSON file, through a temporary file and a rename. It returns the file name.
// The spec must have the type the inbox's AddTask takes. A spool at a bound refuses with ErrFull.
func (s *Spool) Park(spec any) (string, error) {
	t := reflect.TypeOf(spec)
	if t != s.specType && !(s.specType.Kind() == reflect.Pointer && t == s.specType.Elem()) {
		return "", fmt.Errorf("spool: Park takes %v, not %v", s.specType, t)
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("spool: %w", err)
	}
	list, err := s.entries()
	if err != nil {
		return "", err
	}
	var total int64
	for _, e := range list {
		total += e.size
	}
	s.mu.Lock()
	full := len(list) >= s.maxEntries || total+int64(len(data)) > s.maxBytes
	s.mu.Unlock()
	if full {
		return "", ErrFull
	}
	now := time.Now().UTC()
	name := fmt.Sprintf("%020d-%s.json", now.UnixNano(), newID())
	tmp, err := os.CreateTemp(s.dir, ".spool-*.tmp")
	if err != nil {
		return "", fmt.Errorf("spool: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return "", fmt.Errorf("spool: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return "", fmt.Errorf("spool: %w", err)
	}
	if err := os.Chtimes(tmpName, now, now); err != nil {
		os.Remove(tmpName)
		return "", fmt.Errorf("spool: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(s.dir, name)); err != nil {
		os.Remove(tmpName)
		return "", fmt.Errorf("spool: %w", err)
	}
	return name, nil
}

// newID is a random name part, so parks in one instant get distinct names.
func newID() string {
	var b [10]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("spool: crypto/rand: %v", err))
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
}
