// Package priority holds the priority keys of an inbox and the values a task sets for them.
package priority

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// MultiPriorityKey is one key of an inbox. A key earlier in its inbox type wins over a later one. NewKeys sets index.
type MultiPriorityKey struct {
	Name            string
	DefaultPriority int
	index           int
}

// MultiPriority holds one value per key, in key order. The zero value means every key at its default.
type MultiPriority struct {
	// values holds each key's value minus its default, so a missing entry is the default.
	values []int
}

// Set gives a key a value. The receiver gets its own copy of the values, so a copy of p does not change.
func (p *MultiPriority) Set(k *MultiPriorityKey, v int) {
	n := max(len(p.values), k.index+1)
	next := make([]int, n)
	copy(next, p.values)
	next[k.index] = v - k.DefaultPriority
	p.values = next
}

// Get returns a key's value, or its default when no value is set.
func (p MultiPriority) Get(k *MultiPriorityKey) int {
	if k.index < len(p.values) {
		return k.DefaultPriority + p.values[k.index]
	}
	return k.DefaultPriority
}

var (
	// ErrUnknownKey is returned for a value under a name the key list does not have.
	ErrUnknownKey = errors.New("priority: unknown key")
	// ErrBadKey is returned for a key list or a struct tag that is not valid.
	ErrBadKey = errors.New("priority: bad key")
)

var keyName = regexp.MustCompile(`^[a-z0-9]+([-_][a-z0-9]+)*$`)

// Keys is an ordered key list. Each key belongs to one list.
type Keys struct {
	list   []*MultiPriorityKey
	byName map[string]*MultiPriorityKey
}

// NewKeys builds a key list in the order given and sets each key's index.
func NewKeys(keys ...*MultiPriorityKey) (*Keys, error) {
	k := &Keys{byName: map[string]*MultiPriorityKey{}}
	for i, key := range keys {
		if !keyName.MatchString(key.Name) {
			return nil, fmt.Errorf("%w: name %q is not lowercase words", ErrBadKey, key.Name)
		}
		if _, dup := k.byName[key.Name]; dup {
			return nil, fmt.Errorf("%w: name %q is listed twice", ErrBadKey, key.Name)
		}
		key.index = i
		k.byName[key.Name] = key
		k.list = append(k.list, key)
	}
	return k, nil
}

// keyType is the type of a key field.
var keyType = reflect.TypeFor[MultiPriorityKey]()

// FromFields builds the key list from the MultiPriorityKey fields of the struct that ptr points to, in field order.
// The tag `priority:"name,default=N"` gives the name and the default. A field with no name gets its field name in lowercase.
func FromFields(ptr any) (*Keys, error) {
	v := reflect.ValueOf(ptr)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: %T is not a pointer to a struct", ErrBadKey, ptr)
	}
	v = v.Elem()
	t := v.Type()
	var keys []*MultiPriorityKey
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Type != keyType || !f.IsExported() {
			continue
		}
		key := v.Field(i).Addr().Interface().(*MultiPriorityKey)
		name, def, err := parseTag(f.Tag.Get("priority"))
		if err != nil {
			return nil, fmt.Errorf("%w: field %s: %v", ErrBadKey, f.Name, err)
		}
		if name == "" {
			name = strings.ToLower(f.Name)
		}
		key.Name, key.DefaultPriority = name, def
		keys = append(keys, key)
	}
	return NewKeys(keys...)
}

// parseTag reads "name,default=N".
func parseTag(tag string) (string, int, error) {
	name, rest, _ := strings.Cut(tag, ",")
	def := 0
	for opt := range strings.SplitSeq(rest, ",") {
		if opt == "" {
			continue
		}
		val, ok := strings.CutPrefix(opt, "default=")
		if !ok {
			return "", 0, fmt.Errorf("unknown option %q", opt)
		}
		n, err := strconv.Atoi(val)
		if err != nil {
			return "", 0, fmt.Errorf("default %q is not a whole number", val)
		}
		def = n
	}
	return strings.TrimSpace(name), def, nil
}

// List returns the keys in comparison order.
func (k *Keys) List() []*MultiPriorityKey { return slices.Clone(k.list) }

// Lookup returns the key with a name, or nil.
func (k *Keys) Lookup(name string) *MultiPriorityKey { return k.byName[name] }

// New returns a priority with every key at its default.
func (k *Keys) New() MultiPriority { return MultiPriority{} }

// Compare orders priorities: lower first, and the first value that differs decides.
func (k *Keys) Compare(a, b MultiPriority) int {
	for _, key := range k.list {
		if c := a.Get(key) - b.Get(key); c != 0 {
			if c < 0 {
				return -1
			}
			return 1
		}
	}
	return 0
}

// Values names each value of p by its key, defaults included.
func (k *Keys) Values(p MultiPriority) map[string]int {
	out := make(map[string]int, len(k.list))
	for _, key := range k.list {
		out[key.Name] = p.Get(key)
	}
	return out
}

// FromValues builds a priority from values by key name. A name the list does not have is ErrUnknownKey.
func (k *Keys) FromValues(values map[string]int) (MultiPriority, error) {
	var p MultiPriority
	for name, v := range values {
		key := k.byName[name]
		if key == nil {
			return MultiPriority{}, fmt.Errorf("%w: %q", ErrUnknownKey, name)
		}
		p.Set(key, v)
	}
	return p, nil
}
