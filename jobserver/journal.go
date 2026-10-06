package jobserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// The journal is an append-only file of frames.
const (
	frameHeaderSize = 8
	// maxFrameSize bounds a single record, so a corrupt length cannot make the reader allocate the world.
	maxFrameSize = 64 << 20
)

// record kinds
const (
	recCreate   byte = 1
	recStart    byte = 2
	recFinish   byte = 3
	recIdentity byte = 4
	recState    byte = 5
	recControl  byte = 6
	recDeps     byte = 7
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// record is one journal entry. Which fields matter depends on kind.
type record struct {
	kind byte

	// recCreate
	id                             string
	name                           string
	command, deps, inputs, outputs []string
	env                            []string
	dir, key                       string
	force                          bool
	created                        int64
	state                          State

	// recStart
	attempt int
	started int64

	// recFinish, recState
	exitCode  int
	finished  int64
	errMsg    string
	logBytes  int64
	artifacts []Artifact

	// recIdentity
	identity string
	cacheOf  string

	// recControl
	paused bool

	// recDeps
	replaceDeps []string
}

// Encode returns the payload bytes of the record without its frame.
func (r record) encode() []byte {
	e := encoder{}
	e.u8(r.kind)
	switch r.kind {
	case recCreate:
		e.str(r.id)
		e.str(r.name)
		e.list(r.command)
		e.list(r.deps)
		e.list(r.inputs)
		e.list(r.outputs)
		e.list(r.env)
		e.str(r.dir)
		e.str(r.key)
		e.bool(r.force)
		e.i64(r.created)
		e.str(string(r.state))
	case recStart:
		e.str(r.id)
		e.uvarint(uint64(r.attempt))
		e.i64(r.started)
	case recFinish:
		e.str(r.id)
		e.str(string(r.state))
		e.i64(int64(r.exitCode))
		e.i64(r.finished)
		e.str(r.errMsg)
		e.i64(r.logBytes)
		e.uvarint(uint64(len(r.artifacts)))
		for _, a := range r.artifacts {
			e.str(a.Path)
			e.i64(a.Size)
			e.str(a.SHA256)
		}
	case recIdentity:
		e.str(r.id)
		e.str(r.identity)
		e.str(r.cacheOf)
	case recState:
		e.str(r.id)
		e.str(string(r.state))
		e.str(r.errMsg)
	case recControl:
		if r.paused {
			e.u8(1)
		} else {
			e.u8(0)
		}
		e.i64(r.created)
	case recDeps:
		e.str(r.id)
		e.list(r.replaceDeps)
	default:
		panic(fmt.Sprintf("jobserver: unknown record kind %d", r.kind))
	}
	return e.b
}

// decodeRecord parses a payload back into a record.
func decodeRecord(payload []byte) (record, error) {
	d := decoder{b: payload}
	r := record{kind: d.u8()}
	switch r.kind {
	case recCreate:
		r.id = d.str()
		r.name = d.str()
		r.command = d.list()
		r.deps = d.list()
		r.inputs = d.list()
		r.outputs = d.list()
		r.env = d.list()
		r.dir = d.str()
		r.key = d.str()
		r.force = d.bool()
		r.created = d.i64()
		r.state = State(d.str())
	case recStart:
		r.id = d.str()
		r.attempt = int(d.uvarint())
		r.started = d.i64()
	case recFinish:
		r.id = d.str()
		r.state = State(d.str())
		r.exitCode = int(d.i64())
		r.finished = d.i64()
		r.errMsg = d.str()
		r.logBytes = d.i64()
		n := int(d.uvarint())
		if n < 0 {
			return record{}, errors.New("jobserver: negative artifact count")
		}
		r.artifacts = make([]Artifact, 0, n)
		for i := 0; i < n; i++ {
			var a Artifact
			a.Path = d.str()
			a.Size = d.i64()
			a.SHA256 = d.str()
			r.artifacts = append(r.artifacts, a)
		}
	case recIdentity:
		r.id = d.str()
		r.identity = d.str()
		r.cacheOf = d.str()
	case recState:
		r.id = d.str()
		r.state = State(d.str())
		r.errMsg = d.str()
	case recControl:
		r.paused = d.u8() == 1
		r.created = d.i64()
	case recDeps:
		r.id = d.str()
		r.replaceDeps = d.list()
	default:
		return record{}, fmt.Errorf("jobserver: unknown record kind %d", r.kind)
	}
	if d.err != nil {
		return record{}, d.err
	}
	if d.remaining() {
		return record{}, errors.New("jobserver: record has trailing bytes")
	}
	return r, nil
}

// frame wraps a payload with its length and checksum.
func frame(payload []byte) []byte {
	out := make([]byte, frameHeaderSize+len(payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(out[4:8], crc32.Checksum(payload, crcTable))
	copy(out[frameHeaderSize:], payload)
	return out
}

// readFrame reads one frame from r. It returns the payload, or io.EOF at a
// clean end of file, or errTornFrame when the tail is incomplete or corrupt.
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, errTornFrame
	}
	n := binary.BigEndian.Uint32(hdr[0:4])
	if n > maxFrameSize {
		return nil, errTornFrame
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, errTornFrame
	}
	want := binary.BigEndian.Uint32(hdr[4:8])
	if crc32.Checksum(payload, crcTable) != want {
		return nil, errTornFrame
	}
	return payload, nil
}

// errTornFrame marks a frame the writer never finished.
var errTornFrame = errors.New("jobserver: torn record")

// An encoder appends varint-tagged fields.
type encoder struct{ b []byte }

func (e *encoder) u8(v byte) { e.b = append(e.b, v) }
func (e *encoder) bool(v bool) {
	if v {
		e.u8(1)
		return
	}
	e.u8(0)
}
func (e *encoder) uvarint(v uint64) { e.b = binary.AppendUvarint(e.b, v) }
func (e *encoder) i64(v int64)      { e.b = binary.AppendVarint(e.b, v) }

func (e *encoder) str(s string) {
	e.uvarint(uint64(len(s)))
	e.b = append(e.b, s...)
}

func (e *encoder) list(ss []string) {
	e.uvarint(uint64(len(ss)))
	for _, s := range ss {
		e.str(s)
	}
}

// A decoder reads what an encoder wrote.
type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail(msg string) {
	if d.err == nil {
		d.err = errors.New("jobserver: " + msg)
	}
}

func (d *decoder) u8() byte {
	if len(d.b) < 1 {
		d.fail("record ends early")
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) bool() bool { return d.u8() == 1 }

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail("bad varint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) i64() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.fail("bad varint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) str() string {
	n := d.uvarint()
	if d.err != nil {
		return ""
	}
	if n > uint64(len(d.b)) {
		d.fail("string runs past the record")
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}

func (d *decoder) list() []string {
	n := d.uvarint()
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.b)) {
		d.fail("list runs past the record")
		return nil
	}
	out := make([]string, 0, n)
	for i := uint64(0); i < n; i++ {
		out = append(out, d.str())
		if d.err != nil {
			return nil
		}
	}
	return out
}

func (d *decoder) remaining() bool { return len(d.b) > 0 }
