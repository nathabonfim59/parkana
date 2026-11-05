// Package logs holds the in-memory log data model shared by ingest, storage
// and query: label sets, entries and push requests. It has no I/O.
package logs

import (
	"cmp"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Label is one name/value pair.
type Label struct {
	Name, Value string
}

// Labels is a label set sorted by name with unique names. Build it with
// FromMap or New so the invariant holds; the zero value is an empty set.
type Labels []Label

// New builds a sorted label set from pairs. A later duplicate name wins.
func New(pairs ...Label) Labels {
	ls := slices.Clone(pairs)
	slices.SortStableFunc(ls, func(a, b Label) int { return cmp.Compare(a.Name, b.Name) })
	// Keep the last of each run of equal names.
	out := ls[:0]
	for i, l := range ls {
		if i+1 < len(ls) && ls[i+1].Name == l.Name {
			continue
		}
		out = append(out, l)
	}
	return out
}

// FromMap builds a sorted label set from a map.
func FromMap(m map[string]string) Labels {
	ls := make(Labels, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		ls = append(ls, Label{Name: k, Value: m[k]})
	}
	return ls
}

// Map returns the labels as a new map.
func (ls Labels) Map() map[string]string {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

// Get returns the value of name and whether it is present.
func (ls Labels) Get(name string) (string, bool) {
	i, ok := slices.BinarySearchFunc(ls, name, func(l Label, n string) int { return cmp.Compare(l.Name, n) })
	if !ok {
		return "", false
	}
	return ls[i].Value, true
}

// Split partitions ls into the labels named in keep and the rest. Both results
// stay sorted.
func (ls Labels) Split(keep []string) (kept, rest Labels) {
	for _, l := range ls {
		if slices.Contains(keep, l.Name) {
			kept = append(kept, l)
		} else {
			rest = append(rest, l)
		}
	}
	return kept, rest
}

// String renders ls in Loki/Prometheus selector form: {a="1", b="2"}.
func (ls Labels) String() string {
	var b strings.Builder
	b.WriteByte('{')
	for i, l := range ls {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(l.Value))
	}
	b.WriteByte('}')
	return b.String()
}

// Hash returns a stable 64-bit hash of the label set, the same across
// processes and versions (FNV-1a over names and values).
func (ls Labels) Hash() uint64 {
	h := fnv.New64a()
	for _, l := range ls {
		h.Write([]byte(l.Name))
		h.Write([]byte{0xff})
		h.Write([]byte(l.Value))
		h.Write([]byte{0xff})
	}
	return h.Sum64()
}

// Validate checks that names match [a-zA-Z_][a-zA-Z0-9_]* and are unique and
// sorted.
func (ls Labels) Validate() error {
	for i, l := range ls {
		if !validName(l.Name) {
			return fmt.Errorf("logs: invalid label name %q", l.Name)
		}
		if i > 0 && ls[i-1].Name >= l.Name {
			return fmt.Errorf("logs: labels not sorted or duplicated at %q", l.Name)
		}
	}
	return nil
}

func validName(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range []byte(s) {
		switch {
		case c == '_', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// DefaultStream is the dataset name used when no stream label is present.
const DefaultStream = "default"

// StreamName picks the dataset (stream) for ls: the value of
// the first label in keys that is present and non-empty, sanitized to
// [a-z0-9_-] (max 128 bytes). It returns DefaultStream when none matches.
func (ls Labels) StreamName(keys []string) string {
	for _, k := range keys {
		if v, ok := ls.Get(k); ok && v != "" {
			return SanitizeStream(v)
		}
	}
	return DefaultStream
}

// SanitizeStream lower-cases s and replaces every byte outside [a-z0-9_-]
// with '_', so the result is safe as an object-key segment.
func SanitizeStream(s string) string {
	s = strings.ToLower(s)
	if len(s) > 128 {
		s = s[:128]
	}
	b := []byte(s)
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			b[i] = '_'
		}
	}
	if len(b) == 0 {
		return DefaultStream
	}
	return string(b)
}

// Entry is one log line.
type Entry struct {
	Timestamp time.Time
	Line      string
	// Metadata is Loki structured metadata or OTLP attributes (the meta column).
	Metadata Labels
}

// Stream is a Loki stream: one label set and its entries.
type Stream struct {
	Labels  Labels
	Entries []Entry
}

// PushRequest is a decoded Loki push (protobuf or JSON) or OTLP request.
type PushRequest struct {
	Streams []Stream
}

// Entries returns the number of entries over all streams.
func (r *PushRequest) Entries() int {
	n := 0
	for _, s := range r.Streams {
		n += len(s.Entries)
	}
	return n
}
