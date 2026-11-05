package storage

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/nathabonfim59/parkana/internal/schema"
)

// Section tags and value kinds of the canonical identity encoding. Every
// section is tagged and carries an explicit entry count, and every string is
// length-prefixed, so two different identities can never encode to the same
// bytes.
const (
	mergeIDTagLine   byte = 1
	mergeIDTagLabels byte = 2
	mergeIDTagMeta   byte = 3

	// mergeIDKindNull marks a null line or a null map item (only foreign
	// writers produce those); it stays distinct from an empty string.
	mergeIDKindNull  byte = 0
	mergeIDKindValue byte = 1
)

// mergeDedup removes exact duplicate rows while a merge streams its inputs:
// within one global equal-timestamp run, a row whose canonical identity was
// already seen is dropped and counted, keeping the first row in the stable
// merge order (with its pipeline column values).
//
// Identity is the timestamp, the exact line bytes, the complete effective
// label set and the metadata, encoded as canonical bytes:
//
//   - Effective labels: for a name in the caller's promoted list, a non-null
//     column value of the row's input wins over conflicting labels-map
//     entries, a null column falls back to the map entries, and a label
//     absent from both is omitted. Entries under other names pass through.
//     Names compare case-sensitively and sort by raw bytes; a label absent
//     from a row and a label present with an empty value are distinct, and
//     bytes are never normalized (invalid UTF-8 included).
//   - Metadata: a null map and an empty one are the same empty set; entries
//     sort by key; a null item value stays distinct from an empty string.
//
// This is never coarser than read-time dedup (internal/query's distinct row
// identity): identity-equal rows read identically after the merge, so no
// visible row is ever lost. The encoding is copied out of the borrowed Arrow
// arrays before it is retained, so the seen set never aliases input buffers.
// It is released when the global timestamp advances, so the extra memory
// Merge holds is proportional to the largest equal-timestamp run's distinct
// identities, never to the whole file.
type mergeDedup struct {
	// inputs locates each input's identity columns; names is the caller's
	// promoted list sorted by raw bytes, and inputs[i].cols is parallel to
	// it (-1 where the input predates the column).
	inputs []mergeInputCols
	names  []string

	seen    map[string]struct{}
	last    arrow.Timestamp
	hasLast bool
	removed int64

	// Scratch reused across rows: buf accumulates one row's canonical bytes
	// and ents/fbEnts hold its entries, all overwritten by the next check
	// call. Only copied identity bytes are used for the retained seen keys;
	// the entry strings borrow Arrow storage and are never dereferenced
	// after the check that read them.
	buf    []byte
	ents   []mergeIDEnt
	fbEnts []mergeIDEnt
}

// mergeInputCols locates the identity columns of one input inside its record
// batches. The base columns always exist (mergeSchemas requires them);
// promoted names may be missing (-1) when an older input predates the column.
type mergeInputCols struct {
	line, labels, meta int
	cols               []int
}

// mergeIDEnt is one label or metadata entry while a row's identity is built.
// The strings borrow Arrow storage and are copied into the canonical bytes
// before anything is retained.
type mergeIDEnt struct {
	name string
	val  string
	null bool
}

// newMergeDedup prepares deduplication for a merge of m's inputs over the
// caller's promoted label names. Merge must call it only after NewWriter
// succeeded, which guarantees every promoted name is a column of the merged
// schema.
func newMergeDedup(m *mergeSchemaMap, promoted []string) *mergeDedup {
	names := slices.Clone(promoted)
	slices.Sort(names)
	fieldOf := make(map[string]int, m.schema.NumFields())
	for j, f := range m.schema.Fields() {
		fieldOf[f.Name] = j
	}
	d := &mergeDedup{
		inputs: make([]mergeInputCols, len(m.columns)),
		names:  names,
		seen:   make(map[string]struct{}),
	}
	for i := range m.columns {
		d.inputs[i] = mergeInputCols{
			line:   m.columns[i][1],
			labels: m.columns[i][2],
			meta:   m.columns[i][3],
			cols:   make([]int, len(names)),
		}
		for j, name := range names {
			d.inputs[i].cols[j] = m.columns[i][fieldOf[name]]
		}
	}
	return d
}

// beginRun switches the dedup state to ts. Equal-timestamp runs are global
// across inputs, so the seen set is released exactly when the global
// timestamp advances, in either direction from zero; hasLast, not a zero
// timestamp, marks the first run.
func (d *mergeDedup) beginRun(ts arrow.Timestamp) {
	if d.hasLast && d.last == ts {
		return
	}
	clear(d.seen)
	d.last = ts
	d.hasLast = true
}

// check reports whether the row at index row of c's current record duplicates
// an earlier row of the current equal-timestamp run, recording its identity
// otherwise. Malformed runtime arrays are errors (Reader validates decoded
// schemas, so they are defensive), never panics.
func (d *mergeDedup) check(c *mergeCursor, row int64) (bool, error) {
	ic := &d.inputs[c.input]
	rec := c.rec
	r := int(row)

	buf := d.buf[:0]
	buf = binary.LittleEndian.AppendUint64(buf, uint64(c.timestamp))

	line, ok := rec.Column(ic.line).(*array.String)
	if !ok {
		return false, fmt.Errorf("storage: merge input %d: column %q has type %T, want *array.String",
			c.input, schema.ColLine, rec.Column(ic.line))
	}
	buf = append(buf, mergeIDTagLine)
	if line.IsNull(r) {
		buf = append(buf, mergeIDKindNull)
	} else {
		v := line.Value(r)
		buf = append(buf, mergeIDKindValue)
		buf = binary.AppendUvarint(buf, uint64(len(v)))
		buf = append(buf, v...)
	}

	// Effective label set: map entries under promoted names only resolve
	// through the column (or fall back to them below); others pass through.
	labels, lkeys, litems, err := d.mapArray(c, ic.labels, schema.ColLabels)
	if err != nil {
		return false, err
	}
	ents := d.ents[:0]
	fbEnts := d.fbEnts[:0]
	if !labels.IsNull(r) {
		beg, end := labels.ValueOffsets(r)
		for k := beg; k < end; k++ {
			e := mergeIDEnt{
				name: lkeys.Value(int(k)),
				val:  litems.Value(int(k)),
				null: litems.IsNull(int(k)),
			}
			if _, promoted := slices.BinarySearch(d.names, e.name); promoted {
				fbEnts = append(fbEnts, e)
				continue
			}
			ents = append(ents, e)
		}
	}
	// Resolve the promoted names: a non-null column of this input shadows the
	// name's map entries; otherwise they decide. fbEnts is sorted by name and
	// every entry's name is promoted, so each name's entries form one
	// contiguous run and interleaved storage order can neither lose an entry
	// nor reassign it to another name, whatever the column values are.
	slices.SortFunc(fbEnts, cmpMergeIDEnt)
	fi := 0
	for j, name := range d.names {
		run := fi
		for fi < len(fbEnts) && fbEnts[fi].name == name {
			fi++
		}
		if col := ic.cols[j]; col >= 0 {
			pc, ok := rec.Column(col).(*array.String)
			if !ok {
				return false, fmt.Errorf("storage: merge input %d: promoted label %q has type %T, want *array.String",
					c.input, name, rec.Column(col))
			}
			if !pc.IsNull(r) {
				ents = append(ents, mergeIDEnt{name: name, val: pc.Value(r)})
				continue
			}
		}
		ents = append(ents, fbEnts[run:fi]...)
	}
	slices.SortFunc(ents, cmpMergeIDEnt)
	buf = append(buf, mergeIDTagLabels)
	buf = binary.AppendUvarint(buf, uint64(len(ents)))
	for _, e := range ents {
		buf = appendMergeIDEnt(buf, e)
	}
	d.ents = ents
	d.fbEnts = fbEnts

	// Metadata: every entry, sorted; null and empty maps are the same set.
	ents = d.ents[:0]
	meta, mkeys, mitems, err := d.mapArray(c, ic.meta, schema.ColMeta)
	if err != nil {
		return false, err
	}
	if !meta.IsNull(r) {
		beg, end := meta.ValueOffsets(r)
		for k := beg; k < end; k++ {
			ents = append(ents, mergeIDEnt{
				name: mkeys.Value(int(k)),
				val:  mitems.Value(int(k)),
				null: mitems.IsNull(int(k)),
			})
		}
	}
	slices.SortFunc(ents, cmpMergeIDEnt)
	buf = append(buf, mergeIDTagMeta)
	buf = binary.AppendUvarint(buf, uint64(len(ents)))
	for _, e := range ents {
		buf = appendMergeIDEnt(buf, e)
	}
	d.ents = ents

	d.buf = buf
	if _, dup := d.seen[string(buf)]; dup {
		d.removed++
		return true, nil
	}
	d.seen[string(buf)] = struct{}{}
	return false, nil
}

// mapArray returns c's labels or meta column as a validated string-keyed map
// array.
func (d *mergeDedup) mapArray(c *mergeCursor, col int, name string) (*array.Map, *array.String, *array.String, error) {
	a := c.rec.Column(col)
	m, ok := a.(*array.Map)
	if !ok {
		return nil, nil, nil, fmt.Errorf("storage: merge input %d: column %q has type %T, want *array.Map", c.input, name, a)
	}
	keys, ok := m.Keys().(*array.String)
	if !ok {
		return nil, nil, nil, fmt.Errorf("storage: merge input %d: %q map keys have type %T, want *array.String", c.input, name, m.Keys())
	}
	items, ok := m.Items().(*array.String)
	if !ok {
		return nil, nil, nil, fmt.Errorf("storage: merge input %d: %q map items have type %T, want *array.String", c.input, name, m.Items())
	}
	return m, keys, items, nil
}

// cmpMergeIDEnt orders entries by raw name bytes, then nullness, then raw
// value bytes, the same case-sensitive order labels and metadata canonically
// sort in.
func cmpMergeIDEnt(a, b mergeIDEnt) int {
	if c := cmp.Compare(a.name, b.name); c != 0 {
		return c
	}
	if a.null != b.null {
		if a.null {
			return 1
		}
		return -1
	}
	return cmp.Compare(a.val, b.val)
}

// appendMergeIDEnt appends e's canonical bytes to buf: a kind byte, the
// length-prefixed name and, when present, the length-prefixed value.
func appendMergeIDEnt(buf []byte, e mergeIDEnt) []byte {
	if e.null {
		buf = append(buf, mergeIDKindNull)
	} else {
		buf = append(buf, mergeIDKindValue)
	}
	buf = binary.AppendUvarint(buf, uint64(len(e.name)))
	buf = append(buf, e.name...)
	if !e.null {
		buf = binary.AppendUvarint(buf, uint64(len(e.val)))
		buf = append(buf, e.val...)
	}
	return buf
}
