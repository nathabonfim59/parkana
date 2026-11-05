package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/schema"
)

// BuildSidecar returns the encoded search-sidecar bytes (index.Builder's
// Encode) for every line of the Parquet file open in reader, using the given
// sidecar params. It returns an error wrapping index.ErrTooManyGrams when
// the file holds more distinct grams than params.MaxGrams; the caller then
// publishes the file without a sidecar.
//
// The caller owns reader: it is neither closed nor retained, and no borrowed
// Arrow value outlives the call, so the reader stays usable afterwards.
// Lines are streamed through Reader.Records, so peak memory is one reader
// batch plus the builder's gram set (bounded by params.MaxGrams) and, once
// at the end, the encoded output; none of that grows with the file size.
// Invalid params return index's validation error wrapped. Context
// cancellation, read failures and a closed reader abort the build and are
// returned wrapped.
func BuildSidecar(ctx context.Context, reader *Reader, params index.Params) ([]byte, error) {
	b, err := index.NewBuilder(params)
	if err != nil {
		return nil, fmt.Errorf("storage: build sidecar: %w", err)
	}
	if reader == nil {
		return nil, errors.New("storage: build sidecar: nil reader")
	}
	lineIdx := -1
	for rec, err := range reader.Records(ctx) {
		if err != nil {
			return nil, fmt.Errorf("storage: build sidecar: %w", err)
		}
		if lineIdx < 0 {
			// Reader validation guarantees exactly one line column, but its
			// position in the schema is not fixed, so resolve it by name.
			idx := rec.Schema().FieldIndices(schema.ColLine)
			if len(idx) != 1 {
				return nil, fmt.Errorf("storage: build sidecar: %w", errInvalidSchema)
			}
			lineIdx = idx[0]
		}
		line, ok := rec.Column(lineIdx).(*array.String)
		if !ok {
			return nil, fmt.Errorf("storage: build sidecar: column %q has type %T, want a utf8 string array",
				schema.ColLine, rec.Column(lineIdx))
		}
		for i := range int(rec.NumRows()) {
			// A null line holds no bytes, so no needle can match it.
			if !line.IsNull(i) {
				b.Add(line.Value(i))
			}
		}
	}
	data, err := b.Encode()
	if err != nil {
		return nil, fmt.Errorf("storage: build sidecar: %w", err)
	}
	return data, nil
}
