//go:build go1.27 && goexperiment.jsonv2

package betterstack

import (
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"sync"
)

// This is the record encoder from Go 1.27, where encoding/json/v2 exists. It
// replaces json_stdlib.go, which stays the implementation everywhere else, and
// the two must agree on everything json_contract_test.go asserts.
//
// Both halves of the tag are needed. goexperiment.jsonv2 because
// encoding/json/v2's own files carry it, so GOEXPERIMENT=nojsonv2 removes the
// package and a bare go1.27 tag would break those builds. go1.27 because the v2
// API is version-stamped there and a build tag is what sets a file's language
// version; without it go vet reports "requires go1.27 or later". That is also
// why the tag is not widened to go1.26 to catch 1.26 builds that opt into the
// experiment: go1.26 vets there and fails on 1.27.
//
// When the experiment graduates the flag is deleted, this tag stops matching,
// and the file silently stops being compiled — a build tag naming an experiment
// that does not exist is false, not an error. The tag becomes //go:build go1.NN
// then; the condition cannot be written before the release that ends the
// experiment is known, so TestJSONImplementation fails when it happens instead.
//
// The gain is ~9 allocations a record against the v1 API's 11 on the same
// toolchain — avoiding the v1 compatibility layer, not the engine, since a
// map[string]any is walked by reflection either way. v2 is not a drop-in;
// jsonOptions below is what that costs.

// jsonImplementation names the package this build encodes records with.
// TestJSONImplementation reports it, which is how the silent-fallback case
// above is noticed.
const jsonImplementation = "encoding/json/v2"

// jsonOptions pins the two v2 defaults that would otherwise diverge from
// json_stdlib.go.
//
// AllowInvalidUTF8: v2 rejects a string that is not valid UTF-8 with an error,
// where v1 substitutes U+FFFD. For a logging client that is not a stricter
// contract, it is a lost record — a log line carries whatever bytes the
// application put in it, and a stray byte in a message must not be the reason
// the message never arrives. With the option set, v2 substitutes as v1 does.
//
// FormatDurationAsNano: v2 refuses to encode a time.Duration at all, on the
// grounds that it has no obvious representation. v1 writes the nanosecond
// count. A Duration does not reach here through slog — attr.go renders those
// as strings — but it does through a struct field, a nested map, or a direct
// Enqueue, and a value that encodes on one toolchain and errors on another is
// exactly what must not happen.
//
// Not set, deliberately: Deterministic. v1 sorts an object's keys and v2 does
// not, so key order differs between the two builds. JSON defines no order for
// object members, DESIGN §4 records that this package's payload key order is
// unspecified, and sorting would cost an allocation and a sort per record to
// produce agreement no consumer can observe.
var jsonOptions = jsonv2.JoinOptions(
	jsontext.AllowInvalidUTF8(true),
	jsonv1.FormatDurationAsNano(true),
)

// appendJSONObject appends the JSON encoding of m to dst, without a trailing
// newline.
//
// On error dst may have been extended, which callers handle by truncating: the
// contract is that the records already accumulated in the buffer survive, not
// that the bytes past its length are untouched.
func appendJSONObject(dst []byte, m map[string]any) ([]byte, error) {
	w := jsonWriters.Get().(*sliceWriter)
	defer jsonWriters.Put(w)

	// MarshalWrite rather than MarshalEncode with a pooled jsontext.Encoder:
	// the encoder writes a newline after every top-level value, which is
	// NDJSON's separator but wrong for the array encoder, and stripping it back
	// off is more code than the encoder saves.
	w.b = dst
	err := jsonv2.MarshalWrite(w, m, jsonOptions)
	dst, w.b = w.b, nil
	return dst, err
}

// jsonWriters holds the io.Writer MarshalWrite needs — client.go's sliceWriter,
// which appends to a caller-supplied slice, so the record is built in place
// rather than in a scratch buffer that then has to be copied across. The pool
// exists only to keep the writer itself off the heap; the bytes it appends to
// belong to the caller, and are handed straight back.
var jsonWriters = sync.Pool{New: func() any { return new(sliceWriter) }}
