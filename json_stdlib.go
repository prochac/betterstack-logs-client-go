//go:build !(go1.27 && goexperiment.jsonv2)

package betterstack

import (
	"bytes"
	"encoding/json"
	"sync"
)

// This is the record encoder for every toolchain before Go 1.27, and for any
// 1.27 build with the json/v2 experiment turned off. json_v2.go is the other
// half; the two must agree on everything json_contract_test.go asserts.
//
// encoding/json cannot walk a map[string]any cheaply — every value is an
// interface, so the map encoder reflects over each one and boxes it again on
// the way out, at 13 allocations and ~1.4 µs for the default record shape. That
// is the price of the map API (DESIGN §4, "Duplicate keys"), and this file pays
// it rather than reimplementing JSON to avoid it.

// jsonImplementation names the package this build encodes records with, for
// TestJSONImplementation. This file is selected whenever goexperiment.jsonv2 is
// false, which covers a toolchain from before the experiment, one built with
// GOEXPERIMENT=nojsonv2, and — in future — one where json/v2 has graduated and
// the flag is gone. The first two want this file; the third does not.
const jsonImplementation = "encoding/json"

// appendJSONObject appends the JSON encoding of m to dst, without a trailing
// newline.
//
// On error dst may have been extended, which callers handle by truncating: the
// contract is that the records already accumulated in the buffer survive, not
// that the bytes past its length are untouched.
func appendJSONObject(dst []byte, m map[string]any) ([]byte, error) {
	e := jsonEncoders.Get().(*pooledJSONEncoder)
	defer putJSONEncoder(e)

	e.buf.Reset()
	if err := e.enc.Encode(m); err != nil {
		// Encode may have written a partial value into the scratch buffer. dst
		// is untouched because nothing is copied across until Encode succeeds.
		return dst, err
	}
	// Encode terminates every value with a newline. That is NDJSON's separator,
	// but framing belongs to the encoder above, so it comes off here.
	b := e.buf.Bytes()
	return append(dst, b[:len(b)-1]...), nil
}

type pooledJSONEncoder struct {
	buf *bytes.Buffer
	enc *json.Encoder
}

// jsonEncoders pools the scratch buffer and the *json.Encoder together. The
// encoder is stateful only in its escaping and indentation settings, so it can
// be reused for the life of the process once configured.
var jsonEncoders = sync.Pool{
	New: func() any {
		buf := &bytes.Buffer{}
		enc := json.NewEncoder(buf)
		// Without this, every URL, every "a < b", and every "&" in a log
		// message is mangled into < / & on the way to a JSON payload
		// that is never embedded in HTML.
		enc.SetEscapeHTML(false)
		return &pooledJSONEncoder{buf: buf, enc: enc}
	},
}

// maxPooledJSONBuffer is the largest scratch buffer worth keeping alive between
// records. A bytes.Buffer only ever grows, so without a cap a single 5MB record
// leaves 5MB resident for the life of the process — several times that under
// concurrency, one per pooled encoder — to serve records that are ordinarily a
// few hundred bytes. 64KB is well past any usual record and small enough that
// holding one per P costs nothing.
const maxPooledJSONBuffer = 64 << 10

// putJSONEncoder returns e to the pool, unless the record it just encoded grew
// its scratch buffer past what is worth retaining. Dropping it costs one
// allocation of the pair on the next encode, which is the right trade against
// pinning the outlier's capacity forever: the encoder cannot be kept without the
// buffer, since it writes to that buffer and cannot be repointed.
func putJSONEncoder(e *pooledJSONEncoder) {
	if e.buf.Cap() > maxPooledJSONBuffer {
		return
	}
	jsonEncoders.Put(e)
}
