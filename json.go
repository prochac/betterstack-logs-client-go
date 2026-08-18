package betterstack

import (
	"bytes"
	"encoding/json"
	"sync"
)

// This is the record encoder, on every toolchain. It uses the encoding/json v1
// API deliberately, and that is a decision rather than an omission: a second
// implementation over encoding/json/v2 was built, measured and reverted.
//
// The short version: v1's behaviour is pinned by Go's compatibility promise, so
// this file needs no build tag, no options, and no test to notice a toolchain
// change. It sorts map keys, which is worth ~35% on the gzipped body and is the
// property that made the v2 file a net loss once it had to match. When json/v2
// becomes the engine underneath this API, all three of those properties come
// along unchanged.
//
// encoding/json cannot walk a map[string]any cheaply — every value is an
// interface, so the map encoder reflects over each one and boxes it again on
// the way out, at 15 allocations and ~1.3 µs for the default record shape. That
// is the price of the map API, and this file pays
// it rather than reimplementing JSON to avoid it. The fastjson subpackage is
// for callers who would rather not: same output, 0 allocations.

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
