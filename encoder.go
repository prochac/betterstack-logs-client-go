package betterstack

import (
	"bytes"
	"encoding/json"
	"sync"
)

// Encoder turns log records into the bytes of a request body.
//
// Each record is encoded on its own, by Enqueue, on the calling goroutine. The
// sender then concatenates those encodings in arrival order and calls Frame
// once to complete the batch. Encoding one record at a time is what makes an
// encoding error synchronous and returnable, and what keeps the queue carrying
// plain bytes with nothing aliased between goroutines.
//
// A record's encoding must therefore be self-delimiting and independent of its
// position: the encoder never learns where in a batch a record will land, and
// the same bytes may be re-framed into a different batch entirely when an
// oversized one is split. A JSON array satisfies this by giving every record a
// leading comma and having Frame overwrite the first one — see JSONArray.
//
// Implementations must be safe for concurrent use: one Encoder is shared by
// every goroutine calling Enqueue.
type Encoder interface {
	// ContentType is the value for the Content-Type request header. It travels
	// with the encoder so that adding a format does not also require a matching
	// change at the call site.
	ContentType() string

	// AppendRecord encodes v and appends it to dst, returning the extended
	// buffer in the manner of the append builtin.
	//
	// If AppendRecord returns an error, dst must be returned unmodified: the
	// caller keeps using the buffer, so a partially written record would
	// corrupt every record already in it.
	AppendRecord(dst []byte, v map[string]any) ([]byte, error)

	// Frame completes a batch of n records assembled by AppendRecord. It may
	// append to, and modify in place, the batch it is given, and returns the
	// result. For line-delimited formats it is the identity.
	//
	// Frame is called on a buffer the caller is free to reuse afterwards, and
	// it may be called more than once on the same records: splitting an
	// oversized batch re-frames each half from the original record bytes.
	Frame(batch []byte, n int) []byte
}

// NDJSON returns the default encoder: newline-delimited JSON, one record per
// line, sent as application/x-ndjson.
//
// NDJSON is the default because a batch is then simply the concatenation of its
// records — assembly is one append, with no array framing, no separator
// bookkeeping, and no second pass over the batch.
func NDJSON() Encoder { return ndjsonEncoder{} }

type ndjsonEncoder struct{}

func (ndjsonEncoder) ContentType() string { return "application/x-ndjson" }

func (ndjsonEncoder) Frame(batch []byte, _ int) []byte { return batch }

func (ndjsonEncoder) AppendRecord(dst []byte, v map[string]any) ([]byte, error) {
	e := jsonEncoders.Get().(*pooledJSONEncoder)
	defer jsonEncoders.Put(e)

	e.buf.Reset()
	if err := e.enc.Encode(v); err != nil {
		// Encode may have written a partial value into the scratch buffer. dst
		// is untouched because nothing is copied across until Encode succeeds.
		return dst, err
	}
	// json.Encoder.Encode terminates every value with a newline, which is
	// exactly the NDJSON record separator. No separator handling of our own.
	return append(dst, e.buf.Bytes()...), nil
}

// JSONArray returns an encoder that sends a batch as one JSON array, as
// application/json. The ingestion API accepts it (PARITY §1); NDJSON remains the
// default because it needs no framing pass at all.
//
// The separator problem this solves is worth stating, because the obvious
// solution does not work here. A record is encoded by Enqueue, alone, long
// before it is known which batch it will join or what position it will take —
// so an encoder cannot emit a comma "before every record but the first". This
// one gives every record a *leading* comma and lets Frame overwrite the first
// of them with the opening bracket:
//
//	,{"a":1} ,{"b":2} ,{"c":3}      three records, as queued
//	[{"a":1} ,{"b":2} ,{"c":3}]     after Frame
//
// That is one byte written and one appended, whatever the batch size, and it
// holds for any contiguous run of records — which is what lets an oversized
// batch be split and each half re-framed without re-encoding anything.
func JSONArray() Encoder { return jsonArrayEncoder{} }

type jsonArrayEncoder struct{}

func (jsonArrayEncoder) ContentType() string { return "application/json" }

func (jsonArrayEncoder) Frame(batch []byte, n int) []byte {
	if n == 0 {
		// Not reachable through the sender, which never frames an empty batch,
		// but an encoder that answers "[]" is easier to reason about than one
		// with a precondition.
		return append(batch, '[', ']')
	}
	batch[0] = '[' // the first record's leading comma
	return append(batch, ']')
}

func (jsonArrayEncoder) AppendRecord(dst []byte, v map[string]any) ([]byte, error) {
	e := jsonEncoders.Get().(*pooledJSONEncoder)
	defer jsonEncoders.Put(e)

	e.buf.Reset()
	if err := e.enc.Encode(v); err != nil {
		return dst, err
	}
	// Encode's trailing newline is the one byte that is wrong for an array. It
	// is dropped rather than kept as whitespace so that a record's encoded size,
	// which is what the batch byte cap is measured against, is exactly its
	// contribution to the body.
	b := e.buf.Bytes()
	b = b[:len(b)-1]

	dst = append(dst, ',')
	return append(dst, b...), nil
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
