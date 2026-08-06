package betterstack

import (
	"bytes"
	"encoding/json"
	"sync"
)

// Encoder turns log records into the bytes of a request body.
//
// A batch is assembled by calling AppendRecord once per record, in order, and
// then Frame once. Splitting it that way means the sender can accumulate into a
// single growing buffer and know the exact encoded size at every point, without
// a second marshalling pass.
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
	// index is the record's position within the batch, counting from zero. It
	// exists for framings whose separator depends on position — a JSON array
	// needs a comma before every record but the first. Formats that do not care
	// ignore it.
	//
	// If AppendRecord returns an error, dst must be returned unmodified: the
	// sender keeps using the buffer, so a partially written record would
	// corrupt every record already in the batch.
	AppendRecord(dst []byte, index int, v map[string]any) ([]byte, error)

	// Frame completes a batch of n records assembled by AppendRecord. It may
	// append to and return batch. For line-delimited formats it is the identity.
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

func (ndjsonEncoder) AppendRecord(dst []byte, _ int, v map[string]any) ([]byte, error) {
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
