package betterstack

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
	// result. For line-delimited formats it is the identity, and such an
	// encoder should say so by also implementing IdentityFramer.
	//
	// Frame is called on a buffer the caller is free to reuse afterwards, and
	// it may be called more than once on the same records: splitting an
	// oversized batch re-frames each half from the original record bytes.
	Frame(batch []byte, n int) []byte
}

// IdentityFramer is the optional interface an Encoder implements to declare
// that its Frame is the identity — that Frame returns the batch it was given,
// unmodified, whatever the batch, so that a request body is exactly the
// concatenation of its records.
//
// Declaring it lets the client compress the accumulated records where they
// already are. Framing otherwise happens on a scratch buffer the client owns,
// because Frame may write into the buffer it is handed and the unframed records
// have to survive intact for a possible split; that buffer costs a copy of every
// batch, and holds a batch's worth of memory for the life of the client. NDJSON
// implements this interface, and any other line-delimited format should.
//
// The declaration is not inherited by wrapping, in either direction: embedding
// an Encoder promotes the Encoder methods and nothing else, so a wrapper around
// NDJSON that keeps its framing has to say this itself, and one that adds
// framing of its own says nothing and is framed normally. That is the safe way
// round — the client asks the encoder it was given, whose Frame is the one it
// would otherwise call.
type IdentityFramer interface {
	// FrameIsIdentity reports whether Frame returns its batch unmodified.
	//
	// It is asked once, not per batch, so it must answer the same way for the
	// life of the encoder. An encoder that answers true is held to it: Frame is
	// then never called at all.
	FrameIsIdentity() bool
}

// ObjectAppender writes the JSON encoding of one record to dst, appending in
// the manner of the append builtin and adding no trailing newline. It is the
// seam the two JSON encoders are built on, so that the object encoding can be
// replaced without reimplementing the framing around it.
//
// The default is encoding/json, which is the right choice for almost everyone.
// The reason the seam is exported is that encoding/json cannot walk a
// map[string]any cheaply — every value is an interface, so it reflects over
// each one and boxes it again on the way out — and no general-purpose JSON
// library does meaningfully better on that shape, because they win by caching
// reflection over concrete struct types. Beating it takes an appender written
// for this payload specifically, which is what the fastjson subpackage is:
//
//	betterstack.WithEncoder(betterstack.NDJSONWith(fastjson.AppendObject))
//
// An ObjectAppender must be safe for concurrent use: one Encoder is shared by
// every goroutine calling Enqueue. On error it may leave dst extended — the
// callers below truncate, so what matters is that the records already
// accumulated in the buffer survive, not that the bytes past its length are
// untouched.
type ObjectAppender func(dst []byte, m map[string]any) ([]byte, error)

// NDJSON returns the default encoder: newline-delimited JSON, one record per
// line, sent as application/x-ndjson, with records encoded by encoding/json.
//
// NDJSON is the default because a batch is then simply the concatenation of its
// records — assembly is one append, with no array framing, no separator
// bookkeeping, and no second pass over the batch.
func NDJSON() Encoder { return NDJSONWith(appendJSONObject) }

// NDJSONWith is NDJSON with the record encoding supplied by the caller. See
// ObjectAppender. It panics if appendObject is nil, since an encoder that
// cannot encode has no useful behaviour to fall back on and failing at
// construction beats failing per record.
func NDJSONWith(appendObject ObjectAppender) Encoder {
	if appendObject == nil {
		panic("betterstack: NDJSONWith requires a non-nil ObjectAppender")
	}
	return ndjsonEncoder{appendObject: appendObject}
}

type ndjsonEncoder struct{ appendObject ObjectAppender }

func (ndjsonEncoder) ContentType() string { return "application/x-ndjson" }

func (ndjsonEncoder) Frame(batch []byte, _ int) []byte { return batch }

// FrameIsIdentity implements IdentityFramer: a batch of NDJSON is its records
// and nothing else, so there is nothing for Frame to do and no reason for the
// client to copy the records anywhere in order to have it done.
func (ndjsonEncoder) FrameIsIdentity() bool { return true }

func (e ndjsonEncoder) AppendRecord(dst []byte, v map[string]any) ([]byte, error) {
	n := len(dst)
	dst, err := e.appendObject(dst, v)
	if err != nil {
		// The record may have been written into the caller's buffer in part
		// before the failure. Truncating to the original length discards that:
		// what is beyond len is not part of the buffer's contents, and the
		// records already accumulated in it are untouched.
		return dst[:n], err
	}
	// One newline is the whole of NDJSON's framing.
	return append(dst, '\n'), nil
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
func JSONArray() Encoder { return JSONArrayWith(appendJSONObject) }

// JSONArrayWith is JSONArray with the record encoding supplied by the caller.
// See ObjectAppender. It panics if appendObject is nil, for the reason
// NDJSONWith does.
func JSONArrayWith(appendObject ObjectAppender) Encoder {
	if appendObject == nil {
		panic("betterstack: JSONArrayWith requires a non-nil ObjectAppender")
	}
	return jsonArrayEncoder{appendObject: appendObject}
}

type jsonArrayEncoder struct{ appendObject ObjectAppender }

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

func (e jsonArrayEncoder) AppendRecord(dst []byte, v map[string]any) ([]byte, error) {
	n := len(dst)
	// The leading comma, which Frame overwrites for the first record of a
	// batch. Nothing else separates records, so a record's encoded size is
	// exactly its contribution to the body, which is what the batch byte cap is
	// measured against.
	dst = append(dst, ',')

	dst, err := e.appendObject(dst, v)
	if err != nil {
		// See ndjsonEncoder.AppendRecord: truncation is what leaves the records
		// already accumulated in dst intact.
		return dst[:n], err
	}
	return dst, nil
}

// appendJSONObject is the default ObjectAppender, used by NDJSON and JSONArray.
// It lives in json.go, over the encoding/json v1 API, on every toolchain — see
// that file for why there is one implementation and not two.
//
// It writes no trailing newline: NDJSON's separator is added by the caller
// above, because the array encoder must not have one.
