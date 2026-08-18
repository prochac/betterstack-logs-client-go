// Package fastjson provides a reflection-free JSON object appender for
// betterstack's record payloads.
//
// It is an optional, opt-in replacement for the encoding/json appender the
// client uses by default:
//
//	betterstack.WithEncoder(betterstack.NDJSONWith(fastjson.AppendObject))
//	betterstack.WithEncoder(betterstack.JSONArrayWith(fastjson.AppendObject))
//
// On the default record shape it encodes in ~380 ns with no allocations, where
// encoding/json takes ~1.5 µs and 15, which takes a slog.Handler.Handle from 27
// allocations to 16. That cost is paid synchronously on the goroutine that
// called the logger, which is what makes it worth a package.
//
// # Why this is a separate package
//
// This is a second implementation of a format the standard library already
// implements, in a client whose pitch is auditability. Kept in the main package
// it would be code every user has to trust and nobody reads. Here, nobody
// trusts it who has not imported it, the import is visible in review, and a
// binary that does not import it does not link it — which a build tag could not
// have given, being a global flag on the application build rather than a choice
// at the call site.
//
// # What it is, and is not
//
// It is a type switch over the closed set of types the handler's attribute
// conversion produces, plus the numeric types a Converter is likely to add
// directly. Anything else — a struct, a json.Marshaler, a named type, a
// time.Duration — falls through to encoding/json unchanged. So the fast path
// may be incomplete without being wrong, and encoding/json remains the
// definition of the format rather than a thing this package races.
//
// Correctness is differential, not by inspection: the tests compare bytes
// against encoding/json for every case handled, and fuzz the three paths that
// reimplement a rule rather than a shape — string escaping, float formatting
// and time.Time.
//
// # Object keys are sorted
//
// encoding/json sorts map keys and so does this, for the compressor rather than
// for the reader: every record in a batch carries the same keys, so a stable key
// sequence is the longest repeated string in the body, and leaving it to Go's
// map iteration order costs roughly 54% on the gzipped size of a realistic
// batch. That matters because the ingestion API's size limit is measured on
// compressed bytes. No JSON reader is permitted to care about member order and
// nothing here promises one — the sort exists to make consecutive records
// resemble each other.
//
// Sorting is why an object of more than 32 keys costs one allocation; below
// that the key slice lives on the stack.
package fastjson

import (
	"bytes"
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

// AppendObject appends the JSON encoding of m to dst, with no trailing newline,
// in the manner of the append builtin. It satisfies betterstack.ObjectAppender.
//
// It is safe for concurrent use. On error dst is returned truncated to the
// length it arrived with, which discards any partial record: what is beyond len
// is not part of the buffer's contents, and the records already accumulated in
// it are untouched.
func AppendObject(dst []byte, m map[string]any) ([]byte, error) {
	n := len(dst)
	dst, err := appendObject(dst, m)
	if err != nil {
		return dst[:n], err
	}
	return dst, nil
}

// maxStackKeys is how many keys an object can have before sorting them needs the
// heap. Records are shallow — a handful of reserved keys and a context group —
// and slog groups nest into separate objects rather than widening one, so a map
// this side of 32 keys covers the shape the handler produces. Past it, append
// grows the slice normally and costs one allocation on that record alone.
const maxStackKeys = 32

func appendObject(dst []byte, m map[string]any) ([]byte, error) {
	dst = append(dst, '{')

	// Keys are sorted, so that every record in a batch presents the same key
	// sequence to gzip. See the package doc.
	//
	// Not slices.Sorted(maps.Keys(m)): both are Go 1.23 against a 1.21 floor, and
	// it collects into a fresh heap slice where this array stays on the stack.
	var scratch [maxStackKeys]string
	keys := scratch[:0]
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	var err error
	for i, k := range keys {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendString(dst, k)
		dst = append(dst, ':')
		if dst, err = appendValue(dst, m[k]); err != nil {
			return dst, err
		}
	}
	return append(dst, '}'), nil
}

// appendValue appends the JSON encoding of v to dst.
//
// The cases are the types the handler's attribute conversion produces, plus the
// numeric types a Converter is likely to put in a payload directly. Named types
// do not match a type switch on their underlying type — a time.Duration is not
// an int64 here — and that is the correct outcome: they take the encoding/json
// path, which is where their MarshalJSON, MarshalText or default rendering
// lives.
func appendValue(dst []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...), nil
	case string:
		return appendString(dst, x), nil
	case bool:
		return strconv.AppendBool(dst, x), nil
	case int:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int8:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int16:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int32:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(dst, x, 10), nil
	case uint:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint8:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint16:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint32:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint64:
		return strconv.AppendUint(dst, x, 10), nil
	case float32:
		return appendFloat(dst, float64(x), 32, v)
	case float64:
		return appendFloat(dst, x, 64, v)
	case time.Time:
		return appendTime(dst, x)
	case map[string]any:
		return appendObject(dst, x)
	case []any:
		dst = append(dst, '[')
		var err error
		for i, e := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			if dst, err = appendValue(dst, e); err != nil {
				return dst, err
			}
		}
		return append(dst, ']'), nil
	default:
		return appendOther(dst, v)
	}
}

// appendTime appends t as encoding/json would, which is to say as
// time.Time.MarshalJSON does: RFC 3339 with whatever fractional second the
// value carries, quoted.
//
// The two shapes MarshalJSON refuses are refused here too, by handing them to
// it: a year outside [0,9999] and a zone offset of a day or more have no RFC
// 3339 spelling, and emitting a plausible-looking string for them would put a
// timestamp on the wire the ingestion API cannot parse.
func appendTime(dst []byte, t time.Time) ([]byte, error) {
	_, offset := t.Zone()
	if y := t.Year(); y < 0 || y > 9999 || offset <= -24*60*60 || offset >= 24*60*60 {
		return appendOther(dst, t)
	}
	dst = append(dst, '"')
	dst = t.AppendFormat(dst, time.RFC3339Nano)
	return append(dst, '"'), nil
}

// appendFloat appends f in the form encoding/json uses: like %g, but with
// different exponent cutoffs and an unpadded exponent, so that the result is
// what a JavaScript number-to-string conversion would give.
//
// NaN and both infinities have no JSON spelling. They are passed to
// encoding/json rather than rejected here, so the caller gets the error it
// already got, with the value named in it.
func appendFloat(dst []byte, f float64, bits int, orig any) ([]byte, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return appendOther(dst, orig)
	}

	format := byte('f')
	abs := math.Abs(f)
	if abs != 0 {
		// float32 must be compared as a float32: the cutoff has to fall where
		// the shorter representation does.
		if (bits == 64 && (abs < 1e-6 || abs >= 1e21)) ||
			(bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21)) {
			format = 'e'
		}
	}

	n0 := len(dst)
	dst = strconv.AppendFloat(dst, f, format, -1, bits)
	if format == 'e' {
		// strconv pads the exponent to two digits; JSON's convention does not.
		// e-09 becomes e-9.
		if n := len(dst); n-n0 >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst, nil
}

// appendString appends s as a quoted JSON string.
//
// What must be escaped, and nothing more: the quote, the backslash, and the
// control characters below 0x20. HTML escaping is deliberately not done — the
// payload is never embedded in HTML, and escaping mangles every URL and every
// "a < b" in a log message — but the two Unicode line terminators are escaped
// even so, because encoding/json escapes them unconditionally and matching its
// bytes is the contract this function is held to.
//
// Bytes are copied in runs. The loop only ever stops at a byte that needs
// escaping or a rune that needs replacing, so the common case — a string with
// nothing to escape — is a length check per byte and one copy.
func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '"', '\\':
				dst = append(dst, '\\', b)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xf])
			}
			i++
			start = i
			continue
		}

		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			// A byte that is not part of a valid encoding. Substituting U+FFFD
			// is the only option that keeps the output valid UTF-8, which JSON
			// requires. The escape is written rather than the character itself
			// — that is what encoding/json did up to Go 1.26, while its v2
			// engine, the default from 1.27, writes the character. Both are the
			// same JSON string, so this stays as it is and the fuzz test
			// compares decoded values for exactly this class of input.
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', 'f', 'f', 'f', 'd')
		case r == lineSeparator || r == paragraphSeparator:
			// LINE SEPARATOR and PARAGRAPH SEPARATOR are valid in a JSON string
			// but not in a JavaScript one, so escaping them is conventional.
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[r&0xf])
		default:
			i += size
			continue
		}
		i += size
		start = i
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

const hexDigits = "0123456789abcdef"

// LINE SEPARATOR and PARAGRAPH SEPARATOR, named rather than written as rune
// literals because in source they are invisible characters that look like
// nothing at all.
const (
	lineSeparator      = rune(0x2028)
	paragraphSeparator = rune(0x2029)
)

// appendOther encodes a value the fast path does not know, using encoding/json,
// and appends the result.
//
// It is reached for any type outside the payload's usual set, so it is also
// where a value's own MarshalJSON or MarshalText is honoured, and where an
// unencodable value produces the same error it always has.
//
// This deliberately uses the encoding/json v1 API rather than v2: v1's
// behaviour is pinned by Go's compatibility promise, so the fallback needs no
// build tag and no options to stay stable across releases.
func appendOther(dst []byte, v any) ([]byte, error) {
	e := encoders.Get().(*pooledEncoder)
	defer encoders.Put(e)

	e.buf.Reset()
	if err := e.enc.Encode(v); err != nil {
		return dst, err
	}
	// Encode terminates every value with a newline, which is a record separator
	// to the caller and never part of a value.
	b := e.buf.Bytes()
	return append(dst, b[:len(b)-1]...), nil
}

type pooledEncoder struct {
	buf *bytes.Buffer
	enc *json.Encoder
}

// encoders pools the scratch buffer and the *json.Encoder together. The encoder
// is stateful only in its escaping and indentation settings, so it can be
// reused for the life of the process once configured.
//
// Only appendOther reaches this, so on the common record it is never touched.
var encoders = sync.Pool{
	New: func() any {
		buf := &bytes.Buffer{}
		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)
		return &pooledEncoder{buf: buf, enc: enc}
	},
}
