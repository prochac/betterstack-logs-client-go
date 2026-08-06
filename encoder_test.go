package betterstack

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	msgpack "github.com/shamaton/msgpack/v2"
)

func TestNDJSONContentType(t *testing.T) {
	t.Parallel()
	if got, want := NDJSON().ContentType(), "application/x-ndjson"; got != want {
		t.Errorf("ContentType() = %q, want %q", got, want)
	}
}

func TestNDJSONAppendRecordIsOneLine(t *testing.T) {
	t.Parallel()
	enc := NDJSON()

	got, err := enc.AppendRecord(nil, map[string]any{"message": "hello"})
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	if n := bytes.Count(got, []byte("\n")); n != 1 {
		t.Errorf("got %d newlines, want exactly 1: %q", n, got)
	}
	if !bytes.HasSuffix(got, []byte("\n")) {
		t.Errorf("record is not newline-terminated: %q", got)
	}

	var decoded map[string]any
	if err := json.Unmarshal(bytes.TrimSuffix(got, []byte("\n")), &decoded); err != nil {
		t.Fatalf("record is not valid JSON: %v (%q)", err, got)
	}
	if decoded["message"] != "hello" {
		t.Errorf("message = %v, want %q", decoded["message"], "hello")
	}
}

// A batch is the concatenation of its records: this is the property that lets
// the sender accumulate into one buffer and account bytes exactly.
func TestNDJSONAppendRecordAccumulates(t *testing.T) {
	t.Parallel()
	enc := NDJSON()

	var buf []byte
	var err error
	for i := 0; i < 3; i++ {
		buf, err = enc.AppendRecord(buf, map[string]any{"i": i})
		if err != nil {
			t.Fatalf("AppendRecord(%d): %v", i, err)
		}
	}

	lines := bytes.Split(bytes.TrimSuffix(buf, []byte("\n")), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), buf)
	}
	for i, line := range lines {
		var decoded map[string]any
		if err := json.Unmarshal(line, &decoded); err != nil {
			t.Fatalf("line %d is not valid JSON: %v (%q)", i, err, line)
		}
		if decoded["i"] != float64(i) {
			t.Errorf("line %d: i = %v, want %d", i, decoded["i"], i)
		}
	}
}

// AppendRecord must behave like append: an existing prefix survives untouched.
func TestNDJSONAppendRecordPreservesPrefix(t *testing.T) {
	t.Parallel()
	prefix := []byte("PREFIX\n")

	got, err := NDJSON().AppendRecord(prefix, map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	if !bytes.HasPrefix(got, prefix) {
		t.Errorf("prefix was clobbered: %q", got)
	}
}

// On a marshalling failure dst must come back unmodified. The sender keeps
// using that buffer, so a partial write would corrupt every record already
// accumulated in the batch, not just the failing one.
func TestNDJSONAppendRecordErrorLeavesDstIntact(t *testing.T) {
	t.Parallel()
	enc := NDJSON()

	dst, err := enc.AppendRecord(nil, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	before := append([]byte(nil), dst...)

	got, err := enc.AppendRecord(dst, map[string]any{"bad": math.NaN()})
	if err == nil {
		t.Fatal("AppendRecord(NaN) = nil error, want an error")
	}
	if !bytes.Equal(got, before) {
		t.Errorf("dst was modified on error:\n got %q\nwant %q", got, before)
	}
}

// A log message containing a URL or a comparison must survive verbatim. The
// payload is JSON on the wire, never embedded in HTML, so Go's default
// HTML-escaping only corrupts it.
func TestNDJSONDoesNotEscapeHTML(t *testing.T) {
	t.Parallel()
	const msg = `GET /a?x=1&y=2 took <500ms>`

	got, err := NDJSON().AppendRecord(nil, map[string]any{"message": msg})
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	// With escaping on, Go replaces these with their \u00xx forms, so their
	// presence as raw bytes is exactly the property under test.
	for _, r := range []rune{'<', '>', '&'} {
		if !bytes.ContainsRune(got, r) {
			t.Errorf("%q was HTML-escaped away: %q", r, got)
		}
	}
	// Belt and braces: the escaped form must not appear at all.
	if strings.Contains(string(got), `\u00`) {
		t.Errorf("output contains a unicode escape: %q", got)
	}

	var decoded map[string]any
	if err := json.Unmarshal(bytes.TrimSuffix(got, []byte("\n")), &decoded); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if decoded["message"] != msg {
		t.Errorf("message = %q, want %q", decoded["message"], msg)
	}
}

func TestNDJSONFrameIsIdentity(t *testing.T) {
	t.Parallel()
	batch := []byte("{\"a\":1}\n{\"a\":2}\n")

	got := NDJSON().Frame(batch, 2)
	if !bytes.Equal(got, batch) {
		t.Errorf("Frame() = %q, want %q", got, batch)
	}
}

// --- JSON array -------------------------------------------------------------

func TestJSONArrayContentType(t *testing.T) {
	t.Parallel()
	if got, want := JSONArray().ContentType(), "application/json"; got != want {
		t.Errorf("ContentType() = %q, want %q", got, want)
	}
}

// The framing contract: records carry a leading comma, and Frame turns the
// first of them into the opening bracket.
func TestJSONArrayFrames(t *testing.T) {
	t.Parallel()
	enc := JSONArray()

	for _, n := range []int{1, 2, 5} {
		var buf []byte
		for i := 0; i < n; i++ {
			var err error
			if buf, err = enc.AppendRecord(buf, map[string]any{"i": i}); err != nil {
				t.Fatalf("AppendRecord(%d): %v", i, err)
			}
		}
		body := enc.Frame(buf, n)

		var decoded []map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatalf("n=%d: not a JSON array: %v (%q)", n, err, body)
		}
		if len(decoded) != n {
			t.Fatalf("n=%d: decoded %d records: %q", n, len(decoded), body)
		}
		for i, m := range decoded {
			if m["i"] != float64(i) {
				t.Errorf("n=%d: record %d = %v, want i=%d", n, i, m, i)
			}
		}
	}
}

// Records are encoded one at a time and never learn their position, so any
// contiguous run of them must frame into a valid array on its own. That is what
// lets an oversized batch be split without re-encoding anything.
func TestJSONArrayFramesAnySubrun(t *testing.T) {
	t.Parallel()
	enc := JSONArray()

	const total = 6
	var buf []byte
	bounds := make([]int, 0, total)
	for i := 0; i < total; i++ {
		var err error
		if buf, err = enc.AppendRecord(buf, map[string]any{"i": i}); err != nil {
			t.Fatalf("AppendRecord(%d): %v", i, err)
		}
		bounds = append(bounds, len(buf))
	}

	for from := 0; from < total; from++ {
		for to := from + 1; to <= total; to++ {
			lo := 0
			if from > 0 {
				lo = bounds[from-1]
			}
			// A copy, because Frame writes into the buffer it is given.
			run := append([]byte(nil), buf[lo:bounds[to-1]]...)
			body := enc.Frame(run, to-from)

			var decoded []map[string]any
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatalf("records [%d,%d) do not frame: %v (%q)", from, to, err, body)
			}
			if len(decoded) != to-from {
				t.Fatalf("records [%d,%d): decoded %d", from, to, len(decoded))
			}
			if decoded[0]["i"] != float64(from) {
				t.Errorf("records [%d,%d): first is %v, want i=%d", from, to, decoded[0], from)
			}
		}
	}
}

func TestJSONArrayFrameEmpty(t *testing.T) {
	t.Parallel()
	if got := string(JSONArray().Frame(nil, 0)); got != "[]" {
		t.Errorf("Frame(nil, 0) = %q, want %q", got, "[]")
	}
}

// A record must contribute exactly its own bytes: the batch byte cap is
// measured on the accumulated buffer, so a stray newline per record would make
// the accounting drift.
func TestJSONArrayRecordHasNoTrailingNewline(t *testing.T) {
	t.Parallel()

	got, err := JSONArray().AppendRecord(nil, map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	if bytes.ContainsRune(got, '\n') {
		t.Errorf("record contains a newline: %q", got)
	}
	if got[0] != ',' {
		t.Errorf("record does not begin with a comma: %q", got)
	}
}

func TestJSONArrayAppendRecordErrorLeavesDstIntact(t *testing.T) {
	t.Parallel()
	enc := JSONArray()

	dst, err := enc.AppendRecord(nil, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	before := append([]byte(nil), dst...)

	got, err := enc.AppendRecord(dst, map[string]any{"bad": math.NaN()})
	if err == nil {
		t.Fatal("AppendRecord(NaN) = nil error, want an error")
	}
	if !bytes.Equal(got, before) {
		t.Errorf("dst was modified on error:\n got %q\nwant %q", got, before)
	}
}

func TestJSONArrayDoesNotEscapeHTML(t *testing.T) {
	t.Parallel()
	const msg = `GET /a?x=1&y=2 took <500ms>`

	got, err := JSONArray().AppendRecord(nil, map[string]any{"message": msg})
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	if strings.Contains(string(got), `\u00`) {
		t.Errorf("output contains a unicode escape: %q", got)
	}
}

// One Encoder is shared by every goroutine calling Enqueue, so the pooled
// scratch buffer must not leak between them.
func TestNDJSONConcurrentUse(t *testing.T) {
	t.Parallel()
	enc := NDJSON()

	const goroutines, perGoroutine = 8, 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				got, err := enc.AppendRecord(nil, map[string]any{"g": g})
				if err != nil {
					t.Errorf("AppendRecord: %v", err)
					return
				}
				var decoded map[string]any
				if err := json.Unmarshal(bytes.TrimSuffix(got, []byte("\n")), &decoded); err != nil {
					t.Errorf("not valid JSON: %v (%q)", err, got)
					return
				}
				if decoded["g"] != float64(g) {
					t.Errorf("g = %v, want %d (cross-goroutine contamination)", decoded["g"], g)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// --- MessagePack -------------------------------------------------------------
//
// MsgPack ships no codec, so these tests supply one. shamaton/msgpack is a
// test-only dependency and is deliberately a *different* implementation from
// anything the library knows about: it is what makes these tests evidence of
// interoperability rather than of self-consistency. What is under test is the
// array framing, which is the only part of the format this package owns.

func TestMsgPackContentType(t *testing.T) {
	t.Parallel()
	if got, want := MsgPack(msgpack.Marshal).ContentType(), "application/msgpack"; got != want {
		t.Errorf("ContentType() = %q, want %q", got, want)
	}
}

// The library's own Marshal must be usable as-is. An adapter closure would be a
// small thing to require, but requiring it of everyone is the difference between
// an option and a chore.
func TestMsgPackAcceptsALibraryMarshalDirectly(t *testing.T) {
	t.Parallel()
	var _ Marshaler = msgpack.Marshal
}

func TestMsgPackNilMarshalerPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Error("MsgPack(nil) did not panic")
		}
	}()
	MsgPack(nil)
}

// The array header is one, three or five bytes wide depending on the record
// count, which is the whole reason Frame has to shift the batch rather than
// overwrite a reserved byte the way JSONArray does.
func TestMsgPackFrameHeaderWidths(t *testing.T) {
	t.Parallel()
	enc := MsgPack(msgpack.Marshal)

	for _, tc := range []struct {
		n    int
		want []byte
	}{
		{0, []byte{0x90}},
		{1, []byte{0x91}},
		{15, []byte{0x9f}},
		{16, []byte{0xdc, 0x00, 0x10}},
		{65535, []byte{0xdc, 0xff, 0xff}},
		{65536, []byte{0xdd, 0x00, 0x01, 0x00, 0x00}},
	} {
		if got := enc.Frame(nil, tc.n); !bytes.Equal(got, tc.want) {
			t.Errorf("Frame(nil, %d) = % x, want % x", tc.n, got, tc.want)
		}
	}
}

// Frame must not return a re-sliced batch to dodge the shift: pack calls it on a
// buffer it reuses, so a return whose start has moved makes that buffer creep
// forward by the header width on every batch and grow without bound.
func TestMsgPackFrameDoesNotAdvanceTheBuffer(t *testing.T) {
	t.Parallel()
	enc := MsgPack(msgpack.Marshal)

	scratch := make([]byte, 0, 512)
	start := &scratch[:1][0]
	for i := 0; i < 50; i++ {
		var err error
		if scratch, err = enc.AppendRecord(scratch[:0], map[string]any{"i": i}); err != nil {
			t.Fatalf("AppendRecord: %v", err)
		}
		scratch = enc.Frame(scratch, 1)
	}
	if got := &scratch[:1][0]; got != start {
		t.Error("Frame moved the start of the buffer; pack reuses it, so it would grow without bound")
	}
}

// Records are encoded one at a time and never learn their position, so any
// contiguous run of them must frame into a valid array on its own. That is what
// lets an oversized batch be split without re-encoding anything.
func TestMsgPackFramesAnySubrun(t *testing.T) {
	t.Parallel()
	enc := MsgPack(msgpack.Marshal)

	const total = 6
	var buf []byte
	bounds := make([]int, 0, total)
	for i := 0; i < total; i++ {
		var err error
		if buf, err = enc.AppendRecord(buf, map[string]any{"i": i}); err != nil {
			t.Fatalf("AppendRecord(%d): %v", i, err)
		}
		bounds = append(bounds, len(buf))
	}

	for from := 0; from < total; from++ {
		for to := from + 1; to <= total; to++ {
			lo := 0
			if from > 0 {
				lo = bounds[from-1]
			}
			// A copy, because Frame writes into the buffer it is given.
			run := append([]byte(nil), buf[lo:bounds[to-1]]...)
			body := enc.Frame(run, to-from)

			var decoded []map[string]any
			if err := msgpack.Unmarshal(body, &decoded); err != nil {
				t.Fatalf("records [%d,%d) do not frame: %v (% x)", from, to, err, body)
			}
			if len(decoded) != to-from {
				t.Fatalf("records [%d,%d): decoded %d", from, to, len(decoded))
			}
			for k, m := range decoded {
				if got := toInt(t, m["i"]); got != int64(from+k) {
					t.Errorf("records [%d,%d): record %d has i=%d, want %d", from, to, k, got, from+k)
				}
			}
		}
	}
}

// A record must contribute exactly its own bytes: the batch byte cap is measured
// on the accumulated buffer, and Frame's shift assumes the records start at
// offset zero with nothing reserved ahead of them.
func TestMsgPackRecordIsExactlyItsOwnBytes(t *testing.T) {
	t.Parallel()

	rec := map[string]any{"a": 1}
	want, err := msgpack.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := MsgPack(msgpack.Marshal).AppendRecord(nil, rec)
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("AppendRecord = % x, want % x", got, want)
	}
}

// Nothing in the type system stops a JSON marshaller being passed to MsgPack.
// Without this check the symptom is a 406 from the ingestion API on every batch,
// which is a poor way to find out.
func TestMsgPackRejectsANonMapMarshaler(t *testing.T) {
	t.Parallel()

	for name, marshal := range map[string]Marshaler{
		"json":        json.Marshal,
		"array":       func(any) ([]byte, error) { return []byte{0x90}, nil },
		"empty":       func(any) ([]byte, error) { return nil, nil },
		"bare string": func(any) ([]byte, error) { return []byte{0xa1, 'x'}, nil },
	} {
		got, err := MsgPack(marshal).AppendRecord(nil, map[string]any{"a": 1})
		if err == nil {
			t.Errorf("%s marshaller: AppendRecord = % x, want an error", name, got)
		}
	}
}

func TestMsgPackAppendRecordErrorLeavesDstIntact(t *testing.T) {
	t.Parallel()

	// math.NaN is a JSON error but a perfectly good msgpack float64, so the
	// failure has to be injected rather than provoked.
	fail := false
	enc := MsgPack(func(v any) ([]byte, error) {
		if fail {
			return nil, errors.New("boom")
		}
		return msgpack.Marshal(v)
	})

	dst, err := enc.AppendRecord(nil, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	before := append([]byte(nil), dst...)

	fail = true
	got, err := enc.AppendRecord(dst, map[string]any{"bad": true})
	if err == nil {
		t.Fatal("AppendRecord = nil error, want an error")
	}
	if !bytes.Equal(got, before) {
		t.Errorf("dst was modified on error:\n got % x\nwant % x", got, before)
	}
}

// One Encoder is shared by every goroutine calling Enqueue.
func TestMsgPackConcurrentUse(t *testing.T) {
	t.Parallel()
	enc := MsgPack(msgpack.Marshal)

	const goroutines, perGoroutine = 8, 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				got, err := enc.AppendRecord(nil, map[string]any{"g": g})
				if err != nil {
					t.Errorf("AppendRecord: %v", err)
					return
				}
				var decoded map[string]any
				if err := msgpack.Unmarshal(enc.Frame(got, 1)[1:], &decoded); err != nil {
					t.Errorf("not valid msgpack: %v (% x)", err, got)
					return
				}
				if v := toInt(t, decoded["g"]); v != int64(g) {
					t.Errorf("g = %v, want %d (cross-goroutine contamination)", v, g)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// msgpack picks the narrowest integer format that fits, so a decoded value's Go
// type depends on its magnitude. Every test here cares about the number, not the
// width it arrived in.
func toInt(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n)
	default:
		t.Fatalf("not an integer: %v (%T)", v, v)
		return 0
	}
}
