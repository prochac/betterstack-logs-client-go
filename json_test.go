package betterstack

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// --- the encoding contract --------------------------------------------------

// json.go's tests: the behaviour its record encoder is held to, asserted against
// what a consumer decodes rather than against bytes, and the pooling of the
// scratch buffer it encodes into.
//
// This file was written when there were two implementations behind a build tag
// — json_stdlib.go and a json_v2.go over encoding/json/v2 — and its job was to
// stop them drifting. The v2 file has since been removed, so the
// contract now has one implementation to hold, and the reason it is still worth
// asserting is that these are the properties that made v2 unusable as a
// drop-in: invalid UTF-8 survives rather than failing the record, a Duration
// encodes rather than erroring, and keys come out sorted. Any future attempt to
// swap the encoder has to clear this bar first.

// decodeRecord runs a payload through the encoder under test and returns what a
// consumer would see.
func decodeRecord(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()

	b, err := appendJSONObject(nil, payload)
	if err != nil {
		t.Fatalf("appendJSONObject(%v): %v", payload, err)
	}
	if bytes.HasSuffix(b, []byte("\n")) {
		t.Fatalf("appendJSONObject appended a trailing newline: %q\n"+
			"framing belongs to the Encoder, and the array encoder must not get one", b)
	}

	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, b)
	}
	return got
}

// A log message carries whatever bytes the application put in it. Rejecting a
// record for a stray byte would lose the line, and encoding/json/v2 does
// exactly that by default — which is one of the reasons the v1 API is what this
// package encodes with.
func TestJSONInvalidUTF8IsSubstitutedNotRejected(t *testing.T) {
	t.Parallel()

	const replacement = "\ufffd"
	msg := "before " + string([]byte{0xff}) + " after"

	got := decodeRecord(t, map[string]any{"message": msg})

	s, ok := got["message"].(string)
	if !ok {
		t.Fatalf("message came back as %T, want string", got["message"])
	}
	if !strings.Contains(s, replacement) {
		t.Errorf("message = %q, want the invalid byte replaced by %q", s, replacement)
	}
	if !strings.HasPrefix(s, "before ") || !strings.HasSuffix(s, " after") {
		t.Errorf("message = %q, want the surrounding text intact", s)
	}
}

// encoding/json/v2 refuses a time.Duration outright, where encoding/json writes
// the nanosecond count. slog's own durations never arrive here as Duration
// values — attr.go renders them as strings — but a struct field, a nested map
// or a direct Enqueue can carry one.
func TestJSONDurationEncodesAsNanoseconds(t *testing.T) {
	t.Parallel()

	got := decodeRecord(t, map[string]any{"took": 1500 * time.Millisecond})

	n, ok := got["took"].(float64)
	if !ok {
		t.Fatalf("took came back as %T (%v), want a JSON number", got["took"], got["took"])
	}
	if want := float64(1500 * time.Millisecond); n != want {
		t.Errorf("took = %v, want %v nanoseconds", n, want)
	}
}

// The payload is JSON on the wire and never embedded in HTML, so the characters
// Go escapes by default in the v1 API must survive verbatim.
func TestJSONDoesNotEscapeHTML(t *testing.T) {
	t.Parallel()

	const msg = `GET /a?x=1&y=2 took <500ms>`

	b, err := appendJSONObject(nil, map[string]any{"message": msg})
	if err != nil {
		t.Fatalf("appendJSONObject: %v", err)
	}
	for _, r := range []rune{'<', '>', '&'} {
		if !bytes.ContainsRune(b, r) {
			t.Errorf("%q was escaped away: %s", r, b)
		}
	}
	if bytes.Contains(b, []byte(`\u00`)) {
		t.Errorf("output contains a unicode escape: %s", b)
	}
}

// Keys come out sorted, which is the one assertion here that is about bytes
// rather than about what a consumer decodes — because the consumer that cares
// is gzip. Every record in a batch carries the same keys, so a stable key
// sequence is the longest repeated string in the body; leaving it to Go's map
// iteration order costs ~35% on the compressed size, against a limit the
// ingestion API measures on compressed bytes.
//
// This is a regression guard, not an API promise: nothing about payload key
// order is documented, and no consumer should depend on it. It exists because
// the library shipped a release that lost this property without anything
// failing, and it is what would have caught it.
func TestJSONKeysAreSorted(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		"zulu": 1, "alpha": 2, "mike": 3, "bravo": 4, "yankee": 5,
		"nested": map[string]any{"delta": 1, "charlie": 2, "echo": 3},
	}

	b, err := appendJSONObject(nil, payload)
	if err != nil {
		t.Fatalf("appendJSONObject: %v", err)
	}

	for _, keys := range [][]string{
		{"alpha", "bravo", "mike", "nested", "yankee", "zulu"},
		{"charlie", "delta", "echo"},
	} {
		if !sort.StringsAreSorted(keys) {
			t.Fatalf("test bug: %v is not the sorted order", keys)
		}
		if got := keyOrder(string(b), keys); !reflect.DeepEqual(got, keys) {
			t.Errorf("keys appear in order %v, want %v\nencoded: %s", got, keys, b)
		}
	}
}

// keyOrder returns the members of want in the order their quoted forms appear
// in s.
func keyOrder(s string, want []string) []string {
	type at struct {
		key string
		pos int
	}
	found := make([]at, 0, len(want))
	for _, k := range want {
		if i := strings.Index(s, `"`+k+`":`); i >= 0 {
			found = append(found, at{k, i})
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].pos < found[j].pos })

	got := make([]string, len(found))
	for i, f := range found {
		got[i] = f.key
	}
	return got
}

// Integers keep every digit, however large. A log line routinely carries an
// identifier that is exact as an int64 or a uint64 and not as a float64 — a
// snowflake id, a database row id, a nanosecond timestamp — and one that lost
// its low bits here would look entirely plausible and be silently wrong.
//
// Like the sorted-key test this asserts bytes, and for a related reason: a
// consumer decoding into map[string]any parses every JSON number into a
// float64, so an assertion made after decoding would pass just as happily
// against an encoder that had already rounded the value away. Only the
// encoded digits can tell the difference.
//
// What holds it up is that encoding/json writes integer kinds through
// strconv.AppendUint/AppendInt, and attr.go hands uint64 and int64 attributes
// through as themselves (staticValue) rather than widening them. Both halves
// are load-bearing; this fails if either is lost.
func TestJSONLargeIntegersKeepExactDigits(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		"max_uint64": uint64(math.MaxUint64),
		"max_int64":  int64(math.MaxInt64),
		"min_int64":  int64(math.MinInt64),
		// 2^53+1, the first integer float64 cannot represent, either sign.
		"just_past_float64":     int64(1)<<53 + 1,
		"just_past_float64_neg": -(int64(1)<<53 + 1),
		// A realistic id rather than a boundary value.
		"snowflake": uint64(1234567890123456789),
	}

	b, err := appendJSONObject(nil, payload)
	if err != nil {
		t.Fatalf("appendJSONObject: %v", err)
	}

	for key, digits := range map[string]string{
		"max_uint64":            "18446744073709551615",
		"max_int64":             "9223372036854775807",
		"min_int64":             "-9223372036854775808",
		"just_past_float64":     "9007199254740993",
		"just_past_float64_neg": "-9007199254740993",
		"snowflake":             "1234567890123456789",
	} {
		if want := `"` + key + `":` + digits; !bytes.Contains(b, []byte(want)) {
			t.Errorf("encoded body does not contain %s\nencoded: %s", want, b)
		}
	}
}

// dt is the field the ingestion API reads, as RFC 3339 with nanoseconds.
func TestJSONTimeIsRFC3339Nano(t *testing.T) {
	t.Parallel()

	when := time.Date(2026, 8, 17, 10, 11, 12, 123456789, time.UTC)
	got := decodeRecord(t, map[string]any{KeyTime: when})

	s, ok := got[KeyTime].(string)
	if !ok {
		t.Fatalf("%s came back as %T, want string", KeyTime, got[KeyTime])
	}
	if want := "2026-08-17T10:11:12.123456789Z"; s != want {
		t.Errorf("%s = %q, want %q", KeyTime, s, want)
	}
	back, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("%s does not parse as RFC 3339: %v", KeyTime, err)
	}
	if !back.Equal(when) {
		t.Errorf("%s round-tripped to %v, want %v", KeyTime, back, when)
	}
}

// The shapes a record actually takes: the reserved keys, a nested attribute
// tree, the types attr.go produces, and the values a Converter may add.
func TestJSONRecordShapeRoundTrips(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		KeyTime:    time.Date(2026, 8, 17, 10, 11, 12, 0, time.UTC),
		KeyLevel:   "ERROR",
		KeyMessage: "boom",
		DefaultContextKey: map[string]any{
			"index":   int64(42),
			"service": "api",
			"ok":      true,
			"ratio":   0.25,
			"absent":  nil,
			"error":   map[string]any{"message": "wrapped", "type": "*errors.errorString"},
			"list":    []any{1, "two", true, nil},
			"deep":    map[string]any{"a": map[string]any{"b": map[string]any{"c": "d"}}},
		},
	}

	got := decodeRecord(t, payload)

	var want map[string]any
	ref, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("reference marshal: %v", err)
	}
	if err := json.Unmarshal(ref, &want); err != nil {
		t.Fatalf("reference unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decoded record differs from encoding/json's:\n got %#v\nwant %#v", got, want)
	}
}

// A value with its own JSON representation must keep it, whichever
// implementation is compiled in.
type contractMarshaler struct{}

func (contractMarshaler) MarshalJSON() ([]byte, error) { return []byte(`{"custom":true}`), nil }

type contractTextMarshaler struct{}

func (contractTextMarshaler) MarshalText() ([]byte, error) { return []byte("as-text"), nil }

func TestJSONHonoursMarshalers(t *testing.T) {
	t.Parallel()

	got := decodeRecord(t, map[string]any{
		"json": contractMarshaler{},
		"text": contractTextMarshaler{},
	})

	if m, ok := got["json"].(map[string]any); !ok || m["custom"] != true {
		t.Errorf("MarshalJSON was not honoured: %#v", got["json"])
	}
	if got["text"] != "as-text" {
		t.Errorf("MarshalText was not honoured: %#v", got["text"])
	}
}

// NaN and the infinities have no JSON spelling. Both implementations must
// refuse them, and both must leave the caller's buffer holding exactly the
// records that were already in it — the invariant the sender relies on when it
// reuses its accumulation buffer.
func TestJSONUnencodableValueLeavesBufferIntact(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		bad  map[string]any
	}{
		{"NaN", map[string]any{"v": math.NaN()}},
		{"+Inf", map[string]any{"v": math.Inf(1)}},
		{"nested", map[string]any{"a": map[string]any{"b": []any{1, math.Inf(-1)}}}},
		{"channel", map[string]any{"v": make(chan int)}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, enc := range []Encoder{NDJSON(), JSONArray()} {
				dst, err := enc.AppendRecord(nil, map[string]any{"first": "record"})
				if err != nil {
					t.Fatalf("%s: AppendRecord: %v", enc.ContentType(), err)
				}
				before := append([]byte(nil), dst...)

				got, err := enc.AppendRecord(dst, tc.bad)
				if err == nil {
					t.Fatalf("%s: AppendRecord(%s) = nil error, want an error", enc.ContentType(), tc.name)
				}
				if !bytes.Equal(got, before) {
					t.Errorf("%s: buffer was modified on error:\n got %q\nwant %q",
						enc.ContentType(), got, before)
				}
			}
		})
	}
}

// An empty attribute tree is a real record: a log line with no attributes at
// all still has to encode.
func TestJSONEmptyObject(t *testing.T) {
	t.Parallel()

	b, err := appendJSONObject(nil, map[string]any{})
	if err != nil {
		t.Fatalf("appendJSONObject: %v", err)
	}
	if string(b) != "{}" {
		t.Errorf("appendJSONObject(empty) = %q, want %q", b, "{}")
	}
}

// --- the encoder pool -------------------------------------------------------

// The scratch buffer json.go pools only ever grows, so one outsized
// record would otherwise pin its capacity for the life of the process — the
// slow kind of leak that never shows up in a benchmark and only in a long-lived
// process that logs the occasional huge value.
//
// The assertion is one-sided, like the timing ones elsewhere: an empty pool
// hands back a fresh encoder and passes, so this can only fail when the
// oversized buffer really was retained. It is not parallel, because it reads the
// process-wide pool.
func TestOversizedJSONBufferIsNotPooled(t *testing.T) {
	huge := map[string]any{"payload": strings.Repeat("x", 4*maxPooledJSONBuffer)}

	b, err := appendJSONObject(nil, huge)
	if err != nil {
		t.Fatalf("appendJSONObject: %v", err)
	}
	if len(b) < 4*maxPooledJSONBuffer {
		t.Fatalf("encoded %d bytes, want at least %d", len(b), 4*maxPooledJSONBuffer)
	}

	// Drain rather than Get once: the buffer that encoded the record above is
	// the most recent Put on this P, but a pool holds per-P private and shared
	// slots and nothing promises which one answers first.
	for i := 0; i < 16; i++ {
		e := jsonEncoders.Get().(*pooledJSONEncoder)
		if got := e.buf.Cap(); got > maxPooledJSONBuffer {
			t.Fatalf("pooled scratch buffer has capacity %d, over the %d cap: "+
				"a single huge record inflates the pool permanently", got, maxPooledJSONBuffer)
		}
	}
}

// The cap must not quietly disable pooling for ordinary traffic: a record of the
// usual shape has to stay well inside it, or every encode allocates a fresh
// buffer and an *json.Encoder to go with it.
func TestOrdinaryJSONRecordStaysPoolable(t *testing.T) {
	t.Parallel()

	e := jsonEncoders.New().(*pooledJSONEncoder)
	if err := e.enc.Encode(map[string]any{
		"dt":      "2026-08-17T12:00:00.000000Z",
		"level":   "INFO",
		"message": "request served",
		"context": map[string]any{
			"method": "GET",
			"path":   "/healthz",
			"status": 200,
		},
	}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if got := e.buf.Cap(); got > maxPooledJSONBuffer {
		t.Errorf("a typical record grew the scratch buffer to %d, past the %d cap",
			got, maxPooledJSONBuffer)
	}
}
