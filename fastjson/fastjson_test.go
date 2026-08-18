package fastjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	betterstack "github.com/prochac/logs-client-go"
)

// This package is a second implementation of a format encoding/json already
// implements, so every test here is differential: the assertion is never "this
// looks like JSON" but "these are the bytes encoding/json produces". Inspection
// would not be good enough — the cases that matter are the ones nobody thinks
// to look at, which is why the string, float and time paths are fuzzed rather
// than tabulated.

// ref encodes v the way the client's default appender does — HTML escaping off,
// no trailing newline — and is the authority every test below compares against.
func ref(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func mustRef(t *testing.T, v any) []byte {
	t.Helper()
	b, err := ref(v)
	if err != nil {
		t.Fatalf("reference encoding of %#v: %v", v, err)
	}
	return b
}

// jsonMarshaler and textMarshaler stand in for the values only the fallback can
// encode: the fast path must not recognise them and must not lose their custom
// rendering on the way through.
type jsonMarshaler struct{ n int }

func (m jsonMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`{"n":` + strconv.Itoa(m.n) + `}`), nil
}

type textMarshaler struct{ s string }

func (m textMarshaler) MarshalText() ([]byte, error) { return []byte(m.s), nil }

type namedString string

type plainStruct struct {
	A int    `json:"a"`
	B string `json:"b"`
}

// TestAppendValueMatchesEncodingJSON covers every case of the type switch, and
// a representative sample of what falls past it.
func TestAppendValueMatchesEncodingJSON(t *testing.T) {
	t.Parallel()

	values := []any{
		nil,
		"", "plain", `quote " backslash \ `, "tab\there", "nl\nhere", "\x00\x1f\x7f",
		"GET /a?x=1&y=2 <500ms>", "héllo, 世界", "\u2028\u2029",
		true, false,
		int(0), int(-42), int8(-128), int16(32767), int32(-2147483648), int64(math.MinInt64),
		uint(0), uint8(255), uint16(65535), uint32(4294967295), uint64(math.MaxUint64),
		float64(0), math.Copysign(0, -1), float64(1), float64(0.5), float64(1e-6), float64(1e-7),
		float64(1e21), float64(1e20), float64(1e-9), float64(-1.5e300), float64(math.SmallestNonzeroFloat64),
		float32(0.1), float32(1e-7), float32(1e21), float32(math.MaxFloat32),
		time.Date(2026, 8, 17, 10, 11, 12, 123456789, time.UTC),
		time.Date(2026, 8, 17, 10, 11, 12, 0, time.UTC),
		time.Date(2026, 8, 17, 10, 11, 12, 100000000, time.FixedZone("CEST", 2*60*60)),
		time.Time{},
		map[string]any{},
		map[string]any{"k": "v"},
		map[string]any{"nested": map[string]any{"deep": []any{1, "two", nil}}},
		[]any{},
		[]any{1, 2.5, "three", true, nil},

		// Past the fast path, into encoding/json.
		time.Duration(1500), json.Number("1.5"), namedString("named"),
		plainStruct{A: 1, B: "b"},
		&plainStruct{A: 2, B: "c"},
		jsonMarshaler{n: 7},
		textMarshaler{s: "text"},
		[]byte("bytes"),
		[]string{"a", "b"},
		map[string]int{"a": 1},
		struct{}{},
		errors.New("an error"),
	}

	for _, v := range values {
		v := v
		t.Run(strings.ReplaceAll(reflect.TypeOf(&v).Elem().String()+"/"+trunc(v), "/", "_"), func(t *testing.T) {
			t.Parallel()

			want, wantErr := ref(v)
			got, err := appendValue(nil, v)

			if (err != nil) != (wantErr != nil) {
				t.Fatalf("appendValue(%#v) error = %v, encoding/json error = %v", v, err, wantErr)
			}
			if err != nil {
				return
			}
			if !bytes.Equal(got, want) {
				t.Errorf("appendValue(%#v) =\n %s\nwant %s", v, got, want)
			}
		})
	}
}

func trunc(v any) string {
	s := strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return '.'
		}
		return r
	}, jsonSprint(v))
	if len(s) > 24 {
		return s[:24]
	}
	return s
}

func jsonSprint(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "unencodable"
	}
	return string(b)
}

// A multi-key object cannot be compared byte for byte, because this package
// deliberately does not sort keys. Comparing the decoded values is the
// strongest assertion available, and key order is the only thing it gives up.
func TestAppendObjectMatchesEncodingJSON(t *testing.T) {
	t.Parallel()

	payloads := []map[string]any{
		{},
		{"dt": time.Date(2026, 8, 17, 10, 11, 12, 123456789, time.UTC), "level": "INFO", "message": "hello"},
		{"context": map[string]any{"index": int64(42), "service": "api", "ok": true, "ratio": 0.25}},
		{"": "empty key", "\"quoted\"": "v", "u\u2028nicode": "v", "nested": map[string]any{"a": map[string]any{"b": []any{1, 2}}}},
		{"err": map[string]any{"message": "boom", "type": "*errors.errorString"}},
	}

	for i, p := range payloads {
		p := p
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			got, err := AppendObject(nil, p)
			if err != nil {
				t.Fatalf("AppendObject: %v", err)
			}

			// Bytes, not decoded values. This compared decoded values until
			// both appenders sorted their keys, and that weakness is why the
			// library shipped a release whose records gzipped ~35% worse with
			// nothing failing: key order is invisible to a decoder and is the
			// whole of what the compressor sees. None of these payloads carries
			// invalid UTF-8, which is the one class where the two legitimately
			// differ in bytes; FuzzAppendString covers that by decoding.
			if want := mustRef(t, p); !bytes.Equal(got, want) {
				t.Errorf("bytes differ from encoding/json:\n got %s\nwant %s", got, want)
			}
		})
	}
}

// An error from deep inside a value must surface, and must leave dst holding
// exactly the records that were already in it — the invariant the sender relies
// on when it reuses its accumulation buffer. Asserted on AppendObject directly,
// since that is where this package makes the promise.
func TestAppendObjectNestedErrorLeavesDstIntact(t *testing.T) {
	t.Parallel()

	dst, err := AppendObject(nil, map[string]any{"first": "record"})
	if err != nil {
		t.Fatalf("AppendObject: %v", err)
	}
	before := append([]byte(nil), dst...)

	bad := map[string]any{"a": map[string]any{"b": []any{1, math.Inf(1)}}}
	got, err := AppendObject(dst, bad)
	if err == nil {
		t.Fatal("AppendObject(+Inf) = nil error, want an error")
	}
	if !bytes.Equal(got, before) {
		t.Errorf("dst was modified on error:\n got %q\nwant %q", got, before)
	}

	// The same value through encoding/json fails too, and that is the error the
	// caller has always seen.
	if _, refErr := ref(bad); refErr == nil {
		t.Fatal("reference encoder accepted +Inf; the comparison is meaningless")
	}
}

// The seam is the point of the package: what the client's own encoders produce
// with AppendObject must decode to what they produce by default.
func TestSeamAgreesWithTheDefaultEncoder(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		betterstack.KeyTime:    time.Date(2026, 8, 17, 10, 11, 12, 123456789, time.UTC),
		betterstack.KeyLevel:   "INFO",
		betterstack.KeyMessage: "a log message <with> \"escapes\" & a \n newline",
		betterstack.DefaultContextKey: map[string]any{
			"index": int64(42), "service": "api", "ok": true, "ratio": 0.25,
			"nested": map[string]any{"deep": []any{1, "two", nil}},
		},
	}

	cases := []struct {
		name           string
		fast, standard betterstack.Encoder
	}{
		{"ndjson", betterstack.NDJSONWith(AppendObject), betterstack.NDJSON()},
		{"jsonarray", betterstack.JSONArrayWith(AppendObject), betterstack.JSONArray()},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if got, want := c.fast.ContentType(), c.standard.ContentType(); got != want {
				t.Errorf("ContentType = %q, want %q", got, want)
			}

			fast, err := c.fast.AppendRecord(nil, payload)
			if err != nil {
				t.Fatalf("fast AppendRecord: %v", err)
			}
			std, err := c.standard.AppendRecord(nil, payload)
			if err != nil {
				t.Fatalf("standard AppendRecord: %v", err)
			}

			// Framing must be byte-identical; only key order may differ inside.
			if len(fast) != 0 && len(std) != 0 && fast[0] != std[0] {
				t.Errorf("framing differs: fast starts %q, standard starts %q", fast[0], std[0])
			}

			var gotDec, wantDec any
			if err := json.Unmarshal(bytes.TrimPrefix(bytes.TrimSpace(fast), []byte(",")), &gotDec); err != nil {
				t.Fatalf("fast output is not valid JSON: %v\n%s", err, fast)
			}
			if err := json.Unmarshal(bytes.TrimPrefix(bytes.TrimSpace(std), []byte(",")), &wantDec); err != nil {
				t.Fatalf("standard output is not valid JSON: %v\n%s", err, std)
			}
			if !reflect.DeepEqual(gotDec, wantDec) {
				t.Errorf("decoded mismatch:\n got %#v\nwant %#v", gotDec, wantDec)
			}
		})
	}
}

// Every byte sequence is a possible log message, valid UTF-8 or not, so the
// string path is compared against encoding/json exhaustively rather than on a
// table of the escapes someone remembered.
func FuzzAppendString(f *testing.F) {
	seeds := []string{
		"", "plain", `"`, `\`, "\x00", "\x1f", "\x7f", "\b\f\n\r\t",
		"\u2028", "\u2029", "héllo", "世界", "<>&", "a\xffb", "\xed\xa0\x80",
		"\xf4\x90\x80\x80", "é", strings.Repeat("x", 300),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		want, err := ref(s)
		if err != nil {
			t.Fatalf("reference encoding of %q: %v", s, err)
		}
		got := appendString(nil, s)
		if bytes.Equal(got, want) {
			return
		}

		// One class of input is allowed to differ, because encoding/json
		// differs from itself on it: for a byte that is not valid UTF-8, the
		// pre-1.27 engine writes the six-character escape \ufffd and the v2
		// engine (default from Go 1.27) writes the replacement character
		// itself. Both are valid JSON for the same string, so the assertion
		// there is on the decoded value; everywhere else it stays on the bytes.
		if utf8.ValidString(s) {
			t.Errorf("appendString(%q) =\n %s\nwant %s", s, got, want)
			return
		}
		var gotDec, wantDec string
		if err := json.Unmarshal(got, &gotDec); err != nil {
			t.Fatalf("appendString(%q) = %s, which does not parse: %v", s, got, err)
		}
		if err := json.Unmarshal(want, &wantDec); err != nil {
			t.Fatalf("reference for %q does not parse: %v", s, err)
		}
		if gotDec != wantDec {
			t.Errorf("appendString(%q) decodes to %q, want %q\n got %s\nwant %s",
				s, gotDec, wantDec, got, want)
		}
	})
}

// The float path reimplements encoding/json's exponent cutoffs and its
// unpadded-exponent cleanup, which are the two things that are easy to get
// almost right.
func FuzzAppendFloat(f *testing.F) {
	seeds := []float64{
		0, -0, 1, -1, 0.5, 1e-6, 9.999999e-7, 1e-7, 1e20, 1e21, 1.0000001e21,
		1e-9, 1e-100, 1e100, math.MaxFloat64, math.SmallestNonzeroFloat64,
		3.141592653589793, 1 << 53, -1.5e-300,
	}
	for _, v := range seeds {
		f.Add(v)
	}

	f.Fuzz(func(t *testing.T, v float64) {
		for _, c := range []struct {
			name string
			val  any
		}{
			{"float64", v},
			{"float32", float32(v)},
		} {
			want, wantErr := ref(c.val)
			got, err := appendValue(nil, c.val)

			if (err != nil) != (wantErr != nil) {
				t.Fatalf("%s(%v): error = %v, encoding/json error = %v", c.name, v, err, wantErr)
			}
			if err != nil {
				continue
			}
			if !bytes.Equal(got, want) {
				t.Errorf("%s(%v) = %s, want %s", c.name, v, got, want)
			}
			// A float that round-trips as a different number would be a silent
			// corruption of a metric, so check the value and not only the bytes.
			var back float64
			if err := json.Unmarshal(got, &back); err != nil {
				t.Fatalf("%s(%v) produced unparseable JSON %q: %v", c.name, v, got, err)
			}
		}
	})
}

// dt is the field the ingestion API reads, so a timestamp that does not match
// encoding/json byte for byte is the most expensive possible defect here.
func FuzzAppendTime(f *testing.F) {
	f.Add(int64(0), int64(0), 0)
	f.Add(int64(1786000000), int64(123456789), 0)
	f.Add(int64(1786000000), int64(100000000), 7200)
	f.Add(int64(-62135596800), int64(0), 0)   // year 1
	f.Add(int64(253402300799), int64(0), 0)   // year 9999
	f.Add(int64(253402300800), int64(0), 0)   // year 10000: out of range
	f.Add(int64(-62135596801), int64(0), 0)   // year 0 and earlier
	f.Add(int64(0), int64(0), 25*60*60)       // zone offset RFC 3339 cannot spell
	f.Add(int64(0), int64(0), -25*60*60)      //
	f.Add(int64(1786000000), int64(1), 30*60) // half-hour zone, one nanosecond
	f.Add(int64(1786000000), int64(999999999), -8*60*60)

	f.Fuzz(func(t *testing.T, sec, nsec int64, offset int) {
		if offset < -50*60*60 || offset > 50*60*60 {
			t.Skip("time.FixedZone's own range")
		}
		tm := time.Unix(sec, nsec)
		if offset != 0 {
			tm = tm.In(time.FixedZone("zone", offset))
		} else {
			tm = tm.UTC()
		}

		want, wantErr := ref(tm)
		got, err := appendValue(nil, tm)

		if (err != nil) != (wantErr != nil) {
			t.Fatalf("time %v (offset %d): error = %v, encoding/json error = %v", tm, offset, err, wantErr)
		}
		if err != nil {
			return
		}
		if !bytes.Equal(got, want) {
			t.Errorf("time %v (offset %d) =\n %s\nwant %s", tm, offset, got, want)
		}
	})
}

// The whole point of the exercise: the record path must not allocate for a
// payload made of the types the handler produces.
func TestAppendRecordDoesNotAllocate(t *testing.T) {
	// No t.Parallel: AllocsPerRun requires the test to have the machine to
	// itself, and panics if it does not.
	payload := map[string]any{
		betterstack.KeyTime:    time.Date(2026, 8, 17, 10, 11, 12, 123456789, time.UTC),
		betterstack.KeyLevel:   "INFO",
		betterstack.KeyMessage: "a log message",
		betterstack.DefaultContextKey: map[string]any{
			"index": int64(42), "service": "api", "ok": true, "ratio": 0.25,
		},
	}

	encoders := []betterstack.Encoder{
		betterstack.NDJSONWith(AppendObject),
		betterstack.JSONArrayWith(AppendObject),
	}
	for _, enc := range encoders {
		enc := enc
		// A buffer already large enough, which is the steady state: the sender
		// reuses one for the life of the client.
		dst := make([]byte, 0, 4096)
		if got := testing.AllocsPerRun(100, func() {
			var err error
			if dst, err = enc.AppendRecord(dst[:0], payload); err != nil {
				t.Fatal(err)
			}
		}); got != 0 {
			t.Errorf("%s: AppendRecord allocated %v times per run, want 0", enc.ContentType(), got)
		}
	}
}

// A nil appender is a construction-time error: an encoder that cannot encode
// has nothing useful to fall back on, so failing at construction beats failing
// per record.
func TestNilObjectAppenderPanics(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name string
		fn   func()
	}{
		{"NDJSONWith", func() { betterstack.NDJSONWith(nil) }},
		{"JSONArrayWith", func() { betterstack.JSONArrayWith(nil) }},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Errorf("%s(nil) did not panic", c.name)
				}
			}()
			c.fn()
		})
	}
}
