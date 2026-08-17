package betterstack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The record encoder has two implementations, selected by build tag:
// json_stdlib.go on every toolchain, json_v2.go from Go 1.27. This file is the
// contract they share. It runs unchanged under both, and it is the only thing
// stopping the library from behaving differently depending on which Go built
// it — a difference nobody would see until a user on the newer toolchain
// reported missing records.
//
// So the assertions here are about behaviour, never about bytes. Two of them
// exist because encoding/json/v2's defaults differ from encoding/json's and
// json_v2.go has to put them back; the rest would hold for any correct
// implementation, which is what makes them worth asserting.
//
// Deliberately not asserted: the order of an object's keys. v1 sorts them, v2
// does not, and DESIGN §4 leaves payload key order unspecified.

// TestJSONImplementation reports which of the two files this binary was built
// with, and catches the one case the build tag cannot express: when json/v2
// graduates, goexperiment.jsonv2 disappears, json_v2.go silently stops being
// compiled, and everything still passes.
//
// runtime.Version() is what makes it detectable — it reports non-default
// experiments, as "go1.27rc3-X:nojsonv2" — so an explicit opt-out can be told
// apart from a graduation. On a toolchain new enough for json/v2, taking
// json_stdlib.go is legitimate only for the former.
func TestJSONImplementation(t *testing.T) {
	t.Parallel()

	version := runtime.Version()
	t.Logf("records are encoded by %s (%s)", jsonImplementation, version)

	usesV2 := jsonImplementation == "encoding/json/v2"
	optedOut := strings.Contains(version, "nojsonv2")

	switch {
	case usesV2 && !atLeastGo(1, 27):
		t.Errorf("json_v2.go was compiled on %s, where encoding/json/v2 is "+
			"importable but outside the API promise, so a file below go1.27 "+
			"fails go vet's stdversion on 1.27; the build tag has been "+
			"widened too far", version)
	case !usesV2 && atLeastGo(1, 27) && !optedOut:
		t.Errorf("this build uses %s on %s, which has encoding/json/v2 and did not "+
			"opt out of it.\nThe goexperiment.jsonv2 tag in json_v2.go has stopped "+
			"matching — most likely json/v2 graduated and the flag was deleted, in "+
			"which case that tag becomes a plain //go:build go1.NN. See DESIGN §4.",
			jsonImplementation, version)
	}
}

// atLeastGo reports whether the running toolchain is at least the given
// release. It reads runtime.Version(), which is "go1.27rc3", "go1.26.6" or, for
// an unreleased toolchain, something that parses as neither — treated as newer,
// since a devel build is always ahead of the last release.
func atLeastGo(major, minor int) bool {
	v := runtime.Version()
	if !strings.HasPrefix(v, "go") {
		return true
	}
	var gotMajor, gotMinor int
	if _, err := fmt.Sscanf(v, "go%d.%d", &gotMajor, &gotMinor); err != nil {
		return true
	}
	return gotMajor > major || (gotMajor == major && gotMinor >= minor)
}

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
// exactly that unless told otherwise — see json_v2.go's AllowInvalidUTF8.
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

// dt is the field the ingestion API reads. RFC 3339 with nanoseconds is what
// DESIGN §4 specifies and what both implementations must produce.
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
