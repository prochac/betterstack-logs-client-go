package betterstack

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"
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

	got, err := enc.AppendRecord(nil, 0, map[string]any{"message": "hello"})
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
		buf, err = enc.AppendRecord(buf, i, map[string]any{"i": i})
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

	got, err := NDJSON().AppendRecord(prefix, 0, map[string]any{"a": 1})
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

	dst, err := enc.AppendRecord(nil, 0, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	before := append([]byte(nil), dst...)

	got, err := enc.AppendRecord(dst, 1, map[string]any{"bad": math.NaN()})
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

	got, err := NDJSON().AppendRecord(nil, 0, map[string]any{"message": msg})
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
				got, err := enc.AppendRecord(nil, i, map[string]any{"g": g})
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
