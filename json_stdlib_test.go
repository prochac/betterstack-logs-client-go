//go:build !(go1.27 && goexperiment.jsonv2)

package betterstack

import (
	"strings"
	"testing"
)

// The scratch buffer json_stdlib.go pools only ever grows, so one outsized
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
