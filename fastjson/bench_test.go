package fastjson

import (
	"log/slog"
	"testing"
	"time"

	betterstack "github.com/prochac/betterstack-logs-client-go"
)

func benchPayload() map[string]any {
	return map[string]any{
		betterstack.KeyTime:    time.Date(2026, 8, 17, 10, 11, 12, 123456789, time.UTC),
		betterstack.KeyLevel:   "INFO",
		betterstack.KeyMessage: "a log message",
		betterstack.DefaultContextKey: map[string]any{
			"index": int64(42), "service": "api",
		},
	}
}

// The record encode, through the client's own encoder, with each appender.
// Compare BenchmarkAppendRecordStdlib: this is the whole reason the package
// exists, so the two must be measured the same way.
func BenchmarkAppendRecordFast(b *testing.B) {
	benchAppendRecord(b, betterstack.NDJSONWith(AppendObject))
}

func BenchmarkAppendRecordStdlib(b *testing.B) {
	benchAppendRecord(b, betterstack.NDJSON())
}

func benchAppendRecord(b *testing.B, enc betterstack.Encoder) {
	b.Helper()

	payload := benchPayload()
	dst := make([]byte, 0, 4096)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		if dst, err = enc.AppendRecord(dst[:0], payload); err != nil {
			b.Fatal(err)
		}
	}
}

// What a logging call costs the caller's goroutine with each appender, which is
// where the saving is actually spent. Delivery is deliberately not in this path
// — the client is in dry-run mode, so nothing leaves the process.
func BenchmarkHandleFast(b *testing.B) {
	benchHandle(b, betterstack.NDJSONWith(AppendObject))
}

func BenchmarkHandleStdlib(b *testing.B) {
	benchHandle(b, betterstack.NDJSON())
}

func benchHandle(b *testing.B, enc betterstack.Encoder) {
	b.Helper()

	c, err := betterstack.NewClient(
		"bench-token",
		betterstack.WithEncoder(enc),
		betterstack.WithDryRun(true),
		betterstack.WithBatchInterval(time.Hour),
		betterstack.WithOnError(func(error) {}),
	)
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })

	logger := slog.New(betterstack.NewHandler(c))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("a log message", "index", i, "service", "api")
	}
}
