package betterstack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// benchClient returns a client pointed at a server that accepts everything as
// fast as it can, so the measurement is of the client's own work.
func benchClient(b *testing.B, opts ...ClientOption) *Client {
	b.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))

	base := []ClientOption{
		WithEndpoint(srv.URL),
		WithBatchInterval(time.Hour),
		WithOnError(func(error) {}),
	}
	c, err := NewClient(testToken, append(base, opts...)...)
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	b.Cleanup(func() {
		_ = c.Close()
		srv.Close()
	})
	return c
}

// BenchmarkHandle measures what a logging call costs the caller's goroutine:
// attribute tree, conversion, JSON encoding, and the channel send. Delivery is
// deliberately not in this path.
func BenchmarkHandle(b *testing.B) {
	c := benchClient(b)
	logger := slog.New(NewHandler(c))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("a log message", "index", i, "service", "api")
	}
}

func BenchmarkHandleNoAttrs(b *testing.B) {
	c := benchClient(b)
	logger := slog.New(NewHandler(c))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("a log message")
	}
}

// The accumulated WithAttrs/WithGroup state is walked on every record, so its
// depth is a per-record cost.
func BenchmarkHandleWithAttrs(b *testing.B) {
	c := benchClient(b)
	logger := slog.New(NewHandler(c)).
		With("service", "api", "release", "v1.0.0").
		With("region", "eu-central-1")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("a log message", "index", i)
	}
}

func BenchmarkHandleGroups(b *testing.B) {
	c := benchClient(b)
	logger := slog.New(NewHandler(c)).WithGroup("http").With("method", "GET")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("request", slog.Group("user", slog.String("id", "u-1")), "status", 200)
	}
}

func BenchmarkHandleWithSource(b *testing.B) {
	c := benchClient(b)
	logger := slog.New(NewHandler(c, WithAddSource(true)))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("a log message", "index", i)
	}
}

func BenchmarkHandleWithError(b *testing.B) {
	c := benchClient(b)
	logger := slog.New(NewHandler(c))
	err := errors.New("something went wrong")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Error("failed", "error", err)
	}
}

// Records below the level must cost as close to nothing as possible: an
// application that leaves debug logging in place pays this on every call.
func BenchmarkHandleDisabled(b *testing.B) {
	c := benchClient(b)
	logger := slog.New(NewHandler(c, WithLevel(slog.LevelError)))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("never emitted", "index", i)
	}
}

// Contention on the queue is the thing that has to scale, since every
// goroutine in the host application logs through it.
func BenchmarkHandleParallel(b *testing.B) {
	c := benchClient(b, WithMaxQueueSize(1<<20))
	logger := slog.New(NewHandler(c))

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			logger.Info("a log message", "service", "api")
		}
	})
}

// BenchmarkEnqueue isolates the client half: encode plus channel send, with no
// slog conversion.
func BenchmarkEnqueue(b *testing.B) {
	c := benchClient(b, WithMaxQueueSize(1<<20))
	ev := map[string]any{
		KeyMessage: "a log message",
		KeyLevel:   "INFO",
		DefaultContextKey: map[string]any{
			"service": "api",
			"index":   1,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Enqueue(ev)
	}
}

// BenchmarkEnqueueBurstRefused is the cost of the limiter on the path it exists
// for: the bucket is empty, so every call is refused. It should be a clock read
// and an atomic load — cheaper than the encode it replaces, which is the whole
// argument for gating before the encoder rather than after it.
func BenchmarkEnqueueBurstRefused(b *testing.B) {
	c := benchClient(b, WithMaxQueueSize(1<<20), WithBurstProtection(1, time.Hour))
	ev := map[string]any{
		KeyMessage: "a log message",
		KeyLevel:   "INFO",
		DefaultContextKey: map[string]any{
			"service": "api",
			"index":   1,
		},
	}
	_ = c.Enqueue(ev) // drain the one-record bucket

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Enqueue(ev)
	}
}

func BenchmarkNDJSONAppendRecord(b *testing.B) {
	enc := NDJSON()
	ev := map[string]any{
		KeyMessage: "a log message",
		KeyLevel:   "INFO",
		DefaultContextKey: map[string]any{
			"service": "api",
			"index":   1,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, err := enc.AppendRecord(nil, ev)
		if err != nil {
			b.Fatal(err)
		}
		_ = buf
	}
}

// BenchmarkBatchAssembly measures accumulation into one growing buffer, which
// is the whole reason NDJSON is the default framing.
func BenchmarkBatchAssembly(b *testing.B) {
	enc := NDJSON()
	ev := map[string]any{KeyMessage: "a log message", KeyLevel: "INFO"}

	const perBatch = 1000
	buf := make([]byte, 0, 256<<10)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		for j := 0; j < perBatch; j++ {
			var err error
			if buf, err = enc.AppendRecord(buf, ev); err != nil {
				b.Fatal(err)
			}
		}
		_ = enc.Frame(buf, perBatch)
	}
}

func BenchmarkCompress(b *testing.B) {
	enc := NDJSON()
	var body []byte
	for i := 0; i < 1000; i++ {
		var err error
		body, err = enc.AppendRecord(body, map[string]any{
			KeyMessage: fmt.Sprintf("request %d completed", i),
			KeyLevel:   "INFO",
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	gz := newCompressor()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := gz.compress(body)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

// BenchmarkFlush measures the round trip a caller pays for at shutdown.
func BenchmarkFlush(b *testing.B) {
	c := benchClient(b, WithBatchSize(1000))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Enqueue(map[string]any{KeyMessage: "m", KeyLevel: "INFO"}); err != nil {
			b.Fatal(err)
		}
		if err := c.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
