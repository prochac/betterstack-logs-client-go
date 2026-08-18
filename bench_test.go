package betterstack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
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

// discardSink is an enqueuer that does nothing at all, so a benchmark using it
// measures the handler and only the handler.
type discardSink struct{}

func (discardSink) Enqueue(map[string]any) error { return nil }

// BenchmarkHandleConvert measures the slog half alone: attribute tree plus
// Converter, with no encode, no queue and no client behind it.
//
// It exists because BenchmarkHandle above cannot be read as the cost of a log
// call. That one runs a whole client — sender goroutine, gzip, an in-process
// HTTP server — on the same machine, and their CPU and GC land in its ns/op.
// The pair brackets the truth: this is what the calling goroutine spends, and
// BenchmarkHandle is what it spends when the delivery pipeline is competing
// with it for a core.
func BenchmarkHandleConvert(b *testing.B) {
	logger := slog.New(newHandler(discardSink{}))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("a log message", "index", i, "service", "api")
	}
}

func BenchmarkHandleConvertWithAttrs(b *testing.B) {
	logger := slog.New(newHandler(discardSink{})).
		With("service", "api", "release", "v1.0.0").
		With("region", "eu-central-1")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("a log message", "index", i)
	}
}

// BenchmarkStdlibJSONHandler is the yardstick for the two above, and the only
// benchmark here that measures none of this package's code.
//
// slog.JSONHandler writes a record straight into a buffer with no intermediate
// representation, which is what lets it allocate nothing. This handler builds a
// map[string]any because Converter and Encoder are public interfaces typed on
// one (§4, "Duplicate keys"), so it cannot reach zero and is not trying to.
// What the comparison is for is the gap's size and direction over time.
func BenchmarkStdlibJSONHandler(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("a log message", "index", i, "service", "api")
	}
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

// BenchmarkPack measures what an IdentityFramer saves: the copy of the whole
// batch into the framing buffer, which an encoder whose Frame writes nothing
// does not need. The framed variants force the general path on the same encoder
// and the same bytes, so the pair differs by exactly that copy.
//
// The gzip numbers are the honest ones to read first — the copy is a few percent
// of the compression pass that follows it, which is why removing it went
// unprioritised for a long time. What the fast path is really worth is the buffer
// itself: it is never allocated, so the default configuration holds one
// MaxBatchBytes-sized buffer fewer for the life of the client.
func BenchmarkPack(b *testing.B) {
	enc := NDJSON()
	var raw []byte
	var bounds []int
	for i := 0; i < 1000; i++ {
		var err error
		raw, err = enc.AppendRecord(raw, map[string]any{
			KeyMessage: fmt.Sprintf("request %d completed", i),
			KeyLevel:   "INFO",
		})
		if err != nil {
			b.Fatal(err)
		}
		bounds = append(bounds, len(raw))
	}

	for _, tc := range []struct {
		name     string
		comp     Compression
		identity bool
	}{
		{"gzip/identity", CompressionGzip, true},
		{"gzip/framed", CompressionGzip, false},
		{"none/identity", CompressionNone, true},
		{"none/framed", CompressionNone, false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			p := newPacker(enc, tc.comp)
			p.identity = tc.identity

			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := p.pack(raw, bounds); err != nil {
					b.Fatal(err)
				}
			}
		})
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

// BenchmarkFlushBatch measures a whole batch through the flush path, which is
// where the per-batch copies live. BenchmarkFlush's one-record batches show the
// allocation count but not the bytes.
func BenchmarkFlushBatch(b *testing.B) {
	const records = 200
	c := benchClient(b, WithBatchSize(records*2), WithBatchInterval(time.Hour))
	ctx := context.Background()
	ev := event(0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < records; j++ {
			if err := c.Enqueue(ev); err != nil {
				b.Fatal(err)
			}
		}
		if err := c.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSourceValue measures WithAddSource's per-record symbolization, and
// its control: the same work with the PC cache bypassed. The gap is what the
// cache is for, and it is smaller than the whole of WithAddSource's overhead —
// most of that is the map and the boxing, which every record must pay.
func BenchmarkSourceValue(b *testing.B) {
	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])
	pc := pcs[0]

	var sink map[string]any

	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink, _ = sourceValue(pc)
		}
		_ = sink
	})

	// The pathological case the generations exist for: a working set larger than
	// a generation, so every lookup misses and something is evicted. This is the
	// cost of the cache machinery alone — no symbolization — because a program
	// like that pays the 195 ns above on top of it either way.
	b.Run("churn", func(b *testing.B) {
		c := newSourceCache(1024)
		s := callSite{function: "fn", file: "f.go", line: 1}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.remember(uintptr(0x400000+i*16), s)
		}
	})

	b.Run("symbolized", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, _ := runtime.CallersFrames([]uintptr{pc}).Next()
			sink = map[string]any{"function": f.Function, "file": f.File, "line": f.Line}
		}
		_ = sink
	})
}
