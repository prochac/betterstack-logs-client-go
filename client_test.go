package betterstack

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func event(i int) map[string]any {
	return map[string]any{KeyMessage: fmt.Sprintf("record-%d", i), KeyLevel: "INFO"}
}

func enqueueN(t *testing.T, c *Client, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := c.Enqueue(event(i)); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
	}
}

// --- flush triggers ---------------------------------------------------------

func TestFlushTriggerCount(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(3))
	defer c.Close()

	enqueueN(t, c, 3)

	// No explicit Flush: reaching the count is the trigger.
	rec.waitForRequests(t, 1)
	if got := rec.count(); got != 1 {
		t.Fatalf("got %d requests, want 1", got)
	}
	if got := len(rec.all()[0].records); got != 3 {
		t.Errorf("got %d records in the batch, want 3", got)
	}
}

func TestFlushTriggerBytes(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	// One encoded record is comfortably over 10 bytes, so each record fills the
	// batch on its own.
	c, _ := newTestClient(t, rec, WithBatchSize(1000), WithMaxBatchBytes(10))
	defer c.Close()

	enqueueN(t, c, 2)

	rec.waitForRequests(t, 2)
	if got := rec.count(); got != 2 {
		t.Errorf("got %d requests, want 2: the byte trigger did not fire per record", got)
	}
}

func TestFlushTriggerInterval(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000), WithBatchInterval(20*time.Millisecond))
	defer c.Close()

	enqueueN(t, c, 1)

	// Neither the count nor the byte trigger can fire here.
	rec.waitForRequests(t, 1)
	if got := len(rec.all()[0].records); got != 1 {
		t.Errorf("got %d records, want 1", got)
	}
}

// An idle client must do no work: the interval timer is armed when a batch
// takes its first record and stopped when it flushes, so a spurious fire on an
// empty batch is a no-op and never produces an empty request.
func TestIdleClientSendsNothing(t *testing.T) {
	t.Parallel()

	interval := 10 * time.Millisecond
	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000), WithBatchInterval(interval))
	defer c.Close()

	enqueueN(t, c, 1)
	rec.waitForRequests(t, 1)

	// Proving a negative, so a sleep is the right tool — over a window long
	// enough for many intervals to have elapsed.
	time.Sleep(20 * interval)

	if got := rec.count(); got != 1 {
		t.Errorf("got %d requests, want 1: the idle client kept sending", got)
	}
}

// --- Flush ------------------------------------------------------------------

// Flush must not return until the upload it triggered has been acknowledged.
func TestFlushWaitsForUpload(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withDelay(100*time.Millisecond))
	c, _ := newTestClient(t, rec, WithBatchSize(1000))
	defer c.Close()

	enqueueN(t, c, 1)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Checked immediately, with no waiting: if Flush returned early this is 0.
	if got := rec.count(); got != 1 {
		t.Errorf("got %d requests immediately after Flush, want 1", got)
	}
}

// Everything enqueued before the call must be delivered by the time it returns.
func TestFlushDeliversEverythingEnqueuedBefore(t *testing.T) {
	t.Parallel()

	const n = 1000
	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(64))
	defer c.Close()

	enqueueN(t, c, n)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := len(rec.records()); got != n {
		t.Errorf("got %d records delivered, want %d", got, n)
	}
	stats := c.Stats()
	if stats.Sent != n {
		t.Errorf("Stats().Sent = %d, want %d", stats.Sent, n)
	}
	// A burst of a thousand lines against a healthy server is ordinary usage,
	// not backpressure. Nothing may be shed here — an earlier design dropped
	// whole assembled batches whenever every upload slot was momentarily busy,
	// which cost ~20% of this burst.
	if stats.DroppedBacklog != 0 {
		t.Errorf("DroppedBacklog = %d, want 0: batches were dropped against a healthy server", stats.DroppedBacklog)
	}
	if stats.DroppedQueueFull != 0 {
		t.Errorf("DroppedQueueFull = %d, want 0", stats.DroppedQueueFull)
	}
}

func TestFlushEmptyIsNoop(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec)
	defer c.Close()

	if err := c.Flush(context.Background()); err != nil {
		t.Errorf("Flush on an idle client: %v", err)
	}
	if got := rec.count(); got != 0 {
		t.Errorf("got %d requests, want 0", got)
	}
}

// Flush reports the first delivery error since one was last taken, and clears
// it, so a later healthy Flush is not haunted by an old failure.
func TestFlushReturnsAndClearsError(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withStatuses(401))
	c, _ := newTestClient(t, rec, WithBatchSize(1000))
	defer c.Close()

	enqueueN(t, c, 1)
	err := c.Flush(context.Background())
	if err == nil {
		t.Fatal("Flush returned nil after a 401")
	}
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != 401 {
		t.Fatalf("Flush error = %v, want a *StatusError with 401", err)
	}

	enqueueN(t, c, 1)
	if err := c.Flush(context.Background()); err != nil {
		t.Errorf("second Flush returned %v, want nil: the error was not consumed", err)
	}
}

// Flush must honour its context rather than hanging on a stalled upload.
func TestFlushContextCancelled(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withGate())
	c, _ := newTestClient(t, rec, WithBatchSize(1000))
	defer func() {
		rec.release()
		c.Close()
	}()

	enqueueN(t, c, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.Flush(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Flush error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Errorf("Flush took %v; it did not honour the context", elapsed)
	}
}

func TestFlushAfterCloseReturnsErrClosed(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Must return promptly, not hang on a sender that has gone away.
	done := make(chan error, 1)
	go func() { done <- c.Flush(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Flush after Close = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Flush after Close hung")
	}
}

// --- Close ------------------------------------------------------------------

func TestCloseFlushes(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000))

	enqueueN(t, c, 5)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := len(rec.records()); got != 5 {
		t.Errorf("got %d records, want 5: Close did not flush", got)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec)

	first := c.Close()
	second := c.Close()
	if first != second {
		t.Errorf("Close returned %v then %v; it must be stable", first, second)
	}
	if got := rec.count(); got != 0 {
		t.Errorf("got %d requests from a double Close, want 0", got)
	}
}

func TestCloseConcurrent(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000))
	enqueueN(t, c, 1)

	const closers = 32
	errs := make([]error, closers)
	var wg sync.WaitGroup
	wg.Add(closers)
	for i := 0; i < closers; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = c.Close()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != errs[0] {
			t.Errorf("Close %d returned %v, want %v: every caller gets the same error", i, err, errs[0])
		}
	}
	if got := rec.count(); got != 1 {
		t.Errorf("got %d requests, want exactly 1", got)
	}
}

func TestEnqueueAfterClose(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := c.Enqueue(event(0)); !errors.Is(err, ErrClosed) {
		t.Errorf("Enqueue after Close = %v, want ErrClosed", err)
	}
	if got := c.Stats().DroppedClosed; got != 1 {
		t.Errorf("Stats().DroppedClosed = %d, want 1", got)
	}
}

// Enqueue racing Close must not panic. Sending on a closed channel is the bug
// this guards, and the reason queue is never closed at all.
func TestEnqueueRacesClose(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000))

	const producers = 8
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(producers)
	for i := 0; i < producers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// The error is expected once Close lands; the point is that
				// this neither panics nor blocks.
				_ = c.Enqueue(event(0))
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	close(stop)
	wg.Wait()

	assertStatsBalance(t, c)
}

func TestCloseWithSlowServerRespectsShutdownTimeout(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withGate())
	defer rec.release()

	c, _ := newTestClient(t, rec,
		WithBatchSize(1000),
		WithShutdownTimeout(100*time.Millisecond),
	)
	enqueueN(t, c, 1)

	start := time.Now()
	err := c.Close()
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Close returned nil despite never delivering")
	}
	if elapsed > 2*time.Second {
		t.Errorf("Close took %v; it did not respect ShutdownTimeout", elapsed)
	}
}

// --- drops and accounting ---------------------------------------------------

// A full queue must drop and count, never block: Handle runs in the calling
// application's critical path.
func TestQueueFullDropsWithoutBlocking(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withGate())
	c, _ := newTestClient(t, rec,
		WithMaxQueueSize(4),
		WithBatchSize(2),
		WithMaxInFlight(1),
		WithShutdownTimeout(100*time.Millisecond),
	)
	// Release before closing: Close waits for delivery, and the whole point of
	// this fixture is that delivery never completes.
	defer func() {
		rec.release()
		c.Close()
	}()

	const n = 5000
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := c.Enqueue(event(i)); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
	}
	elapsed := time.Since(start)

	// Every upload is gated, so if Enqueue blocked anywhere this would take
	// the full test timeout rather than milliseconds.
	if elapsed > 2*time.Second {
		t.Errorf("%d enqueues took %v; Enqueue is blocking", n, elapsed)
	}

	stats := c.Stats()
	if stats.Enqueued != n {
		t.Errorf("Stats().Enqueued = %d, want %d", stats.Enqueued, n)
	}
	if stats.DroppedQueueFull+stats.DroppedBacklog == 0 {
		t.Error("nothing was dropped despite a queue of 4 and 5000 records with every upload stalled")
	}
}

// Once Close has returned, every record handed to Enqueue is accounted for
// exactly once.
func assertStatsBalance(t *testing.T, c *Client) {
	t.Helper()
	s := c.Stats()
	accounted := s.Sent + s.DroppedQueueFull + s.DroppedBacklog +
		s.DroppedRejected + s.DroppedOversize + s.DroppedClosed
	if accounted != s.Enqueued {
		t.Errorf("stats do not balance: enqueued %d, accounted %d (%+v)", s.Enqueued, accounted, s)
	}
}

func TestStatsBalance(t *testing.T) {
	t.Parallel()

	t.Run("healthy delivery", func(t *testing.T) {
		t.Parallel()
		rec := newRecorder(t)
		c, _ := newTestClient(t, rec, WithBatchSize(10))
		enqueueN(t, c, 100)
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		assertStatsBalance(t, c)
		if got := c.Stats().Sent; got != 100 {
			t.Errorf("Sent = %d, want 100", got)
		}
	})

	t.Run("terminal rejection", func(t *testing.T) {
		t.Parallel()
		rec := newRecorder(t, withStatuses(401, 401, 401, 401, 401, 401, 401, 401, 401, 401))
		c, _ := newTestClient(t, rec, WithBatchSize(10))
		enqueueN(t, c, 100)
		_ = c.Close()
		assertStatsBalance(t, c)
		if got := c.Stats().DroppedRejected; got == 0 {
			t.Error("DroppedRejected = 0 despite every batch being rejected")
		}
	})

	t.Run("retry exhaustion", func(t *testing.T) {
		t.Parallel()
		rec := newRecorder(t, withStatuses(503, 503, 503))
		c, _ := newTestClient(t, rec,
			WithBatchSize(1000),
			WithMaxRetries(2),
			WithRetryBackoff(time.Millisecond),
		)
		enqueueN(t, c, 7)
		_ = c.Close()
		assertStatsBalance(t, c)
		if got := c.Stats().DroppedRejected; got != 7 {
			t.Errorf("DroppedRejected = %d, want 7", got)
		}
	})

	t.Run("under stall and overflow", func(t *testing.T) {
		t.Parallel()
		rec := newRecorder(t, withGate())
		c, _ := newTestClient(t, rec,
			WithMaxQueueSize(8),
			WithBatchSize(4),
			WithMaxInFlight(1),
			WithShutdownTimeout(100*time.Millisecond),
		)
		for i := 0; i < 2000; i++ {
			_ = c.Enqueue(event(i))
		}
		rec.release()
		_ = c.Close()
		assertStatsBalance(t, c)
	})
}

// --- construction -----------------------------------------------------------

// A vendor SDK must not crash the host application over an unset environment
// variable, and a rejected configuration must not leak goroutines — goleak
// proves the second half.
func TestNewClientValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tok  string
		opts []ClientOption
		want string
	}{
		{"empty token", "", nil, "source token"},
		{"blank token", "   ", nil, "source token"},
		{"endpoint without a scheme", testToken, []ClientOption{WithEndpoint("in.logs.betterstack.com")}, "http or https"},
		{"endpoint with the wrong scheme", testToken, []ClientOption{WithEndpoint("ftp://example.com")}, "http or https"},
		{"zero batch size", testToken, []ClientOption{WithBatchSize(0)}, "WithBatchSize"},
		{"negative in-flight", testToken, []ClientOption{WithMaxInFlight(-1)}, "WithMaxInFlight"},
		{"zero queue size", testToken, []ClientOption{WithMaxQueueSize(0)}, "WithMaxQueueSize"},
		{"negative retries", testToken, []ClientOption{WithMaxRetries(-1)}, "WithMaxRetries"},
		{"zero batch interval", testToken, []ClientOption{WithBatchInterval(0)}, "WithBatchInterval"},
		{"negative timeout", testToken, []ClientOption{WithTimeout(-time.Second)}, "WithTimeout"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewClient(tt.tok, tt.opts...)
			if err == nil {
				c.Close()
				t.Fatal("NewClient accepted an invalid configuration")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err, tt.want)
			}
		})
	}
}

func TestNewClientEmptyTokenIsErrNoSourceToken(t *testing.T) {
	t.Parallel()
	if _, err := NewClient(""); !errors.Is(err, ErrNoSourceToken) {
		t.Errorf("NewClient(\"\") = %v, want ErrNoSourceToken", err)
	}
}

// --- error surfacing --------------------------------------------------------

// An encoding failure is local and synchronous, so it comes back from Enqueue
// and therefore from Handle. Nothing is queued.
func TestEnqueueReturnsEncodeError(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec)
	defer c.Close()

	err := c.Enqueue(map[string]any{"bad": math.NaN()})
	if err == nil {
		t.Fatal("Enqueue accepted an unencodable value")
	}
	if got := c.Stats().Enqueued; got != 0 {
		t.Errorf("Stats().Enqueued = %d, want 0", got)
	}
}

func TestHandleReturnsEncodeError(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec)
	defer c.Close()

	h := NewHandler(c)
	r := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	r.AddAttrs(slog.Float64("bad", math.NaN()))

	if err := h.Handle(context.Background(), r); err == nil {
		t.Error("Handle returned nil for an unencodable record")
	}
}

// A panic in the user's OnError must not kill the sender goroutine, which would
// take the host process with it.
func TestOnErrorPanicDoesNotKillTheClient(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withStatuses(401))
	c, err := NewClient(testToken,
		WithEndpoint(rec.endpoint()),
		WithBatchInterval(time.Hour),
		WithBatchSize(1000),
		WithOnError(func(error) { panic("user callback blew up") }),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	enqueueN(t, c, 1)
	_ = c.Flush(context.Background()) // triggers the 401 and the panicking callback

	// The client must still be working.
	enqueueN(t, c, 1)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("client is broken after an OnError panic: %v", err)
	}
	if got := c.Stats().Sent; got != 1 {
		t.Errorf("Stats().Sent = %d, want 1", got)
	}
}

// Handlers derived from one client all feed the same queue, so a single batch
// carries records from all of them.
func TestHandlersShareOneBatch(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000))
	defer c.Close()

	base := NewHandler(c)
	logger := slog.New(base)
	logger.Info("one")
	slog.New(base.WithAttrs([]slog.Attr{slog.String("a", "1")})).Info("two")
	slog.New(base.WithGroup("g")).Info("three")

	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := rec.count(); got != 1 {
		t.Errorf("got %d requests, want 1: the derived handlers are not sharing a queue", got)
	}
	if got := len(rec.records()); got != 3 {
		t.Errorf("got %d records, want 3", got)
	}
}
