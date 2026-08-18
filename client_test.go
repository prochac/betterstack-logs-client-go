package betterstack

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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

// Flush documents a nil context as Background. Nothing else in the suite passes
// one, so without this the substitution would go untested.
func TestFlushNilContext(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000))
	defer c.Close()

	enqueueN(t, c, 1)
	//nolint:staticcheck // SA1012: the nil context is what this test is for.
	if err := c.Flush(nil); err != nil {
		t.Fatalf("Flush(nil): %v", err)
	}

	if got := len(rec.records()); got != 1 {
		t.Errorf("got %d records delivered, want 1", got)
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
	if !errors.As(err, &se) || se.StatusCode != http.StatusUnauthorized {
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
	if !errors.Is(first, second) {
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
		if !errors.Is(err, errs[0]) {
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

// A record whose Enqueue lands after Close has counted the queue is the one
// exception the Stats identity documents (invariant 10), and Close's leftover
// count is the code that keeps it an exception rather than a hole: anything
// still queued when the sender has gone is counted as dropped instead of
// silently lost.
//
// The window is between the sender's exit and the count a few statements later,
// which is too narrow for another goroutine to be scheduled into reliably — so
// the test occupies it from the inside, through the seam that exists for it,
// doing exactly what the racing Enqueue would have done: count the record as
// offered, then put it in the queue.
func TestCloseCountsWhatIsLeftInTheQueue(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	var c *Client
	late := func(cfg *clientConfig) {
		cfg.beforeLeftoverCount = func() {
			buf, err := c.cfg.encoder.AppendRecord(nil, event(99))
			if err != nil {
				t.Errorf("AppendRecord: %v", err)
				return
			}
			c.stats.enqueued.Add(1)
			c.queue <- &buf
		}
	}
	c, errs := newTestClient(t, rec, WithBatchSize(1), late)

	enqueueN(t, c, 2)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	stats := c.Stats()
	if stats.Sent != 2 {
		t.Errorf("Stats().Sent = %d, want 2: the two records that made it were delivered", stats.Sent)
	}
	// Delivery was healthy and nothing was abandoned, so this can only be the
	// leftover record.
	if stats.DroppedClosed != 1 {
		t.Errorf("Stats().DroppedClosed = %d, want 1: the record left in the queue was not counted", stats.DroppedClosed)
	}
	assertStatsBalance(t, c)

	var de *DropError
	for _, err := range errs.all() {
		if errors.As(err, &de) && de.Reason == DropClosed {
			return
		}
	}
	t.Errorf("no DropClosed summary was reported; got %v", errs.all())
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

// …and it must drop without paying for the encode first. The queue fills exactly
// when the application is logging hardest, so marshalling records for the bin is
// the worst possible time to spend that CPU.
func TestQueueFullDropSkipsTheEncode(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withGate())
	defer rec.release() // before the server's cleanup, and after Close below
	enc := &tallyEncoder{Encoder: NDJSON()}
	c, _ := newTestClient(t, rec,
		WithEncoder(enc),
		WithBatchSize(1),
		WithMaxInFlight(1),
		WithMaxQueueSize(4),
		WithShutdownTimeout(100*time.Millisecond), // nothing here is ever delivered
	)
	defer c.Close()

	// Wedge the sender: one batch on the worker the gate holds, one in the
	// hand-off slot behind it, and the third leaves the sender blocked. From
	// here nothing drains the queue.
	enqueueN(t, c, 3)
	waitFor(t, "the upload pool to saturate", func() bool {
		c.pool.mu.Lock()
		defer c.pool.mu.Unlock()
		return c.pool.dispatched == 2
	})

	// Fill the queue behind the wedge. Driven until a drop proves it full rather
	// than counted out, because the sender may still be on its way to the batch
	// it blocks on and would take one more record with it.
	waitFor(t, "the queue to fill", func() bool {
		_ = c.Enqueue(event(0))
		return c.Stats().DroppedQueueFull > 0
	})

	const offered = 50
	before := enc.calls()
	dropped := c.Stats().DroppedQueueFull
	for i := 0; i < offered; i++ {
		_ = c.Enqueue(event(i))
	}

	if got := enc.calls() - before; got != 0 {
		t.Errorf("AppendRecord ran %d times for %d records offered to a full queue, want 0", got, offered)
	}
	// The saving must not have cost the accounting: every one of them is still a
	// counted drop, which is what keeps the Stats identity whole.
	if got := c.Stats().DroppedQueueFull - dropped; got != offered {
		t.Errorf("DroppedQueueFull grew by %d, want %d", got, offered)
	}
}

// The pre-check above is an optimisation, not the decision. The decision is the
// non-blocking send, and it is the only one a producer reaches that loses the
// race — passing the pre-check while the queue still had room, and finding it
// full a microsecond later. That is the ordinary case under load, and three
// things ride on it: that Enqueue still neither blocks nor errors, that the
// drop is counted (without which the Stats identity holds everywhere except
// under contention), and that the record's buffer goes back to the pool
// instead of being leaked or, worse, handed out twice.
//
// The race is made rather than waited for. Two producers are held inside the
// encode at once, behind a sender that cannot drain, so both have passed the
// pre-check against an empty queue with one slot: whichever sends second must
// take the branch below it.
func TestQueueFullDropAfterTheEncode(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withGate())
	defer rec.release() // before the server's cleanup, and after Close below
	enc := &gateEncoder{Encoder: NDJSON(), entered: make(chan struct{}, 2), release: make(chan struct{})}
	c, _ := newTestClient(t, rec,
		WithEncoder(enc),
		WithBatchSize(1),
		WithMaxInFlight(1),
		WithMaxQueueSize(1),
		WithShutdownTimeout(100*time.Millisecond), // nothing here is ever delivered
	)
	defer c.Close()

	// Wedge the sender: one batch on the gated worker, one in the hand-off slot
	// behind it, and the third leaves the sender blocked. One record at a time,
	// each waited out of the queue before the next — offering all three at once
	// risks a drop against a queue of one, and this test's whole subject is
	// which drop happened. Once the third is out of the queue the sender is
	// committed to the hand-off, so the queue is empty from here on and stays
	// empty: nothing is left to drain it.
	for i := 0; i < 3; i++ {
		if err := c.Enqueue(event(i)); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
		waitFor(t, "the sender to take the record", func() bool { return len(c.queue) == 0 })
	}
	waitFor(t, "the upload pool to saturate", func() bool {
		c.pool.mu.Lock()
		defer c.pool.mu.Unlock()
		return c.pool.dispatched == 2
	})

	// Counted from here: a wedging record may itself have found the queue full
	// on the way in, and that drop is the pre-check's, not this test's.
	before := c.Stats().DroppedQueueFull

	enc.arm()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.Enqueue(event(i))
		}(i)
	}
	for i := 0; i < 2; i++ {
		<-enc.entered // both are inside the encode, so both are past the pre-check
	}
	close(enc.release)
	wg.Wait()
	enc.disarm()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Enqueue(%d) = %v, want nil: a dropped record is not an error", i, err)
		}
	}
	// Exactly one: the queue had one slot, and the loser of the race cannot
	// have been refused anywhere earlier, having reached the encoder at all.
	if got := c.Stats().DroppedQueueFull - before; got != 1 {
		t.Errorf("DroppedQueueFull grew by %d, want 1", got)
	}
	if got := enc.calls(); got != 2 {
		t.Errorf("the encoder ran %d times for the two racers, want 2: "+
			"one of them was refused by the pre-check, so this no longer tests the drop below it", got)
	}
}

// gateEncoder holds every record inside AppendRecord once armed, so a test can
// have several producers in the encode at the same moment. The format is
// delegated, so the recorder still checks the wire.
type gateEncoder struct {
	Encoder
	on      atomic.Bool
	n       atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (e *gateEncoder) arm()    { e.on.Store(true) }
func (e *gateEncoder) disarm() { e.on.Store(false) }

func (e *gateEncoder) AppendRecord(dst []byte, event map[string]any) ([]byte, error) {
	if e.on.Load() {
		e.n.Add(1)
		e.entered <- struct{}{}
		<-e.release
	}
	return e.Encoder.AppendRecord(dst, event)
}

func (e *gateEncoder) calls() int64 { return e.n.Load() }

// tallyEncoder counts AppendRecord calls so a test can assert a record never
// reached the encoder at all. The format is delegated, so the recorder still
// checks the wire.
type tallyEncoder struct {
	Encoder
	n atomic.Int64
}

func (e *tallyEncoder) AppendRecord(dst []byte, event map[string]any) ([]byte, error) {
	e.n.Add(1)
	return e.Encoder.AppendRecord(dst, event)
}

func (e *tallyEncoder) calls() uint64 { return uint64(e.n.Load()) }

// --- framing ----------------------------------------------------------------

// An encoder that declares its framing to be the identity is taken at its word:
// the accumulated records are the body already, so they are never copied into
// the packer's framing buffer on the way to the compressor.
func TestIdentityFramingSkipsTheFramingBuffer(t *testing.T) {
	t.Parallel()

	raw := []byte("{\"a\":1}\n{\"b\":2}\n")
	bounds := []int{8, 16}

	p := newPacker(NDJSON(), CompressionNone, nil)
	b, err := p.pack(raw, bounds)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if p.scratch != nil {
		t.Errorf("the framing buffer holds %d byte(s) after an identity-framed batch, want none at all", len(p.scratch))
	}
	if got := string(b.body); got != string(raw) {
		t.Errorf("body = %q, want %q", got, raw)
	}

	// The other side of it: an encoder that really frames still gets its buffer,
	// so the saving is not a framing pass quietly skipped for everyone.
	q := newPacker(JSONArray(), CompressionNone, nil)
	framed, err := q.pack([]byte(`,{"a":1},{"b":2}`), bounds)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if q.scratch == nil {
		t.Error("the framing buffer is unused for an encoder whose Frame is not the identity")
	}
	if got, want := string(framed.body), `[{"a":1},{"b":2}]`; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

// The fast path is a declaration, not a type assertion on this package's own
// NDJSON encoder — which is the whole reason IdentityFramer is exported. A
// third-party line-delimited encoder gets it too, and gets the promise that goes
// with it: Frame is not called at all.
func TestIdentityFramingIsOpenToAnyEncoder(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	enc := &lineEncoder{Encoder: NDJSON()}
	c, _ := newTestClient(t, rec, WithEncoder(enc), WithBatchSize(3))
	defer c.Close()

	enqueueN(t, c, 3)
	rec.waitForRequests(t, 1)

	if got := enc.frames(); got != 0 {
		t.Errorf("Frame ran %d time(s) for an encoder declaring it the identity, want 0", got)
	}
	// The recorder decodes the NDJSON, so this is also the check that skipping
	// the framing pass left a body the server can still read.
	if got := len(rec.records()); got != 3 {
		t.Errorf("got %d records on the wire, want 3", got)
	}
}

// lineEncoder is a third-party-shaped encoder: its own type, delegating the
// format, declaring its own framing to be the identity, and counting the Frame
// calls that declaration promises will not happen. The declaration has to be
// made here — embedding an Encoder promotes the Encoder methods and nothing
// else, so a wrapper never inherits one.
type lineEncoder struct {
	Encoder
	n atomic.Int64
}

var _ IdentityFramer = (*lineEncoder)(nil)

func (e *lineEncoder) FrameIsIdentity() bool { return true }

func (e *lineEncoder) Frame(batch []byte, n int) []byte {
	e.n.Add(1)
	return e.Encoder.Frame(batch, n)
}

func (e *lineEncoder) frames() int64 { return e.n.Load() }

// --- the batch buffer pool --------------------------------------------------

// A batch's record buffer only ever grows, and the sender checks a batch for
// fullness only after appending a record — so one outsized record leaves that
// batch over MaxBatchBytes, and pooling it would pin that capacity for the life
// of the client. Ordinary traffic is inside the cap by construction, since a
// batch is at most MaxBatchBytes plus the record that crossed it.
//
// The assertion is one-sided, like the JSON encoder pool's: a pool that answers
// with nothing passes, so this can only fail when the oversized set really was
// retained.
func TestOversizedBatchBuffersAreNotPooled(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithMaxBatchBytes(1<<10))
	defer c.Close()

	limit := 2 * c.cfg.maxBatchBytes
	c.putBatchBufs(&batchBufs{raw: make([]byte, 0, 4*limit)})

	// Drain rather than Get once: a pool holds per-P private and shared slots
	// and nothing promises which one answers first.
	for i := 0; i < 16; i++ {
		bufs, _ := c.batchBufPool.Get().(*batchBufs)
		if bufs == nil {
			continue
		}
		if got := cap(bufs.raw); got > limit {
			t.Fatalf("pooled batch buffer has capacity %d, over the %d cap: "+
				"one outsized record inflates the pool permanently", got, limit)
		}
	}
}

// --- burst protection -------------------------------------------------------

// A window of an hour makes the limit a plain count for the duration of the
// test: the bucket starts full and never refills, so exactly the burst size
// gets through and everything after it is refused.
func TestBurstProtectionDropsOverTheLimit(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec,
		WithBatchSize(5),
		WithBurstProtection(10, time.Hour),
	)
	defer c.Close()

	enqueueN(t, c, 100)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	stats := c.Stats()
	if stats.Enqueued != 100 {
		t.Errorf("Stats().Enqueued = %d, want 100: a refused record was still offered", stats.Enqueued)
	}
	if stats.DroppedBurst != 90 {
		t.Errorf("Stats().DroppedBurst = %d, want 90", stats.DroppedBurst)
	}
	if stats.Sent != 10 {
		t.Errorf("Stats().Sent = %d, want 10", stats.Sent)
	}
	if got := len(rec.accepted()); got != 10 {
		t.Errorf("the endpoint received %d records, want 10", got)
	}
}

// The limiter runs before the encoder, which is the entire point: a burst that
// is going to be dropped must not first be marshalled. An event holding a value
// no JSON encoder can take proves the order — Enqueue returns nil, so the
// encoder was never reached.
func TestBurstProtectionRefusesBeforeEncoding(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBurstProtection(1, time.Hour))
	defer c.Close()

	unencodable := map[string]any{KeyMessage: "nope", "ch": make(chan int)}

	// Sanity: while budget remains, the event reaches the encoder and is
	// rejected by it.
	if err := c.Enqueue(unencodable); err == nil {
		t.Fatal("Enqueue accepted an unencodable event")
	}

	// The offer above consumed the only slot in the bucket, so the next one is
	// refused by the limiter and never reaches the encoder.
	if err := c.Enqueue(unencodable); err != nil {
		t.Errorf("Enqueue = %v, want nil: the record should have been refused before encoding", err)
	}
	if got := c.Stats().DroppedBurst; got != 1 {
		t.Errorf("Stats().DroppedBurst = %d, want 1", got)
	}
}

// Once Close has returned, every record handed to Enqueue is accounted for
// exactly once.
func assertStatsBalance(t *testing.T, c *Client) {
	t.Helper()
	s := c.Stats()
	accounted := s.Sent + s.DroppedQueueFull + s.DroppedBurst + s.DroppedBacklog +
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

	// Splitting moves records between batches after the accounting has started,
	// which is exactly where a double-count or a lost count would hide.
	t.Run("under batch splitting", func(t *testing.T) {
		t.Parallel()
		rec := newRecorder(t, withMaxAcceptedBytes(200))
		c, _ := newTestClient(t, rec, WithBatchSize(64), WithCompression(CompressionNone))
		enqueueN(t, c, 256)
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		assertStatsBalance(t, c)
		if got := c.Stats().Sent; got != 256 {
			t.Errorf("Sent = %d, want 256", got)
		}
	})

	t.Run("dry run", func(t *testing.T) {
		t.Parallel()
		rec := newRecorder(t)
		c, _ := newTestClient(t, rec, WithBatchSize(10), WithDryRun(true))
		enqueueN(t, c, 100)
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		assertStatsBalance(t, c)
		if got := c.Stats().Sent; got != 100 {
			t.Errorf("Sent = %d, want 100: a dry run still accounts for its records", got)
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

	t.Run("under burst protection", func(t *testing.T) {
		t.Parallel()
		rec := newRecorder(t)
		c, _ := newTestClient(t, rec,
			WithBatchSize(10),
			WithBurstProtection(10, time.Hour),
		)
		enqueueN(t, c, 100)
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		assertStatsBalance(t, c)
		if got := c.Stats().DroppedBurst; got != 90 {
			t.Errorf("DroppedBurst = %d, want 90", got)
		}
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
		{"unparseable endpoint", testToken, []ClientOption{WithEndpoint("http://[::1")}, "invalid endpoint"},
		{"endpoint without a host", testToken, []ClientOption{WithEndpoint("http:///v1")}, "no host"},
		{"zero batch size", testToken, []ClientOption{WithBatchSize(0)}, "WithBatchSize"},
		{"negative in-flight", testToken, []ClientOption{WithMaxInFlight(-1)}, "WithMaxInFlight"},
		{"zero queue size", testToken, []ClientOption{WithMaxQueueSize(0)}, "WithMaxQueueSize"},
		{"negative retries", testToken, []ClientOption{WithMaxRetries(-1)}, "WithMaxRetries"},
		{"zero batch interval", testToken, []ClientOption{WithBatchInterval(0)}, "WithBatchInterval"},
		{"negative timeout", testToken, []ClientOption{WithTimeout(-time.Second)}, "WithTimeout"},
		// Zero is legal here and means "retry at once", so only a negative one
		// is refused. See TestBackoffWithoutABase.
		{"negative retry backoff", testToken, []ClientOption{WithRetryBackoff(-time.Millisecond)}, "WithRetryBackoff"},
		{"burst maximum without a window", testToken, []ClientOption{WithBurstProtection(10, 0)}, "WithBurstProtection"},
		{"burst window without a maximum", testToken, []ClientOption{WithBurstProtection(0, time.Second)}, "WithBurstProtection"},
		{"negative burst maximum", testToken, []ClientOption{WithBurstProtection(-1, -time.Second)}, "WithBurstProtection"},
		// Compression is an exported int enum, so a value outside it is
		// representable. Unchecked it would read as "not gzip" and silently
		// disable compression.
		{"unknown compression", testToken, []ClientOption{WithCompression(Compression(7))}, "WithCompression"},
		{"negative compression", testToken, []ClientOption{WithCompression(Compression(-1))}, "WithCompression"},
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

// --- defaults ---------------------------------------------------------------

// defaultClient builds a client with nothing configured, which is the
// configuration a user gets from NewClient(token). Dry run keeps the suite off
// the network; it sets one flag and leaves every default below untouched.
func defaultClient(t *testing.T) *Client {
	t.Helper()

	c, err := NewClient(testToken, WithDryRun(true))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

// The subject here is the default configuration: the default* const block and
// the clientConfig literal NewClient installs it into. The behaviour those
// numbers drive is tested elsewhere — batching triggers in client_test.go,
// retry and backoff in transport_test.go, the rate limiter in limiter_test.go.
// What is asserted here is only the values, and that each lands in the right
// field.
//
// They are not free choices: each is quoted from an official Better Stack
// client in PARITY.md §2, and recognising the knobs from another language is
// itself an adoption criterion (§7.3). A default that drifts out of agreement
// with that document is a silent parity regression — invisible in review,
// because the new number looks every bit as reasonable as the old one.
//
// The assertions read the config the client actually runs with rather than the
// package constants, so they also catch a field wired to its neighbour's value:
// timeout: defaultConnectTimeout type checks, reads correctly at a glance, and
// halves every request deadline.
//
// This pins the code against the document, not the document against Better
// Stack. If Java changes batchInterval tomorrow, this still passes; re-reading
// the sources stays a human job, and PARITY's "Fetched" stamp is what speaks to
// how current the record is.
func TestClientDefaultsMatchTheSiblingClients(t *testing.T) {
	t.Parallel()

	c := defaultClient(t)
	cfg := c.cfg

	t.Run("counts", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			got  int
			want int
			why  string
		}{
			{"BatchSize", cfg.batchSize, 1000, "JS and Java agree; Erlang's 50 is the outlier"},
			{"MaxQueueSize", cfg.maxQueueSize, 100_000, "Java maxQueueSize; JS syncQueuedMax bounds requests, not records"},
			{"MaxInFlight", cfg.maxInFlight, 5, "JS syncMax; Erlang's pool holds 10 connections"},
			{"MaxRetries", cfg.maxRetries, 5, "Java maxRetries; JS retryCount and Erlang are both 3"},
			{
				"MaxBatchBytes", cfg.maxBatchBytes, 5 << 20,
				"no sibling: JS batchSizeKiB defaults to 0, no size trigger at all. " +
					"Here it is always on, because it is what keeps a batch under §1's " +
					"10 MiB request limit rather than a tuning knob, and 5 MiB " +
					"uncompressed is conservative against a limit measured compressed",
			},
		}
		for _, tt := range tests {
			if tt.got != tt.want {
				t.Errorf("%s = %d, want %d (%s)", tt.name, tt.got, tt.want, tt.why)
			}
		}
	})

	t.Run("durations", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			got  time.Duration
			want time.Duration
			why  string
		}{
			{
				"BatchInterval", cfg.batchInterval, time.Second,
				"JS batchInterval. Java is 3s and Erlang 5s, but with a single timer the " +
					"interval is also the latency floor for a lone log line (§4)",
			},
			{"RetryBackoff", cfg.retryBackoff, 300 * time.Millisecond, "Java retrySleepMilliseconds; JS retryBackoff is 100ms"},
			{"ConnectTimeout", cfg.connectTimeout, 5 * time.Second, "Java connectTimeout"},
			{"Timeout", cfg.timeout, 10 * time.Second, "Java readTimeout; JS transport is 30s"},
			{
				"RetryCeiling", cfg.retryCeiling, 60 * time.Second,
				"no sibling documents a total retry budget; OTel's otlploghttp caps at 1 minute (§6.4)",
			},
			{
				"ShutdownTimeout", cfg.shutdownTimeout, 15 * time.Second,
				"no sibling documents one, but every one of them documents flushing " +
					"before exit as mandatory (§2), which is what this bounds",
			},
		}
		for _, tt := range tests {
			if tt.got != tt.want {
				t.Errorf("%s = %v, want %v (%s)", tt.name, tt.got, tt.want, tt.why)
			}
		}
	})

	t.Run("wire", func(t *testing.T) {
		t.Parallel()

		if got, want := cfg.endpoint, "https://in.logs.betterstack.com"; got != want {
			t.Errorf("Endpoint = %q, want %q (JS endpoint, Java ingestUrl)", got, want)
		}
		// The 10 MiB request limit is measured on compressed bytes (§1), so
		// compression directly multiplies throughput.
		if cfg.compression != CompressionGzip {
			t.Errorf("Compression = %v, want CompressionGzip", cfg.compression)
		}
		// NDJSON, a documented Better Stack encoding (§1), over the JSON array:
		// a record's encoding is then self-delimiting and position-independent.
		if got, want := cfg.encoder.ContentType(), "application/x-ndjson"; got != want {
			t.Errorf("default encoder ContentType = %q, want %q", got, want)
		}
	})

	// The one place this client deliberately refuses a sibling's default rather
	// than adopting or adapting it. JS ships burstProtectionMax 10000 per
	// burstProtectionMilliseconds 5000, on by default; that ceiling is
	// calibrated for Node, and silently capping a Go service at 2000 rec/s
	// would be a surprise (DESIGN §2). Backpressure is shed at the queue and
	// nowhere else; the limiter is an admission ceiling an operator declares.
	t.Run("burst protection is opt-in", func(t *testing.T) {
		t.Parallel()

		if cfg.burstMax != 0 || cfg.burstWindow != 0 {
			t.Errorf("burst protection configured by default: max=%d window=%v", cfg.burstMax, cfg.burstWindow)
		}
		// The config is the input; the limiter is what Enqueue consults on every
		// record. Only the second one can actually throttle a caller.
		if c.limiter != nil {
			t.Error("a limiter was built with no WithBurstProtection")
		}
	})
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
