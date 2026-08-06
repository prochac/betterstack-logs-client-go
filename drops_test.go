package betterstack

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// reportDrops emits deltas, so a steady state produces no repeat reports and a
// new drop produces exactly one. Driven directly because the periodic path is
// gated on a five-second interval that no test should wait for.
func TestReportDropsEmitsDeltas(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec)
	defer c.Close()

	s := newSender(c)
	defer s.timer.Stop()

	// Nothing dropped yet.
	s.reportDrops()
	if got := errs.len(); got != 0 {
		t.Fatalf("OnError fired %d times with nothing dropped", got)
	}

	c.stats.droppedQueueFull.Add(7)
	s.reportDrops()
	if got := errs.len(); got != 1 {
		t.Fatalf("OnError fired %d times, want 1", got)
	}
	var de *DropError
	if !errors.As(errs.all()[0], &de) {
		t.Fatalf("error is %T, want *DropError", errs.all()[0])
	}
	if de.Records != 7 || de.Reason != DropQueueFull {
		t.Errorf("got %d %v, want 7 %v", de.Records, de.Reason, DropQueueFull)
	}

	// Nothing new: no repeat report.
	s.reportDrops()
	if got := errs.len(); got != 1 {
		t.Errorf("OnError fired %d times, want 1: the same drops were reported twice", got)
	}

	// Only the delta.
	c.stats.droppedQueueFull.Add(3)
	s.reportDrops()
	if got := errs.len(); got != 2 {
		t.Fatalf("OnError fired %d times, want 2", got)
	}
	if !errors.As(errs.all()[1], &de) || de.Records != 3 {
		t.Errorf("second report = %v, want a delta of 3", errs.all()[1])
	}
}

// Rejections are already reported individually as *StatusError, carrying the
// status code and the server's explanation, so summarising them again would be
// duplicate noise.
func TestReportDropsSkipsRejections(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec)
	defer c.Close()

	s := newSender(c)
	defer s.timer.Stop()

	c.stats.droppedRejected.Add(5)
	s.reportDrops()

	if got := errs.len(); got != 0 {
		t.Errorf("OnError fired %d times for rejections, want 0: %v", got, errs.all())
	}
}

func TestMaybeReportDropsIsRateLimited(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec)
	defer c.Close()

	s := newSender(c)
	defer s.timer.Stop()

	c.stats.droppedQueueFull.Add(1)
	s.maybeReportDrops()
	if got := errs.len(); got != 0 {
		t.Errorf("a summary was emitted within the rate-limit window (%v)", dropReportInterval)
	}

	// Pretend the interval has elapsed.
	s.lastDropReport = time.Now().Add(-2 * dropReportInterval)
	s.maybeReportDrops()
	if got := errs.len(); got != 1 {
		t.Errorf("OnError fired %d times after the interval elapsed, want 1", got)
	}
}

// Close reports whatever was lost, unconditionally: by then the sender has
// exited, so nothing else will.
func TestCloseReportsFinalDropSummary(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withGate())
	c, errs := newTestClient(t, rec,
		WithMaxQueueSize(2),
		WithBatchSize(1),
		WithMaxInFlight(1),
		WithShutdownTimeout(100*time.Millisecond),
	)

	for i := 0; i < 500; i++ {
		_ = c.Enqueue(event(i))
	}
	rec.release()
	_ = c.Close()

	var summary *DropError
	for _, err := range errs.all() {
		var de *DropError
		if errors.As(err, &de) && de.Reason == DropQueueFull {
			summary = de
		}
	}
	if summary == nil {
		t.Fatalf("Close emitted no queue-full drop summary: %v", errs.all())
	}
	if got := c.Stats().DroppedQueueFull; uint64(summary.Records) != got {
		t.Errorf("summary reported %d records, Stats says %d", summary.Records, got)
	}
}

// Burst drops go through the same aggregation as every other drop reason. The
// JavaScript client prints a console line per window instead; here the periodic
// summary already exists and reporting one callback per refused record is the
// error storm it was built to prevent.
func TestBurstDropsAreSummarised(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec,
		WithBatchSize(1000),
		WithBurstProtection(4, time.Hour),
	)

	enqueueN(t, c, 100)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var summary *DropError
	for _, err := range errs.all() {
		var de *DropError
		if errors.As(err, &de) && de.Reason == DropBurst {
			summary = de
		}
	}
	if summary == nil {
		t.Fatalf("Close emitted no burst drop summary: %v", errs.all())
	}
	if summary.Records != 96 {
		t.Errorf("summary reported %d records, want 96", summary.Records)
	}
	if got := c.Stats().DroppedBurst; uint64(summary.Records) != got {
		t.Errorf("summary reported %d records, Stats says %d", summary.Records, got)
	}
}

// The final summary must not fire when nothing was lost.
func TestCloseIsQuietWhenNothingDropped(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec, WithBatchSize(1000))
	enqueueN(t, c, 5)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := errs.len(); got != 0 {
		t.Errorf("OnError fired %d times on a clean shutdown: %v", got, errs.all())
	}
}

// --- option coverage --------------------------------------------------------

func TestWithEncoder(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000), WithEncoder(countingEncoder{}))
	defer c.Close()

	enqueueN(t, c, 2)
	if err := c.Flush(nil); err != nil { // a nil context is treated as Background
		t.Fatalf("Flush: %v", err)
	}

	if got := rec.all()[0].header.Get("Content-Type"); got != "application/x-test" {
		t.Errorf("Content-Type = %q; the encoder's content type did not travel with it", got)
	}
}

// countingEncoder proves the Encoder interface is actually pluggable: a format
// with its own content type and its own framing, neither of which a bare
// marshaller function could express.
type countingEncoder struct{}

func (countingEncoder) ContentType() string { return "application/x-test" }

func (countingEncoder) AppendRecord(dst []byte, _ map[string]any) ([]byte, error) {
	return append(dst, "x\n"...), nil
}

// A framing that depends on the record count, so that a batch which is split
// and re-framed cannot pass by accident.
func (countingEncoder) Frame(batch []byte, n int) []byte {
	return append(batch, []byte(fmt.Sprintf("count=%d\n", n))...)
}

func TestEncoderFramesEachBatch(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000),
		WithEncoder(countingEncoder{}), WithCompression(CompressionNone))
	defer c.Close()

	enqueueN(t, c, 3)
	if err := c.Flush(nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got, want := string(rec.all()[0].body), "x\nx\nx\ncount=3\n"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

func TestWithConnectTimeout(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithConnectTimeout(2*time.Second))
	defer c.Close()

	if got := c.cfg.connectTimeout; got != 2*time.Second {
		t.Errorf("connectTimeout = %v, want 2s", got)
	}
}

// Without WithOnError the client must still have a working sink rather than a
// nil callback waiting to panic on the first delivery failure.
func TestDefaultOnErrorIsInstalled(t *testing.T) {
	t.Parallel()

	c, err := NewClient(testToken)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if c.cfg.onError == nil {
		t.Fatal("onError is nil by default")
	}
	// Reaching the end without a panic is the assertion; the default writes one
	// line to stderr.
	c.report(&DropError{Records: 1, Reason: DropQueueFull})
}

func TestWithJSONArrayEncoder(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000), WithEncoder(JSONArray()))
	defer c.Close()

	enqueueN(t, c, 3)
	if err := c.Flush(nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	req := rec.all()[0]
	if got := req.header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	// The recorder has already asserted the body parses as an array of objects;
	// what is left is that all three records are in it, in order.
	if got := len(req.records); got != 3 {
		t.Fatalf("got %d records in the array, want 3: %q", got, req.body)
	}
	for i, m := range req.records {
		if got, want := m[KeyMessage], fmt.Sprintf("record-%d", i); got != want {
			t.Errorf("record %d = %v, want %q", i, got, want)
		}
	}
}

// The same client-side backstop as TestOversizeRecordIsDroppedBeforeSending,
// but with something to split: the batch is halved locally rather than thrown
// away, exactly as it would be had the server been the one to complain.
func TestOversizeBatchIsSplitBeforeSending(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec,
		WithBatchSize(4),
		WithCompression(CompressionNone), // so the body size is predictable
		WithMaxBatchBytes(hardMaxRequestBytes*4),
	)
	defer c.Close()

	// Four records of a third of the limit each: the batch is over, every half
	// and quarter is not.
	big := strings.Repeat("x", hardMaxRequestBytes/3)
	for i := 0; i < 4; i++ {
		if err := c.Enqueue(map[string]any{KeyMessage: big, KeyLevel: "INFO"}); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
	}
	if err := c.Flush(nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := c.Stats().Sent; got != 4 {
		t.Errorf("Stats().Sent = %d, want 4", got)
	}
	if got := c.Stats().DroppedOversize; got != 0 {
		t.Errorf("Stats().DroppedOversize = %d, want 0: the batch was splittable", got)
	}
	if got := rec.count(); got < 2 {
		t.Errorf("got %d requests, want at least 2: the batch was not split", got)
	}
	for _, req := range rec.all() {
		if len(req.body) > hardMaxRequestBytes {
			t.Errorf("a request of %d bytes went out over the %d limit", len(req.body), hardMaxRequestBytes)
		}
	}
	if got := errs.len(); got != 0 {
		t.Errorf("OnError fired %d times: %v", got, errs.all())
	}
}

// A single record over the hard request limit is dropped before it is sent: it
// cannot be split, and the server would reject it, so spending a request and a
// retry budget to find that out wastes the customer's quota and delays
// everything behind it.
func TestOversizeRecordIsDroppedBeforeSending(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec,
		WithBatchSize(1),
		WithCompression(CompressionNone), // so the body size is predictable
		WithMaxBatchBytes(hardMaxRequestBytes*2),
	)
	defer c.Close()

	huge := strings.Repeat("x", hardMaxRequestBytes+(1<<20))
	if err := c.Enqueue(map[string]any{KeyMessage: huge, KeyLevel: "INFO"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := c.Flush(nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := rec.count(); got != 0 {
		t.Errorf("got %d requests, want 0: a doomed body was sent anyway", got)
	}
	if got := c.Stats().DroppedOversize; got != 1 {
		t.Errorf("Stats().DroppedOversize = %d, want 1", got)
	}
	var de *DropError
	found := false
	for _, err := range errs.all() {
		if errors.As(err, &de) && de.Reason == DropOversize {
			found = true
		}
	}
	if !found {
		t.Errorf("no oversize drop reported: %v", errs.all())
	}
}

// --- dry run ----------------------------------------------------------------

// A dry run runs the whole pipeline and skips only the request. That is what
// makes it useful: the encoding, batching and compression a real send would do
// still happen, so a bug in any of them still shows up.
func TestDryRunSendsNothing(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec, WithBatchSize(10), WithDryRun(true))
	defer c.Close()

	enqueueN(t, c, 25)
	if err := c.Flush(nil); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := rec.count(); got != 0 {
		t.Errorf("got %d requests, want 0: the kill switch did not stop them", got)
	}
	if got := c.Stats().Sent; got != 25 {
		t.Errorf("Stats().Sent = %d, want 25", got)
	}
	if got := errs.len(); got != 0 {
		t.Errorf("OnError fired %d times on a dry run: %v", got, errs.all())
	}
}

// The point of the switch is running without credentials, so it must not demand
// one. Everything else is still validated.
func TestDryRunNeedsNoSourceToken(t *testing.T) {
	t.Parallel()

	c, err := NewClient("", WithDryRun(true))
	if err != nil {
		t.Fatalf("NewClient(\"\", WithDryRun(true)) = %v, want it to succeed", err)
	}
	defer c.Close()

	if err := c.Enqueue(event(0)); err != nil {
		t.Errorf("Enqueue: %v", err)
	}

	if _, err := NewClient("", WithDryRun(true), WithBatchSize(0)); err == nil {
		t.Error("a dry run accepted WithBatchSize(0): only the token check is waived")
	}
	if _, err := NewClient(""); !errors.Is(err, ErrNoSourceToken) {
		t.Errorf("NewClient(\"\") = %v, want ErrNoSourceToken when not a dry run", err)
	}
}

// --- retry ceiling ----------------------------------------------------------

// The ceiling is the second of the two retry limits, and the tighter one wins.
// Here it stops a batch that MaxRetries alone would have kept alive.
func TestRetryCeilingCutsRetriesShort(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withStatuses(503, 503, 503, 503, 503, 503, 503, 503, 503, 503))
	c, _ := newTestClient(t, rec,
		WithBatchSize(1000),
		WithMaxRetries(100),
		WithRetryBackoff(50*time.Millisecond),
		WithRetryCeiling(150*time.Millisecond),
	)
	defer c.Close()

	enqueueN(t, c, 2)
	if err := c.Flush(context.Background()); err == nil {
		t.Fatal("Flush = nil, want the batch to have been given up on")
	}

	// An exact request count would be a flake: full jitter can make any single
	// backoff near zero, so the number of attempts that fit inside the ceiling
	// varies. What must hold is that 101 attempts did not.
	if got := rec.count(); got > 20 {
		t.Errorf("got %d requests: the ceiling did not bound the retries", got)
	}
	if got := c.Stats().DroppedRejected; got != 2 {
		t.Errorf("Stats().DroppedRejected = %d, want 2", got)
	}
}

func TestRetryCeilingIsValidated(t *testing.T) {
	t.Parallel()

	if _, err := NewClient(testToken, WithRetryCeiling(0)); err == nil {
		t.Error("WithRetryCeiling(0) was accepted")
	}
}

func TestErrorValueNil(t *testing.T) {
	t.Parallel()
	if got := errorValue(nil); got != nil {
		t.Errorf("errorValue(nil) = %v, want nil", got)
	}
}
