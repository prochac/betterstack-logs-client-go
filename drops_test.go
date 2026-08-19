package betterstack

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// withDropReportInterval shortens the drop-summary period. Unexported and
// test-only: the interval has no user-facing knob, and the tests that exercise
// the periodic path would otherwise have to wait out five real seconds.
func withDropReportInterval(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.dropReportInterval = d }
}

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
	defer s.reportTicker.Stop()

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
	defer s.reportTicker.Stop()

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
	defer s.reportTicker.Stop()

	c.stats.droppedQueueFull.Add(1)
	s.maybeReportDrops()
	if got := errs.len(); got != 0 {
		t.Errorf("a summary was emitted within the rate-limit window (%v)", defaultDropReportInterval)
	}

	// Pretend the interval has elapsed.
	s.lastDropReport = time.Now().Add(-2 * defaultDropReportInterval)
	s.maybeReportDrops()
	if got := errs.len(); got != 1 {
		t.Errorf("OnError fired %d times after the interval elapsed, want 1", got)
	}
}

// hasDrop reports whether a summary for the given reason has been delivered.
func hasDrop(errs *errorSink, reason DropReason) bool {
	for _, err := range errs.all() {
		var de *DropError
		if errors.As(err, &de) && de.Reason == reason {
			return true
		}
	}
	return false
}

// Drop summaries are paced by the sender's own ticker, not by batches
// completing. During an outage the sender is parked in the hand-off waiting for
// an upload slot that nothing is freeing, so a summary emitted only at a flush
// point would arrive when the incident ends — while the queue sheds records for
// the whole of it, which is the one thing an operator cannot see any other way.
func TestDropSummariesArriveWhileTheSenderIsWedged(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withGate())
	defer rec.release() // before the server's cleanup, and after Close below
	c, errs := newTestClient(t, rec,
		WithBatchSize(1),
		WithMaxInFlight(1),
		WithMaxQueueSize(4),
		WithShutdownTimeout(100*time.Millisecond), // nothing here will ever be delivered
		withDropReportInterval(20*time.Millisecond),
	)
	defer c.Close()

	// Saturate the pool: the first batch reaches the worker the gate holds, the
	// second the single hand-off slot behind it.
	enqueueN(t, c, 3)
	waitFor(t, "the upload pool to saturate", func() bool {
		c.pool.mu.Lock()
		defer c.pool.mu.Unlock()
		return c.pool.dispatched == 2
	})

	// From here the sender is wedged in the hand-off with a batch it cannot
	// place, so nothing drains the queue and these overflow it. No Flush and no
	// Close: the summary has to arrive on its own.
	for i := 0; i < 100; i++ {
		_ = c.Enqueue(event(i))
	}
	waitFor(t, "a drop summary while the pipeline is wedged", func() bool {
		return hasDrop(errs, DropQueueFull)
	})
}

// The other half of the same gap: drops that happen just before the traffic
// stops. Nothing is wedged here — there is simply no next flush, and on a client
// that has gone quiet that used to mean nothing was reported until Close.
func TestDropSummariesArriveWhileTheSenderIsIdle(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec,
		WithBatchSize(1000),               // no flush trigger can fire
		WithBurstProtection(1, time.Hour), // one record admitted, the rest refused
		withDropReportInterval(20*time.Millisecond),
	)
	defer c.Close()

	enqueueN(t, c, 5)
	waitFor(t, "a drop summary from an idle sender", func() bool {
		return hasDrop(errs, DropBurst)
	})
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

// Everything abandoned because the shutdown deadline expired is counted
// DropClosed, wherever in the pipeline it was standing. The three places a
// batch can be lost that way — sleeping out a retry backoff, in a request, and
// waiting for an upload slot — used to be filed as "rejected by ingest" and
// "upload backlog full", both of which point a post-mortem at a problem that
// was not there: nothing rejected these batches, and the backlog was full only
// because the process was already on its way out.
func TestShutdownCasualtiesAreCountedClosed(t *testing.T) {
	t.Parallel()

	// A Retry-After is honoured verbatim, with no jitter, so the worker parks
	// for ten seconds inside a hundred-millisecond shutdown: the cancellation
	// lands in the backoff sleep every time.
	rec := newRecorder(t, withStatuses(429), withResponseHeader("Retry-After", "10"))
	c, errs := newTestClient(t, rec,
		WithBatchSize(1),
		WithMaxInFlight(1),
		WithRetryCeiling(time.Minute), // long enough that the wait is taken
		WithShutdownTimeout(100*time.Millisecond),
	)

	// Three batches, one per record, for the three losing paths: the first is
	// in the worker, the second in the single hand-off slot behind it, and the
	// third leaves the sender blocked in dispatch with nowhere to put it.
	enqueueN(t, c, 3)
	rec.waitForRequests(t, 1)
	waitFor(t, "the upload pool to saturate", func() bool {
		c.pool.mu.Lock()
		defer c.pool.mu.Unlock()
		return c.pool.dispatched == 2
	})

	_ = c.Close() // returns an error: nothing was delivered

	s := c.Stats()
	if s.DroppedClosed != 3 {
		t.Errorf("Stats().DroppedClosed = %d, want 3: %+v", s.DroppedClosed, s)
	}
	if s.DroppedRejected != 0 {
		t.Errorf("Stats().DroppedRejected = %d, want 0: nothing rejected these batches", s.DroppedRejected)
	}
	if s.DroppedBacklog != 0 {
		t.Errorf("Stats().DroppedBacklog = %d, want 0: the backlog was incidental to the shutdown", s.DroppedBacklog)
	}
	assertStatsBalance(t, c)

	var summary *DropError
	for _, err := range errs.all() {
		var de *DropError
		if errors.As(err, &de) && de.Reason == DropClosed {
			summary = de
		}
	}
	if summary == nil || summary.Records != 3 {
		t.Errorf("final summary = %v, want 3 records dropped as %v: %v", summary, DropClosed, errs.all())
	}
}

// The other side of the same discrimination: a live client whose Flush deadline
// expires with every upload slot taken has a genuine backlog, and must still be
// counted as one.
func TestFlushTimeoutIsCountedBacklog(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withGate())
	c, _ := newTestClient(t, rec,
		WithBatchSize(1000), // nothing dispatches except through a Flush
		WithMaxInFlight(1),
	)

	// Saturate the pool one batch at a time: the first goes to the worker,
	// which the gate holds, the second to the single hand-off slot behind it.
	// Both Flushes wait for uploads that will not finish until release, so they
	// run in the background.
	for i := 0; i < 2; i++ {
		if err := c.Enqueue(event(i)); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
		go func() { _ = c.Flush(context.Background()) }()
		want := uint64(i + 1)
		waitFor(t, "the batch to be dispatched", func() bool {
			c.pool.mu.Lock()
			defer c.pool.mu.Unlock()
			return c.pool.dispatched == want
		})
	}

	// This one has nowhere to go, and its deadline is the caller's.
	if err := c.Enqueue(event(2)); err != nil {
		t.Fatalf("Enqueue(2): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush = %v, want the deadline to have expired", err)
	}

	// Flush and the sender wake on the same expiry, and Flush usually wins the
	// race back, so the count lands a moment later. Polled rather than slept
	// on; if it is ever filed as a shutdown drop instead, this is where the
	// test fails.
	waitFor(t, "the abandoned batch to be counted as a backlog drop", func() bool {
		return c.Stats().DroppedBacklog == 1
	})
	if got := c.Stats().DroppedClosed; got != 0 {
		t.Errorf("Stats().DroppedClosed = %d, want 0: the client is not closing", got)
	}

	rec.release()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertStatsBalance(t, c)
}

// --- option coverage --------------------------------------------------------

func TestWithEncoder(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000), WithEncoder(countingEncoder{}))
	defer c.Close()

	enqueueN(t, c, 2)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := rec.all()[0].header.Get("Content-Type"); got != "application/x-test" {
		t.Errorf("Content-Type = %q; the encoder's content type did not travel with it", got)
	}
}

// contentTypeUnchecked is countingEncoder's content type, and the one body shape
// recorder.check deliberately does not decode. Everything else must be
// recognised there — see the default case.
const contentTypeUnchecked = "application/x-test"

// countingEncoder proves the Encoder interface is actually pluggable: a format
// with its own content type and its own framing, neither of which a bare
// marshaller function could express.
type countingEncoder struct{}

func (countingEncoder) ContentType() string { return contentTypeUnchecked }

func (countingEncoder) AppendRecord(dst []byte, _ map[string]any) ([]byte, error) {
	return append(dst, "x\n"...), nil
}

// A framing that depends on the record count, so that a batch which is split
// and re-framed cannot pass by accident.
func (countingEncoder) Frame(batch []byte, n int) []byte {
	return fmt.Appendf(batch, "count=%d\n", n)
}

func TestEncoderFramesEachBatch(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000),
		WithEncoder(countingEncoder{}), WithCompression(CompressionNone))
	defer c.Close()

	enqueueN(t, c, 3)
	if err := c.Flush(context.Background()); err != nil {
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
	if err := c.Flush(context.Background()); err != nil {
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
	if err := c.Flush(context.Background()); err != nil {
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
//
// The record here fits by itself and is pushed over by the framing, which is
// what keeps this on the sender's check rather than Enqueue's: Enqueue judges
// the record, dispatch judges the finished body, and only the second one can
// see what framing and compression did to it.
func TestOversizeRecordIsDroppedBeforeSending(t *testing.T) {
	t.Parallel()

	enc := JSONArray() // frames a one-record batch to exactly one byte more
	rec := newRecorder(t)
	c, errs := newTestClient(t, rec,
		WithBatchSize(1),
		WithEncoder(enc),
		WithCompression(CompressionNone), // so the body size is predictable
		WithMaxBatchBytes(hardMaxRequestBytes*2),
	)
	defer c.Close()

	// Exactly at the limit: every added "x" is one more byte of JSON, so the
	// padding needed is the difference from an empty message.
	empty, err := enc.AppendRecord(nil, map[string]any{KeyMessage: "", KeyLevel: "INFO"})
	if err != nil {
		t.Fatalf("sizing the record: %v", err)
	}
	huge := strings.Repeat("x", hardMaxRequestBytes-len(empty))
	if err := c.Enqueue(map[string]any{KeyMessage: huge, KeyLevel: "INFO"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if got := c.Stats().DroppedOversize; got != 0 {
		t.Fatalf("Stats().DroppedOversize = %d before the flush, want 0: "+
			"Enqueue refused a record that fits, so this no longer tests the sender", got)
	}
	if err := c.Flush(context.Background()); err != nil {
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

// The same doomed record, refused a stage earlier. With compression off the
// encoded size is the deciding one, so Enqueue can tell it will never fit
// without the record taking a queue slot and forcing its capacity on the
// sender's accumulation buffer and the packer's scratch for the life of the
// client. The proof that it never got that far is that the count is already
// there with nothing yet flushed and the sender never woken.
func TestOversizeRecordIsRefusedAtEnqueue(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec,
		WithBatchSize(1000), // so nothing here would flush on its own
		WithCompression(CompressionNone),
		withDropReportInterval(20*time.Millisecond),
	)
	defer c.Close()

	huge := strings.Repeat("x", hardMaxRequestBytes+(1<<20))
	if err := c.Enqueue(map[string]any{KeyMessage: huge, KeyLevel: "INFO"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got := c.Stats().DroppedOversize; got != 1 {
		t.Errorf("Stats().DroppedOversize = %d immediately after Enqueue, want 1: "+
			"the record was queued rather than refused on the caller's goroutine", got)
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := rec.count(); got != 0 {
		t.Errorf("got %d requests, want 0: a doomed body was sent anyway", got)
	}
	if got := c.Stats().DroppedOversize; got != 1 {
		t.Errorf("Stats().DroppedOversize = %d after Flush, want 1: counted twice", got)
	}
	// Aggregated, not reported inline: a drop discovered on the caller's
	// goroutine goes through the summary like every other one, so the sender's
	// ticker is what delivers it.
	waitFor(t, "an oversize drop summary", func() bool {
		return hasDrop(errs, DropOversize)
	})
}

// Why that refusal is conditional on compression being off: gzipped, a record
// several times the request limit fits with room to spare, so judging one on
// its encoded size would throw away records the server is happy to take. This
// is the test that fails if the Enqueue check is ever made unconditional.
func TestCompressibleOversizeRecordStillShips(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(t, rec, WithBatchSize(1)) // gzip, the default
	defer c.Close()

	huge := strings.Repeat("x", hardMaxRequestBytes+(1<<20))
	if err := c.Enqueue(map[string]any{KeyMessage: huge, KeyLevel: "INFO"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := c.Stats().DroppedOversize; got != 0 {
		t.Errorf("Stats().DroppedOversize = %d, want 0: it compressed to well under the limit", got)
	}
	if got := c.Stats().Sent; got != 1 {
		t.Errorf("Stats().Sent = %d, want 1", got)
	}
	if got := errs.len(); got != 0 {
		t.Errorf("OnError fired %d times: %v", got, errs.all())
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
	if err := c.Flush(context.Background()); err != nil {
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

// A Retry-After longer than the ceiling ends the batch on its first attempt,
// rather than being honoured past the budget. The recorder answers 202 once its
// script runs out, so a client that took the wait anyway would deliver the
// batch on the retry — which is exactly what must not happen here.
func TestRetryAfterBeyondCeilingGivesUpAtOnce(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t,
		withStatuses(429),
		withResponseHeader("Retry-After", "1"),
	)
	c, errs := newTestClient(t, rec,
		WithBatchSize(1000),
		WithRetryBackoff(time.Millisecond), // irrelevant: Retry-After overrides it
		WithRetryCeiling(200*time.Millisecond),
	)
	defer c.Close()

	enqueueN(t, c, 3)
	if err := c.Flush(context.Background()); err == nil {
		t.Fatal("Flush = nil, want the batch to have been given up on")
	}

	if got := rec.count(); got != 1 {
		t.Errorf("got %d requests, want 1: the 1s Retry-After was waited out inside a 200ms ceiling", got)
	}
	if got := c.Stats().DroppedRejected; got != 3 {
		t.Errorf("Stats().DroppedRejected = %d, want 3", got)
	}
	reported := errs.all()
	if len(reported) == 0 {
		t.Fatal("OnError never fired for a batch given up on")
	}
	// The give-up message reports the requests actually made, not the number
	// the configuration would have permitted: the loop broke on the ceiling
	// after one.
	if got := reported[0].Error(); !strings.Contains(got, "after 1 attempt(s)") {
		t.Errorf("OnError got %q, want it to report 1 attempt", got)
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
