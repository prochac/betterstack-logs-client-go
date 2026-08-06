package betterstack

import (
	"errors"
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

// countingEncoder proves the Encoder interface is actually pluggable, and that
// AppendRecord receives the record's index within the batch — which is what a
// JSON-array or MessagePack framing needs and what a bare marshaller cannot
// express.
type countingEncoder struct{}

func (countingEncoder) ContentType() string { return "application/x-test" }

func (countingEncoder) AppendRecord(dst []byte, index int, _ map[string]any) ([]byte, error) {
	return append(dst, []byte(strings.Repeat("x", index+1)+"\n")...), nil
}

func (countingEncoder) Frame(batch []byte, _ int) []byte { return batch }

func TestEncoderReceivesRecordIndex(t *testing.T) {
	t.Parallel()

	enc := countingEncoder{}
	var buf []byte
	for i := 0; i < 3; i++ {
		var err error
		if buf, err = enc.AppendRecord(buf, i, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := string(buf), "x\nxx\nxxx\n"; got != want {
		t.Errorf("got %q, want %q: the index was not threaded through", got, want)
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

// A batch over the hard request limit is dropped before it is sent: the server
// would reject it, so spending a request and a retry budget to find that out
// wastes the customer's quota and delays everything behind it.
func TestOversizeBatchIsDroppedBeforeSending(t *testing.T) {
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

func TestErrorValueNil(t *testing.T) {
	t.Parallel()
	if got := errorValue(nil); got != nil {
		t.Errorf("errorValue(nil) = %v, want nil", got)
	}
}
