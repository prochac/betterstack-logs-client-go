package betterstack

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"
)

// --- classification ---------------------------------------------------------

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status int
		want   bool
		why    string
	}{
		// The docs name 403 for a rejected source token, but the live endpoint
		// answers 401. Both must be terminal: retrying a bad token burns quota
		// forever and can never succeed.
		{401, false, "rejected source token, as actually returned"},
		{403, false, "rejected source token, as documented"},
		{402, false, "quota exceeded"},
		{406, false, "unparseable body: our bug, not a transient one"},
		{413, false, "too large: the same bytes will fail again"},
		{400, false, "terminal by default"},
		{404, false, "terminal by default"},
		{418, false, "unknown 4xx is terminal by default"},

		{408, true, "request timeout"},
		{429, true, "rate limited"},
		{500, true, "server error"},
		{502, true, "bad gateway"},
		{503, true, "unavailable"},
		{599, true, "top of the 5xx range"},
	}

	for _, tt := range tests {
		if got := isRetryable(tt.status); got != tt.want {
			t.Errorf("isRetryable(%d) = %v, want %v (%s)", tt.status, got, tt.want, tt.why)
		}
	}
}

// --- backoff ----------------------------------------------------------------

func TestBackoffFullJitter(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(1))
	base := 300 * time.Millisecond

	for attempt := 1; attempt <= 64; attempt++ {
		// The ceiling for this attempt, computed the same way but without
		// jitter.
		ceiling := base
		for i := 1; i < attempt && ceiling < maxBackoff; i++ {
			ceiling *= 2
		}
		if ceiling > maxBackoff {
			ceiling = maxBackoff
		}

		for i := 0; i < 200; i++ {
			d := backoff(base, attempt, 0, rnd)
			if d < 0 {
				// At attempt 63 a shift-based implementation overflows into a
				// negative duration, which turns backoff into an instant
				// retry storm.
				t.Fatalf("backoff(attempt=%d) = %v, negative", attempt, d)
			}
			if d > ceiling {
				t.Fatalf("backoff(attempt=%d) = %v, over the ceiling %v", attempt, d, ceiling)
			}
		}
	}
}

func TestBackoffGrows(t *testing.T) {
	t.Parallel()

	// With full jitter any single sample can be near zero, so growth is only
	// observable in the maximum over many samples.
	rnd := rand.New(rand.NewSource(2))
	base := 100 * time.Millisecond

	maxOf := func(attempt int) time.Duration {
		var m time.Duration
		for i := 0; i < 500; i++ {
			if d := backoff(base, attempt, 0, rnd); d > m {
				m = d
			}
		}
		return m
	}
	if a, b := maxOf(1), maxOf(4); b <= a {
		t.Errorf("backoff is not growing: attempt 1 peaked at %v, attempt 4 at %v", a, b)
	}
}

// The server knows when it will be ready and we do not, so Retry-After wins.
func TestBackoffRetryAfterOverrides(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(3))
	if got := backoff(time.Second, 5, 2*time.Second, rnd); got != 2*time.Second {
		t.Errorf("backoff with Retry-After = %v, want 2s", got)
	}
	// A hostile or mistaken header must not be able to stall shutdown.
	if got := backoff(time.Second, 1, 24*time.Hour, rnd); got != maxRetryAfter {
		t.Errorf("backoff with an absurd Retry-After = %v, want it capped at %v", got, maxRetryAfter)
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"5", 5 * time.Second},
		{"0", 0},
		{"-1", 0},
		{"not-a-number", 0},
		{now.Add(30 * time.Second).Format(http.TimeFormat), 30 * time.Second},
		{now.Add(-30 * time.Second).Format(http.TimeFormat), 0}, // already past
	}
	for _, tt := range tests {
		if got := parseRetryAfter(tt.in, now); got != tt.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestSleepCtx(t *testing.T) {
	t.Parallel()

	t.Run("returns early on cancellation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := sleepCtx(ctx, time.Hour); !errors.Is(err, context.Canceled) {
			t.Errorf("sleepCtx = %v, want context.Canceled", err)
		}
	})

	t.Run("zero duration on a live context", func(t *testing.T) {
		t.Parallel()
		if err := sleepCtx(context.Background(), 0); err != nil {
			t.Errorf("sleepCtx(0) = %v, want nil", err)
		}
	})
}

// --- retry behaviour --------------------------------------------------------

func TestRetryThenSuccess(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withStatuses(500, 502, 503))
	c, _ := newTestClient(t, rec,
		WithBatchSize(1000),
		WithRetryBackoff(time.Millisecond),
	)
	defer c.Close()

	enqueueN(t, c, 3)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := rec.count(); got != 4 {
		t.Errorf("got %d requests, want 4 (three failures then success)", got)
	}
	if got := c.Stats().Retries; got != 3 {
		t.Errorf("Stats().Retries = %d, want 3", got)
	}
	if got := c.Stats().Sent; got != 3 {
		t.Errorf("Stats().Sent = %d, want 3", got)
	}

	// Every attempt must carry byte-identical bytes: a retry re-sends, it does
	// not re-encode or re-compress.
	reqs := rec.all()
	for i := 1; i < len(reqs); i++ {
		if !bytes.Equal(reqs[0].body, reqs[i].body) {
			t.Errorf("attempt %d sent a different body from attempt 0", i)
		}
	}
}

func TestTerminalStatusesAreNotRetried(t *testing.T) {
	t.Parallel()

	// 413 is absent deliberately: it is terminal in the sense that the same
	// bytes are never resent, but the records are not abandoned — they go out
	// as two smaller batches. See TestPayloadTooLargeSplitsTheBatch.
	for _, status := range []int{400, 401, 402, 403, 406, 404} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()

			rec := newRecorder(t, withStatuses(status, status, status, status, status, status, status))
			c, errs := newTestClient(t, rec,
				WithBatchSize(1000),
				WithRetryBackoff(time.Millisecond),
			)
			defer c.Close()

			enqueueN(t, c, 2)
			err := c.Flush(context.Background())

			if got := rec.count(); got != 1 {
				t.Errorf("got %d requests, want exactly 1: %d was retried", got, status)
			}
			if got := c.Stats().Retries; got != 0 {
				t.Errorf("Stats().Retries = %d, want 0", got)
			}

			var se *StatusError
			if !errors.As(err, &se) || se.StatusCode != status {
				t.Fatalf("Flush error = %v, want a *StatusError with %d", err, status)
			}
			if se.Retryable {
				t.Error("StatusError.Retryable is true for a terminal status")
			}
			if errs.len() != 1 {
				t.Errorf("OnError fired %d times, want exactly 1", errs.len())
			}
		})
	}
}

// A 413 splits the batch and resends both halves. The same bytes are never
// retried — that would fail identically — but the records are not lost either.
func TestPayloadTooLargeSplitsTheBatch(t *testing.T) {
	t.Parallel()

	// Only the first request is refused, so each half is accepted on its own.
	rec := newRecorder(t, withStatuses(413))
	c, errs := newTestClient(t, rec, WithBatchSize(1000))
	defer c.Close()

	enqueueN(t, c, 4)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := c.Stats().Sent; got != 4 {
		t.Errorf("Stats().Sent = %d, want 4: the split halves did not all land", got)
	}
	if got := c.Stats().DroppedOversize; got != 0 {
		t.Errorf("Stats().DroppedOversize = %d, want 0: nothing was too large to split", got)
	}
	if got := c.Stats().Retries; got != 0 {
		t.Errorf("Stats().Retries = %d, want 0: a split is not a retry", got)
	}
	if got := rec.count(); got != 3 {
		t.Errorf("got %d requests, want 3 (one refused, two halves)", got)
	}
	if got := errs.len(); got != 0 {
		t.Errorf("OnError fired %d times for a recovered 413: %v", got, errs.all())
	}

	// Every record must arrive exactly once: splitting must not duplicate the
	// record on the boundary, nor lose it.
	seen := map[string]int{}
	for _, m := range rec.accepted() {
		seen[m[KeyMessage].(string)]++
	}
	if len(seen) != 4 {
		t.Errorf("got %d distinct records, want 4: %v", len(seen), seen)
	}
	for msg, n := range seen {
		if n != 1 {
			t.Errorf("record %q delivered %d times, want 1", msg, n)
		}
	}
}

// Splitting has to converge: the client keeps halving until the pieces fit,
// against a server that answers on size rather than to a script.
func TestPayloadTooLargeSplitsUntilItFits(t *testing.T) {
	t.Parallel()

	const records = 16
	rec := newRecorder(t)
	c, errs := newTestClient(t, rec, WithBatchSize(1000), WithCompression(CompressionNone))
	defer c.Close()

	// Sized from a real record so the limit forces several rounds of halving
	// rather than one.
	probe, err := NDJSON().AppendRecord(nil, event(0))
	if err != nil {
		t.Fatalf("sizing a record: %v", err)
	}
	rec.mu.Lock()
	rec.maxBytes = len(probe) * 3
	rec.mu.Unlock()

	enqueueN(t, c, records)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := c.Stats().Sent; got != records {
		t.Errorf("Stats().Sent = %d, want %d", got, records)
	}
	if got := c.Stats().DroppedOversize; got != 0 {
		t.Errorf("Stats().DroppedOversize = %d, want 0", got)
	}
	if got := errs.len(); got != 0 {
		t.Errorf("OnError fired %d times: %v", got, errs.all())
	}
	if got := len(rec.accepted()); got != records {
		t.Errorf("%d records arrived, want %d", got, records)
	}
}

// Splitting re-frames each half from the original record bytes, so it is the
// encoder's framing that has to survive it — and a JSON array is the framing
// where getting that wrong produces a body the server cannot parse rather than
// one that is merely wrong. Run against both compression settings, since the
// packer's buffer handling differs between them.
func TestPayloadTooLargeSplitsWithJSONArray(t *testing.T) {
	t.Parallel()

	for _, comp := range []struct {
		name string
		opt  Compression
	}{{"gzip", CompressionGzip}, {"none", CompressionNone}} {
		comp := comp
		t.Run(comp.name, func(t *testing.T) {
			t.Parallel()

			const records = 16
			probe, err := JSONArray().AppendRecord(nil, event(0))
			if err != nil {
				t.Fatalf("sizing a record: %v", err)
			}

			rec := newRecorder(t, withMaxAcceptedBytes(len(probe)*3))
			c, errs := newTestClient(t, rec,
				WithBatchSize(1000),
				WithEncoder(JSONArray()),
				WithCompression(comp.opt),
			)
			defer c.Close()

			enqueueN(t, c, records)
			if err := c.Flush(context.Background()); err != nil {
				t.Fatalf("Flush: %v", err)
			}

			// The recorder has already failed the test if any body — including
			// the refused ones — was not a valid JSON array of objects.
			if got := c.Stats().Sent; got != records {
				t.Errorf("Stats().Sent = %d, want %d", got, records)
			}
			if got := len(rec.accepted()); got != records {
				t.Errorf("%d records arrived, want %d", got, records)
			}
			if got := errs.len(); got != 0 {
				t.Errorf("OnError fired %d times: %v", got, errs.all())
			}

			seen := map[string]int{}
			for _, m := range rec.accepted() {
				seen[m[KeyMessage].(string)]++
			}
			if len(seen) != records {
				t.Errorf("got %d distinct records, want %d", len(seen), records)
			}
		})
	}
}

// Splitting bottoms out. Against a server that refuses everything on size, the
// batch is halved all the way down to single records, each of which is then
// dropped and accounted — rather than looping, or losing the count on the way.
func TestPayloadTooLargeSplitsDownToNothing(t *testing.T) {
	t.Parallel()

	const records = 8
	rec := newRecorder(t, withMaxAcceptedBytes(1))
	c, errs := newTestClient(t, rec, WithBatchSize(1000), WithCompression(CompressionNone))
	defer c.Close()

	enqueueN(t, c, records)
	err := c.Flush(context.Background())

	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != 413 {
		t.Fatalf("Flush error = %v, want a *StatusError with 413", err)
	}
	if got := c.Stats().Sent; got != 0 {
		t.Errorf("Stats().Sent = %d, want 0", got)
	}
	if got := c.Stats().DroppedOversize; got != records {
		t.Errorf("Stats().DroppedOversize = %d, want %d", got, records)
	}
	if got := c.Stats().DroppedRejected; got != 0 {
		t.Errorf("Stats().DroppedRejected = %d, want 0", got)
	}
	// One report per unsplittable record, and no more: the halving itself is
	// silent, so the user hears about the eight records, not about the fifteen
	// requests it took to isolate them.
	if got := errs.len(); got != records {
		t.Errorf("OnError fired %d times, want %d: %v", got, records, errs.all())
	}
}

// A single record the server will not take cannot be made smaller. It is
// dropped, counted as oversize, and reported once — naming the knob that fixes
// it, since nothing else in the message would tell the user what to do.
func TestPayloadTooLargeSingleRecordIsDropped(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withStatuses(413))
	c, errs := newTestClient(t, rec, WithBatchSize(1))
	defer c.Close()

	enqueueN(t, c, 1)
	err := c.Flush(context.Background())

	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != 413 {
		t.Fatalf("Flush error = %v, want a *StatusError with 413", err)
	}
	if got := c.Stats().DroppedOversize; got != 1 {
		t.Errorf("Stats().DroppedOversize = %d, want 1", got)
	}
	if got := c.Stats().DroppedRejected; got != 0 {
		t.Errorf("Stats().DroppedRejected = %d, want 0: a 413 is an oversize drop", got)
	}
	if got := rec.count(); got != 1 {
		t.Errorf("got %d requests, want 1: an unsplittable record was resent", got)
	}
	found := false
	for _, e := range errs.all() {
		if strings.Contains(e.Error(), "WithMaxBatchBytes") {
			found = true
		}
	}
	if !found {
		t.Errorf("no error named WithMaxBatchBytes: %v", errs.all())
	}
}

func TestRetryExhaustion(t *testing.T) {
	t.Parallel()

	// Always fails: more scripted failures than the client will ever attempt.
	rec := newRecorder(t, withStatuses(503, 503, 503, 503, 503, 503, 503, 503))
	c, errs := newTestClient(t, rec,
		WithBatchSize(1000),
		WithMaxRetries(3),
		WithRetryBackoff(time.Millisecond),
	)
	defer c.Close()

	enqueueN(t, c, 5)
	err := c.Flush(context.Background())

	// MaxRetries counts retries after the initial attempt: 3 means 4 requests.
	if got := rec.count(); got != 4 {
		t.Errorf("got %d requests, want 4 (1 initial + 3 retries)", got)
	}
	if got := c.Stats().Retries; got != 3 {
		t.Errorf("Stats().Retries = %d, want 3", got)
	}
	if got := c.Stats().DroppedRejected; got != 5 {
		t.Errorf("Stats().DroppedRejected = %d, want 5", got)
	}
	if err == nil {
		t.Error("Flush returned nil after exhausting retries")
	}
	if errs.len() == 0 {
		t.Error("OnError never fired for the exhausted batch")
	}
}

// WithMaxRetries(0) means send once and never retry.
func TestMaxRetriesZero(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withStatuses(503, 503))
	c, _ := newTestClient(t, rec, WithBatchSize(1000), WithMaxRetries(0))
	defer c.Close()

	enqueueN(t, c, 1)
	_ = c.Flush(context.Background())

	if got := rec.count(); got != 1 {
		t.Errorf("got %d requests, want exactly 1", got)
	}
}

func TestRetryAfterIsHonoured(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t,
		withStatuses(429),
		withResponseHeader("Retry-After", "1"),
	)
	c, _ := newTestClient(t, rec,
		WithBatchSize(1000),
		WithRetryBackoff(time.Millisecond), // would be far faster without the header
	)
	defer c.Close()

	enqueueN(t, c, 1)
	start := time.Now()
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	elapsed := time.Since(start)

	// A lower bound only. Full jitter makes upper bounds unsound, but
	// Retry-After is not jittered, so the wait must be at least the header.
	if elapsed < time.Second {
		t.Errorf("retried after %v, want at least the 1s from Retry-After", elapsed)
	}
	if got := rec.count(); got != 2 {
		t.Errorf("got %d requests, want 2", got)
	}
}

// A network-level failure is retryable. Pointing the client at a closed port
// produces a connection error on every attempt.
func TestNetworkErrorIsRetried(t *testing.T) {
	t.Parallel()

	c, errs := func() (*Client, *errorSink) {
		errs := &errorSink{}
		// A port nothing is listening on.
		cl, err := NewClient(testToken,
			WithEndpoint("http://127.0.0.1:1"),
			WithBatchInterval(time.Hour),
			WithBatchSize(1000),
			WithMaxRetries(2),
			WithRetryBackoff(time.Millisecond),
			WithShutdownTimeout(2*time.Second),
			WithOnError(errs.add),
		)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		return cl, errs
	}()
	defer c.Close()

	enqueueN(t, c, 1)
	if err := c.Flush(context.Background()); err == nil {
		t.Error("Flush returned nil despite the endpoint being unreachable")
	}

	if got := c.Stats().Retries; got != 2 {
		t.Errorf("Stats().Retries = %d, want 2", got)
	}
	if got := c.Stats().DroppedRejected; got != 1 {
		t.Errorf("Stats().DroppedRejected = %d, want 1", got)
	}
	if errs.len() == 0 {
		t.Error("OnError never fired")
	}
}

// --- wire format ------------------------------------------------------------

func TestGzipRoundTrip(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000))
	defer c.Close()

	enqueueN(t, c, 10)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	req := rec.all()[0]
	if got := req.header.Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want %q", got, "gzip")
	}
	// The recorder decompresses and parses; reaching here with 10 records means
	// the round trip worked.
	if got := len(req.records); got != 10 {
		t.Errorf("got %d records after decompression, want 10", got)
	}
}

func TestCompressionNone(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000), WithCompression(CompressionNone))
	defer c.Close()

	enqueueN(t, c, 3)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	req := rec.all()[0]
	if got := req.header.Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding = %q, want it absent", got)
	}
	if got := len(req.records); got != 3 {
		t.Errorf("got %d records, want 3", got)
	}
}

func TestRequestHeaders(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000))
	defer c.Close()

	enqueueN(t, c, 1)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	h := rec.all()[0].header
	if got, want := h.Get("Content-Type"), "application/x-ndjson"; got != want {
		t.Errorf("Content-Type = %q, want %q", got, want)
	}
	if got := h.Get("User-Agent"); !strings.HasPrefix(got, clientName+"/") {
		t.Errorf("User-Agent = %q, want the %s/<version> convention", got, clientName)
	}
	// Authorization is asserted for every request by the recorder itself.
}

// --- connection reuse -------------------------------------------------------

// Without keep-alive every batch costs a fresh TCP and TLS handshake. This is
// invisible to any test that only asserts status codes, which is exactly why it
// is asserted directly.
func TestConnectionReuse(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000), WithMaxInFlight(1))
	defer c.Close()

	for i := 0; i < 5; i++ {
		enqueueN(t, c, 1)
		if err := c.Flush(context.Background()); err != nil {
			t.Fatalf("Flush %d: %v", i, err)
		}
	}

	if got := rec.count(); got != 5 {
		t.Fatalf("got %d requests, want 5", got)
	}
	if got := rec.connections(); got != 1 {
		t.Errorf("opened %d connections for 5 sequential flushes, want 1", got)
	}
}

// The same, across error responses: an early return that skips the body drain
// leaks the connection just as effectively as never closing it.
func TestConnectionReuseAcrossErrors(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withStatuses(500, 500))
	c, _ := newTestClient(t, rec,
		WithBatchSize(1000),
		WithMaxInFlight(1),
		WithRetryBackoff(time.Millisecond),
	)
	defer c.Close()

	enqueueN(t, c, 1)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := rec.count(); got != 3 {
		t.Fatalf("got %d requests, want 3", got)
	}
	if got := rec.connections(); got != 1 {
		t.Errorf("opened %d connections, want 1: an error path is not draining the body", got)
	}
}

func TestTransportTuning(t *testing.T) {
	t.Parallel()

	cfg := clientConfig{maxInFlight: 7, connectTimeout: 5 * time.Second}
	tr := newTransport(cfg)

	// Go only auto-configures HTTP/2 on a Transport it recognises as
	// unmodified, and setting DialContext — which WithConnectTimeout requires —
	// disqualifies it. Without this flag the client silently drops to HTTP/1.1
	// against a host that speaks h2.
	if !tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 is false; HTTP/2 will be silently disabled")
	}
	// Defaults to 2. All traffic goes to one host, so the default would close
	// and re-handshake connections after every flush.
	if got := tr.MaxIdleConnsPerHost; got != cfg.maxInFlight {
		t.Errorf("MaxIdleConnsPerHost = %d, want %d", got, cfg.maxInFlight)
	}
	if tr.Proxy == nil {
		t.Error("Proxy is nil; the environment's proxy settings will be ignored")
	}
	if tr.DialContext == nil {
		t.Error("DialContext is nil; WithConnectTimeout has no effect")
	}
}

// A caller-supplied client is not ours to tear down.
func TestWithHTTPClientIsNotOwned(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	hc := &http.Client{}
	c, err := NewClient(testToken,
		WithEndpoint(rec.endpoint()),
		WithBatchInterval(time.Hour),
		WithBatchSize(1000),
		WithHTTPClient(hc),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	enqueueN(t, c, 1)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if c.ownsTransport {
		t.Error("the client claims ownership of a caller-supplied http.Client")
	}
	if got := rec.count(); got != 1 {
		t.Errorf("got %d requests, want 1", got)
	}
}

// Shutdown must abort an upload parked in a retry backoff rather than outliving
// the shutdown budget.
func TestShutdownCancelsInFlightUploads(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, withDelay(10*time.Second))
	c, _ := newTestClient(t, rec,
		WithBatchSize(1000),
		WithTimeout(30*time.Second),
		WithShutdownTimeout(150*time.Millisecond),
	)

	enqueueN(t, c, 1)

	start := time.Now()
	err := c.Close()
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Close returned nil despite never delivering")
	}
	if elapsed > 3*time.Second {
		t.Errorf("Close took %v; in-flight uploads were not cancelled", elapsed)
	}
}
