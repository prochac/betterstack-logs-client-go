package betterstack

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"time"
)

const (
	// maxBackoff caps the exponential growth.
	maxBackoff = 30 * time.Second
	// maxRetryAfter caps what a Retry-After header can ask for, so a hostile or
	// mistaken value cannot stall shutdown.
	maxRetryAfter = 60 * time.Second
	// maxDurationSeconds is the largest whole second a time.Duration holds.
	maxDurationSeconds = int64(math.MaxInt64) / int64(time.Second)
	// bodySnippetBytes is how much of an error response is kept for the error
	// message.
	bodySnippetBytes = 4 << 10
	// bodyDrainBytes is how much of a response body is read before closing, to
	// make the connection reusable.
	bodyDrainBytes = 64 << 10
)

// newTransport builds the HTTP transport used when the caller has not supplied
// their own client.
//
// Four details here silently defeat connection reuse if missed, and each costs
// a full TCP and TLS handshake per batch when it is.
//
// One thing is deliberately absent: an HTTP/2 health-check ping. A connection
// killed silently by a NAT or a load balancer while idle is not discovered
// until something is written into it, so the first upload after a quiet spell
// fails and spends one attempt out of that batch's retry budget before the
// redial succeeds. The standard library does expose the knob —
// Transport.HTTP2.SendPingTimeout, added in Go 1.24 — but go.mod's floor is
// 1.21, so reaching it means raising the floor or carrying a second
// build-tagged pair of files, to pre-empt a network error the retry loop
// already classifies as retryable and absorbs.
func newTransport(cfg *clientConfig) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cfg.connectTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns: cfg.maxInFlight,
		// Defaults to 2 (http.DefaultMaxIdleConnsPerHost). All traffic here
		// goes to one host, so with the default and MaxInFlight of 5, three
		// connections would be closed after every flush and re-handshaked on
		// the next one.
		MaxIdleConnsPerHost: cfg.maxInFlight,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		// Load-bearing, not a hint. Go only auto-configures HTTP/2 on a
		// Transport it recognises as unmodified, and setting DialContext —
		// which WithConnectTimeout requires — disqualifies it, after which the
		// transport silently falls back to HTTP/1.1. The ingesting host speaks
		// h2, where all in-flight uploads multiplex over one connection.
		ForceAttemptHTTP2: true,
	}
}

// worker is one upload goroutine's private state.
//
// It exists for the packer. Splitting a batch after a 413 means framing and
// compressing again, off the sender goroutine, and a gzip.Writer cannot be
// shared — so each worker gets its own, built the first time it actually needs
// one. Most workers, in most processes, never build one at all.
type worker struct {
	c   *Client
	rnd *rand.Rand
	pk  *packer
}

func (w *worker) packer() *packer {
	if w.pk == nil {
		w.pk = newPacker(w.c.cfg.encoder, w.c.cfg.compression)
	}
	return w.pk
}

// upload delivers one batch, owning the entire retry budget for it.
//
// It returns only when the batch has been accepted, terminally rejected, run
// out of budget, or the client is shutting down.
func (w *worker) upload(ctx context.Context, b *batch) error {
	if w.c.cfg.dryRun {
		// The kill switch. Everything up to here has run for real — conversion,
		// encoding, batching, framing, compression — which is what makes a dry
		// run worth anything as a test of the pipeline.
		w.c.stats.sent.Add(uint64(b.records))
		return nil
	}
	return w.uploadBy(ctx, b, time.Now().Add(w.c.cfg.retryCeiling))
}

// uploadBy is upload against an already-established deadline. A batch produced
// by splitting inherits its parent's, so that halving cannot buy more time than
// the original batch was granted.
func (w *worker) uploadBy(ctx context.Context, b *batch, deadline time.Time) error {
	c := w.c
	rnd := w.rnd

	var (
		lastErr    error
		retryAfter time.Duration
		// Requests actually made, which is not maxRetries+1 whenever the loop
		// breaks early on the deadline check below.
		attempts int
	)

	for attempt := 0; attempt <= c.cfg.maxRetries; attempt++ {
		if attempt > 0 {
			delay := backoff(c.cfg.retryBackoff, attempt, retryAfter, rnd)
			if time.Now().Add(delay).After(deadline) {
				// The wait is not taken at all if it would end past the
				// deadline, which is what makes a long Retry-After terminal
				// rather than slow: at the default ceiling, a first-attempt
				// 429 asking for 60s gives up right here. See WithRetryCeiling
				// for why that is the wanted answer.
				break
			}
			if err := sleepCtx(ctx, delay); err != nil {
				// Shutting down: ctx here is always the client's worker
				// context, which only Close cancels. Nothing rejected this
				// batch, so it is a shutdown casualty and counted DropClosed —
				// DropRejected would send whoever reads the counters after an
				// unclean shutdown looking for a problem at the ingest end.
				c.stats.droppedClosed.Add(uint64(b.records))
				return err
			}
			c.stats.retries.Add(1)
		}
		retryAfter = 0

		status, hdrRetryAfter, body, err := c.do(ctx, b)
		attempts++

		switch {
		case err != nil:
			if ctx.Err() != nil {
				// The parent context is done, so this is shutdown rather than
				// a per-attempt timeout. Checking the parent, rather than
				// inspecting the error, is what reliably distinguishes them.
				// A shutdown casualty, not a rejection — see the backoff branch
				// above.
				c.stats.droppedClosed.Add(uint64(b.records))
				return err
			}
			lastErr = err

		case status/100 == 2: // 2xx
			c.stats.sent.Add(uint64(b.records))
			return nil

		default:
			// 413 is not retryable — the same bytes would fail again — but it
			// is recoverable, which is a different thing. Half the batch may
			// well fit. This is the only case the local size check cannot
			// pre-empt: MaxBatchBytes is measured before compression and the
			// server's limit applies after it, so how much fits is not
			// knowable until the server says.
			//
			// Checked before the StatusError is built: this path never reports
			// one, since nothing was given up on.
			if status == http.StatusRequestEntityTooLarge && b.records > 1 {
				return w.splitAndSend(ctx, b, deadline)
			}

			retryable := isRetryable(status)
			se := &StatusError{
				StatusCode: status,
				Body:       body,
				Records:    b.records,
				Retryable:  retryable,
			}

			if !retryable {
				if status == http.StatusRequestEntityTooLarge {
					// One record, too large on its own. Nothing to split.
					c.stats.droppedOversize.Add(uint64(b.records))
				} else {
					c.stats.droppedRejected.Add(uint64(b.records))
				}
				c.report(se)
				return se
			}
			lastErr, retryAfter = se, hdrRetryAfter
		}
	}

	c.stats.droppedRejected.Add(uint64(b.records))
	err := fmt.Errorf("betterstack: giving up on %d record(s) after %d attempt(s): %w",
		b.records, attempts, lastErr)
	c.report(err)
	return err
}

// splitAndSend halves a batch the server called too large and delivers both
// pieces, recursively, until each is small enough or is down to the single
// record that cannot be split further.
//
// Each half restarts the attempt count but inherits the parent's deadline, so
// the recursion — at most log2(BatchSize) deep — is bounded in wall-clock time
// by RetryCeiling however many times it splits.
//
// The two halves go out one after the other on this worker rather than back
// through the pool. Handing them to the pool would be a deadlock: the pool's
// dispatch blocks when every worker is busy, and this worker, being one of the
// busy ones, would be waiting on itself.
func (w *worker) splitAndSend(ctx context.Context, b *batch, deadline time.Time) error {
	left, right, err := w.packer().split(b)
	if err != nil {
		w.c.stats.droppedRejected.Add(uint64(b.records))
		err = fmt.Errorf("betterstack: splitting %d record(s) after a 413: %w", b.records, err)
		w.c.report(err)
		return err
	}

	// Both halves are attempted whatever the first one does: they are separate
	// batches now, and one failing is no reason to abandon the other.
	errLeft := w.uploadBy(ctx, left, deadline)
	errRight := w.uploadBy(ctx, right, deadline)
	if errLeft != nil {
		return errLeft
	}
	return errRight
}

// do performs one upload attempt.
func (c *Client) do(ctx context.Context, b *batch) (status int, retryAfter time.Duration, body string, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.timeout)
	defer cancel()

	// bytes.NewReader rather than bytes.NewBuffer, so ContentLength and GetBody
	// are populated and the body survives a redirect.
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.cfg.endpoint, bytes.NewReader(b.body))
	if err != nil {
		return 0, 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.sourceToken)
	req.Header.Set("Content-Type", c.cfg.encoder.ContentType())
	req.Header.Set("User-Agent", c.userAgent)
	if c.cfg.compression == CompressionGzip {
		req.Header.Set("Content-Encoding", "gzip")
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, 0, "", err
	}
	// Drain before closing, on every path including the error returns above
	// this point in the response handling. A body closed while unread makes
	// Transport discard the connection instead of returning it to the pool, so
	// keep-alive silently stops working — invisible to any test that only
	// asserts status codes. The deferred form is what makes this structural
	// rather than a thing to remember.
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, bodyDrainBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode/100 != 2 { // !2xx
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, bodySnippetBytes))
		body = string(snippet)
	}
	return resp.StatusCode, parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), body, nil
}

// isRetryable classifies a response status.
//
// The rule is terminal by default: retry only what is explicitly retryable, and
// drop everything else. That matters because the documented status list is not
// what the endpoint actually returns — the docs name 403 for a bad source
// token, but the live host answers 401 (observed 2026-08-06). A client
// that retried anything it did not recognise would put the single most common
// misconfiguration into an infinite retry loop, burning the customer's quota
// forever without ever succeeding.
//
// Terminal, and each for a reason no amount of waiting changes: 401 and 403
// (the token is wrong), 402 (out of quota), 406 (we sent something unparseable
// — our bug), 413 (the body is too big, so the same bytes can only fail again;
// a batch of more than one record is split and resent instead of retried).
func isRetryable(status int) bool {
	switch {
	case status == http.StatusRequestTimeout, // 408
		status == http.StatusTooManyRequests: // 429
		return true
	case status >= 500 && status <= 599:
		return true
	default:
		return false
	}
}

// backoff returns the delay before the given attempt, counting from 1.
//
// Exponential from base with full jitter. A server-supplied Retry-After wins
// over local configuration outright — the server knows when it will be ready
// and we do not.
func backoff(base time.Duration, attempt int, retryAfter time.Duration, rnd *rand.Rand) time.Duration {
	if retryAfter > 0 {
		if retryAfter > maxRetryAfter {
			return maxRetryAfter
		}
		return retryAfter
	}
	if base <= 0 {
		return 0
	}

	d := base
	// A loop rather than a shift: at attempt 63 a shift overflows into a
	// negative duration, and a negative sleep is an instant retry storm.
	for i := 1; i < attempt && d < maxBackoff; i++ {
		d *= 2
	}
	if d <= 0 || d > maxBackoff {
		d = maxBackoff
	}
	return time.Duration(rnd.Int63n(int64(d) + 1))
}

// parseRetryAfter reads a Retry-After header in either documented form: delay
// seconds, or an HTTP date. Anything else, or a date already in the past,
// yields zero, meaning "use the computed backoff".
func parseRetryAfter(h string, now time.Time) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil {
		if secs <= 0 {
			return 0
		}
		// Saturate rather than multiply into an overflow. Past ~292 years the
		// product wraps negative, and a negative duration reads to backoff as
		// "no Retry-After at all" — an instant retry where the server asked for
		// the longest wait it could express. Anything this far out is nonsense
		// either way, and backoff caps every value at maxRetryAfter, so the
		// clamp only keeps the arithmetic honest on the way there.
		if int64(secs) > maxDurationSeconds {
			return time.Duration(math.MaxInt64)
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// sleepCtx waits for d, or returns early if ctx is done. The timer is always
// stopped, so a cancelled shutdown leaves nothing pending.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
