package betterstack

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"time"
)

const (
	// retryCeiling bounds the total time spent on one batch across all
	// attempts, independently of MaxRetries. OpenTelemetry's OTLP exporter uses
	// the same minute. Promoted to an option in a later milestone.
	retryCeiling = 60 * time.Second
	// maxBackoff caps the exponential growth.
	maxBackoff = 30 * time.Second
	// maxRetryAfter caps what a Retry-After header can ask for, so a hostile or
	// mistaken value cannot stall shutdown.
	maxRetryAfter = 60 * time.Second
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
func newTransport(cfg clientConfig) *http.Transport {
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

// upload delivers one batch, owning the entire retry budget for it.
//
// It returns only when the batch has been accepted, terminally rejected, run
// out of budget, or the client is shutting down.
func (c *Client) upload(ctx context.Context, b *batch, rnd *rand.Rand) error {
	deadline := time.Now().Add(retryCeiling)

	var (
		lastErr    error
		retryAfter time.Duration
	)

	for attempt := 0; attempt <= c.cfg.maxRetries; attempt++ {
		if attempt > 0 {
			delay := backoff(c.cfg.retryBackoff, attempt, retryAfter, rnd)
			if time.Now().Add(delay).After(deadline) {
				break
			}
			if err := sleepCtx(ctx, delay); err != nil {
				// Shutting down. The batch is lost; account for it below.
				c.stats.droppedRejected.Add(uint64(b.records))
				return err
			}
			c.stats.retries.Add(1)
		}
		retryAfter = 0

		status, hdrRetryAfter, body, err := c.do(ctx, b)

		switch {
		case err != nil:
			if ctx.Err() != nil {
				// The parent context is done, so this is shutdown rather than
				// a per-attempt timeout. Checking the parent, rather than
				// inspecting the error, is what reliably distinguishes them.
				c.stats.droppedRejected.Add(uint64(b.records))
				return err
			}
			lastErr = err

		case status/100 == 2:
			c.stats.sent.Add(uint64(b.records))
			return nil

		default:
			retryable := isRetryable(status)
			se := &StatusError{
				StatusCode: status,
				Body:       body,
				Records:    b.records,
				Retryable:  retryable,
			}
			if !retryable {
				if status == http.StatusRequestEntityTooLarge {
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
		b.records, c.cfg.maxRetries+1, lastErr)
	c.report(err)
	return err
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

	if resp.StatusCode/100 != 2 {
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
// token, but the live host answers 401 (PARITY §1, probed 2026-08-06). A client
// that retried anything it did not recognise would put the single most common
// misconfiguration into an infinite retry loop, burning the customer's quota
// forever without ever succeeding.
//
// Terminal, and each for a reason no amount of waiting changes: 401 and 403
// (the token is wrong), 402 (out of quota), 406 (we sent something unparseable
// — our bug), 413 (the body is too big; splitting it is a later milestone).
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
