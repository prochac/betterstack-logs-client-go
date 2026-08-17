package betterstack

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// ErrClosed is returned by Enqueue and Flush after Close has been called.
var ErrClosed = errors.New("betterstack: client is closed")

// ErrNoSourceToken is returned by NewClient when the source token is empty.
// A vendor SDK must not crash the host application over an unset environment
// variable, so this is an error rather than a panic.
var ErrNoSourceToken = errors.New("betterstack: source token is empty")

// StatusError reports a non-2xx response from the ingestion endpoint.
//
// Callers can branch on StatusCode to distinguish a misconfiguration (401, 403:
// bad source token) from a quota problem (402) from a client-side bug (406).
type StatusError struct {
	// StatusCode is the HTTP status code of the response.
	StatusCode int
	// Body is a prefix of the response body, truncated to a few KiB. It is
	// included because the endpoint reports the reason there, e.g.
	// {"error": "Unauthorized"}.
	Body string
	// Records is the number of log records the rejected request carried.
	Records int
	// Retryable reports whether the client will retry this status.
	Retryable bool
}

func (e *StatusError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "betterstack: ingest returned %d for %d record(s)", e.StatusCode, e.Records)
	if hint := statusHint(e.StatusCode); hint != "" {
		b.WriteString(": ")
		b.WriteString(hint)
	}
	if e.Body != "" {
		fmt.Fprintf(&b, ": %s", strings.TrimSpace(e.Body))
	}
	return b.String()
}

// statusHint turns the status codes documented in PARITY.md §1 into the action
// the operator has to take. The endpoint's own body is terse ({"error":
// "Unauthorized"}), and a logging client's errors are read by someone who is
// not looking at our source.
func statusHint(code int) string {
	switch code {
	case 401, 403:
		// The docs name 403 for a bad token, but the live endpoint answers 401
		// with {"error": "Unauthorized"} for both a missing and a bogus token
		// (PARITY §1, probed 2026-08-06). Both are handled, both are terminal.
		return "the source token was rejected; check the token passed to NewClient"
	case 402:
		return "quota exceeded"
	case 406:
		return "the endpoint could not parse the request body; this is a bug in this client"
	case 413:
		return "request over the size limit; lower WithMaxBatchBytes"
	default:
		return ""
	}
}

// DropReason explains why records were discarded without reaching Better Stack.
type DropReason int

const (
	// DropQueueFull means the application produced records faster than the
	// sender could batch them.
	DropQueueFull DropReason = iota
	// DropBacklog means every upload slot was busy and the hand-off to the
	// uploaders was full for as long as a caller's Flush context allowed, so a
	// completed batch could not be dispatched. A batch lost the same way during
	// shutdown is DropClosed, not this.
	DropBacklog
	// DropRejected means the endpoint terminally rejected the batch, or the
	// retry budget was exhausted.
	DropRejected
	// DropOversize means the request exceeded the hard size limit.
	DropOversize
	// DropClosed means the records never left because the client shut down:
	// enqueued after Close, still queued when Close returned, or in flight —
	// waiting for an upload slot, sleeping in a retry backoff, or mid-request —
	// when the shutdown deadline expired.
	DropClosed
	// DropBurst means the records were over the WithBurstProtection rate
	// limit. Appended last: the constants are iota-based, so inserting one
	// would silently renumber the reasons a caller has already stored.
	DropBurst
)

func (r DropReason) String() string {
	switch r {
	case DropQueueFull:
		return "queue full"
	case DropBacklog:
		return "upload backlog full"
	case DropRejected:
		return "rejected by ingest"
	case DropOversize:
		return "over the size limit"
	case DropClosed:
		return "client closed"
	case DropBurst:
		return "over the burst limit"
	default:
		return fmt.Sprintf("DropReason(%d)", int(r))
	}
}

// DropError reports records discarded without reaching Better Stack.
//
// It is delivered to OnError, never returned from Enqueue: drops are aggregated
// into periodic summaries rather than reported per record, because during an
// outage a per-record callback is itself a denial of service.
type DropError struct {
	Records int
	Reason  DropReason
}

func (e *DropError) Error() string {
	return fmt.Sprintf("betterstack: dropped %d record(s): %s", e.Records, e.Reason)
}

// defaultOnError writes one line to stderr. It is deliberately not the standard
// logger and never slog: reporting a logging failure through the logger being
// configured recurses.
func defaultOnError(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
}

// safeReport delivers err to an OnError callback, containing any panic it
// raises.
//
// OnError is called from the sender goroutine and from up to MaxInFlight upload
// workers, so it must be safe for concurrent use. The recover is not defensive
// tidiness: on the sender goroutine an unrecovered panic in user code takes
// down the entire host process, which is not an acceptable failure mode for the
// error-reporting path of a logging library.
func safeReport(onError func(error), err error) {
	if err == nil || onError == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "betterstack: OnError panicked: %v\n", r)
		}
	}()
	onError(err)
}
