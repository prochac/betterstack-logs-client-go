package betterstack

import (
	"math"
	"sync/atomic"
	"time"
)

// limiter is the admission rate limit behind WithBurstProtection: at most max
// records per window, with a full window's worth admissible back to back.
//
// It is a token bucket kept as a single monotone timestamp rather than as a
// token count, which is what lets the whole thing be one atomic word. The
// state is the theoretical arrival time — the instant at which the record just
// admitted would have arrived had every record so far arrived exactly interval
// apart. How far that has run ahead of the clock is the bucket's fill level,
// and window is how far ahead it may run before the bucket is empty.
//
// Two properties matter here and neither is incidental:
//
//   - The refusal path does not write. A client being hammered — which is
//     precisely when this code runs — takes one atomic load per record and
//     contends on nothing. Only an admitted record does a compare-and-swap.
//   - tat only ever moves forward, so the compare-and-swap has no ABA hazard:
//     a value that compares equal to the one loaded is the same value, not a
//     later one that happens to coincide.
type limiter struct {
	tat atomic.Int64 // theoretical arrival time, nanoseconds on now's scale

	interval int64 // window / max: the sustained spacing between records
	window   int64 // how far tat may run ahead of now — the burst allowance

	// now reads a monotonic clock as nanoseconds since the limiter was built.
	// It is a field so tests can drive it directly: a rate limiter tested
	// against the wall clock is either slow or flaky, and the suite refuses
	// both.
	now func() int64
}

// newLimiter builds a limiter admitting maxRecords per window.
func newLimiter(maxRecords int, window time.Duration) *limiter {
	base := time.Now()
	return &limiter{
		// Integer division, so a maximum larger than the window in nanoseconds
		// gives an interval of 0 and the limiter admits everything. That is
		// the right answer for a limit finer than the clock can express, and
		// it is why this cannot divide by zero: newLimiter is only called with
		// both values positive (clientConfig.validate).
		interval: int64(window) / int64(maxRecords),
		window:   int64(window),
		now:      func() int64 { return int64(time.Since(base)) },
	}
}

// allow reports whether one record may be admitted, and consumes its budget if
// so. It is safe for concurrent use and never blocks.
func (l *limiter) allow() bool {
	now := l.now()
	for {
		old := l.tat.Load()

		// An idle client's tat has fallen behind the clock; the bucket is full
		// and refilling no further, so start from now rather than from a debt
		// that has already been forgiven. (max is a builtin as of Go 1.21,
		// which is this module's floor.)
		t := max(old, now)

		// The obvious form of this test is t+interval-window > now. Written as
		// a subtraction on each side instead, neither operand can overflow: t
		// is at least now and at most now+window, so the left side is between
		// 0 and window, and interval is window/max so the right side is never
		// negative. The refusal path — the one that runs during the burst this
		// exists to survive — is then a load, a compare and nothing else.
		if t-now > l.window-l.interval {
			// The arrival time is already a full window ahead of the clock:
			// the bucket is empty.
			return false
		}

		next := t + l.interval
		if next < t {
			// Only reachable for a window measured in centuries, where the
			// interval alone overshoots the end of the clock. Saturate rather
			// than wrap: a negative tat would read as a permanently empty
			// bucket on one call and a full one on the next.
			next = math.MaxInt64
		}
		if l.tat.CompareAndSwap(old, next) {
			return true
		}
		// Another goroutine took the slot we costed. Recompute against its
		// tat; now is deliberately not re-read, so a caller cannot be starved
		// by repeatedly losing the race and repeatedly paying for a later
		// clock reading.
	}
}
