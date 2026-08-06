package betterstack

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testLimiter builds a limiter driven by an explicit clock instead of the wall
// one. A rate limiter tested against real time is either slow or flaky, and the
// suite refuses both; the returned function advances the clock.
func testLimiter(t *testing.T, maxRecords int, window time.Duration) (*limiter, func(time.Duration)) {
	t.Helper()

	var now atomic.Int64 // atomic: the concurrency test reads it from many goroutines
	l := newLimiter(maxRecords, window)
	l.now = now.Load

	return l, func(d time.Duration) { now.Add(int64(d)) }
}

func TestLimiterAdmitsAFullBurstThenThrottles(t *testing.T) {
	t.Parallel()

	const (
		maxRecords = 10
		window     = time.Second
		interval   = window / maxRecords
	)
	l, advance := testLimiter(t, maxRecords, window)

	for i := 0; i < maxRecords; i++ {
		if !l.allow() {
			t.Fatalf("refused record %d of a %d-record burst from a full bucket", i, maxRecords)
		}
	}
	if l.allow() {
		t.Fatalf("admitted record %d with a limit of %d per %v", maxRecords, maxRecords, window)
	}

	// The bucket refills at one record per interval, not in a lump at the end
	// of the window.
	advance(interval)
	if !l.allow() {
		t.Errorf("refused a record after %v, one interval's worth of refill", interval)
	}
	if l.allow() {
		t.Errorf("admitted two records after %v of refill; only one had accrued", interval)
	}

	// A full window idle restores the full burst.
	advance(window)
	for i := 0; i < maxRecords; i++ {
		if !l.allow() {
			t.Fatalf("refused record %d of the burst after idling a full window", i)
		}
	}
	if l.allow() {
		t.Error("admitted more than a full burst after idling")
	}
}

// A long idle period must not bank credit beyond one window's worth: the bucket
// has a capacity, and the arithmetic that forgives an old arrival time is where
// an unbounded one would hide.
func TestLimiterDoesNotBankIdleTime(t *testing.T) {
	t.Parallel()

	const maxRecords, window = 5, time.Second
	l, advance := testLimiter(t, maxRecords, window)

	advance(1000 * window)

	admitted := 0
	for i := 0; i < 10*maxRecords; i++ {
		if l.allow() {
			admitted++
		}
	}
	if admitted != maxRecords {
		t.Errorf("admitted %d records after idling 1000 windows, want exactly %d", admitted, maxRecords)
	}
}

// The limiter is called from every goroutine that logs, so its accounting has to
// be exact under contention, not merely approximate. With the clock frozen the
// answer is a single number and any lost or double-counted compare-and-swap
// shows up immediately.
func TestLimiterIsExactUnderConcurrency(t *testing.T) {
	t.Parallel()

	const (
		maxRecords   = 100
		goroutines   = 16
		perGoroutine = 200
	)
	l, _ := testLimiter(t, maxRecords, time.Second)

	var admitted atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				if l.allow() {
					admitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := admitted.Load(); got != maxRecords {
		t.Errorf("admitted %d of %d records, want exactly %d",
			got, goroutines*perGoroutine, maxRecords)
	}
}

// A limit finer than a nanosecond makes the emission interval zero. That must
// admit everything rather than divide by zero or refuse everything.
func TestLimiterWithSubNanosecondInterval(t *testing.T) {
	t.Parallel()

	l, _ := testLimiter(t, 1_000_000, time.Microsecond) // interval rounds to 0ns

	for i := 0; i < 10_000; i++ {
		if !l.allow() {
			t.Fatalf("refused record %d despite an interval of 0", i)
		}
	}
}

// The other end of the range: a window so long the emission interval alone runs
// off the end of the clock. The arithmetic must saturate rather than wrap,
// because a negative arrival time reads as an empty bucket on one call and a
// full one on the next.
func TestLimiterWithAnAbsurdlyLongWindow(t *testing.T) {
	t.Parallel()

	l, advance := testLimiter(t, 1, time.Duration(math.MaxInt64))

	if !l.allow() {
		t.Fatal("refused the first record from a full bucket")
	}
	for i := 0; i < 100; i++ {
		if l.allow() {
			t.Fatalf("admitted record %d against a limit of 1 per 292 years", i+1)
		}
		advance(time.Hour)
	}
}

// The saturating add in allow. Reaching it takes a window of centuries *and* a
// clock already halfway to the end of int64, so it is a guard rather than a
// path — but the property it guarantees is worth pinning: the arrival time
// never goes negative, because a negative one would read as an empty bucket on
// one call and a full one on the next.
func TestLimiterSaturatesRatherThanWrapping(t *testing.T) {
	t.Parallel()

	l, advance := testLimiter(t, 2, time.Duration(math.MaxInt64))
	advance(time.Duration(math.MaxInt64/2) + 2)

	for i := 0; i < 10; i++ {
		l.allow()
		if got := l.tat.Load(); got < 0 {
			t.Fatalf("arrival time wrapped to %d on call %d", got, i)
		}
	}
}

func TestLimiterUsesAMonotonicClockByDefault(t *testing.T) {
	t.Parallel()

	// The default clock starts at zero and only moves forward, so a limiter
	// built and used immediately sees a full bucket rather than a random
	// offset from the epoch.
	l := newLimiter(3, time.Hour)
	for i := 0; i < 3; i++ {
		if !l.allow() {
			t.Fatalf("refused record %d immediately after construction", i)
		}
	}
	if l.allow() {
		t.Error("admitted a fourth record against a limit of 3 per hour")
	}
}
