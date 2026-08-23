package betterstack

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A gzip.Writer cannot fail on well-formed input: every error it returns comes
// from the writer underneath it. That is why compression failure needs a seam
// at all, and it is why one seam covers a whole family of untested code —
// compress's two error returns, pack's, both of split's, and, hanging off
// those, the sender's pack-failure and split-failure accounting and the
// worker's failure to split a batch the server called too large.
//
// Each of those paths returns pooled buffers, counts records as dropped, or
// both, so none of them is cosmetic: a mistake there breaks the Stats identity
// (invariant 10) or leaves two goroutines holding the same bytes (invariant 3).

var errBrokenSink = errors.New("test: the compressor's sink is broken")

// failingCompression fails a client's compressions from a chosen one onwards.
//
// It counts every compression the client performs, whichever packer performs
// it: the sender's, or the private one an upload worker builds the first time a
// 413 makes it split. That shared count is what lets a test say "the batch
// packs, and the split that follows does not" without knowing which goroutine
// did which.
type failingCompression struct {
	begun    atomic.Int64
	failFrom int64
}

// withFailingCompression makes the client's compressions fail from the
// failFrom'th onwards, counting from zero.
func withFailingCompression(failFrom int64) ClientOption {
	f := &failingCompression{failFrom: failFrom}
	return func(c *clientConfig) { c.newCompressSink = f.newSink }
}

func (f *failingCompression) newSink() compressSink { return &failingSink{f: f} }

// failingSink is a sliceWriter that refuses to accept the bytes of a failing
// compression. One sink belongs to one packer, and so to one goroutine; only
// the shared counter is touched from more than one.
type failingSink struct {
	sliceWriter
	f      *failingCompression
	broken bool
}

// reset is called once per compression, before anything is written, which is
// what makes it the place to decide whether this one fails.
func (s *failingSink) reset() {
	// Add returns the count including this compression, so the one just begun
	// is number count-1 and fails from failFrom onwards.
	s.broken = s.f.begun.Add(1) > s.f.failFrom
	s.sliceWriter.reset()
}

func (s *failingSink) Write(p []byte) (int, error) {
	if s.broken {
		return 0, errBrokenSink
	}
	return s.sliceWriter.Write(p)
}

// withHardMaxBytes shrinks the request size past which the sender splits a
// batch rather than sending it. See clientConfig.hardMaxBytes: it is the only
// way to reach the splitting paths without building ten-megabyte bodies.
func withHardMaxBytes(n int) ClientOption {
	return func(c *clientConfig) { c.hardMaxBytes = n }
}

// writeFailSink fails from a chosen Write onwards, which is how the two error
// returns inside compress are told apart: gzip writes its header from Write,
// and flushes the compressed block and the trailer from Close.
type writeFailSink struct {
	sliceWriter
	failFrom int
	writes   int
}

func (s *writeFailSink) Write(p []byte) (int, error) {
	s.writes++
	if s.writes > s.failFrom {
		return 0, errBrokenSink
	}
	return s.sliceWriter.Write(p)
}

// Both of compress's error returns surface the sink's error rather than
// swallowing it, which is what everything below depends on to notice at all.
func TestCompressSurfacesSinkFailure(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"message":"hello","level":"INFO"}`)

	// Nothing accepted: the failure lands on the header write, inside Write.
	t.Run("failing on the first write", func(t *testing.T) {
		t.Parallel()
		sink := &writeFailSink{}
		gz := newCompressor(func() compressSink { return sink })
		if _, err := gz.compress(payload); !errors.Is(err, errBrokenSink) {
			t.Fatalf("compress = %v, want %v", err, errBrokenSink)
		}
	})

	// One write accepted: gzip gets its header out and fails later, on the
	// flush or the trailer, both of which happen inside Close. If a future gzip
	// ever made only one write in total, compress would return nil here and
	// this test would say so rather than quietly stop covering Close.
	t.Run("failing after the first write", func(t *testing.T) {
		t.Parallel()
		sink := &writeFailSink{failFrom: 1}
		gz := newCompressor(func() compressSink { return sink })
		if _, err := gz.compress(payload); !errors.Is(err, errBrokenSink) {
			t.Fatalf("compress = %v, want %v", err, errBrokenSink)
		}
		if sink.writes < 2 {
			t.Errorf("the sink saw %d writes, want at least 2: "+
				"the failure was not the one Close makes", sink.writes)
		}
	})
}

// A batch that cannot be compressed is accounted and its buffers go back to the
// pool. The pool half is the reason this drives a sender directly rather than
// going through Enqueue: sync.Pool hands a buffer back to the goroutine that
// returned it far more readily than to any other, so only a flush on this
// goroutine can assert the return without depending on the scheduler.
func TestFlushPackFailureReturnsBuffers(t *testing.T) {
	t.Parallel()

	errs := &errorSink{}
	c, err := NewClient(
		testToken,
		WithDryRun(true), // no server: nothing here ever reaches a request
		WithBatchInterval(time.Hour),
		WithOnError(errs.add),
		withFailingCompression(0),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// A sender of this test's own, so that flush runs here rather than on the
	// client's sender goroutine. The client's own sender stays idle throughout:
	// nothing is enqueued, and its batch interval is an hour.
	s := newSender(c)
	defer s.timer.Stop()
	defer s.reportTicker.Stop()

	rec, err := c.cfg.encoder.AppendRecord(nil, event(0))
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}

	// A single round trip would be the whole test if sync.Pool guaranteed one,
	// and it does not: a GC may drop the item, and a goroutine moved to another
	// P cannot see what is sitting in the first one's private slot. Both are
	// rare and neither is systematic, so the question is asked repeatedly with
	// a fresh marker each time — an implementation that dropped the buffers on
	// this path would never hand any of them back, however many times it ran.
	const attempts = 10
	returned := false
	flushes := 0
	for i := 0; i < attempts && !returned; i++ {
		// Emptied first, so that a marker left behind by an earlier round
		// cannot stand in for this round's.
		drainBatchBufs(c)
		seed := &batchBufs{}
		c.batchBufPool.Put(seed)

		s.appendRecord(rec)
		s.flush(context.Background())
		flushes++

		returned = c.getBatchBufs() == seed
	}
	if !returned {
		t.Errorf("the buffers of a batch that failed to pack came back from the pool in none of %d attempts", attempts)
	}
	if got, want := c.Stats().DroppedRejected, uint64(flushes); got != want {
		t.Errorf("Stats().DroppedRejected = %d, want %d: one record per failed flush", got, want)
	}
	assertReported(t, errs, "compressing 1 record(s)")
}

// drainBatchBufs empties a client's batch buffer pool.
func drainBatchBufs(c *Client) {
	for {
		if c.batchBufPool.Get() == nil {
			return
		}
	}
}

// The same failure seen from the outside: the records are counted, nothing is
// sent, and the caller learns why.
func TestPackFailureIsReportedAndCounted(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, errs := newTestClient(
		t, rec,
		WithBatchSize(2),
		withFailingCompression(0),
	)
	defer c.Close()

	enqueueN(t, c, 2)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := rec.count(); got != 0 {
		t.Errorf("got %d requests, want 0: a batch that could not be compressed was sent anyway", got)
	}
	if got := c.Stats().DroppedRejected; got != 2 {
		t.Errorf("Stats().DroppedRejected = %d, want 2", got)
	}
	assertReported(t, errs, "compressing 2 record(s)")
	assertStatsBalance(t, c)
}

// A split that fails partway is the aliasing question invariants 3 and 4 exist
// for: one half may already be packed over the parent's raw buffer. The batch
// is released exactly once and every record in it is accounted, whichever of
// the two halves was the one that failed.
func TestSplitFailureIsReportedAndCounted(t *testing.T) {
	t.Parallel()

	// The batch packs (compression 0) and the split does not. Which half fails
	// picks out which of split's two pack calls returns the error; the outcome
	// must be the same either way.
	for _, tt := range []struct {
		name     string
		failFrom int64
	}{
		{"the first half fails to pack", 1},
		{"the second half fails to pack", 2},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := newRecorder(t)
			c, errs := newTestClient(
				t, rec,
				WithBatchSize(2),
				withHardMaxBytes(1), // every body is over the limit, so every batch splits
				withFailingCompression(tt.failFrom),
			)
			defer c.Close()

			enqueueN(t, c, 2)
			if err := c.Flush(context.Background()); err != nil {
				t.Fatalf("Flush: %v", err)
			}

			if got := rec.count(); got != 0 {
				t.Errorf("got %d requests, want 0: a half that failed to pack was sent anyway", got)
			}
			if got := c.Stats().DroppedRejected; got != 2 {
				t.Errorf("Stats().DroppedRejected = %d, want 2", got)
			}
			assertReported(t, errs, "splitting 2 record(s)")
			assertStatsBalance(t, c)
		})
	}
}

// The worker's copy of the same thing, on the path only a 413 reaches: the
// server refuses the body, the worker builds a packer of its own to halve it,
// and that packer is the one that fails.
func TestSplitFailureAfter413IsReportedAndCounted(t *testing.T) {
	t.Parallel()

	// 1 byte accepted, so the first request is a 413 whatever it contains.
	rec := newRecorder(t, withMaxAcceptedBytes(1))
	c, errs := newTestClient(
		t, rec,
		WithBatchSize(2),
		WithMaxInFlight(1), // one worker, so one packer builds after the 413
		// The sender's own pack is compression 0 and must succeed, or the batch
		// never reaches the server to be refused. The worker's first — the left
		// half of the split — is compression 1.
		withFailingCompression(1),
	)
	defer c.Close()

	enqueueN(t, c, 2)
	err := c.Flush(context.Background())
	if err == nil {
		t.Fatal("Flush reported success although the batch was lost to a failed split")
	}
	if !errors.Is(err, errBrokenSink) {
		t.Errorf("Flush = %v, want it to carry %v", err, errBrokenSink)
	}

	if got := c.Stats().DroppedRejected; got != 2 {
		t.Errorf("Stats().DroppedRejected = %d, want 2", got)
	}
	if got := c.Stats().Sent; got != 0 {
		t.Errorf("Stats().Sent = %d, want 0", got)
	}
	assertReported(t, errs, "after a 413")
	assertStatsBalance(t, c)
}

// assertReported fails unless some error reached OnError carrying both the
// sink's failure and the context the client added to it.
func assertReported(t *testing.T, errs *errorSink, want string) {
	t.Helper()
	for _, err := range errs.all() {
		if errors.Is(err, errBrokenSink) && strings.Contains(err.Error(), want) {
			return
		}
	}
	t.Errorf("no reported error wrapped %v and mentioned %q; got %v", errBrokenSink, want, errs.all())
}
