package betterstack

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// sender owns batch assembly. Every field here is touched by exactly one
// goroutine, which is why none of them are synchronised and why the gzip writer
// can be reused across batches.
type sender struct {
	c *Client

	buf []byte // accumulated encoded records
	n   int    // records in buf

	timer *time.Timer
	// armed is true exactly when the timer is running and its fire value has
	// not yet been consumed. It is what makes the Stop/Reset dance below sound
	// without the usual "drain the channel, unless it was already drained"
	// guesswork.
	armed bool

	gz *compressor

	lastDropReport time.Time
	reported       dropSnapshot
}

type dropSnapshot struct {
	queueFull, backlog, rejected, oversize, closed uint64
}

func newSender(c *Client) *sender {
	s := &sender{
		c:              c,
		buf:            make([]byte, 0, 64<<10),
		timer:          newStoppedTimer(),
		lastDropReport: time.Now(),
	}
	if c.cfg.compression == CompressionGzip {
		s.gz = newCompressor()
	}
	return s
}

// newStoppedTimer returns a timer that is stopped with an empty channel.
//
// Creating it already-fired and draining once is safe here because nothing else
// exists yet that could receive from the channel.
func newStoppedTimer() *time.Timer {
	t := time.NewTimer(time.Hour)
	if !t.Stop() {
		<-t.C
	}
	return t
}

// arm starts the batch interval. It is called only on the empty-to-non-empty
// transition, so its precondition — timer stopped, channel empty — is exactly
// the state in which Reset is documented to be correct.
func (s *sender) arm() {
	if s.armed {
		return
	}
	s.timer.Reset(s.c.cfg.batchInterval)
	s.armed = true
}

// disarm stops the batch interval before a flush triggered by anything other
// than the timer itself.
func (s *sender) disarm() {
	if !s.armed {
		return
	}
	if !s.timer.Stop() {
		// Stop reports false and armed was true, so the fire value has not been
		// consumed and no other goroutine can consume it: the runtime's
		// non-blocking send into the capacity-1 channel must have succeeded.
		// This receive cannot block.
		<-s.timer.C
	}
	s.armed = false
}

func (s *sender) run() {
	defer close(s.c.senderDone)
	defer s.c.pool.shutdown()
	defer s.timer.Stop()

	for {
		select {
		case rec := <-s.c.queue:
			s.appendRecord(rec)
			if s.full() {
				s.disarm()
				s.flush(s.c.workerCtx)
			}

		case <-s.timer.C:
			// Consume the fire before deciding anything: from here on the
			// timer is stopped with an empty channel.
			s.armed = false
			if s.n > 0 {
				s.flush(s.c.workerCtx)
			}
			// A fire on an empty batch is a no-op. It happens when a flush
			// raced the timer, and is cheaper to tolerate than to prevent.

		case req := <-s.c.flushC:
			s.drain(req.ctx)
			s.disarm()
			s.flush(req.ctx)
			// Register interest in the uploads dispatched so far and go back to
			// the select rather than blocking here, so Enqueue keeps being
			// serviced for the whole duration of a slow flush.
			s.c.pool.await(req.reply)

		case <-s.c.shutdown:
			s.drain(s.c.workerCtx)
			s.disarm()
			s.flush(s.c.workerCtx)
			return
		}
	}
}

func (s *sender) full() bool {
	return s.n >= s.c.cfg.batchSize || len(s.buf) >= s.c.cfg.maxBatchBytes
}

func (s *sender) appendRecord(rec []byte) {
	s.buf = append(s.buf, rec...)
	s.n++
	if s.n == 1 {
		s.arm()
	}
}

// drain moves everything currently buffered in the queue into the batch,
// dispatching whenever a size threshold is crossed. It returns as soon as the
// queue is momentarily empty.
//
// This is what gives Flush its guarantee: a record whose Enqueue returned
// before the flush request was sent is necessarily in the queue by the time the
// sender receives that request, and so is necessarily picked up here.
func (s *sender) drain(ctx context.Context) {
	for {
		select {
		case rec := <-s.c.queue:
			s.appendRecord(rec)
			if s.full() {
				s.flush(ctx)
			}
		default:
			return
		}
	}
}

// flush completes the current batch and hands it to the upload pool.
//
// ctx bounds the wait for an upload slot: a caller's Flush context for a flush
// they asked for, the client's worker context otherwise, so that Close can
// unwedge a sender parked on a stalled upload.
func (s *sender) flush(ctx context.Context) {
	if s.n == 0 {
		return
	}

	records := s.n
	body := s.c.cfg.encoder.Frame(s.buf, records)
	raw := len(body)

	if s.gz != nil {
		compressed, err := s.gz.compress(body)
		if err != nil {
			s.c.report(fmt.Errorf("betterstack: compressing %d record(s): %w", records, err))
			s.c.stats.droppedRejected.Add(uint64(records))
			s.reset()
			return
		}
		body = compressed
	}

	// Invariant 3: both s.buf and the compressor's output buffer are reused, so
	// the batch takes its own copy before crossing to an upload worker.
	b := &batch{
		body:     append([]byte(nil), body...),
		records:  records,
		rawBytes: raw,
	}
	s.reset()

	if len(b.body) > hardMaxRequestBytes {
		// Doomed: the server would reject it. Drop it here with an accurate
		// count rather than spending a request and a retry budget finding out.
		s.c.stats.droppedOversize.Add(uint64(b.records))
		s.c.report(&DropError{Records: b.records, Reason: DropOversize})
		s.maybeReportDrops()
		return
	}

	s.c.pool.dispatch(ctx, b)
	s.maybeReportDrops()
}

func (s *sender) reset() {
	s.buf = s.buf[:0]
	s.n = 0
}

// maybeReportDrops emits an aggregated summary at most once per interval.
//
// It is called at flush points rather than per record, both to keep time.Now
// out of the hot path and because a summary is only interesting once a batch
// boundary has passed.
func (s *sender) maybeReportDrops() {
	if time.Since(s.lastDropReport) < dropReportInterval {
		return
	}
	s.lastDropReport = time.Now()
	s.reportDrops()
}

// reportDrops sends one *DropError per reason for everything dropped since the
// last report. Counts, never individual records: during an outage a per-record
// callback is itself a denial of service.
func (s *sender) reportDrops() {
	cur := dropSnapshot{
		queueFull: s.c.stats.droppedQueueFull.Load(),
		backlog:   s.c.stats.droppedBacklog.Load(),
		rejected:  s.c.stats.droppedRejected.Load(),
		oversize:  s.c.stats.droppedOversize.Load(),
		closed:    s.c.stats.droppedClosed.Load(),
	}

	for _, d := range []struct {
		reason    DropReason
		cur, prev uint64
	}{
		{DropQueueFull, cur.queueFull, s.reported.queueFull},
		{DropBacklog, cur.backlog, s.reported.backlog},
		{DropOversize, cur.oversize, s.reported.oversize},
		{DropClosed, cur.closed, s.reported.closed},
	} {
		if delta := d.cur - d.prev; delta > 0 {
			s.c.report(&DropError{Records: int(delta), Reason: d.reason})
		}
	}
	// DropRejected is deliberately not summarised here: every rejection has
	// already been reported individually as a *StatusError carrying the status
	// code and the server's explanation, which is strictly more useful.
	s.reported = cur
}

// uploadPool runs at most MaxInFlight uploads concurrently and lets Flush wait
// for the ones it is responsible for.
type uploadPool struct {
	c    *Client
	jobs chan *batch
	wg   sync.WaitGroup

	mu sync.Mutex
	// dispatched and completed are monotone. A flush records the dispatched
	// count at the moment it finishes handing over its batches, and waits for
	// completed to reach it.
	//
	// A sync.WaitGroup cannot express this: it cannot be reused across flush
	// generations without racing Add against Wait, and a goroutine per flush
	// blocked in Wait would break goleak-clean shutdown.
	dispatched uint64
	completed  uint64
	err        error // first delivery error since a waiter last took one
	waiters    []*flushWaiter
	closed     bool
}

type flushWaiter struct {
	target uint64
	reply  chan error // capacity 1
}

func newUploadPool(c *Client) *uploadPool {
	p := &uploadPool{
		c: c,
		// Buffered, not unbuffered. With an unbuffered channel a non-blocking
		// send succeeds only if a worker is already parked in its receive, so a
		// batch would be dropped in the microseconds between a worker
		// finishing an upload and looping round. Buffering makes "the send
		// failed" mean unambiguously: every worker is busy and every hand-off
		// slot is taken.
		jobs: make(chan *batch, c.cfg.maxInFlight),
	}
	p.wg.Add(c.cfg.maxInFlight)
	for i := 0; i < c.cfg.maxInFlight; i++ {
		go func(seed int64) {
			defer p.wg.Done()
			// A per-worker source: no global mutex, and no dependence on
			// process-global seeding. math/rand/v2 is Go 1.22, out of reach.
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() ^ seed))
			for b := range p.jobs {
				p.complete(c.upload(c.workerCtx, b, rnd))
			}
		}(int64(i) * 7919)
	}
	return p
}

// dispatch hands a completed batch to the workers, waiting for a slot.
//
// It blocks, and that is deliberate. The tempting alternative — drop the batch
// when every worker is busy, so that assembly never stalls — throws records
// away during an ordinary burst: fill the queue faster than MaxInFlight
// uploads can drain it, which any application does when it logs a thousand
// lines at once, and whole assembled batches evaporate while the server is
// perfectly healthy.
//
// Blocking here instead propagates backpressure to the queue, which is the one
// place records are meant to be shed: it is explicitly sized by the caller
// through WithMaxQueueSize, it drops with an accurate count, and Enqueue still
// never blocks the application. Assembling batches that have nowhere to go
// would only move unbounded memory from the queue, where it is bounded and
// accounted, to a backlog where it is neither.
//
// Called only from the sender goroutine, which is what makes incrementing
// dispatched after the send safe: no waiter can register in between.
func (p *uploadPool) dispatch(ctx context.Context, b *batch) {
	select {
	case p.jobs <- b:
		p.recordDispatch()
	case <-ctx.Done():
		// Only reachable at shutdown, or when a caller's Flush context
		// expires. The batch is lost; account for it.
		p.c.stats.droppedBacklog.Add(uint64(b.records))
	}
}

func (p *uploadPool) recordDispatch() {
	p.mu.Lock()
	p.dispatched++
	p.mu.Unlock()
}

// complete records the outcome of one upload and wakes any waiter it satisfies.
func (p *uploadPool) complete(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completed++
	if err != nil && p.err == nil {
		p.err = err
	}
	p.signalLocked()
}

// await registers a waiter for everything dispatched so far, replying on ch.
// Called only from the sender goroutine.
func (p *uploadPool) await(reply chan error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.completed >= p.dispatched || p.closed {
		reply <- p.takeErrLocked()
		return
	}
	p.waiters = append(p.waiters, &flushWaiter{target: p.dispatched, reply: reply})
}

func (p *uploadPool) signalLocked() {
	if len(p.waiters) == 0 {
		return
	}
	kept := p.waiters[:0]
	for _, w := range p.waiters {
		if p.completed >= w.target || p.closed {
			// Capacity 1 and one send per waiter, so this never blocks while
			// holding the lock.
			w.reply <- p.takeErrLocked()
		} else {
			kept = append(kept, w)
		}
	}
	p.waiters = kept
}

// takeErrLocked returns the first error since the last caller took one, and
// clears it, so a later flush over a healthy period reports success.
func (p *uploadPool) takeErrLocked() error {
	err := p.err
	p.err = nil
	return err
}

// shutdown stops the workers. Called from the sender's defer, so the sender —
// the only goroutine that sends on jobs — has already left its loop.
func (p *uploadPool) shutdown() {
	close(p.jobs)
	p.wg.Wait()

	p.mu.Lock()
	p.closed = true
	p.signalLocked() // release any waiter that will never be satisfied now
	p.mu.Unlock()
}

// report delivers err to the configured callback, containing any panic.
func (c *Client) report(err error) { safeReport(c.cfg.onError, err) }

// reportFinalDrops emits the closing summary, unconditionally: by this point
// the sender has exited, so nothing else will.
func (c *Client) reportFinalDrops() {
	s := c.stats.snapshot()
	for _, d := range []struct {
		reason DropReason
		n      uint64
	}{
		{DropQueueFull, s.DroppedQueueFull},
		{DropBacklog, s.DroppedBacklog},
		{DropOversize, s.DroppedOversize},
		{DropClosed, s.DroppedClosed},
	} {
		if d.n > 0 {
			c.report(&DropError{Records: int(d.n), Reason: d.reason})
		}
	}
}
