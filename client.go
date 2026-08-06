package betterstack

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Three invariants hold everything below together. They are stated once, here,
// because most of the code's shape is a consequence of them.
//
//  1. queue and flushC are NEVER closed. Termination is signalled by closing
//     the signal channels — done, shutdown, senderDone — which are only ever
//     received from. Send-on-closed-channel, otherwise the likeliest bug in a
//     library like this, is therefore impossible rather than defended against.
//     Nothing needs queue to be closed: the sender stops on shutdown, not on a
//     range loop running dry.
//
//  2. flushC is UNBUFFERED. A send on it can only succeed while the sender is
//     parked in its select, so once the sender has returned, Flush's
//     select{case flushC <- req; case <-senderDone} deterministically takes the
//     senderDone branch. A buffered flushC would let a request land in a
//     channel nobody will ever read, hanging Flush until its context expired.
//
//  3. Every buffer handed across a goroutine boundary is owned by the receiver.
//     The sender reuses both its accumulation buffer and the gzip output
//     buffer, so a dispatched batch carries its own copy. The library this one
//     replaces could hand its reused buffer straight to a send only because
//     that send was synchronous; with concurrent uploads the same code
//     silently corrupts in-flight request bodies.

// DefaultEndpoint is Better Stack's ingesting host.
const DefaultEndpoint = "https://in.logs.betterstack.com"

// Defaults for the client options, sourced from the sibling official clients.
// See PARITY.md §2 for where each number comes from.
const (
	defaultBatchSize       = 1000                   // JS and Java agree
	defaultBatchInterval   = time.Second            // JS; Java is 3s, 1s ships a lone line sooner
	defaultMaxBatchBytes   = 5 << 20                // conservative against the 10 MiB limit
	defaultMaxQueueSize    = 100_000                // Java maxQueueSize, drop over
	defaultMaxRetries      = 5                      // Java
	defaultRetryBackoff    = 300 * time.Millisecond // Java retrySleepMilliseconds
	defaultMaxInFlight     = 5                      // JS syncMax
	defaultTimeout         = 10 * time.Second       // Java readTimeout
	defaultConnectTimeout  = 5 * time.Second        // Java connectTimeout
	defaultShutdownTimeout = 15 * time.Second
)

const (
	// hardMaxRequestBytes is the ingestion API's documented per-request limit,
	// measured on the compressed body (PARITY §1). MaxBatchBytes is an
	// uncompressed assembly cap and so is far more conservative than this;
	// this is the backstop that stops a doomed request being sent at all.
	hardMaxRequestBytes = 10 << 20

	// dropReportInterval rate-limits drop summaries. Drops are aggregated
	// rather than reported per record because during an outage the per-record
	// shape is an error storm that is itself a denial of service.
	dropReportInterval = 5 * time.Second
)

// Compression selects the request body encoding.
type Compression int

const (
	// CompressionGzip compresses request bodies. It is the default: the 10 MiB
	// request limit is measured on compressed bytes, so this directly
	// multiplies throughput.
	CompressionGzip Compression = iota
	// CompressionNone sends request bodies uncompressed.
	CompressionNone
)

// Stats counts everything the client has done with the records handed to it.
//
// Once Close has returned, the counts satisfy
//
//	Enqueued == Sent + DroppedQueueFull + DroppedBacklog + DroppedRejected +
//	            DroppedOversize + DroppedClosed
//
// so no record is unaccounted for and no drop is silent.
//
// Records rejected by Enqueue with an encoding error are outside this
// accounting entirely: nothing was ever handed over, and the caller was told
// synchronously.
type Stats struct {
	// Enqueued counts records offered to Enqueue, including those refused
	// because the client was already closed.
	Enqueued uint64
	Sent     uint64 // acknowledged by the ingestion endpoint
	Retries  uint64 // upload attempts after the first

	DroppedQueueFull uint64 // the application outran the sender
	DroppedBacklog   uint64 // every upload slot busy and the hand-off full
	DroppedRejected  uint64 // terminal status, or the retry budget ran out
	DroppedOversize  uint64 // over the hard request size limit, or a 413
	DroppedClosed    uint64 // enqueued after Close, or still queued at Close
}

// counters is the atomic backing for Stats. atomic.Uint64 rather than bare
// uint64 with atomic.AddUint64, so there is no 64-bit alignment hazard on
// 32-bit platforms.
type counters struct {
	enqueued atomic.Uint64
	sent     atomic.Uint64
	retries  atomic.Uint64

	droppedQueueFull atomic.Uint64
	droppedBacklog   atomic.Uint64
	droppedRejected  atomic.Uint64
	droppedOversize  atomic.Uint64
	droppedClosed    atomic.Uint64
}

func (c *counters) snapshot() Stats {
	return Stats{
		Enqueued:         c.enqueued.Load(),
		Sent:             c.sent.Load(),
		Retries:          c.retries.Load(),
		DroppedQueueFull: c.droppedQueueFull.Load(),
		DroppedBacklog:   c.droppedBacklog.Load(),
		DroppedRejected:  c.droppedRejected.Load(),
		DroppedOversize:  c.droppedOversize.Load(),
		DroppedClosed:    c.droppedClosed.Load(),
	}
}

type clientConfig struct {
	endpoint        string
	sourceToken     string
	batchSize       int
	batchInterval   time.Duration
	maxBatchBytes   int
	maxQueueSize    int
	maxRetries      int
	retryBackoff    time.Duration
	maxInFlight     int
	timeout         time.Duration
	connectTimeout  time.Duration
	shutdownTimeout time.Duration
	compression     Compression
	encoder         Encoder
	onError         func(error)
	httpClient      *http.Client // set only by WithHTTPClient
}

// ClientOption configures a Client.
type ClientOption func(*clientConfig)

// WithEndpoint overrides the ingesting host. The default is DefaultEndpoint.
func WithEndpoint(endpoint string) ClientOption {
	return func(c *clientConfig) {
		if endpoint != "" {
			c.endpoint = endpoint
		}
	}
}

// WithBatchSize sets how many records accumulate before a batch is sent.
// Default 1000, matching the JavaScript and Java clients.
func WithBatchSize(n int) ClientOption {
	return func(c *clientConfig) { c.batchSize = n }
}

// WithBatchInterval sets how long a partial batch waits before being sent
// anyway. The timer starts when a batch takes its first record, so an idle
// client does no work. Default 1s.
func WithBatchInterval(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.batchInterval = d }
}

// WithMaxBatchBytes caps the uncompressed size of an accumulating batch,
// sending it early if the limit is reached. Default 5 MiB.
//
// This is an assembly and memory cap, not the API's request limit: the API
// limit of 10 MiB is measured on the compressed body, which log JSON reaches
// only far beyond this. Worst-case memory held outside the sender is roughly
// 2 × MaxInFlight batches of this size, before compression.
func WithMaxBatchBytes(n int) ClientOption {
	return func(c *clientConfig) { c.maxBatchBytes = n }
}

// WithMaxQueueSize bounds the queue between Enqueue and the sender, in records.
// Records offered when the queue is full are dropped and counted, never
// blocked: Handle runs in the calling application's critical path, so blocking
// there would turn a Better Stack outage into an application outage. Default
// 100000, matching the Java client.
func WithMaxQueueSize(n int) ClientOption {
	return func(c *clientConfig) { c.maxQueueSize = n }
}

// WithMaxRetries sets how many times a failed upload is retried after the
// initial attempt, so the default of 5 permits at most 6 requests and
// WithMaxRetries(0) means "send once, never retry". This matches the Java
// client's maxRetries.
//
// Only retryable failures are retried: 408, 429, 5xx and network errors. A
// rejected source token or an unparseable body is terminal, because retrying
// those burns quota forever without ever succeeding.
func WithMaxRetries(n int) ClientOption {
	return func(c *clientConfig) { c.maxRetries = n }
}

// WithRetryBackoff sets the base delay for retries, which grow exponentially
// with full jitter. A Retry-After response header overrides it. Default 300ms.
func WithRetryBackoff(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.retryBackoff = d }
}

// WithMaxInFlight caps concurrent uploads. Default 5, matching the JavaScript
// client's syncMax.
//
// The ingesting host speaks HTTP/2, so concurrent uploads multiplex over one
// TCP connection: concurrency costs streams, not sockets or handshakes.
func WithMaxInFlight(n int) ClientOption {
	return func(c *clientConfig) { c.maxInFlight = n }
}

// WithTimeout sets the timeout for a single upload attempt. Default 10s,
// matching the Java client's readTimeout. It is applied as a per-request
// context deadline and so is honoured by any HTTP client, including one
// supplied through WithHTTPClient.
func WithTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.timeout = d }
}

// WithConnectTimeout sets the TCP connect timeout. Default 5s, matching the
// Java client. It has no effect when WithHTTPClient is used, since the dialer
// belongs to the supplied client's transport.
func WithConnectTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.connectTimeout = d }
}

// WithShutdownTimeout bounds how long Close waits for queued and in-flight
// records. Default 15s.
func WithShutdownTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.shutdownTimeout = d }
}

// WithCompression selects the request body encoding. Default CompressionGzip.
func WithCompression(comp Compression) ClientOption {
	return func(c *clientConfig) { c.compression = comp }
}

// WithEncoder selects the body format. Default NDJSON.
func WithEncoder(enc Encoder) ClientOption {
	return func(c *clientConfig) {
		if enc != nil {
			c.encoder = enc
		}
	}
}

// WithOnError sets the callback for delivery failures — non-2xx responses,
// exhausted retries, and dropped records. It defaults to writing one line per
// event to stderr.
//
// The callback is invoked from the sender goroutine and from up to MaxInFlight
// upload workers, so it must be safe for concurrent use. It must not log
// through the slog handler this client backs: that recurses. A panic inside it
// is caught and reported to stderr rather than being allowed to take down the
// host process.
//
// Drops are delivered as aggregated *DropError summaries, not one call per
// lost record.
func WithOnError(f func(error)) ClientOption {
	return func(c *clientConfig) {
		if f != nil {
			c.onError = f
		}
	}
}

// WithHTTPClient supplies the HTTP client used for uploads, replacing the
// tuned one this package builds.
//
// The supplied client is not owned: its idle connections are left alone by
// Close. WithConnectTimeout has no effect when this is used; WithTimeout still
// applies, being a per-request context deadline.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *clientConfig) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// Client batches log records and delivers them to Better Stack.
//
// It owns everything with a lifecycle — the queue, batching, compression, HTTP,
// retry — and knows nothing about log/slog. Construct one, pass it to
// NewHandler, and close it before the process exits:
//
//	client, err := betterstack.NewClient(os.Getenv("BETTERSTACK_SOURCE_TOKEN"))
//	if err != nil {
//	        return err
//	}
//	defer client.Close()
//
//	logger := slog.New(betterstack.NewHandler(client))
//
// Close is not optional. Records are batched, so whatever is still accumulating
// when the process exits is lost unless it is flushed. Every official Better
// Stack client documents the same requirement.
//
// A Client is safe for concurrent use.
type Client struct {
	cfg clientConfig

	hc            *http.Client
	transport     *http.Transport // nil when the caller supplied the client
	ownsTransport bool
	userAgent     string

	queue  chan []byte    // invariant 1: never closed
	flushC chan *flushReq // invariants 1 and 2: unbuffered, never closed

	done       chan struct{} // closed first by Close: Enqueue starts refusing
	shutdown   chan struct{} // closed after the final flush: the sender exits
	senderDone chan struct{} // closed by the sender on its way out

	pool       *uploadPool
	workerCtx  context.Context
	cancelWork context.CancelFunc

	closeOnce sync.Once
	closeErr  error

	stats counters
}

type flushReq struct {
	ctx   context.Context
	reply chan error // buffered, capacity 1
}

// NewClient returns a Client delivering to Better Stack with the given source
// token.
//
// It returns an error rather than panicking on a missing token: a vendor SDK
// must not crash the host application over an unset environment variable.
func NewClient(sourceToken string, opts ...ClientOption) (*Client, error) {
	cfg := clientConfig{
		endpoint:        DefaultEndpoint,
		sourceToken:     sourceToken,
		batchSize:       defaultBatchSize,
		batchInterval:   defaultBatchInterval,
		maxBatchBytes:   defaultMaxBatchBytes,
		maxQueueSize:    defaultMaxQueueSize,
		maxRetries:      defaultMaxRetries,
		retryBackoff:    defaultRetryBackoff,
		maxInFlight:     defaultMaxInFlight,
		timeout:         defaultTimeout,
		connectTimeout:  defaultConnectTimeout,
		shutdownTimeout: defaultShutdownTimeout,
		compression:     CompressionGzip,
		encoder:         NDJSON(),
		onError:         defaultOnError,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	// Validate before starting anything, so a rejected configuration leaks no
	// goroutines. goleak enforces this.
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	c := &Client{
		cfg:        cfg,
		userAgent:  userAgent(),
		queue:      make(chan []byte, cfg.maxQueueSize),
		flushC:     make(chan *flushReq), // unbuffered: invariant 2
		done:       make(chan struct{}),
		shutdown:   make(chan struct{}),
		senderDone: make(chan struct{}),
	}

	if cfg.httpClient != nil {
		c.hc = cfg.httpClient
	} else {
		c.transport = newTransport(cfg)
		c.ownsTransport = true
		c.hc = &http.Client{Transport: c.transport}
	}

	c.workerCtx, c.cancelWork = context.WithCancel(context.Background())
	c.pool = newUploadPool(c)

	go newSender(c).run()

	return c, nil
}

func (cfg *clientConfig) validate() error {
	if strings.TrimSpace(cfg.sourceToken) == "" {
		return ErrNoSourceToken
	}
	u, err := url.Parse(cfg.endpoint)
	if err != nil {
		return fmt.Errorf("betterstack: invalid endpoint %q: %w", cfg.endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("betterstack: endpoint %q must be an http or https URL", cfg.endpoint)
	}
	if u.Host == "" {
		return fmt.Errorf("betterstack: endpoint %q has no host", cfg.endpoint)
	}

	for _, check := range []struct {
		name string
		got  int
		min  int
	}{
		{"WithBatchSize", cfg.batchSize, 1},
		{"WithMaxBatchBytes", cfg.maxBatchBytes, 1},
		{"WithMaxQueueSize", cfg.maxQueueSize, 1},
		{"WithMaxInFlight", cfg.maxInFlight, 1},
		{"WithMaxRetries", cfg.maxRetries, 0},
	} {
		if check.got < check.min {
			return fmt.Errorf("betterstack: %s(%d) must be >= %d", check.name, check.got, check.min)
		}
	}
	for _, check := range []struct {
		name string
		got  time.Duration
	}{
		{"WithBatchInterval", cfg.batchInterval},
		{"WithTimeout", cfg.timeout},
		{"WithShutdownTimeout", cfg.shutdownTimeout},
	} {
		if check.got <= 0 {
			return fmt.Errorf("betterstack: %s(%v) must be positive", check.name, check.got)
		}
	}
	if cfg.retryBackoff < 0 {
		return fmt.Errorf("betterstack: WithRetryBackoff(%v) must not be negative", cfg.retryBackoff)
	}
	return nil
}

// Enqueue encodes an event and queues it for delivery.
//
// It never blocks on the network and never blocks on a full queue: if the
// application is producing records faster than they can be shipped, the record
// is dropped and counted rather than stalling the caller.
//
// The returned error is local only — an encoding failure, or ErrClosed after
// Close. A dropped record is not an error return: doing that would report every
// drop through Handle, and so through any error-handling middleware, once per
// lost record, which is the error storm the aggregated reporting exists to
// avoid. Drops are counted in Stats and summarised through OnError.
func (c *Client) Enqueue(event map[string]any) error {
	select {
	case <-c.done:
		// Counted as both offered and dropped, so the accounting identity in
		// Stats holds: a record the caller handed over and that never reached
		// Better Stack is a drop, whether it was refused at the door or lost
		// later.
		c.stats.enqueued.Add(1)
		c.stats.droppedClosed.Add(1)
		return ErrClosed
	default:
	}

	// Encoding happens here, on the caller's goroutine. Encoding errors are
	// therefore synchronous and returnable; byte accounting downstream is exact
	// and free; and the queue carries []byte, so no record data is shared
	// between goroutines and there is nothing to alias.
	buf, err := c.cfg.encoder.AppendRecord(nil, 0, event)
	if err != nil {
		return fmt.Errorf("betterstack: encoding record: %w", err)
	}

	c.stats.enqueued.Add(1)
	select {
	case c.queue <- buf:
		return nil
	default:
		// Deliberately no <-c.done case here: with a default branch present,
		// both would be ready once done is closed and the choice would be
		// random. The check at the top of the function is the gate.
		c.stats.droppedQueueFull.Add(1)
		return nil
	}
}

// Flush delivers everything enqueued before the call and waits for it to be
// acknowledged.
//
// It returns the first delivery error observed since the previous Flush
// consumed one, and clears it, so a subsequent Flush over a healthy period
// returns nil. Records enqueued concurrently with the call may or may not be
// included.
//
// Flush returns ErrClosed if the client is already closed, and ctx.Err() if ctx
// expires first — it will not hang waiting on a sender that has gone away.
func (c *Client) Flush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	req := &flushReq{ctx: ctx, reply: make(chan error, 1)}

	select {
	case c.flushC <- req:
	case <-c.senderDone:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes and shuts the client down. It is idempotent and safe to call
// concurrently; every caller gets the same error.
//
// After it returns, Enqueue reports ErrClosed and counts the record as dropped.
// Close is the one place a delivery error can be surfaced meaningfully, because
// the caller has a stack to receive it — so it returns one, in addition to the
// final drop summary sent to OnError.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		// Step 1: stop accepting. done and shutdown must be separate channels;
		// one would stop the sender before it had drained the queue.
		close(c.done)

		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.shutdownTimeout)
		defer cancel()

		// Step 2: drain, upload, and wait for the in-flight uploads.
		c.closeErr = c.Flush(ctx)

		// Step 3: stop the sender, which closes the job channel and lets the
		// upload workers finish.
		close(c.shutdown)

		select {
		case <-c.senderDone:
		case <-ctx.Done():
			// The budget went on a retry backoff or a slow response. Abort the
			// in-flight uploads rather than outliving the timeout.
			c.cancelWork()
			<-c.senderDone
		}
		c.cancelWork() // idempotent; releases the context's resources

		if c.ownsTransport {
			// Retires net/http's per-connection read and write goroutines.
			// Without this, goleak fails and the tempting fix is to ignore
			// those frames, which would hide a client that never releases its
			// connections.
			c.transport.CloseIdleConnections()
		}

		// Anything that slipped into the queue between the done check in
		// Enqueue and now is accounted rather than silently lost.
		if leftover := len(c.queue); leftover > 0 {
			c.stats.droppedClosed.Add(uint64(leftover))
		}
		c.reportFinalDrops()
	})
	return c.closeErr
}

// Stats returns a snapshot of the client's counters. It is cheap and safe to
// call at any time, including after Close.
func (c *Client) Stats() Stats { return c.stats.snapshot() }

// batch is a completed, framed, optionally compressed request body together
// with the accounting needed to report on it.
type batch struct {
	body     []byte // owned by the batch: invariant 3
	records  int
	rawBytes int // uncompressed size, for diagnostics
}

// compressor wraps a reusable gzip.Writer and its output buffer.
//
// Reuse is safe only because compression happens on the sender goroutine, which
// is the single writer. Compressing there rather than in the upload workers is
// deliberate: it keeps that reuse valid, shrinks the memory held by queued
// batches, and means a retry re-sends bytes instead of recompressing them.
type compressor struct {
	buf *sliceWriter
	w   *gzip.Writer
}

func newCompressor() *compressor {
	sw := &sliceWriter{}
	// BestSpeed: log JSON still compresses several-fold at level 1, and CPU
	// inside the customer's process is the scarce resource, not bandwidth.
	w, err := gzip.NewWriterLevel(sw, gzip.BestSpeed)
	if err != nil {
		// Only reachable with an invalid level constant.
		panic(fmt.Sprintf("betterstack: gzip.NewWriterLevel: %v", err))
	}
	return &compressor{buf: sw, w: w}
}

// compress returns the gzip encoding of src. The result is only valid until the
// next call, so the caller must copy it before handing it on.
func (c *compressor) compress(src []byte) ([]byte, error) {
	c.buf.reset()
	c.w.Reset(c.buf)
	if _, err := c.w.Write(src); err != nil {
		return nil, err
	}
	if err := c.w.Close(); err != nil {
		return nil, err
	}
	return c.buf.b, nil
}

// sliceWriter is an io.Writer over a reusable byte slice. bytes.Buffer would do
// as well; this avoids the extra copy out of it.
type sliceWriter struct{ b []byte }

func (w *sliceWriter) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

func (w *sliceWriter) reset() { w.b = w.b[:0] }
