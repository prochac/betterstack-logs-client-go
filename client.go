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
	defaultRetryCeiling    = 60 * time.Second // OpenTelemetry otlploghttp
)

const (
	// hardMaxRequestBytes is the ingestion API's documented per-request limit,
	// measured on the compressed body (PARITY §1). MaxBatchBytes is an
	// uncompressed assembly cap and so is far more conservative than this;
	// this is the backstop that stops a doomed request being sent at all.
	hardMaxRequestBytes = 10 << 20

	// defaultDropReportInterval rate-limits drop summaries. Drops are
	// aggregated rather than reported per record because during an outage the
	// per-record shape is an error storm that is itself a denial of service.
	//
	// It is also the period of the sender's report ticker, so it is both "at
	// most one summary per reason per interval" and "at least one look per
	// interval", whether or not any batch is moving.
	defaultDropReportInterval = 5 * time.Second
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
//	Enqueued == Sent + DroppedQueueFull + DroppedBurst + DroppedBacklog +
//	            DroppedRejected + DroppedOversize + DroppedClosed
//
// so no record is unaccounted for and no drop is silent.
//
// Records rejected by Enqueue with an encoding error are outside this
// accounting entirely: nothing was ever handed over, and the caller was told
// synchronously.
//
// The identity assumes no Enqueue is still running when Close is called. An
// Enqueue that passed its closed check just before Close closed the client can
// complete its hand-off to the queue after Close has counted what was left
// there; that record is counted Enqueued and nothing else, and the sum is short
// by it. Enqueue is lock-free by design, so this is a caveat rather than a bug
// to fix: logging into a client another goroutine is closing is already a race
// in the caller — the record is delivered or lost depending on the instant it
// lands — and the counters can only decline to describe it. Let the last
// Enqueue return before calling Close and the identity is exact.
type Stats struct {
	// Enqueued counts records offered to Enqueue, including those refused
	// because the client was already closed.
	Enqueued uint64
	Sent     uint64 // acknowledged by the ingestion endpoint
	Retries  uint64 // upload attempts after the first

	DroppedQueueFull uint64 // the application outran the sender
	DroppedBurst     uint64 // over the WithBurstProtection rate limit
	DroppedBacklog   uint64 // every upload slot busy and the hand-off full
	DroppedRejected  uint64 // terminal status, or the retry budget ran out
	DroppedOversize  uint64 // over the hard request size limit, or a 413
	// DroppedClosed counts everything lost to shutdown: enqueued after Close,
	// still queued when Close returned, or in flight when the shutdown deadline
	// expired. A batch abandoned mid-backoff or mid-request is counted here and
	// not DroppedRejected — nothing rejected it — and one abandoned while
	// waiting for an upload slot here and not DroppedBacklog.
	DroppedClosed uint64
}

// counters is the atomic backing for Stats. atomic.Uint64 rather than bare
// uint64 with atomic.AddUint64, so there is no 64-bit alignment hazard on
// 32-bit platforms.
type counters struct {
	enqueued atomic.Uint64
	sent     atomic.Uint64
	retries  atomic.Uint64

	droppedQueueFull atomic.Uint64
	droppedBurst     atomic.Uint64
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
		DroppedBurst:     c.droppedBurst.Load(),
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
	burstMax        int           // 0 = burst protection disabled
	burstWindow     time.Duration // 0 = burst protection disabled
	maxRetries      int
	retryBackoff    time.Duration
	retryCeiling    time.Duration
	maxInFlight     int
	timeout         time.Duration
	connectTimeout  time.Duration
	shutdownTimeout time.Duration
	compression     Compression
	encoder         Encoder
	onError         func(error)
	dryRun          bool
	httpClient      *http.Client // set only by WithHTTPClient

	// dropReportInterval paces the sender's drop summaries. It has no option:
	// it is a seam so the tests can drive the periodic path without waiting out
	// the real interval, in the same spirit as the limiter's clock. Production
	// only ever sees defaultDropReportInterval.
	dropReportInterval time.Duration
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

// WithBurstProtection caps how fast records are admitted at all: at most
// maxRecords per window, with a full window's worth admissible back to back.
// Records over the limit are dropped and counted DroppedBurst. Disabled by
// default; WithBurstProtection(0, 0) disables it again.
//
// This is a different limit from WithMaxQueueSize, not a smaller one. The queue
// sheds when the sender falls behind, so it engages only once the pipeline is
// saturated — and by then every dropped record has already been converted and
// encoded on the calling goroutine. Burst protection is a ceiling the operator
// chooses, applied before that work happens, so a runaway loop logging inside a
// hot path costs one atomic load per record rather than a JSON encode.
//
// It is off by default because the right ceiling is a property of the
// application, not of the library, and a limit guessed on the application's
// behalf would drop its logs without being asked. The JavaScript client, the
// only official one with this feature, ships 10000 records per 5s; that is a
// reasonable starting point, and a deliberately conservative one for Go.
func WithBurstProtection(maxRecords int, window time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.burstMax = maxRecords
		c.burstWindow = window
	}
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

// WithRetryCeiling bounds the total time one batch may spend across all of its
// attempts, including the waits between them. Default 60s, OpenTelemetry's
// number for the same job.
//
// It is a second limit alongside MaxRetries, and the tighter of the two wins: a
// generous retry count cannot keep a batch alive indefinitely against a server
// that answers slowly, or one whose Retry-After keeps asking for more time.
// Shutdown bounds it further still — Close cancels in-flight uploads when
// ShutdownTimeout expires, aborting any batch parked in a backoff.
//
// Its interaction with Retry-After is sharper than "the tighter wins" suggests,
// and the arithmetic is worth stating outright: a wait that would end past the
// deadline is not taken at all, so at the default ceiling a 429 answering the
// very first attempt with Retry-After: 60 drops the batch there and then, and
// Retry-After: 30 buys exactly one retry. That is deliberate. A server asking
// to be left alone for as long as the entire budget has, in effect, declined
// the batch, and waiting it out would pin one of the MaxInFlight upload slots
// for a minute with the queue backing up behind it. OpenTelemetry's exporter
// resolves the same collision the same way, giving up with "max retry time
// would elapse" rather than honouring the throttle past its ceiling. Raise the
// ceiling if surviving long throttles matters more than shedding under them.
//
// A batch split after a 413 keeps its parent's ceiling rather than starting a
// fresh one, so splitting cannot extend the budget either.
func WithRetryCeiling(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.retryCeiling = d }
}

// WithMaxInFlight caps concurrent uploads. Default 5, matching the JavaScript
// client's syncMax.
//
// The ingesting host speaks HTTP/2, so concurrent uploads multiplex over one
// TCP connection: concurrency costs streams, not sockets or handshakes.
//
// A slot is held for a batch's entire retry sequence, backoff sleeps included,
// so this bounds batches in progress rather than requests on the wire. During
// an outage every worker can be asleep rather than uploading, and a batch that
// becomes deliverable the moment the server recovers still waits out the sleep
// in progress ahead of it. RetryCeiling is what bounds that wait — it gates
// every sleep, so no slot is held sleeping for longer than it. Raising
// MaxInFlight buys pipelining through such a window; it does not shorten the
// sleeps.
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
//
// Compression runs on the single sender goroutine, because no gzip.Writer may
// be shared, which makes it the client's whole-process throughput ceiling: one
// core's worth of gzip.BestSpeed, which BenchmarkCompress measures at ~210MB/s
// of raw NDJSON on a 2023 laptop core. That is far above any real logging
// volume, and it is the first wall a synthetic throughput benchmark meets.
// CompressionNone removes the ceiling and costs roughly 8–12× the bytes on the
// wire, which is the wrong trade unless something else is paying for the
// bandwidth.
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
// lost record: at most one summary per reason every five seconds, carrying the
// count since the previous one, plus a final summary from Close covering
// everything lost over the client's life.
//
// The summaries are paced by the sender's own ticker rather than by batches
// completing, so they keep arriving through the incident that produces them —
// including while the sender is parked waiting for an upload slot that a stalled
// server is not freeing, which is precisely when the queue is shedding fastest.
// Their granularity is the five seconds: a burst of drops is visible within one
// interval of starting, never sooner, and as a count rather than as the records
// themselves, which are gone.
func WithOnError(f func(error)) ClientOption {
	return func(c *clientConfig) {
		if f != nil {
			c.onError = f
		}
	}
}

// WithDryRun runs the whole pipeline except the request itself.
//
// Records are still converted, encoded, queued, batched, framed and compressed,
// and Flush and Close still behave normally — only the POST is skipped, and the
// records are counted as Sent. It is the kill switch the JavaScript client
// spells sendLogsToBetterStack, and it exists so that tests and local
// development exercise the real code path without spending quota.
//
// A dry-run client needs no source token: NewClient("", WithDryRun(true))
// succeeds, since demanding a credential for the mode whose point is not having
// one would defeat it. Every other setting is validated exactly as usual.
func WithDryRun(dry bool) ClientOption {
	return func(c *clientConfig) { c.dryRun = dry }
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

	limiter *limiter // nil unless WithBurstProtection was given

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
		retryCeiling:    defaultRetryCeiling,
		maxInFlight:     defaultMaxInFlight,
		timeout:         defaultTimeout,
		connectTimeout:  defaultConnectTimeout,
		shutdownTimeout: defaultShutdownTimeout,
		compression:     CompressionGzip,
		encoder:         NDJSON(),
		onError:         defaultOnError,

		dropReportInterval: defaultDropReportInterval,
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

	// Left nil when unconfigured, so the default path through Enqueue pays one
	// predictable branch and never reads a clock.
	if cfg.burstMax > 0 {
		c.limiter = newLimiter(cfg.burstMax, cfg.burstWindow)
	}

	if cfg.httpClient != nil {
		c.hc = cfg.httpClient
	} else {
		c.transport = newTransport(&cfg)
		c.ownsTransport = true
		c.hc = &http.Client{Transport: c.transport}
	}

	c.workerCtx, c.cancelWork = context.WithCancel(context.Background())
	c.pool = newUploadPool(c)

	go newSender(c).run()

	return c, nil
}

func (cfg *clientConfig) validate() error {
	// A dry run needs no credential, since not spending one is the point.
	// Everything below is still checked, so turning the switch off later cannot
	// surface a configuration error that was hidden while it was on.
	if !cfg.dryRun && strings.TrimSpace(cfg.sourceToken) == "" {
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
		{"WithRetryCeiling", cfg.retryCeiling},
		{"WithShutdownTimeout", cfg.shutdownTimeout},
	} {
		if check.got <= 0 {
			return fmt.Errorf("betterstack: %s(%v) must be positive", check.name, check.got)
		}
	}
	if cfg.retryBackoff < 0 {
		return fmt.Errorf("betterstack: WithRetryBackoff(%v) must not be negative", cfg.retryBackoff)
	}
	// Compression is an exported int enum, so an unknown value is
	// representable. Both places that read it test for CompressionGzip, so an
	// unchecked typo would silently disable compression rather than fail — the
	// one option whose misuse would otherwise be invisible.
	if cfg.compression != CompressionGzip && cfg.compression != CompressionNone {
		return fmt.Errorf("betterstack: WithCompression(%d) is not a known compression", cfg.compression)
	}
	// Burst protection is the one option that is legitimately zero — that is
	// how it stays off — so it cannot join either table above. Half of it is
	// never meaningful: a maximum without a window has no rate in it, and a
	// window without a maximum has nothing to limit.
	if (cfg.burstMax > 0) != (cfg.burstWindow > 0) || cfg.burstMax < 0 || cfg.burstWindow < 0 {
		return fmt.Errorf("betterstack: WithBurstProtection(%d, %v) needs both a positive maximum and a positive window, or neither",
			cfg.burstMax, cfg.burstWindow)
	}
	return nil
}

// Enqueue encodes an event and queues it for delivery.
//
// It never blocks on the network and never blocks on a full queue: if the
// application is producing records faster than they can be shipped, the record
// is dropped and counted rather than stalling the caller. WithBurstProtection,
// if configured, drops the record earlier still and for a different reason —
// over the operator's rate ceiling, whether or not delivery is keeping up.
//
// A record encoding to more than the API's per-request limit is also refused
// here, counted DroppedOversize, when that is knowable locally — which it is
// only with WithCompression(CompressionNone). Compressed, the same record is
// judged on its finished body by the sender, or by the server.
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

	// Before the encode, deliberately: refusing a record here is meant to cost
	// one atomic load, not a JSON marshal. Counted as both offered and dropped
	// for the same reason as the branch above.
	if c.limiter != nil && !c.limiter.allow() {
		c.stats.enqueued.Add(1)
		c.stats.droppedBurst.Add(1)
		return nil
	}

	// Encoding happens here, on the caller's goroutine. Encoding errors are
	// therefore synchronous and returnable; byte accounting downstream is exact
	// and free; and the queue carries []byte, so no record data is shared
	// between goroutines and there is nothing to alias.
	buf, err := c.cfg.encoder.AppendRecord(nil, event)
	if err != nil {
		return fmt.Errorf("betterstack: encoding record: %w", err)
	}

	c.stats.enqueued.Add(1)

	// A record that cannot fit in a request of its own is refused here rather
	// than after it has taken a queue slot and been copied into the sender's
	// accumulation buffer and the packer's framing scratch — both of which keep
	// the capacity a multi-megabyte record forces on them for the life of the
	// client — only to meet the same limit in dispatch, which cannot split a
	// batch of one. Same reason, same count, several copies earlier.
	//
	// Sound only with compression off, where the body is the records plus
	// framing and framing never shrinks them. With gzip how much fits is not
	// knowable here at all: the limit is measured on the compressed body, and a
	// record many times this size routinely compresses well under it. That case
	// stays where it was — the sender's check on the finished body, and failing
	// that the server's 413.
	if c.cfg.compression == CompressionNone && len(buf) > hardMaxRequestBytes {
		c.stats.droppedOversize.Add(1)
		return nil
	}

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
// expires first — it will not hang waiting on a sender that has gone away. A
// nil ctx is treated as context.Background().
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
// An Enqueue still running when Close is called is a race in the caller: the
// record may be delivered, dropped, or — see Stats — counted as neither.
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
		// Enqueue and now is accounted rather than silently lost. This read is
		// also where the Stats identity's fine print comes from: an Enqueue
		// still in flight at this point can land its record after the count,
		// and that record is then Enqueued and nothing else.
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

// closing reports whether Close has begun.
//
// It exists to file drops accurately. A batch lost to a cancelled context is
// lost for one of two quite different reasons — the process is shutting down,
// or a live caller's Flush deadline expired — and the context alone cannot say
// which, since Close performs its final flush through a Flush context of its
// own. done is closed first by Close and never otherwise, so it is the
// discriminator.
func (c *Client) closing() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// batch is a completed, framed, optionally compressed request body together
// with the accounting needed to report on it, and the material needed to split
// it if the server says it is too big.
type batch struct {
	body     []byte // owned by the batch: invariant 3
	records  int
	rawBytes int // framed, uncompressed size, for diagnostics

	// raw is the concatenated per-record encodings, before framing and before
	// compression, and bounds[i] is the end offset of record i within it. Two
	// slices and one integer per record is what a 413 costs in steady state;
	// the alternative — recovering record boundaries from a compressed, framed
	// body — is not possible for an arbitrary Encoder.
	//
	// Both are owned by the batch, like body. A batch produced by splitting
	// another aliases its parent's raw, which is sound precisely because the
	// parent is discarded at that point and nothing ever writes into raw.
	raw    []byte
	bounds []int
}

// packer turns a run of encoded records into a request body. It owns reusable
// scratch buffers, so exactly one goroutine may use a given packer: the sender
// has one, and an upload worker builds its own the first time it has to split a
// batch. That per-goroutine ownership is what keeps the reused gzip.Writer
// sound now that compression is no longer confined to the sender.
type packer struct {
	scratch []byte      // framing buffer, reused across batches
	gz      *compressor // nil when compression is off
}

func newPacker(comp Compression) *packer {
	p := &packer{}
	if comp == CompressionGzip {
		p.gz = newCompressor()
	}
	return p
}

// split divides b in half by record count. The caller must have checked that b
// holds at least two records; a single record that is too large cannot be made
// smaller and is dropped instead.
//
// Both halves are re-framed and re-compressed from the original record bytes,
// which is why raw is kept unframed: the array encoder's opening bracket
// belongs to the batch, not to the records, so a half cannot simply be a byte
// range of the parent's body.
func (p *packer) split(enc Encoder, b *batch) (left, right *batch, err error) {
	mid := b.records / 2
	cut := b.bounds[mid-1]

	// The right half's offsets are rebased onto its own slice. b is dead after
	// this, so rewriting its bounds in place would be safe, but a batch that
	// quietly corrupts its parent is a poor thing to leave lying around.
	rightBounds := make([]int, b.records-mid)
	for i, end := range b.bounds[mid:] {
		rightBounds[i] = end - cut
	}

	if left, err = p.pack(enc, b.raw[:cut], b.bounds[:mid]); err != nil {
		return nil, nil, err
	}
	if right, err = p.pack(enc, b.raw[cut:], rightBounds); err != nil {
		return nil, nil, err
	}
	return left, right, nil
}

// pack frames, compresses and wraps a run of encoded records into a batch that
// owns its body.
//
// raw must already be owned by the caller on the batch's behalf: it is retained
// as-is, so that a later split can re-frame from it. Framing happens on the
// packer's scratch buffer rather than on raw, because Frame may write into the
// buffer it is given and raw has to survive intact.
func (p *packer) pack(enc Encoder, raw []byte, bounds []int) (*batch, error) {
	p.scratch = enc.Frame(append(p.scratch[:0], raw...), len(bounds))
	framed := p.scratch

	body := framed
	if p.gz != nil {
		compressed, err := p.gz.compress(framed)
		if err != nil {
			return nil, err
		}
		body = compressed
	}
	return &batch{
		// Invariant 3: both the scratch buffer and the compressor's output
		// buffer are reused, so the batch takes its own copy of whichever one
		// it ended up with.
		body:     append([]byte(nil), body...),
		records:  len(bounds),
		rawBytes: len(framed),
		raw:      raw,
		bounds:   bounds,
	}, nil
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
