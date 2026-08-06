// Command example is a runnable demonstration of the Better Stack logging
// client: a small HTTP service that logs through betterstack.Handler, pulls a
// request ID out of each request's context, and shuts down without losing the
// records it produced on the way out.
//
// Run it with no configuration at all:
//
//	go run ./example
//
// With no BETTERSTACK_SOURCE_TOKEN in the environment it runs in dry-run mode,
// which exercises the whole pipeline — conversion, encoding, batching, framing,
// compression, Flush and Close — and skips only the request itself. Set the
// variable to send the records for real:
//
//	BETTERSTACK_SOURCE_TOKEN=... go run ./example
//
// On startup it drives a handful of requests against itself so there is
// something to look at, then serves until interrupted. Press Ctrl-C to watch the
// shutdown path; the endpoints are also there to be poked at by hand:
//
//	curl localhost:8080/          a normal request
//	curl localhost:8080/health    filtered out, never sent
//	curl localhost:8080/fail      logs at error level
//
// Pass -addr to move it off a busy port, or -addr localhost:0 to let the
// operating system pick one.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	betterstack "github.com/prochac/logs-client-go"
)

var (
	addr     = flag.String("addr", "localhost:8080", "address to serve on")
	endpoint = flag.String("endpoint", "", "send records here instead of to Better Stack (e.g. a local sink)")
)

func main() {
	flag.Parse()

	// Signals are wired up here rather than inside run so that the context is
	// live for the whole of it, including construction.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// run does the work and returns; main does the exiting. os.Exit and
	// log.Fatal skip deferred calls, so calling either one inside run would
	// skip the client's Close and lose whatever was still batched — the single
	// most common way to lose logs with a batching client.
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "example:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	// Close flushes what is still accumulating, waits for the uploads in
	// flight, and returns the first delivery error it saw. It is not optional.
	defer func() {
		if err := client.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "example: close:", err)
		}
		printStats(client.Stats())
	}()

	slog.SetDefault(slog.New(newHandler(client)))

	// Bind before serving so the demo requests below cannot race the listener
	// into existence. Waiting on the real thing is what lets this example hold
	// to the project's rule against sleeping for a result.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	srv := &http.Server{Handler: withRequestID(routes())}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	base := "http://" + ln.Addr().String()
	slog.InfoContext(ctx, "service started", "addr", ln.Addr().String())

	drive(ctx, base)

	// To stderr as well as to Better Stack: the log records go to the endpoint,
	// and the person who just ran this needs the URL in front of them.
	fmt.Fprintf(os.Stderr, "\nexample: serving on %s — try %s/ , %s/health , %s/fail\n",
		base, base, base, base)
	fmt.Fprintln(os.Stderr, "example: press Ctrl-C to shut down")

	slog.InfoContext(ctx, "serving; press Ctrl-C to shut down")

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
	case <-ctx.Done():
	}

	return shutdown(srv, client)
}

// shutdown stops accepting requests, then lets the logging client go last.
//
// The ordering is the point. Shutting the HTTP server down first means the
// records produced while it drains — the last requests, and the shutdown log
// lines themselves — are still enqueued against a live client. Closing the
// client first would leave them with nowhere to go, and Enqueue would count
// them as DroppedClosed.
func shutdown(srv *http.Server, client *betterstack.Client) error {
	// A fresh context: the one in run is already cancelled, that being what got
	// us here, and a cancelled context makes Shutdown return immediately
	// instead of draining.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.InfoContext(ctx, "shutting down")

	if err := srv.Shutdown(ctx); err != nil {
		slog.ErrorContext(ctx, "graceful shutdown failed", "err", err)
		// Reported, not returned: the deferred Close still has to run, and a
		// server that would not drain is no reason to also discard the logs
		// explaining why.
	}

	// A checkpoint flush, for the case where what comes next is risky enough
	// that you would rather not find out afterwards that its logs were still in
	// a buffer. Close would flush anyway; this puts the delivery error in reach
	// while there is still somewhere useful to report it.
	if err := client.Flush(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "example: flush:", err)
	}

	slog.InfoContext(ctx, "shutdown complete")
	return nil
}

// newClient builds the client, falling back to dry run when no source token is
// configured so that the example is runnable with no credentials.
func newClient() (*betterstack.Client, error) {
	// Delivery failures are reported here rather than through the logging call
	// that produced the record, which by then has long since returned. This
	// callback must not log through the handler the client backs: that would
	// turn one delivery failure into an unbounded loop of them.
	errLog := log.New(os.Stderr, "betterstack: ", log.LstdFlags)

	opts := []betterstack.ClientOption{
		betterstack.WithOnError(func(err error) {
			errLog.Println(err)
		}),
		// Small enough that the demo requests below produce visible traffic
		// rather than sitting in a partial batch until the process ends. The
		// defaults — 1000 records, 1s — are what a real service wants.
		betterstack.WithBatchSize(16),
		betterstack.WithBatchInterval(500 * time.Millisecond),
	}

	token := os.Getenv("BETTERSTACK_SOURCE_TOKEN")

	switch {
	case *endpoint != "":
		// Pointed at something local. The source token still has to be
		// non-empty, since NewClient validates it whatever the endpoint is, but
		// a sink of your own will not be checking it.
		opts = append(opts, betterstack.WithEndpoint(*endpoint))
		if token == "" {
			token = "local-sink"
		}
		fmt.Fprintf(os.Stderr, "example: sending records to %s\n", *endpoint)

	case token == "":
		fmt.Fprintln(os.Stderr, "example: no BETTERSTACK_SOURCE_TOKEN set; running in dry-run mode.")
		fmt.Fprintln(os.Stderr, "example: records are converted, encoded, batched and compressed as usual,")
		fmt.Fprintln(os.Stderr, "example: and only the request is skipped, so nothing is printed for them here.")
		fmt.Fprintln(os.Stderr, "example: to see the records on the wire, run a sink and pass -endpoint.")
		// Dry run needs no token, that being the whole point of it. Everything
		// else is validated exactly as usual.
		opts = append(opts, betterstack.WithDryRun(true))
	}

	return betterstack.NewClient(token, opts...)
}

func newHandler(client *betterstack.Client) slog.Handler {
	return betterstack.NewHandler(client,
		betterstack.WithLevel(slog.LevelInfo),
		betterstack.WithAddSource(true),

		// Facts about the process rather than about any one record. Anything
		// more specific wins over these: an attribute of the same key from the
		// record, from a With(...) chain, or from a context extractor.
		betterstack.WithExtraFields(map[string]any{
			"service": "example",
			"env":     envOr("ENV", "development"),
		}),

		// Context extraction. Attributes land at the root of the tree, outside
		// any group opened with WithGroup, because a request ID describes the
		// ambient request and not the call site that happened to log.
		//
		// This is why the code below calls slog.InfoContext and not slog.Info:
		// the extractor only ever sees the context the record carries, and the
		// context-less variants carry context.Background().
		betterstack.WithAttrFromContext(requestIDAttr),

		// Not level filtering — the record already exists here, with its
		// context and attributes in hand, so the predicate can ask questions a
		// level cannot. Health checks are the canonical case: every-few-seconds
		// noise that is worth serving and not worth paying to store.
		//
		// Returning true sends the record.
		betterstack.WithFilter(func(_ context.Context, r slog.Record) bool {
			send := true
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "path" && a.Value.String() == "/health" {
					send = false
					return false
				}
				return true
			})
			return send
		}),
	)
}

// requestIDKey is an unexported type, so nothing outside this package can
// collide with the value stored under it.
type requestIDKey struct{}

// requestIDAttr is the context extractor installed with WithAttrFromContext.
// Returning nil for a context with no request ID leaves the record untouched.
func requestIDAttr(ctx context.Context) []slog.Attr {
	id, ok := ctx.Value(requestIDKey{}).(string)
	if !ok {
		return nil
	}
	return []slog.Attr{slog.String("request_id", id)}
}

// withRequestID is the middleware that puts a request ID into the context,
// where the extractor above can find it. Nothing downstream has to thread a
// logger through or remember to attach the ID: every record logged with the
// request's context carries it, including records from code that knows nothing
// about this middleware.
func withRequestID(next http.Handler) http.Handler {
	var n atomic.Uint64
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := "req-" + strconv.FormatUint(n.Add(1), 10)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// InfoContext, not Info: the request ID is in the context and only the
		// context-carrying variants pass it to the handler.
		slog.InfoContext(r.Context(), "request served",
			"path", r.URL.Path,
			"method", r.Method,
		)
		fmt.Fprintln(w, "ok")
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// Logged unconditionally, and dropped by the filter before it reaches
		// the client. A filtered record appears nowhere in Stats: it was not
		// dropped, it was never sent for.
		slog.InfoContext(r.Context(), "request served", "path", r.URL.Path)
		fmt.Fprintln(w, "ok")
	})

	mux.HandleFunc("/fail", func(w http.ResponseWriter, r *http.Request) {
		err := errors.New("connection refused")
		// An error value is expanded to {message, type} rather than flattened
		// to a string, so it stays queryable at the far end.
		slog.ErrorContext(r.Context(), "upstream call failed",
			"path", r.URL.Path,
			"err", err,
		)
		http.Error(w, "upstream call failed", http.StatusBadGateway)
	})

	return mux
}

// drive makes a few requests against the example itself, so that running it
// produces output without a second terminal.
func drive(ctx context.Context, base string) {
	for _, path := range []string{"/", "/", "/health", "/fail"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if err != nil {
			continue
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			slog.WarnContext(ctx, "demo request failed", "path", path, "err", err)
			continue
		}
		resp.Body.Close()
	}
}

// printStats shows the accounting identity the client guarantees once Close has
// returned: every record offered to Enqueue was either sent or dropped for
// exactly one countable reason. Records rejected by the filter are in neither
// column — they never reached the client at all.
func printStats(s betterstack.Stats) {
	dropped := s.DroppedQueueFull + s.DroppedBurst + s.DroppedBacklog +
		s.DroppedRejected + s.DroppedOversize + s.DroppedClosed

	fmt.Fprintf(os.Stderr, "\nenqueued=%d sent=%d dropped=%d retries=%d\n",
		s.Enqueued, s.Sent, dropped, s.Retries)
	if dropped > 0 {
		fmt.Fprintf(os.Stderr, "  queue_full=%d burst=%d backlog=%d rejected=%d oversize=%d closed=%d\n",
			s.DroppedQueueFull, s.DroppedBurst, s.DroppedBacklog, s.DroppedRejected,
			s.DroppedOversize, s.DroppedClosed)
	}
	if s.Enqueued != s.Sent+dropped {
		fmt.Fprintln(os.Stderr, "  BUG: stats do not balance")
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
