// Package betterstack sends structured logs to Better Stack.
//
// It provides a [log/slog] handler backed by a batching, retrying HTTP client,
// and has no dependencies outside the standard library.
//
// # Usage
//
// There are two objects. A [Client] owns everything with a lifecycle — the
// queue, batching, compression, HTTP, retry — and knows nothing about slog. A
// [Handler] converts records and does no I/O. The caller constructs the client
// explicitly and closes it:
//
//	client, err := betterstack.NewClient(os.Getenv("BETTERSTACK_SOURCE_TOKEN"))
//	if err != nil {
//	        return err
//	}
//	defer client.Close()
//
//	logger := slog.New(betterstack.NewHandler(client))
//	logger.Info("service started", "port", 8080)
//
// # Close before exiting
//
// Records are batched, so whatever is still accumulating when the process exits
// is lost unless it is flushed. Close is not optional, and it is the reason the
// client is a separate object the caller holds rather than something hidden
// inside the handler: the thing that must be closed should be the thing you are
// holding.
//
// [Client.Flush] does the same without shutting down, for a checkpoint before a
// risky operation.
//
// # Failure behaviour
//
// Logging must not be able to take down the application it is instrumenting, so
// nothing here blocks the caller on the network. Handle converts and encodes on
// the calling goroutine and hands the bytes to a bounded queue; if the
// application outruns delivery, records are dropped at the queue and counted,
// never blocked. Size the queue with [WithMaxQueueSize].
//
// Delivery failures cannot be reported through Handle's error return, because
// by the time they happen Handle has long since returned — and reporting a
// logging failure through the logger would recurse. They go to the [WithOnError]
// callback instead, which defaults to one line per event on stderr, with drops
// aggregated into periodic summaries rather than fired per record.
//
// Every record is accounted for. [Client.Stats] reports what was sent and what
// was dropped, by reason, and the counts balance once Close has returned.
//
// Retries cover transient failures only — 408, 429, 5xx and network errors —
// with exponential backoff, full jitter, and Retry-After honoured. A rejected
// source token, an exhausted quota and an unparseable body are terminal:
// retrying those burns quota forever and can never succeed.
//
// A batch the server rejects as too large is halved and both pieces resent,
// repeatedly if need be. Only a single record that is too large on its own is
// dropped, since nothing can be done with it.
//
// # Wire format
//
// Records are sent as newline-delimited JSON, gzip-compressed, to
// [DefaultEndpoint]. Attributes are nested under "context"; see
// [DefaultConverter] for the record shape and [WithContextKey] to change or
// flatten the nesting. [JSONArray] sends a JSON array instead, and
// [WithEncoder] takes any other implementation of [Encoder].
package betterstack
