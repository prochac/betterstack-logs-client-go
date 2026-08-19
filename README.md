# betterstack-logs-client-go

[![CI](https://github.com/prochac/betterstack-logs-client-go/actions/workflows/ci.yml/badge.svg)](https://github.com/prochac/betterstack-logs-client-go/actions/workflows/ci.yml)

A [Better Stack](https://betterstack.com/logs) logging client for Go: a
`log/slog` handler backed by a batching, retrying HTTP client.

Standard library only — no dependencies outside it, and none planned. Go 1.21 or
newer.

```sh
go get github.com/prochac/betterstack-logs-client-go
```

```go
import betterstack "github.com/prochac/betterstack-logs-client-go"
```

The module path ends in `logs-client-go`, prefixed with `betterstack-` because a
personal namespace has to say what it is a client *of*; the package is named
`betterstack`, so import it under that name rather than the trailing path
element.

## Quickstart

```go
package main

import (
        "log/slog"
        "os"

        betterstack "github.com/prochac/betterstack-logs-client-go"
)

func main() {
        client, err := betterstack.NewClient(os.Getenv("BETTERSTACK_SOURCE_TOKEN"))
        if err != nil {
                panic(err)
        }
        defer client.Close()

        logger := slog.New(betterstack.NewHandler(client))
        slog.SetDefault(logger)

        slog.Info("service started", "port", 8080)
}
```

There are two objects, and the split is deliberate:

- **`Client`** owns everything with a lifecycle — the queue, batching,
  compression, HTTP, retry — and knows nothing about slog.
- **`Handler`** converts records and does no I/O.

You construct the client, so you are holding the thing that has to be closed.

## Close before exiting

**`Close` is not optional.** Records are batched, so whatever is still
accumulating when the process exits is lost unless it is flushed. `defer
client.Close()` in `main` is the pattern, and it is the same requirement every
official Better Stack client documents.

`Close` flushes, waits for in-flight uploads up to `WithShutdownTimeout`, and
returns the first delivery error it saw. `Client.Flush(ctx)` does the flush
without shutting down, for a checkpoint before something risky.

A caveat that costs people logs: `os.Exit` and `log.Fatal` skip deferred calls
entirely. Structure `main` so that shutdown actually runs.

## Failure behaviour

Logging must not be able to take down the application it instruments, so nothing
here blocks the caller on the network.

- `Handle` converts and encodes on the calling goroutine, then hands the bytes to
  a bounded queue and returns. It never touches the network.
- If the application outruns delivery, records are **dropped at the queue and
  counted** — never blocked. A record offered to a queue that is already full is
  not encoded, so an outage sheds for the price of a length read. Size the queue
  with `WithMaxQueueSize`.
- Delivery failures surface through `WithOnError`, not through `Handle`: by the
  time they happen `Handle` has long since returned, and reporting a logging
  failure through the logger would recurse. Drops arrive as aggregated summaries
  rather than one callback per lost record — at most one per reason every five
  seconds, paced by the client itself, so an outage reports what it is costing
  while it is still going on.
- `WithBurstProtection` is the other half of that, and is **off by default**: it
  caps how fast records are admitted at all, at the cost of an atomic load per
  record, whether or not delivery is keeping up. The queue sheds only once the
  sender is behind; this is a ceiling you declare.
- `Client.Stats()` accounts for every record. After `Close` returns:

  ```
  Enqueued == Sent + DroppedQueueFull + DroppedBurst + DroppedBacklog +
              DroppedRejected + DroppedOversize + DroppedClosed
  ```

  provided the last `Enqueue` returned before `Close` was called — a record
  handed over by a goroutine racing the shutdown may land after the accounting
  has closed. See `Stats` for the fine print.

Retries cover transient failures only — 408, 429, 5xx and network errors — with
exponential backoff, full jitter, and `Retry-After` honoured. A rejected source
token, an exhausted quota and an unparseable body are terminal: retrying those
burns quota forever and can never succeed. A batch refused as too large is
halved and both pieces resent; only a single record too large on its own is
dropped.

`Retry-After` is honoured up to the retry ceiling and no further: a wait that
would end past the batch's 60s budget is not taken at all, so a throttle asking
for the whole window drops the batch instead of parking an upload slot for a
minute. Raise `WithRetryCeiling` if you would rather wait it out.

## Client options

| Option                                    | Default                           | Notes                                                                                                                            |
|-------------------------------------------|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `WithEndpoint(string)`                    | `https://in.logs.betterstack.com` | the ingesting host                                                                                                               |
| `WithBatchSize(int)`                      | `1000`                            | records per batch                                                                                                                |
| `WithBatchInterval(time.Duration)`        | `1s`                              | how long a partial batch waits; the timer starts on the batch's first record, so an idle client does no work                     |
| `WithMaxBatchBytes(int)`                  | `5 MiB`                           | uncompressed assembly cap, not the API's limit                                                                                   |
| `WithMaxQueueSize(int)`                   | `100000`                          | records; the queue is where backpressure is shed                                                                                 |
| `WithBurstProtection(int, time.Duration)` | disabled                          | at most *n* records per window, refused before encoding; the JavaScript client's `10000` per `5s` is a reasonable starting point |
| `WithMaxRetries(int)`                     | `5`                               | retries **after** the first attempt, so at most 6 requests; `0` means send once                                                  |
| `WithRetryBackoff(time.Duration)`         | `300ms`                           | base delay, exponential with full jitter                                                                                         |
| `WithRetryCeiling(time.Duration)`         | `60s`                             | total time one batch may spend across all attempts                                                                               |
| `WithMaxInFlight(int)`                    | `5`                               | concurrent uploads                                                                                                               |
| `WithTimeout(time.Duration)`              | `10s`                             | per request attempt                                                                                                              |
| `WithConnectTimeout(time.Duration)`       | `5s`                              | TCP connect; no effect with `WithHTTPClient`                                                                                     |
| `WithShutdownTimeout(time.Duration)`      | `15s`                             | how long `Close` waits                                                                                                           |
| `WithCompression(Compression)`            | `CompressionGzip`                 | `CompressionNone` to disable                                                                                                     |
| `WithEncoder(Encoder)`                    | `NDJSON()`                        | also `JSONArray()`, and `NDJSONWith`/`JSONArrayWith` for a custom `ObjectAppender`                                               |
| `WithOnError(func(error))`                | one line per event to stderr      | must not log through this handler                                                                                                |
| `WithDryRun(bool)`                        | `false`                           | run everything except the request                                                                                                |
| `WithHTTPClient(*http.Client)`            | tuned internal client             | escape hatch; the client is not owned or closed                                                                                  |

## Handler options

| Option                                                      | Default            | Notes                                                                            |
|-------------------------------------------------------------|--------------------|----------------------------------------------------------------------------------|
| `WithLevel(slog.Leveler)`                                   | `slog.LevelInfo`   | pass a `*slog.LevelVar` to change it at runtime                                  |
| `WithAddSource(bool)`                                       | `false`            | adds `source` with function, file and line                                       |
| `WithReplaceAttr(func([]string, slog.Attr) slog.Attr)`      | none               | `slog.HandlerOptions.ReplaceAttr` semantics; does not apply to the reserved keys |
| `WithAttrFromContext(...func(context.Context) []slog.Attr)` | none               | trace and request IDs; placed at the root, outside open groups                   |
| `WithExtraFields(map[string]any)`                           | none               | merged into every record                                                         |
| `WithFilter(func(context.Context, slog.Record) bool)`       | none               | return **true to send**                                                          |
| `WithConverter(Converter)`                                  | `DefaultConverter` | the supported way to change the record shape                                     |
| `WithContextKey(string)`                                    | `"context"`        | `""` flattens attributes to the top level                                        |

The default level is `Info`, not `Debug`: shipping debug records to a metered
endpoint unless you opt out is a billing surprise.

One difference worth knowing if you are porting a `ReplaceAttr` function from
`slog.TextHandler` or `slog.JSONHandler`: theirs is called for the built-in
`time`, `level`, `msg` and `source` fields as well as for attributes, and this
one is not called for `dt`, `level` or `message`. Those three are the ingestion
API's own fields rather than attributes, and rewriting them produces a payload
the server cannot read. A redaction function that rewrites `msg` will therefore
be silently skipped for the message — use `WithConverter` to change the record
shape, and note that `source`, which is an attribute here, still goes through
`ReplaceAttr` as usual.

## Record shape

```json
{
  "dt": "2026-08-06T10:11:12.123456789Z",
  "level": "ERROR",
  "message": "upstream call failed",
  "context": {
    "user": {"id": "user-123"},
    "err": {"message": "connection refused", "type": "*net.OpError"},
    "source": {"function": "main.call", "file": "main.go", "line": 42}
  }
}
```

- `dt`, `level` and `message` are reserved. Attributes nest under `context`;
  `WithContextKey("")` flattens them, and the reserved keys still win on
  collision.
- A record with no timestamp **omits `dt`** rather than substituting send time.
  That is the slog contract, and the server's own reception time is more accurate
  than a client-side stamp applied a batch interval and several backoffs later.
- Errors are expanded to `{message, type}` instead of being flattened to a
  string.
- The library name and version travel in `User-Agent`, not in every record.

## Encoding and compression

NDJSON (`application/x-ndjson`) by default, gzip-compressed. A batch is then
simply the concatenation of its records, with no framing pass at all.

`WithEncoder(betterstack.JSONArray())` sends `application/json` instead, and
`Encoder` is a three-method interface if you need another format. A format whose
batch really is just its records — any other line-delimited one — should also
implement `IdentityFramer`, one method returning `true`, which tells the client
there is nothing to frame and saves it a copy of every batch and the buffer it
would have copied into.

Gzip is on by default because the API's 10 MiB request limit is measured on
*compressed* bytes, so it multiplies how much fits in a request.

### What each encoder costs

One record — the payload a `logger.Info` with six attributes produces —
appended to the buffer the sender reuses, which is exactly what `Enqueue` does
on the calling goroutine. go1.26.6, linux/amd64, i7-1355U:

| Encoder                                               | ns/op |  B/op | allocs/op | wire bytes |
|-------------------------------------------------------|------:|------:|----------:|-----------:|
| `NDJSON()`                                            |  3600 |   769 |        23 |        222 |
| `JSONArray()`                                         |  3500 |   769 |        23 |        222 |
| `NDJSONWith(fastjson.AppendObject)`                   |   910 | **0** |     **0** |        222 |
| `JSONArrayWith(fastjson.AppendObject)`                |   880 | **0** |     **0** |        222 |

And the same choice measured where you actually pay for it, one whole
`logger.Info` call on a dry-run client — attribute tree, `Converter`, encode and
the queue hand-off:

| Encoder                                | ns/op | B/op    | allocs/op |
|----------------------------------------|------:|--------:|----------:|
| `NDJSON()`                             |  6500 | 2.6 KiB |        39 |
| `JSONArray()`                          |  6900 | 2.6 KiB |        40 |
| `NDJSONWith(fastjson.AppendObject)`    |  4300 | 2.2 KiB |        21 |
| `JSONArrayWith(fastjson.AppendObject)` |  4200 | 2.2 KiB |        21 |

Two things to read off them:

- **The framing is free; the encoding is not.** NDJSON and JSONArray differ by
  one byte per record and nothing measurable, because both hand the record to
  the same `ObjectAppender`. Choose the framing your receiver wants, and treat
  the appender as the performance decision.
- **`fastjson` produces the same bytes.** A batch encoded with it is byte-for-byte
  what `encoding/json` produces for this payload shape — same key order, same
  size — for a quarter of the time and none of the allocations. That is asserted
  in the suite, not just observed here.

Timings on a laptop swing about ±20% between runs, so they are rounded; the
allocation and byte columns are exact and did not vary, and the ordering held in
every run.

The `encoding/json` rows are the ones that move with the toolchain. On go1.27,
where the v2 engine lands under the v1 API, the same record encodes in 13
allocations and 200 B rather than 23 and 769, and a whole log call costs 29
allocations rather than 39. The reflection-free appender still allocates nothing
and still encodes in about a third of the time, so the choice does not change —
but it is worth re-running the numbers on your own toolchain before acting on
them.

### Faster JSON, if you log a lot

Records are encoded by `encoding/json`, on the goroutine that called the logger.
That is the right default, but it is not cheap: a payload is a `map[string]any`,
so every value is an interface the encoder has to reflect over and box again on
the way out.

The `fastjson` subpackage is a reflection-free appender for exactly this payload
shape. Opt in at the call site:

```go
import "github.com/prochac/betterstack-logs-client-go/fastjson"

betterstack.WithEncoder(betterstack.NDJSONWith(fastjson.AppendObject))
```

It encodes the record in about a quarter of the time and allocates nothing at
all — see the table above — and the body it produces is byte-for-byte what
`encoding/json` produces, keys in the same order.

It is a separate package on purpose. It is a second implementation of a format
the standard library already implements, and it is not something you should have
to trust because you imported the client — so a binary that does not import it
does not contain it. Read [its documentation](https://pkg.go.dev/github.com/prochac/betterstack-logs-client-go/fastjson)
before adopting it: it is a type switch over the types the handler produces,
with anything else falling through to `encoding/json`, so it can be incomplete
without being wrong.

Anything else that satisfies `ObjectAppender` works too, but note that a general
-purpose JSON library is unlikely to help: they win by caching reflection over
concrete struct types, and a `map[string]any` gives them nothing to cache.
`goccy/go-json` is about 20% faster than `encoding/json` here and
`json-iterator` is slower.

### Why there is no MessagePack encoder

There was one, and it was removed after measuring it: it gained nothing. Encode
time tied `fastjson`, which costs no dependency, and the gzipped body — the
thing the API's request limit and your bill are measured on — came out **34%
larger** than NDJSON, because a batch's repeated key sequence is exactly what
gzip compresses best. Precision is not a reason either: JSON *text* carries
`int64`/`uint64` as exact digits, and a value that must survive every pipeline
regardless of decoders belongs in a string. If you want speed, use `fastjson`
above; DESIGN §4 has the full record of the measurements.

## Extra fields and filtering

```go
handler := betterstack.NewHandler(client,
        betterstack.WithExtraFields(map[string]any{
                "service": "checkout",
                "env":     os.Getenv("ENV"),
        }),
        betterstack.WithFilter(func(_ context.Context, r slog.Record) bool {
                return !strings.HasPrefix(r.Message, "health check")
        }),
)
```

Extra fields are the most general thing said about a record, so anything more
specific wins: an attribute of the same key from the record, from a `With(...)`
chain, or from a context extractor.

`WithFilter` is not level filtering. `WithLevel` decides whether a record is
built at all; a filter runs on one that already exists, with its context and
attributes available. A filtered record is never offered to the client, so it
appears nowhere in `Stats` — it was not dropped, it was never sent for.

## Dry run

```go
client, err := betterstack.NewClient("", betterstack.WithDryRun(true))
```

Everything runs — conversion, encoding, batching, framing, compression, `Flush`,
`Close` — and only the request is skipped; the records are counted as `Sent`. A
dry-run client needs no source token, since not having one is the point.
Everything else is validated exactly as usual.

## Logging to more than one place

Shipping to Better Stack *and* printing to the terminal is the usual want in
development. There is deliberately no option for it here: it is composition, and
`log/slog` does it.

```go
logger := slog.New(slog.NewMultiHandler(
        betterstack.NewHandler(client),
        slog.NewTextHandler(os.Stderr, nil),
))
```

`slog.MultiHandler` is Go 1.26. This module supports 1.21, so on an older
toolchain use [`samber/slog-multi`](https://github.com/samber/slog-multi)'s
`slogmulti.Fanout`, which predates it and does the same job. Either way it is a
dependency of your program, not of this one.

Three things worth knowing:

- **Levels stay independent.** `MultiHandler.Handle` re-checks each handler's
  `Enabled`, so a `Debug` console handler alongside this one — which defaults to
  `Info` — sends debug records to the console only. Nothing extra is shipped and
  nothing is filtered twice.
- **The console handler is not slowed down.** `Handle` here converts, encodes and
  queues; it never touches the network, so the handler beside it is not waiting
  on an upload.
- **The lifecycle does not move.** The client is still yours to `Close`, and
  composing handlers does not change that.

## Example

[`example/`](./example) is a small HTTP service that puts the pieces together:
context extraction, extra fields, a filter, and a shutdown that closes the
client last so the records produced while draining are not lost.

```sh
go run ./example
```

It needs no credentials — with no `BETTERSTACK_SOURCE_TOKEN` it runs in dry-run
mode. To see what actually goes on the wire without an account, point it at any
local HTTP server:

```sh
go run ./example -endpoint http://localhost:9999
```

It drives a few requests against itself at startup, then serves until Ctrl-C and
prints the `Stats` balance on the way out.

## Documentation

[`DESIGN.md`](./DESIGN.md) is the specification: every option and its default,
the concurrency model, the wire format, and the retry and error policy, with the
reasoning behind each. [`PARITY.md`](./PARITY.md) is the research it rests on —
the ingestion API contract, the documented defaults of the official Java, Erlang
and JavaScript clients, and live probes of the endpoint.

## Licence

ISC — Better Stack's house licence for their client libraries. See
[`LICENSE`](./LICENSE).

This is an independent implementation, not affiliated with or endorsed by Better
Stack.
