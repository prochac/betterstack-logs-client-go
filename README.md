# logs-client-go

A [Better Stack](https://betterstack.com/logs) logging client for Go: a
`log/slog` handler backed by a batching, retrying HTTP client.

Standard library only — no dependencies outside it, and none planned. Go 1.21 or
newer. (The optional MessagePack encoder takes a codec as an argument rather than
bundling one, so it does not change that; see [MessagePack](#messagepack).)

```sh
go get github.com/prochac/logs-client-go
```

```go
import betterstack "github.com/prochac/logs-client-go"
```

The module path ends in `logs-client-go`; the package is named `betterstack`.

## Quickstart

```go
package main

import (
        "log/slog"
        "os"

        betterstack "github.com/prochac/logs-client-go"
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
  counted** — never blocked. Size it with `WithMaxQueueSize`.
- Delivery failures surface through `WithOnError`, not through `Handle`: by the
  time they happen `Handle` has long since returned, and reporting a logging
  failure through the logger would recurse. Drops arrive as periodic aggregated
  summaries rather than one callback per lost record.
- `WithBurstProtection` is the other half of that, and is **off by default**: it
  caps how fast records are admitted at all, before they are encoded, so a
  runaway loop inside a hot path costs an atomic load per record instead of a
  JSON marshal. The queue only bounds memory, and only once the burst has
  already been paid for.
- `Client.Stats()` accounts for every record. After `Close` returns:

  ```
  Enqueued == Sent + DroppedQueueFull + DroppedBurst + DroppedBacklog +
              DroppedRejected + DroppedOversize + DroppedClosed
  ```

Retries cover transient failures only — 408, 429, 5xx and network errors — with
exponential backoff, full jitter, and `Retry-After` honoured. A rejected source
token, an exhausted quota and an unparseable body are terminal: retrying those
burns quota forever and can never succeed. A batch refused as too large is
halved and both pieces resent; only a single record too large on its own is
dropped.

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
| `WithEncoder(Encoder)`                    | `NDJSON()`                        | `JSONArray()` and `MsgPack(marshal)` also provided                                                                               |
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
`Encoder` is a three-method interface if you need another format.

Gzip is on by default because the API's 10 MiB request limit is measured on
*compressed* bytes, so it multiplies how much fits in a request.

### MessagePack

`MsgPack` sends `application/msgpack`, but it ships no codec — you pass one in.
Most libraries' `Marshal` can be handed over directly:

```go
import "github.com/shamaton/msgpack/v2"       // or vmihailenco/msgpack/v5

betterstack.WithEncoder(betterstack.MsgPack(msgpack.Marshal))
```

One built around a reusable handle needs a closure:

```go
import "github.com/ugorji/go/codec"

h := &codec.MsgpackHandle{}
h.WriteExt = true // without this the timestamp extension degrades to raw bytes

betterstack.WithEncoder(betterstack.MsgPack(func(v any) ([]byte, error) {
    var out []byte
    return out, codec.NewEncoderBytes(&out, h).Encode(v)
}))
```

No codec is bundled for two reasons. Libraries disagree about timestamps, struct
tags and whether a Go string becomes `str` or `bin`, and that is your call to
make — as is keeping it patched. And you probably have one in the build already;
picking for you would add a second. This package contributes only the array
framing, which is a length prefix rather than a serialiser, so it stays
dependency-free.

**Do not switch expecting smaller requests.** Bodies are gzipped by default, and
gzipped MessagePack is usually no smaller than gzipped JSON — sometimes larger,
because JSON's repeated keys and ASCII digits compress extremely well. The
reasons to choose it are exact `int64`/`uint64`, native binary, the timestamp
extension type, and matching what the Node client sends.

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
