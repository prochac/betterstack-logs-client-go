# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project goal

A **Better Stack logging client for Go**, built to be adopted by Better Stack as first-party rather than to live as a third-party wrapper. There is no Go client in either of their orgs (`logtail`, `BetterStackHQ`), so the slot is unclaimed — see PARITY.md §8.

Module path is `github.com/prochac/logs-client-go`, package `betterstack`, mirroring their `BetterStackHQ/logs-client-<target>` convention. Adoption is then a one-line change to `github.com/betterstackhq/logs-client-go` with every identifier and import name unchanged.

This repository is a **greenfield rewrite**, not a fork. It replaced `prochac/slog-betterstack` (itself a fork of `samber/slog-betterstack`), which is now abandoned.

**Read [DESIGN.md](./DESIGN.md) before changing behaviour.** It is the spec: the two-object shape, every option and its default, the concurrency model, the wire format, the retry and error policy, and the milestone split. [PARITY.md](./PARITY.md) is the research behind it — the ingestion API contract, the documented defaults from the official Java/Erlang/JavaScript clients, and live probes of the endpoint. DESIGN.md cites PARITY.md rather than repeating it.

Sections of DESIGN.md marked **[amended]** were corrected during implementation, when writing the code showed the original decision was wrong or self-contradictory. If you find another such case, fix the code *and* amend DESIGN.md in the same change — the two must not drift.

## Status

**v0.1 is complete** (DESIGN §10): `Client` + `Handler`, NDJSON, batching on all three triggers, bounded queue with drop accounting, `Flush`/`Close`, HTTP status classification, retry with backoff, connection reuse, `OnError`, `Stats`, gzip. Passes `testing/slogtest`.

**v0.2 is complete**: 413 batch splitting (and the local hard-limit check splits too), `WithExtraFields`, `WithFilter`, `WithDryRun`, `WithRetryCeiling`, the `JSONArray` encoder, and `README.md`. "Separate connect/request timeouts", listed under v0.2 in DESIGN §10, had in fact shipped with v0.1's transport. `Encoder.AppendRecord` lost its `index` parameter — see DESIGN §4's amendment before reinstating it.

Next milestone, not started: **v0.3** — MessagePack, burst protection, mirroring to a second `slog.Handler`, `example/`.

## Commands

There is no Makefile. Everything is plain `go`:

```sh
go build ./...
go vet ./...                       # REQUIRED — see the Go 1.21 floor below
go test -race ./...
go test -race -count=5 ./...       # flake check; timing tests must survive this
go test -race -coverprofile=cover.out ./... && go tool cover -func=cover.out
go test -run '^$' -bench . -benchmem -benchtime 2s ./...
go test -race -run TestSlogtest ./...   # handler conformance alone
```

**`go vet` is not optional.** `go.mod` declares `go 1.21` and the local toolchain is much newer. The compiler will happily build a post-1.21 stdlib symbol; only `go vet`'s `stdversion` analyzer catches it. Verified: `slices.Concat` compiles and passes tests, and `go vet` reports `requires go1.22 or later (module is go1.21)`.

## Architecture

One package, standard library only. `go.uber.org/goleak` is the sole dependency and is test-only.

- **`client.go`** — the public `Client` API, all `ClientOption`s and their defaults, `clientConfig.validate`, `Stats`/`counters`, `Enqueue`/`Flush`/`Close`, the `batch` and its `split`, and the `packer` (framing scratch plus the gzip `compressor`). Opens with the three invariants; read those first.
- **`sender.go`** — the sender goroutine (batch accumulation with record boundaries, the timer state machine, flush triggers, the hard-limit split, drop summaries) and the `uploadPool` (worker pool plus the flush rendezvous).
- **`transport.go`** — the tuned `http.Transport`, the per-goroutine `worker`, one upload attempt (`do`), the retry loop (`uploadBy`), the 413 split (`splitAndSend`), status classification, backoff, and `Retry-After` parsing.
- **`handler.go`** — `Handler`, `HandlerOption`s, and the `slog.Handler` implementation. Depends on the unexported `enqueuer` interface, not on `*Client` directly, so the handler is testable with no network stack.
- **`attr.go`** — the attribute half of the `log/slog` contract: `groupOrAttrs` accumulation, the recursive `appendAttr`, group materialisation, source resolution, context extraction.
- **`converter.go`** — `Converter`, `DefaultConverter`, the reserved payload keys, the record shape.
- **`encoder.go`** — the `Encoder` interface and the NDJSON and JSON-array implementations, over one shared pool of `*json.Encoder`s.
- **`errors.go`**, **`version.go`**, **`doc.go`** — error types and the `OnError` funnel; `User-Agent` from build info; the package doc.

## Invariants — do not break these silently

Each of these was arrived at the hard way, and several have a test whose only job is to keep them true.

1. **`queue` and `flushC` are never closed.** Termination is signalled by closing `done`, `shutdown` and `senderDone`, which are only ever *received* from. This makes send-on-closed-channel impossible rather than defended against. Closing `queue` would buy nothing: the sender stops on `shutdown`, not on a drained range loop.
2. **`flushC` is unbuffered.** That is what makes `Flush` racing `Close` return `ErrClosed` instead of hanging on a request nobody will read.
3. **A batch owns its bytes.** The sender reuses both the accumulation buffer and the gzip output buffer, so `flush` copies before dispatch. The fork could hand its reused buffer straight to a send only because that send was synchronous.
4. **No `gzip.Writer` is ever shared.** The buffers live in a `packer`, owned by exactly one goroutine: the sender has one, and an upload worker builds its own lazily the first time a 413 makes it split. Compression is otherwise the sender's alone — in the workers it would be a data race. The split path is the sole exception, and it cannot be avoided: handing the halves back to the sender would deadlock, since the sender's dispatch blocks on the pool the worker occupies.
5. **Backpressure is shed at the queue, and nowhere else.** The batch hand-off blocks. An earlier version dropped there instead and lost ~20% of an ordinary 1000-record burst against a healthy server.
6. **`Enqueue` never blocks and never returns an error for a dropped record.** Drops are counted and aggregated into `OnError` summaries. Returning them would fire error middleware once per lost record, which is the storm the aggregation exists to prevent.
7. **`Handle` does no I/O**, and its error is local only: an encoding failure or `ErrClosed`.
8. **Statuses are terminal by default.** Only 408, 429, 5xx and network errors retry. 401 is terminal — that is what the live endpoint actually returns for a bad token, despite the docs naming 403. **413 is terminal but not fatal**: the same bytes are never resent, yet the records are not abandoned — the batch is halved and both pieces sent. Do not move 413 into `isRetryable` to express that; splitting is a separate mechanism, and conflating them makes a loop.
9. **A record's encoding is self-delimiting and position-independent.** `Enqueue` encodes one record at a time, before it is known which batch it will join, and a split re-frames the same bytes into a different batch. This is why `AppendRecord` has no index and why `JSONArray` puts the comma *before* each record for `Frame` to overwrite.
10. **The stats identity holds after `Close`**: `Enqueued == Sent + all Dropped*`. `TestStatsBalance` asserts it across healthy, rejected, exhausted, splitting, dry-run and overflowing runs.
11. **`OnError` may be called concurrently**, from the sender and from every upload worker, and a panic inside it must not escape `safeReport`.

## Testing

- **`goleak.VerifyTestMain` with no `Ignore` options**, deliberately. It is a design constraint, not hygiene: it is what forces the sender and workers to terminate on `Close`, and why `Close` calls `CloseIdleConnections` on a transport it owns. If it fails, fix the lifecycle — do not add an `IgnoreTopFunction` for `net/http`'s connection loops.
- **`recorder_test.go`** is the fixture. Its handler asserts the auth header, content type, gzip round-trip and NDJSON or JSON-array framing on *every* request, so all tests get those invariants for free. Use `newRecorder` / `newTestClient`. `withMaxAcceptedBytes` answers 413 on size rather than to a script, which is how splitting is tested to convergence. Once retries or splitting are involved use `rec.accepted()`, not `rec.records()`: a refused request carried its records too, and counting those makes every record look duplicated.
- **`slogtest.TestHandler`, not `slogtest.Run`** — `Run` is Go 1.22. The results mapper in `handler_test.go` remaps `dt`→`time` and `message`→`msg` and hoists `context.*`, which `TestHandler`'s own docs bless.
- **Non-flakiness rules**, applied throughout: never `time.Sleep` to wait for a result (`waitFor` polls at 1 ms, fails at 2 s); `Sleep` only to prove a negative; timing assertions are one-sided **lower** bounds only, since full jitter can make any backoff near zero; every deterministic test sets `WithBatchInterval(time.Hour)` so only the trigger under test can fire; `t.Parallel()` everywhere with a per-test client and server.
- **Always close the client before the server.** `httptest.Server.Close` waits for outstanding requests, and a client still retrying into it will deadlock the cleanup. A gated recorder needs `rec.release()` before `c.Close()`.
- Coverage is ~94%. When adding a feature, check the new code is actually reached — `reportDrops` sat at 0% because its path is gated behind a five-second interval.

## Provenance — this constrains what you may write

The greenfield rewrite only buys clean licensing if no source is copied. `samber/slog-betterstack` (MIT © 2023 Samuel Berthe), `samber/slog-common` (MIT), and the `alistairjevans/slog-betterstack` fork are **prior art to read, not code to move**. Reimplementing an idea is fine — ideas are not copyrightable — but lifting a function body re-triggers MIT attribution and defeats the entire reason this repository exists.

`attr.go` in particular is written against `log/slog`'s documented handler contract (`$(go env GOROOT)/src/log/slog/handler.go`, the `Handle` and `ReplaceAttr` doc comments) and validated by `testing/slogtest`. That is what makes it defensible on its own terms rather than as a paraphrase of `slog-common`.

`LICENSE` is ISC — Better Stack's house licence for clients — under the actual author's copyright. Ship their licence *text* so adoption is frictionless; do not assert their copyright over code they have not accepted.

## Repository conventions

- **The exported API is not frozen**, but it is close to it. This is a new module with its own v0, so breaking changes are cheap now and expensive after the first tag. Decide API shape before tagging v0.1.
- No CI, no Makefile, no `example/` yet — a deliberate scope choice, not an oversight. `example/` is scheduled for v0.3.
- No git remote is configured.
