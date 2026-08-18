# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project goal

A **Better Stack logging client for Go**, built to be adopted by Better Stack as first-party rather than to live as a third-party wrapper. There is no Go client in either of their orgs (`logtail`, `BetterStackHQ`), so the slot is unclaimed — see PARITY.md §8.

Module path is `github.com/prochac/betterstack-logs-client-go`, package `betterstack`. The tail mirrors their `BetterStackHQ/logs-client-<target>` convention; the `betterstack-` prefix is there only because a personal namespace has to name the vendor the client is *for*, and it drops away under theirs. Adoption is then a one-line change to `github.com/betterstackhq/logs-client-go` with every identifier and import name unchanged.

The `User-Agent` is `logs-client-go/<version>` and deliberately does **not** track the repository name — it is the `<lib>` half of the sibling clients' convention (DESIGN §4), so `clientName` in `version.go` stays unprefixed while `modulePath` must match `go.mod` exactly.

This repository is a **greenfield rewrite**, not a fork. It replaced `prochac/slog-betterstack` (itself a fork of `samber/slog-betterstack`), which is now abandoned.

**Read [DESIGN.md](./DESIGN.md) before changing behaviour.** It is the spec: the two-object shape, every option and its default, the concurrency model, the wire format, the retry and error policy, and the milestone split. [PARITY.md](./PARITY.md) is the research behind it — the ingestion API contract, the documented defaults from the official Java/Erlang/JavaScript clients, and live probes of the endpoint. DESIGN.md cites PARITY.md rather than repeating it.

Sections of DESIGN.md marked **[amended]** were corrected during implementation, when writing the code showed the original decision was wrong or self-contradictory. If you find another such case, fix the code *and* amend DESIGN.md in the same change — the two must not drift.

## Status

**v0.1 is complete** (DESIGN §10): `Client` + `Handler`, NDJSON, batching on all three triggers, bounded queue with drop accounting, `Flush`/`Close`, HTTP status classification, retry with backoff, connection reuse, `OnError`, `Stats`, gzip. Passes `testing/slogtest`.

**v0.2 is complete**: 413 batch splitting (and the local hard-limit check splits too), `WithExtraFields`, `WithFilter`, `WithDryRun`, `WithRetryCeiling`, the `JSONArray` encoder, and `README.md`. "Separate connect/request timeouts", listed under v0.2 in DESIGN §10, had in fact shipped with v0.1's transport. `Encoder.AppendRecord` lost its `index` parameter — see DESIGN §4's amendment before reinstating it.

**In progress, unreleased:** the `ObjectAppender` seam and the `fastjson` subpackage (see the JSON paragraphs below). This is a prototype of the placement decision, not a tagged release. Also in the tree, prototype status and undecided: a per-client **record-buffer pool** — `Client.recordBufs`, with the queue carrying `*[]byte` and the sender returning each buffer via `putRecordBuf` after copying. Measured: ~27% fewer allocations per log call with `fastjson` (21→15 allocs, −22% B/op), wall-clock unchanged; the win is GC pressure, not latency. Keep or revert is an open call.

**v0.3 is complete.** `example/` has landed — a runnable HTTP service demonstrating context extraction and graceful shutdown, verified against a local sink. **Burst protection** has landed as `WithBurstProtection(max, window)`: a token bucket in `limiter.go`, checked in `Enqueue` *before* the encode, counted `DroppedBurst`. DESIGN §10 named it but never specified it; §2 and §3 now carry the spec, including why it is **opt-in** where every other option ships the sibling clients' default.

**MessagePack was removed, deliberately — do not re-add it.** It shipped in v0.3 as `MsgPack(Marshaler)` and was taken out (untagged) because it gained nothing: measured on the payload this library actually produces, encode time **tied** the in-tree, dependency-free `fastjson` appender, and the gzipped body — the surface the API's 10 MiB limit and the bill are measured on — came out **34% larger** than NDJSON, because a batch's repeated key sequence is the compressor's best input. The exactness pitch also does not survive: `encoding/json` writes `int64`/`uint64` as exact decimal digits (only a float64-parsing decoder loses them, and such a pipeline degrades a MessagePack int64 downstream just the same), and a value that must survive every pipeline goes as a string. An append-shaped `MsgPackWith` seam was prototyped and benchmarked before deciding; it tied at the whole-log-call level, settling that the format — not the seam — was the limit. DESIGN §4's removal amendment carries the full record, including the +8.73 MB `ugorji` binary probe that killed bundling a codec back when the feature existed. If someone asks for MessagePack, point them at that amendment: every argument for it was measured and came up empty.

**The JSON record encode is one implementation, `json.go`, over the `encoding/json` v1 API, with no build tag.** A tagged `encoding/json/v2` variant existed briefly and was **reverted** — do not reintroduce it without reading DESIGN §4's "encoding/json/v2 was tried and reverted". The short version: v2 does not sort map keys, `Deterministic` restores that but is not free the way v1's always-sort is, and with it v2 came to **2 fewer allocations for 43% more encode time** — against two build-tagged files that had to be exact complements, a four-configuration test matrix, and a test whose only job was to notice the experiment graduating and silently disabling the file. Reverting also removes the graduation hazard outright: the v1 API keeps sorting keys, substituting U+FFFD and writing `Duration` as nanos when v2 becomes the engine underneath, because the compatibility promise pins all three.

**Sorted keys are load-bearing and were lost once.** Unsorted keys cost ~54% on the *gzipped* body, because every record in a batch shares its key sequence and that is the compressor's biggest back-reference source; the API's limit is measured on compressed bytes. It shipped broken because key order is invisible to every decoder and no test looked at bytes. `TestJSONKeysAreSorted` and `fastjson`'s byte-level `TestAppendObjectMatchesEncodingJSON` are the guards — the latter was verified to fail against an unsorted appender.

**The reflection-free encoder shipped, as the `fastjson` subpackage.** It was written first and declined as too much to ask every user to trust, then placed rather than reverted: `ObjectAppender` is the seam (`NDJSONWith`, `JSONArrayWith`), `fastjson.AppendObject` is the implementation, and the main package does not import it — so a binary that does not opt in does not link it, verified with `go tool nm`. It is worth 1533→383 ns and 15→0 allocations on the encode, 4217→2449 ns and 27→16 on `Handle`. It sorts keys into a stack array, so unlike `encoding/json/v2`'s `Deterministic` that costs it nothing — do not replace it with `slices.Sorted(maps.Keys(m))`, which is Go 1.23 against the 1.21 floor and heap-collects besides. Read DESIGN §4's amendment before touching it. Two things it records that are easy to get wrong on your own: a build tag was considered and is worse (global to the application build, invisible at the call site, and it doubles the tag matrix), and a `Marshaler func(any) ([]byte, error)` seam (the shape the removed `MsgPack` used) does **not** work for JSON — `goccy/go-json` buys 20% on a `map[string]any` and `json-iterator` is slower than the standard library, because they cache reflection over concrete struct types and a map of interfaces gives them nothing to cache. That is why `ObjectAppender` is append-shaped.

Two items left v0.3 rather than being implemented, both recorded in DESIGN §10: benchmarks were listed there but shipped with v0.1, and **mirroring to a second `slog.Handler` is struck** — `slog.MultiHandler` (Go 1.26) and `samber/slog-multi` already do it, and reimplementing it inside the handler would duplicate the `log/slog` contract surface for no new capability. Do not add a `WithMirror`; README's "Logging to more than one place" is the answer.

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

Plus `golangci-lint`, configured by `.golangci.yml` (golangci-lint v2 schema; `golangci-lint config verify` checks it). The repository lints and formats clean, so any output is a regression:

```sh
golangci-lint run ./...
golangci-lint fmt --diff ./...          # gofumpt -extra plus goimports; empty output
```

Read the header of `.golangci.yml` before adding a linter or an exclusion: it records why `gosec`'s G115 is off wholesale, why `goconst` ignores tests, and the two blind spots the run has.

**There are no build tags in this module, and that is worth keeping.** One `go test -race ./...` covers the library; the four-configuration matrix that used to live here went away with `json_v2.go`. A second toolchain is still worth a pass before a release, since the standard library is where behaviour can move underneath this code:

```sh
go test -race ./...          # the suite
go1.27rc3 test -race ./...   # same suite, v2 engine under the v1 API
```

`json_contract_test.go` is what makes the second run meaningful: it asserts the behaviours json/v2 changes by default — invalid UTF-8 survives, `Duration` encodes as nanos, keys come out sorted — which are exactly the properties Go's compatibility promise obliges the v1 API to preserve when v2 becomes the engine underneath it. If one of them ever fails on a new toolchain, that is a stdlib regression and not a local one.

**`go vet` is not optional.** `go.mod` declares `go 1.21` and the local toolchain is much newer. The compiler will happily build a post-1.21 stdlib symbol; only `go vet`'s `stdversion` analyzer catches it. Verified: `slices.Concat` compiles and passes tests, and `go vet` reports `requires go1.22 or later (module is go1.21)`.

## Architecture

Two packages, standard library only: `betterstack` at the root, and `fastjson`, which the root **does not import** — it is opt-in and must stay unlinked for anyone who does not ask for it. One dependency, test-only and never built by anything that imports the module: `go.uber.org/goleak`.

- **`client.go`** — the public `Client` API, all `ClientOption`s and their defaults, `clientConfig.validate`, `Stats`/`counters`, `Enqueue`/`Flush`/`Close`, the `batch` and its `split`, and the `packer` (the gzip `compressor`, plus a framing scratch that an `IdentityFramer` encoder never causes to be allocated). Opens with the three invariants; read those first.
- **`sender.go`** — the sender goroutine (batch accumulation with record boundaries, the timer state machine, flush triggers, the hard-limit split, the blocking hand-off to the pool, drop summaries) and the `uploadPool` (worker pool plus the flush rendezvous). Drop summaries are paced by a `time.Ticker` the sender owns, and that ticker is received in **both** places the sender waits — its `select` and `handOff` — because both are states in which records drop and no flush completes. That is also why the hand-off's blocking `select` lives in `handOff` rather than in `uploadPool`; the pool keeps only `abandon`, the accounting for a batch that never got a slot.
- **`transport.go`** — the tuned `http.Transport`, the per-goroutine `worker`, one upload attempt (`do`), the retry loop (`uploadBy`), the 413 split (`splitAndSend`), status classification, backoff, and `Retry-After` parsing.
- **`handler.go`** — `Handler`, `HandlerOption`s, and the `slog.Handler` implementation. Depends on the unexported `enqueuer` interface, not on `*Client` directly, so the handler is testable with no network stack.
- **`attr.go`** — the attribute half of the `log/slog` contract: `groupOrAttrs` accumulation, the recursive `appendAttr`, group materialisation, source resolution, context extraction.
- **`converter.go`** — `Converter`, `DefaultConverter`, the reserved payload keys, the record shape.
- **`encoder.go`** — the `Encoder` interface and the NDJSON and JSON-array implementations. Both JSON encoders are written in terms of an `ObjectAppender`; `NDJSON()`/`JSONArray()` supply `appendJSONObject`, which has two build-tagged implementations, and `NDJSONWith`/`JSONArrayWith` take the caller's. `IdentityFramer` is the optional companion an encoder implements to declare `Frame` a no-op — NDJSON does, the packer then skips the framing copy *and* the buffer behind it, and `Frame` is consequently never called on such an encoder. It is deliberately not inherited by embedding an `Encoder`, so a wrapper must re-declare it (DESIGN §4).
- **`fastjson/`** — the opt-in reflection-free `ObjectAppender`, with its differential fuzzing. **The main package must never import it**; that is the whole point of it being a package, and `go tool nm` on `example/`'s binary is how it stays true. Its fallback uses the `encoding/json` **v1** API deliberately: v1's behaviour is pinned by Go's compatibility promise, so the subpackage needs no build tag of its own.
- **`json.go`** — `appendJSONObject` over a pool of `*json.Encoder`s, each paired with the `bytes.Buffer` it writes to. The only implementation, on every toolchain, over the `encoding/json` **v1 API** deliberately: v1's behaviour is pinned by the compatibility promise, which is what lets this file carry no build tag and no options. Encoders go back through `putJSONEncoder`, which drops any whose buffer has grown past 64KB: a `bytes.Buffer` only grows, so one huge record would otherwise pin its capacity for the life of the process.
- **`limiter.go`** — `WithBurstProtection`'s token bucket, held as a single monotone timestamp in one `atomic.Int64`. Unexported in full. Its `now` field is a clock seam, and it exists so the tests are deterministic rather than timed.
- **`errors.go`**, **`version.go`**, **`doc.go`** — error types and the `OnError` funnel; `User-Agent` from build info; the package doc.

## Invariants — do not break these silently

Each of these was arrived at the hard way, and several have a test whose only job is to keep them true.

1. **`queue` and `flushC` are never closed.** Termination is signalled by closing `done`, `shutdown` and `senderDone`, which are only ever *received* from. This makes send-on-closed-channel impossible rather than defended against. Closing `queue` would buy nothing: the sender stops on `shutdown`, not on a drained range loop.
2. **`flushC` is unbuffered.** That is what makes `Flush` racing `Close` return `ErrClosed` instead of hanging on a request nobody will read.
3. **A batch owns its bytes.** The sender reuses both the accumulation buffer and the gzip output buffer, so `flush` copies before dispatch. The fork could hand its reused buffer straight to a send only because that send was synchronous.
4. **No `gzip.Writer` is ever shared.** The buffers live in a `packer`, owned by exactly one goroutine: the sender has one, and an upload worker builds its own lazily the first time a 413 makes it split. Compression is otherwise the sender's alone — in the workers it would be a data race. The split path is the sole exception, and it cannot be avoided: handing the halves back to the sender would deadlock, since the sender's dispatch blocks on the pool the worker occupies.
5. **Backpressure is shed at the queue, and nowhere else.** The batch hand-off blocks. An earlier version dropped there instead and lost ~20% of an ordinary 1000-record burst against a healthy server. `WithBurstProtection` is not a counter-example: backpressure is shedding because the *downstream* cannot keep up, and there is still exactly one of those. The limiter is an admission ceiling the operator declares, enforced whether or not delivery is healthy, and off unless asked for.
6. **`Enqueue` never blocks and never returns an error for a dropped record.** Drops are counted and aggregated into `OnError` summaries. Returning them would fire error middleware once per lost record, which is the storm the aggregation exists to prevent.
7. **`Handle` does no I/O**, and its error is local only: an encoding failure or `ErrClosed`.
8. **Statuses are terminal by default.** Only 408, 429, 5xx and network errors retry. 401 is terminal — that is what the live endpoint actually returns for a bad token, despite the docs naming 403. **413 is terminal but not fatal**: the same bytes are never resent, yet the records are not abandoned — the batch is halved and both pieces sent. Do not move 413 into `isRetryable` to express that; splitting is a separate mechanism, and conflating them makes a loop.
9. **A record's encoding is self-delimiting and position-independent.** `Enqueue` encodes one record at a time, before it is known which batch it will join, and a split re-frames the same bytes into a different batch. This is why `AppendRecord` has no index and why `JSONArray` puts the comma *before* each record for `Frame` to overwrite. A future encoder whose `Frame` prepends a variable-width header must **shift** the batch, never return a re-sliced `batch[k:]`: `pack` calls `Frame` on a buffer it reuses, so a moved start creeps forward by the header width on every batch and grows without bound (learned on the removed MessagePack encoder — DESIGN §4).
10. **The stats identity holds after `Close`**: `Enqueued == Sent + all Dropped*`, given that no `Enqueue` was still running when `Close` was called — that precondition is stated on `Stats` and in DESIGN §5, and it is the one window the identity cannot close without putting two atomics per record on `Enqueue`'s hot path. `TestStatsBalance` asserts it across healthy, rejected, exhausted, splitting, dry-run, overflowing and burst-limited runs. A new drop reason means touching seven places — `Stats`, `counters`, `snapshot`, the `DropReason` iota (append, never insert), `dropSnapshot`, `reportDrops` and `reportFinalDrops` — plus `assertStatsBalance` and `example/`'s `printStats`, which recompute the identity by hand.
11. **`OnError` may be called concurrently**, from the sender and from every upload worker, and a panic inside it must not escape `safeReport`.
12. **The payload never carries a duplicate key; last write wins.** `slog` permits them and `slog.JSONHandler` emits them (golang/go#59365); the attribute tree here is a `map[string]any`, so collisions collapse at build time in the order that makes the more specific statement win. DESIGN §4 "Duplicate keys" records why that is a decision and not just a consequence of using a map. It is load-bearing for the public API: `Encoder.AppendRecord` and `Converter` are both typed on `map[string]any`, and a plain map is the one payload shape every third-party serialiser already knows. Do not switch to forwarding duplicates so the server can resolve them; and note it could not be done in an `Encoder` anyway, since Go randomises map iteration and "keep the first" would have no defined meaning there.

## Testing

- **`goleak.VerifyTestMain` with no `Ignore` options**, deliberately. It is a design constraint, not hygiene: it is what forces the sender and workers to terminate on `Close`, and why `Close` calls `CloseIdleConnections` on a transport it owns. If it fails, fix the lifecycle — do not add an `IgnoreTopFunction` for `net/http`'s connection loops.
- **`recorder_test.go`** is the fixture. Its handler asserts the auth header, content type, gzip round-trip and NDJSON or JSON-array framing on *every* request, so all tests get those invariants for free. Use `newRecorder` / `newTestClient`. `withMaxAcceptedBytes` answers 413 on size rather than to a script, which is how splitting is tested to convergence. Once retries or splitting are involved use `rec.accepted()`, not `rec.records()`: a refused request carried its records too, and counting those makes every record look duplicated.
- **`check`'s content-type switch has a strict `default`** that fails the test. Before it did, an encoder whose content type the recorder did not know contributed no records at all, every `records()`/`accepted()` assertion passed vacuously, and the suite reported success for a format it never looked at. A new format means teaching `check` to decode it; the one deliberate exemption is `countingEncoder`'s `contentTypeUnchecked`, which has no payload worth decoding.
- **`slogtest.TestHandler`, not `slogtest.Run`** — `Run` is Go 1.22. The results mapper in `handler_test.go` remaps `dt`→`time` and `message`→`msg` and hoists `context.*`, which `TestHandler`'s own docs bless.
- **Non-flakiness rules**, applied throughout: never `time.Sleep` to wait for a result (`waitFor` polls at 1 ms, fails at 2 s); `Sleep` only to prove a negative; timing assertions are one-sided **lower** bounds only, since full jitter can make any backoff near zero; every deterministic test sets `WithBatchInterval(time.Hour)` so only the trigger under test can fire; `t.Parallel()` everywhere with a per-test client and server. Clocks are seams, never waits: `withDropReportInterval` shortens the five-second drop-summary period the same way the limiter's `now` replaces its clock, so the periodic path is tested in milliseconds.
- **Always close the client before the server.** `httptest.Server.Close` waits for outstanding requests, and a client still retrying into it will deadlock the cleanup. A gated recorder needs `rec.release()` before `c.Close()`.
- Coverage is ~93%. When adding a feature, check the new code is actually reached — `reportDrops` sat at 0% because its path is gated behind a five-second interval. The standing uncovered remainder is the gzip-failure branches in `split`/`pack`/`splitAndSend`, which need a seam to inject a broken writer; that is why `compress` sits at 71.4%.

## Provenance — this constrains what you may write

The greenfield rewrite only buys clean licensing if no source is copied. `samber/slog-betterstack` (MIT © 2023 Samuel Berthe), `samber/slog-common` (MIT), and the `alistairjevans/slog-betterstack` fork are **prior art to read, not code to move**. Reimplementing an idea is fine — ideas are not copyrightable — but lifting a function body re-triggers MIT attribution and defeats the entire reason this repository exists.

`attr.go` in particular is written against `log/slog`'s documented handler contract (`$(go env GOROOT)/src/log/slog/handler.go`, the `Handle` and `ReplaceAttr` doc comments) and validated by `testing/slogtest`. That is what makes it defensible on its own terms rather than as a paraphrase of `slog-common`.

`LICENSE` is ISC — Better Stack's house licence for clients — under the actual author's copyright. Ship their licence *text* so adoption is frictionless; do not assert their copyright over code they have not accepted.

## Repository conventions

- **The exported API is not frozen**, but it is close to it. This is a new module with its own v0, so breaking changes are cheap now and expensive after the first tag. Decide API shape before tagging v0.1.
- No CI and no Makefile — a deliberate scope choice, not an oversight. `.golangci.yml` is therefore a checklist a human runs, not a gate something enforces.
- **Suppress in the config, not at the site.** `nolintlint` runs with `require-explanation`, `require-specific` and `allow-unused: false`, but the stronger rule is upstream of it: if a check can only ever be answered with a suppression, turn the check off once in `.golangci.yml` with the reason, rather than repeating it at every hit. Two `gocritic` checks are off for exactly that (`hugeParam`, `exitAfterDefer`), as is `gosec`'s G115. That leaves two `//nolint`s in the whole tree — the retry jitter's `math/rand`, and the `Flush(nil)` one test exists to check — and both mark something genuinely local.
- **`gocritic`'s `appendAssign` is enabled, and `attr.go` must keep it satisfied.** It fired on the group-path `append(groups, key)` and was right to: two sibling groups nested in a third borrow the same spare slot in the parent's array, so a `ReplaceAttr` that keeps the slice it was handed sees its path rewritten. `slog.JSONHandler` has the same edge — its group stack is pooled, pushed and popped — and fails the test below, so this is a guarantee *stronger* than the standard library's, not a bug fix. Both sites go through `childPath`, an exact-size copy like `withGroupOrAttrs`, which benchmarks as no change at all. `TestReplaceAttr/the_group_path_is_not_shared_between_siblings` holds it; it has been verified to fail against a plain `append`, and note that the *shape* matters — one nested group does not reproduce it, because nothing overwrites the slot afterwards.
- **`example/` is `package main` inside this module**, so `go build ./...` and `go vet ./...` cover it and it cannot rot. It therefore lives under the same Go 1.21 floor as the library: no `for range int`, no method patterns in `http.ServeMux` (`"GET /x"` is 1.22), nothing else `stdversion` would catch.
- The example must stay runnable with no credentials. `go run ./example` falls back to dry run; `-endpoint` points it at a local sink, which is how its wire output was verified.
- No git remote is configured.
