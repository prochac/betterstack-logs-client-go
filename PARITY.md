# BetterStack client parity checklist

Target: a Go client good enough for BetterStack to adopt as first-party. This tracks what their existing official clients do, and what this repo would need to match.

All defaults below are quoted from the sources listed at the bottom. Where a doc page is silent, it says so — absence here means "not documented", not "not implemented".

## 1. The ingestion contract

From the [HTTP REST API docs](https://betterstack.com/docs/logs/http-rest-api/):

| Aspect | Spec |
| --- | --- |
| Request | `POST https://$INGESTING_HOST` (this repo defaults to `https://in.logs.betterstack.com/`) |
| Auth | `Authorization: Bearer $SOURCE_TOKEN` |
| Body encodings | `application/json` (single object **or** array), `application/x-ndjson`, `application/msgpack` |
| Timestamp | `dt` — UNIX seconds/millis/nanos, RFC 3339 / ISO 8601 string, or MessagePack timestamp ext. Unparseable values are stored as a plain string and the server's reception time is used instead |
| Attributes | Arbitrary nested objects allowed alongside `message` |
| Size limits | 10 MiB compressed per request; 10 MiB uncompressed per record, **≤100 KiB per record recommended** |
| Rate limits | "There is no limit to the number of requests" |

Response codes and the correct client reaction:

| Code | Meaning | Retryable? |
| --- | --- | --- |
| `202` | Accepted | — |
| `402` | Quota exceeded | No — backoff hard / surface to user |
| `403` | `Unauthorized` (bad token) | **No** — retrying burns quota forever |
| `406` | `Couldn't parse JSON content.` | **No** — client-side bug, drop and report |
| `413` | Payload reached size limit | **No** as-is — split the batch and resend |
| 5xx / network | — | Yes, with backoff |

The API page does not document 401/429/5xx or a `Content-Encoding` header, but the official Node transport sends `Content-Encoding: gzip`, and the 10 MiB limit is specified on *compressed* data — so gzip is accepted in practice.

**Probed against the live endpoint, 2026-08-06** (`in.logs.betterstack.com`, 91.98.89.252):

- **Auth failure is `401`, not the documented `403`.** Both a missing `Authorization` header and a bogus bearer token return `401` with body `{"error": "Unauthorized"}`, identically over HTTP/2 and HTTP/1.1. A client that classifies only the documented codes puts the most common misconfiguration in the retry path.
- **Auth is checked before the body is parsed** — a bogus token with malformed JSON still returns `401` — so `406` is only reachable with a valid token.
- **TLS 1.3, ALPN negotiates `h2`.** Requests complete as `HTTP/2.0`, so concurrent uploads multiplex over one TCP connection. Note that Go disables automatic HTTP/2 on any `Transport` with a custom `DialContext`; measured, the same config negotiates HTTP/1.1 without `ForceAttemptHTTP2: true` and HTTP/2 with it.
- No `Retry-After` header on `401`. Not yet observed on a throttled or 5xx response.

## 2. What the official clients do

### JavaScript / Node — `@logtail/node`, the most complete reference

Defaults straight from `packages/core/src/base.ts`:

| Option | Default | Notes |
| --- | --- | --- |
| `endpoint` | `https://in.logs.betterstack.com` | |
| `batchSize` | `1000` | flush trigger by count |
| `batchSizeKiB` | `0` | flush trigger by serialized size, 0 = disabled |
| `batchInterval` | `1000` (ms) | flush trigger by time |
| `retryCount` | `3` | |
| `retryBackoff` | `100` (ms) | minimum wait before retry |
| `syncMax` | `5` | max concurrent in-flight requests |
| `syncQueuedMax` | `100` | queued requests beyond this are **dropped** |
| `burstProtectionMilliseconds` | `5000` | burst window |
| `burstProtectionMax` | `10000` | max logs accepted per window |
| `ignoreExceptions` | `false` | swallow send errors (takes precedence) |
| `throwExceptions` | `false` | raise send errors to the caller |
| `contextObjectMaxDepth` | `50` | + `contextObjectMaxDepthWarn: true` |
| `contextObjectCircularRefWarn` | `true` | |
| `sendLogsToConsoleOutput` | `false` | mirror to stdout |
| `sendLogsToBetterStack` | `true` | kill switch, useful in tests |
| `captureStackContext` | `true` | auto file/line/method |
| `timeout` (node only) | 30 s in transport | 0 disables |
| `useIPv6` (node only) | IPv4 (`family: 4`) | |

Transport: MessagePack → gzip → `POST`, `User-Agent: logtail-js(node)`, non-2xx rejects. `flush()` is public and documented as required before process exit.

### Java — `com.logtail:logback-logtail`

| Option | Default |
| --- | --- |
| `ingestUrl` | `https://in.logs.betterstack.com` |
| `batchSize` | `1000` |
| `batchInterval` | `3000` ms |
| `maxQueueSize` | `100000` — "messages over the limit will be dropped" |
| `maxRetries` | `5` |
| `retrySleepMilliseconds` | `300` |
| `connectTimeout` | `5000` ms |
| `readTimeout` | `10000` ms |

Plus `mdcFields` / `mdcTypes` (string, boolean, int, long) for context enrichment, `appName` for indexing, `objectMapperModule` for custom Jackson serializers, and JSON-tail parsing into a `message_json` field.

### Erlang

| Option | Default |
| --- | --- |
| `upload_batch_max_size` | `50` |
| `upload_batch_inteval_ms` | `5000` (typo is theirs) |
| `upload_failed_retry_count` | `3` |
| `upload_failed_retry_delay_ms` | `1000` |
| `http_pool_options.timeout` | `15000` ms |
| `http_pool_options.max_connections` | `10` |
| `extra_fields` | — key/value pairs appended to **every** event |

### Ruby / Rails, Python, PHP, .NET — thinner docs, but consistent themes

- **Flush on shutdown is a first-class, documented concern in every one of them**: Ruby `logger.close`, .NET `Log.CloseAndFlush()`, PHP a `pcntl_signal` handler for `SIGINT`, Python "use `sys.exit()` not `os._exit()`", Erlang `init:stop()`, JS `logtail.flush()`.
- **Scoped context blocks**: Ruby `Logtail.with_context({user: {id: 123}})`, Python `with logtail.context(user={'id': 123}):`.
- **Send-time filtering hooks**: Ruby `config.logtail.filter_sent_to_better_stack { |log_entry| }` and parameter/header redaction filters.
- **Structured payload nesting**: .NET puts structured data under `context.properties` (NLog) / `properties` (Serilog).
- Batching/retry/timeout internals are **not documented** for Ruby, Python, PHP or .NET.

## 3. Gap checklist for this repo

Ordered by what blocks adoption. Current state is one goroutine + one `http.Client` + one POST per record (`handler.go:88-99`, `handler.go:122-155`).

### Blockers

- [ ] **Batching.** Flush on count / interval / serialized size, matching the three-trigger model. Suggested defaults, splitting the difference between Java (1000 / 3000 ms) and JS (1000 / 1000 ms): `BatchSize: 1000`, `BatchInterval: 3s`, `BatchSizeBytes` opt-in. Requires shared state across handlers derived by `WithAttrs`/`WithGroup` — put a `*batcher` in `Option` or a shared struct so all clones feed one queue.
- [ ] **`Flush()` / `Close()`.** Every official client has this and documents it as mandatory before exit. `example/example.go` currently `time.Sleep(1 * time.Second)` instead. Blocks writing real tests, too.
- [ ] **Bounded queue with an explicit drop policy.** Java drops over `maxQueueSize: 100000`; JS drops over `syncQueuedMax: 100`. Today the code spawns unbounded goroutines — a logging burst is unbounded memory and unbounded sockets.
- [ ] **Retry with backoff, and a retryable/terminal split.** Retry 5xx and network errors; never retry 403/406. Suggested `MaxRetries: 5`, `RetryBackoff: 300ms` (Java's numbers).
- [ ] **Check the response status.** `send` ignores `resp.StatusCode` entirely (`handler.go:147-154`), so 403 (bad token) is indistinguishable from success. Nothing else on this list works without it.
- [ ] **Reuse connections.** A fresh `http.Client` per record (`handler.go:123`) means no keep-alive. One shared client with a tuned `http.Transport` — Erlang's pool is `max_connections: 10`.
- [ ] **Fix the ignored timeout.** `send` hardcodes `10 * time.Second` on the client and ignores its own `timeout` parameter (`handler.go:124`).

### Parity features

- [ ] **Error policy option.** JS has `ignoreExceptions` / `throwExceptions`; Go's idiom is `OnError func(error)` plus a dropped-record counter. `Handle` returning `nil` unconditionally is fine, but the user needs *some* channel for delivery failures.
- [ ] **gzip.** `Content-Encoding: gzip` — the 10 MiB limit applies to compressed bytes, so this directly multiplies throughput.
- [ ] **MessagePack.** What the Node transport actually uses. Lower priority than gzip; keep JSON the default and make the codec pluggable (`Option.Marshaler` is already close, but it needs to carry a Content-Type).
- [ ] **Split oversized batches / guard oversized records.** Enforce 10 MiB per request; warn or truncate past the recommended 100 KiB per record.
- [ ] **Separate connect vs. request timeouts.** Java: `connectTimeout: 5000`, `readTimeout: 10000`.
- [ ] **`ExtraFields` global attributes.** Erlang's `extra_fields`, Java's `appName`. Partly covered by `logger.With(...)`, but a handler-level option matches the other clients and applies before any `With` chain.
- [ ] **Send-time filter hook.** Ruby's `filter_sent_to_better_stack`. A `func(record) bool` predicate, distinct from level filtering.
- [ ] **Console mirroring + kill switch.** JS `sendLogsToConsoleOutput` / `sendLogsToBetterStack`. Cheap, and makes local dev and tests sane.
- [ ] **User-Agent.** Currently sends the bare `name` const (`handler.go:145`); the convention is `<lib>/<version>`, cf. `logtail-js(node)`.
- [ ] **Burst protection.** JS-only (`10000` logs / `5000` ms). Lowest priority — the bounded queue covers most of the same failure.

### Wire-shape questions to settle

- [ ] **`ContextKey` mismatch.** `converter.go:10` sets `ContextKey = "extra"`, but the README documents `slogbetterstack.ContextKey = "context"`. The official clients nest structured data under `context` (.NET uses `context.properties`). One of the two is wrong and it's a user-visible schema decision — worth confirming with BetterStack before v2.
- [ ] **`dt` encoding.** Currently `record.Time.UTC()` through `encoding/json`, i.e. RFC 3339 nanos — valid per the API. Verify sub-microsecond precision survives, since the docs' examples stop at microseconds.
- [ ] **Flat `logger.name` / `logger.version` keys.** Dotted top-level keys are unusual; confirm this is what BetterStack wants versus a nested object.

## 4. Prior art: the `alistairjevans` fork

Two commits ahead of upstream (`8de39c8` "Add batching and compression support", `2f0c410` "Self-review feedback"): `batcher.go` (+268), `handler.go` (+69/-59), `handler_test.go` (+478), README and example. Worth mining, not worth adopting wholesale.

### Take

- **The shared-batcher structure.** One `*batcher` in the handler, propagated by `WithAttrs`/`WithGroup` to every clone. This is the right answer to the immutability problem in §3.
- **The concurrency fix, which is independent of batching and should be backported on its own.** Upstream's `append(h.attrs, fromContext...)` (`handler.go:90`) appends into a slice shared by every handler clone. It needs two conditions to bite — `AttrFromContext` configured (so there is something to append) and `h.attrs` carrying spare capacity from `AppendAttrsToGroup` — but when both hold, two concurrent `Handle` calls write the same backing array. A genuine data race, present today. The fork fixes it with `slices.Clip(h.attrs)`.
- **Response-body drain before close** (`io.Copy(io.Discard, io.LimitReader(resp.Body, 1MiB))`) so the connection is actually reusable — easy to forget, and keep-alive silently doesn't work without it.
- **Reused `gzip.Writer` + output buffer**, safe because the sender goroutine is the only writer.
- **The test suite.** 11 tests over an `httptest` recorder covering each flush trigger, close-flush, concurrent close, concurrent handle, custom marshaler, and batcher sharing across `WithAttrs`/`WithGroup`. Straightforwardly reusable.
- **A 5 MB uncompressed batch cap**, conservative against the 10 MiB compressed limit.

### Leave

- **The `runtime.SetFinalizer` machinery.** The `batcher`/`batcherState` split exists *only* so the finalizer can fire, and it forces a `runtime.KeepAlive(h.batcher)` into the hot path of `Handle`. The root cause is that `NewBetterstackHandler()` returns `slog.Handler`, so `Close` is unreachable without a type assertion — the fork's own README tells users to write `defer handler.(*slogbetterstack.BetterstackHandler).Close()`. The finalizer is compensation for an awkward API, and its own doc comment concedes finalizers don't run at process exit, i.e. it misses the case that actually matters. **Fix the API instead**: return a concrete `*Handler` from the constructor (see §7 — no compatibility constraint blocks this) and delete ~25 lines of finalizer subtlety.
- **The two-timer `MinBatchWait` / `MaxBatchWait` debounce.** No official client has this — Java (`batchInterval: 3000`), JS (`batchInterval: 1000`) and Erlang (`upload_batch_inteval_ms: 5000`) each ship exactly one interval. The min-timer is also inert precisely under sustained load, since every new record pushes `minDeadline` forward. Its one real benefit — a lone log line shipping in 1 s instead of 3 s — is obtained just as well by a single interval set to 1 s. Collapse to one `BatchInterval`.
- **The parallel `raws [][]byte` bookkeeping.** Marshaling each record separately to track batch bytes, then hand-assembling the JSON array in `assemble()`, is defensible alone — but with a custom `Marshaler` it marshals every record twice and leaks the caveat "MaxBatchByteSize is measured on the default JSON encoding" into the public option docs. Appending into a single `bytes.Buffer` as records arrive gets the same byte accounting without the second slice or the caveat.

### Still missing after the fork

- **Errors are discarded anyway.** `send` carefully checks `resp.StatusCode >= 400` and builds an error — and the only caller writes `_ = b.send(body)`. A bad token is still silent. Nothing on this list matters more than wiring that to an `OnError` hook.
- **No retry.** Still zero, against JS 3 / Java 5 / Erlang 3. One transient 5xx silently drops up to 1000 records.
- **`enqueue` blocks the application when the buffer fills** (`2*MaxBatchSize` slots). Java and JS both *drop* on overflow instead. Compounded by a single synchronous in-flight request (JS allows `syncMax: 5`): during an outage, each batch stalls for `Timeout` while the buffer backs up, and then application goroutines block inside `Handle`. Blocking the app on a logging outage is the wrong default.
- **No dropped-record accounting** on either the post-`Close` path or overflow.
- **No 413 batch-splitting**, and no distinction between retryable and terminal statuses (§1).

## 5. How to surface errors (settles the `OnError` item in §3)

`Handle`'s error return is a dead end for delivery failures. The stdlib says so outright in `log/slog/handler.go`:

> `[Logger] discards any errors from Handle. Wrap the Handle method to process any errors from Handlers.`

and `logger.go:256` / `logger.go:276` are literally `_ = l.Handler().Handle(ctx, r)`. (The lone exception is `handlerWriter` at `logger.go:102`, the `slog.NewLogLogger` bridge, where the error does reach `log.Logger`.)

So the error return exists for **middleware**, not applications — `slogmulti.RecoverHandlerError(func(ctx, record, err))` is the in-family realisation of the stdlib's "wrap the Handle method", and `slogmulti.Failover` needs it to decide when to fall through to the next handler.

Batching splits errors into two classes that must be surfaced differently:

- **Synchronous / local** — marshal failure, closed handler. Return these from `Handle`. `slog-datadog` does exactly this (`return err` on marshal error) even though `Logger` drops it, because it keeps the slog-multi middlewares composable. Cheap to preserve; do it.
- **Asynchronous / delivery** — network failure, non-2xx, retry exhaustion, queue overflow. By the time these happen `Handle` has long since returned `nil`, and the failing record is not the one on the stack. No middleware can observe them. These need an out-of-band channel.

Three established shapes for that channel:

| Pattern | Example | Trade-off |
| --- | --- | --- |
| Global error handler | OTel `otel.Handle(err)` → `otel.SetErrorHandler`, default logs to stderr; used by `sdk/log`'s `BatchProcessor` (`batch.go:107`, `:211`) | Zero config, but global mutable state |
| Per-instance error sink | zap `zap.ErrorOutput(zapcore.WriteSyncer)`, stderr by default | Explicit, no globals |
| Callback | `slogmulti.RecoveryFunc(ctx, record, err)` | Most flexible; matches this repo's option-struct style |

**Recommendation**: `Option.OnError func(error)`, defaulting to a stderr write. Match `slogmulti.RecoveryFunc`'s argument order if a record is ever included.

Two rules that fall out of the survey:

- **Never report a logging error through the logger.** It recurses. This is precisely why zap keeps a separate `WriteSyncer` and OTel a separate global handler.
- **Aggregate, don't fire per record.** OTel's `logDroppedRecords()` reports a *count* — `global.Warn("dropped log records", "dropped", d)` plus a `ProcessedQueueFull` metric — rather than one callback per lost record. During an outage the per-record shape is an error storm that is itself a denial of service.

On blocking: nothing in the wild blocks the application. OTel's `OnEmit` enqueues and returns immediately, dropping when the queue is full (`batch.go:271-280`); Java drops past `maxQueueSize`; JS drops past `syncQueuedMax`. `Handle` runs on the caller's goroutine in its critical path, so blocking there converts a BetterStack outage into an application outage. Bounded queue, drop on overflow, count the drops — and note that `Close`/`Flush` is the one place a real error *can* be returned meaningfully, because the caller has a stack to receive it.

## 6. OTel's layering, and how much of it to copy

Yes — the transport is a separate object handed to the bridge as an option:

```go
otelslog.NewHandler("my/pkg", otelslog.WithLoggerProvider(provider))
```

The full stack:

| Layer | Package | Responsibility |
| --- | --- | --- |
| `otelslog.Handler` | `contrib/bridges/otelslog` | slog→OTel record conversion. Nothing else. |
| `log.Logger` | `otel/log` | Bridge-author API; `Emit(ctx, Record)` |
| `LoggerProvider` | `otel/sdk/log` | Owns the pipeline; `Shutdown` / `ForceFlush` live here |
| `Processor` | `otel/sdk/log` | `Enabled` / `OnEmit` / `Shutdown` / `ForceFlush` — `BatchProcessor` (queue, batching, drop accounting) |
| `Exporter` | `otel/sdk/log` | `Export(ctx, []Record) error` / `Shutdown` / `ForceFlush` — the wire, encoding, **and retry** |

Four details worth stealing outright:

1. **`NewHandler` returns `*Handler`, not `slog.Handler`** — independent confirmation of the §4 fix for this repo's `Close` ergonomics.
2. **`Handle` is two lines and returns `nil` unconditionally**: `h.logger.Emit(ctx, h.convertRecord(record)); return nil`. Zero I/O on the caller's goroutine.
3. **Lifecycle belongs to the transport object, not the handler.** The caller already holds a `provider` they must `Shutdown`. This dissolves the fork's finalizer problem completely — no type assertion, no `SetFinalizer`, because the thing needing cleanup was never hidden behind an interface in the first place.
4. **Retry lives in the exporter, stated as a contract**: "All retry logic must be contained in this function. The SDK does not implement any retry logic." Batching policy and wire policy stay separate. `otlploghttp`'s default: retry 5 s after a retryable error, exponential, 1 minute total — and *server-supplied backoff wins over local config*, which matters for honouring `Retry-After`.

**Do not copy the layer count.** Four layers exist because OTel is vendor-neutral and must swap exporters and processors. A single-vendor sink needs two objects, not four:

- a `Client` — batching, gzip, HTTP, retry, `Flush`/`Close`; constructed and owned by the caller
- a `Handler` — conversion only; holds a `*Client`

`slog-datadog` already establishes the in-family shape for this: its `Option.Client *datadog.APIClient` takes the transport as a field. So `Option.Client *Client`, with a nil value meaning "build a default one and let the handler own it", is both the OTel lesson and consistent with the sibling libraries.

## 7. What dropping backward compatibility unlocks

Upstream's "no breaking changes before v2.0.0" is a promise to *upstream's* users. This fork is headed for a new module path under a new owner, which makes it a new module with a fresh v0/v1 — nothing to break. Every item below is cheap now and expensive after the first official release, so decide them before tagging.

1. **Constructor returns a concrete type, and an error.** `func (o Option) NewBetterstackHandler() slog.Handler` becomes `func New(...) (*Handler, error)`. Two fixes in one: `Close` stops needing a type assertion (§4, §6), and `panic("missing Betterstack token")` — unacceptable in a vendor SDK, since a missing env var should not crash the host application — becomes a returned error. Consider functional options (`New(token string, opts ...Option)`), which is what Go vendor SDKs and `otelslog` ship; the `Option`-struct-with-method style is a samber-family convention this module is leaving.
2. **Fix the brand capitalisation.** The code says `BetterstackHandler`, `BetterstackEndpoint`, `NewBetterstackHandler`; the product is "Better Stack", and their own packages brand as `BetterStack.Logs.NLog` / `BetterStack.Logs.Serilog`. Lowercase `s` in an official module's exported identifiers is the sort of thing that never gets fixed after v1.
3. **Align option names with the sibling clients.** `Token` → `SourceToken` (every other client calls it a source token); `Endpoint`/`IngestingHost`; `BatchSize`, `BatchInterval`, `MaxQueueSize`, `MaxRetries` per §2. Cross-language consistency is itself an adoption criterion — a BetterStack user moving from Java should recognise the knobs.
4. **Settle the payload shape** — the `ContextKey = "extra"` vs `"context"` split and the flat `logger.name` / `logger.version` keys (§3). No longer a migration problem, just a decision.
5. **Go to zero dependencies.** `slog-common` pulls `samber/lo` and `golang.org/x/text` into the module graph. Only eight helpers are actually used — `ContextExtractor`, `AppendAttrsToGroup`, `AppendRecordAttrsToAttrs`, `ReplaceError`, `Source`, `ReplaceAttrs`, `RemoveEmptyAttrs`, `AttrsToMap` — a few hundred lines to inline. A stdlib-only logging client is a genuine selling point for a vendor and removes third-party supply-chain surface from every customer's build.
6. **Drop the `sed`-replaced `VERSION` placeholder** in `version.go` for `runtime/debug.ReadBuildInfo()`, so a module built from source reports a real version instead of the literal string `VERSION`.
7. **Ungate the release workflow**, currently `if: github.triggering_actor == 'samber'`.
8. **Package and module naming** — see §8, which resolves this from their actual repos.

Unchanged by any of this: the go 1.21 floor, and the MIT notice (© 2023 Samuel Berthe), which must be retained in derivative distributions — add Better Stack's copyright line alongside it rather than replacing it.

## 8. Greenfield, naming, and licence — resolved from Better Stack's own repos

**There is no Go client.** Neither org has one: `logtail` holds 13 client repos (JS, Python, Ruby ×3, PHP, Java, .NET, Fluentd, Lambda) and `BetterStackHQ` holds 31, and not one is a logging client for Go. The gap this module targets is real and unclaimed.

**They already ship Go**, so a Go client is not foreign tooling to them: `BetterStackHQ/terraform-provider-better-uptime` and `terraform-provider-logtail` are both Go and actively pushed.

### Naming, from their own convention

| Era | Convention | Examples |
| --- | --- | --- |
| Legacy (Logtail brand) | `logtail/logtail-<language>` | `logtail-js`, `logtail-python`, `logtail-ruby` |
| Current (Better Stack brand) | `BetterStackHQ/logs-client-<target>` | `logs-client-nlog`, `logs-client-serilog` — the replacements for the archived `logtail/logtail-dotnet` |

Their Go module paths are **lowercase** regardless of the org's display casing: `module github.com/betterstackhq/terraform-provider-logtail`.

So the target they would land on is `github.com/betterstackhq/logs-client-go`. Build it now as **`github.com/<owner>/logs-client-go`**, package **`betterstack`** — adoption is then a one-line module-path change with every identifier and import name unchanged. Reads as `betterstack.New(...)`, `betterstack.Handler`. `logs-client-go` (language) rather than `logs-client-slog` (framework) matches the legacy language-named repos; .NET is framework-named only because it has two competing logging frameworks, whereas Go has one obvious target.

### Licence

Better Stack's house licence for clients is **ISC**, `Copyright (c) <year>, Better Stack, Inc.` — `logtail-js` is detected as ISC, and `logs-client-serilog` / `logs-client-nlog` carry the identical ISC text (GitHub reports `NOASSERTION` only because the file is titled "License" rather than "ISC License"). Not perfectly uniform — `logback-logtail` is MIT, and the Terraform providers are Apache-2.0 per HashiCorp norms — but ISC is the default for the newer clients.

### Recommendation: greenfield

Start clean rather than carrying the fork base.

- **There is very little to carry.** The whole repo is ~200 lines of substance (`handler.go` 155, `converter.go` 40), and most of it is thin glue over `slog-common`, which §7 wants dropped anyway. The genuine asset produced so far is this document, not the code.
- **Clean provenance.** The current `LICENSE` is MIT © 2023 Samuel Berthe and must be retained in any derivative. Greenfield lets the module ship ISC © Better Stack from the first commit, matching their house licence, with nothing for their legal review to reconcile.
- **The module path has to change regardless**, so there is no continuity to preserve — no import path, no tag history, no downstream users.
- **It frees the API from samber-family idioms** (`Option`-struct-with-method constructor, the `slogcommon` dependency) that §7 already recommends leaving behind.

One boundary worth keeping clean: reimplementing the fork's *ideas* — shared batcher, response drain, reused gzip writer — is fine, ideas aren't copyrightable. Copying its 478-line `handler_test.go` verbatim would make the result a derivative work and re-trigger MIT attribution. Write the tests fresh against the same behaviours; §4 lists what they should cover.

What survives the reset: this document, the ingestion contract in §1, the defaults table in §2, and the design conclusions in §5–§7.

## Sources

Fetched 2026-08-06:

- <https://betterstack.com/docs/logs/http-rest-api/> — ingestion contract, status codes, size limits
- <https://betterstack.com/docs/logs/javascript/>, `/install/`, `/logging/` — API surface, `flush()`
- `logtail/logtail-js` — `packages/core/src/base.ts` (defaults), `packages/types/src/types.ts` (option docs), `packages/node/src/node.ts` (msgpack + gzip transport)
- <https://betterstack.com/docs/logs/java/> — the most completely documented set of batching/retry/timeout defaults
- <https://betterstack.com/docs/logs/erlang/> — batching, retry, connection pool
- <https://betterstack.com/docs/logs/ruby-and-rails/>, `/python/`, `/php/`, `/net-c/` — lifecycle, context blocks, filtering

For §4 and §5:

- `alistairjevans/slog-betterstack` @ `8de39c8`, `2f0c410` — `batcher.go`, `handler.go`, `handler_test.go`
- Go stdlib `log/slog` — `handler.go` (`Handle` doc), `logger.go:102,256,276` (error discarded)
- `samber/slog-multi` — `recover.go` (`RecoverHandlerError`, `RecoveryFunc`), `failover.go`
- `samber/slog-datadog` — `handler.go`, the closest sibling with batching + `Flush`
- `open-telemetry/opentelemetry-go` — `sdk/log/batch.go` (drop accounting, `otel.Handle`), `handler.go` (`SetErrorHandler`)
- `uber-go/zap` — `options.go` (`ErrorOutput`)

For §6:

- `opentelemetry-go-contrib` — `bridges/otelslog/handler.go` (`NewHandler`, `WithLoggerProvider`, `Handle`)
- `opentelemetry-go` — `sdk/log/exporter.go`, `sdk/log/processor.go` (interfaces), `exporters/otlp/otlplog/otlploghttp/config.go` (`WithRetry` defaults)
