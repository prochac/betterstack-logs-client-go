# DESIGN.md

The design of the greenfield Better Stack Go client.

[PARITY.md](./PARITY.md) is the research: the ingestion contract, the official clients' defaults, and the reasoning behind the conclusions below. This document is the decisions. Where a choice is contentious, it cites the section that settles it rather than re-arguing it.

> **Amended during v0.1 implementation.** Writing the code surfaced places where this document contradicted itself or the `log/slog` contract. Those sections now carry the resolution inline, marked **[amended]**, with the original reasoning kept where it still stands. The unamended original lives in the `slog-betterstack` repo as a record of the decisions as first made.

## 0. Scope

Greenfield rewrite, not an evolution of the fork (PARITY §8). Nothing in the current tree is carried over as source.

- **Module**: `github.com/prochac/logs-client-go`, so Better Stack's adoption is a one-line path change to `github.com/betterstackhq/logs-client-go`.
- **Package**: `betterstack`. Reads as `betterstack.NewClient(...)`, `betterstack.NewHandler(...)`.
- **Licence**: ISC — Better Stack's house licence for clients — under the actual author's copyright, `Copyright (c) 2026, Tomáš Procházka`. Shipping their licence *text* is what makes adoption frictionless; asserting their copyright over code they have not accepted would be a defect their legal review has to unwind, which is precisely what greenfield exists to avoid (PARITY §8). The holder line is a one-line change on adoption.
- **Go floor**: 1.21, matching what `log/slog` shipped in. No post-1.21 stdlib APIs.
- **Dependencies**: none. Standard library only, test dependencies included where possible.

Because this is a new module in a new repo, upstream's MIT `LICENSE` does not follow it — but that only holds if no upstream source is copied. See §9.

## 1. Two objects

```go
client, err := betterstack.NewClient(sourceToken)
if err != nil { ... }
defer client.Close()

logger := slog.New(betterstack.NewHandler(client))
```

- **`*Client`** owns everything with a lifecycle: the queue, batching, encoding, gzip, HTTP, retry, `Flush`, `Close`, and drop accounting. It knows nothing about `slog`.
- **`*Handler`** owns conversion: `slog.Record` → payload map, attrs, groups, `ReplaceAttr`, source location, context extraction. It holds a `*Client` and does no I/O.

Two objects rather than OTel's four (PARITY §6) — the extra layers exist there for vendor neutrality, which a single-vendor client does not need.

**The client is always explicit; the handler never constructs one.** Every official Better Stack client documents flush-before-exit as mandatory (PARITY §2), so the object that must be closed should be the one the user is holding. This is also what dissolves the `alistairjevans` fork's `runtime.SetFinalizer` machinery (PARITY §4): the fork hid the client inside a handler returned as `slog.Handler`, which made `Close` reachable only by type assertion, which motivated a finalizer that by its own admission doesn't run at process exit. There is no ownership ambiguity here, so none of that is needed.

Both constructors return concrete types. `NewClient` returns an error rather than panicking on a missing token — a vendor SDK must not crash the host application over an unset env var (PARITY §7.1).

`NewHandler` cannot fail and returns `*Handler` with no error.

## 2. Public API

```go
// Client
func NewClient(sourceToken string, opts ...ClientOption) (*Client, error)
func (c *Client) Enqueue(event map[string]any) error
func (c *Client) Flush(ctx context.Context) error
func (c *Client) Close() error
func (c *Client) Stats() Stats

// Handler
func NewHandler(c *Client, opts ...HandlerOption) *Handler
func (h *Handler) Enabled(context.Context, slog.Level) bool
func (h *Handler) Handle(context.Context, slog.Record) error
func (h *Handler) WithAttrs([]slog.Attr) slog.Handler
func (h *Handler) WithGroup(string) slog.Handler
```

Functional options, not the `Option`-struct-with-constructor-method idiom. That idiom is a samber-family convention this module is leaving; functional options are what Go vendor SDKs and `otelslog` ship, and they keep the zero value of every option meaningful.

Separate `ClientOption` and `HandlerOption` types so the compiler enforces which knob belongs to which object.

### Client options and defaults

| Option                                    | Default                           | Source                                                                               |
|-------------------------------------------|-----------------------------------|--------------------------------------------------------------------------------------|
| `WithEndpoint(string)`                    | `https://in.logs.betterstack.com` | PARITY §1                                                                            |
| `WithBatchSize(int)`                      | `1000`                            | JS + Java agree                                                                      |
| `WithBatchInterval(time.Duration)`        | `1s`                              | JS; Java is 3 s, and 1 s is the better single-timer value (PARITY §4)                |
| `WithMaxBatchBytes(int)`                  | `5 MiB`                           | conservative against the 10 MiB compressed limit                                     |
| `WithMaxQueueSize(int)`                   | `100_000` records                 | Java `maxQueueSize`, drop over                                                       |
| `WithBurstProtection(int, time.Duration)` | disabled                          | JS `burstProtectionMax` / `burstProtectionMilliseconds`, `10_000` / `5s` (PARITY §2) |
| `WithMaxRetries(int)`                     | `5`                               | Java                                                                                 |
| `WithRetryBackoff(time.Duration)`         | `300ms` base                      | Java                                                                                 |
| `WithRetryCeiling(time.Duration)`         | `60s`                             | total elapsed per batch; OTel `otlploghttp` (§5)                                     |
| `WithMaxInFlight(int)`                    | `5`                               | JS `syncMax`                                                                         |
| `WithTimeout(time.Duration)`              | `10s`                             | per request, Java `readTimeout`                                                      |
| `WithConnectTimeout(time.Duration)`       | `5s`                              | Java                                                                                 |
| `WithHTTPClient(*http.Client)`            | tuned internal client             | escape hatch; disables the two timeout options                                       |
| `WithCompression(Compression)`            | `CompressionGzip`                 | `CompressionNone` to disable                                                         |
| `WithEncoder(Encoder)`                    | `NDJSON`                          | §4 — also `JSONArray`, `MsgPack(Marshaler)`                                          |
| `WithOnError(func(error))`                | write to `os.Stderr`              | PARITY §5                                                                            |
| `WithShutdownTimeout(time.Duration)`      | `15s`                             | used by `Close`                                                                      |
| `WithDryRun(bool)`                        | `false`                           | JS `sendLogsToBetterStack` kill switch                                               |

**[amended] `WithBurstProtection` is off by default**, where every other option ships the sibling clients' number. JavaScript is the only official client with the feature and enables it at 10 000 records per 5 s — a 2 000 rec/s ceiling calibrated for Node's throughput, which a Go service can exceed without anything being wrong. A default that silently discards a correctly-behaving application's logs is the same class of surprise as a default level of `Debug`, and §2 has already rejected that. The right ceiling is a property of the application, so the library declines to guess one and the docs quote JS's numbers as a starting point instead.

The two values are one option, not two. Neither means anything alone — a maximum without a window has no rate in it, a window without a maximum has nothing to limit — so a single call makes the half-configured state unrepresentable rather than merely invalid. `validate` still rejects it, because `WithBurstProtection(10_000, 0)` is a plausible typo.

**[amended]** `WithDryRun(true)` **waives the source-token check** and nothing else. The whole pipeline still runs — convert, encode, queue, batch, frame, compress, `Flush`, `Close` — and the records are counted as `Sent`; only the POST is skipped. Requiring a credential for the one mode whose purpose is not spending one would defeat it, and the mode exists for tests and local development, where there is no token to give. Every other setting is validated exactly as usual, so turning the switch off later cannot surface a configuration error that was hidden while it was on.

### Handler options and defaults

| Option                                                      | Default                                                                              |
|-------------------------------------------------------------|--------------------------------------------------------------------------------------|
| `WithLevel(slog.Leveler)`                                   | `slog.LevelInfo`                                                                     |
| `WithAddSource(bool)`                                       | `false`                                                                              |
| `WithReplaceAttr(func([]string, slog.Attr) slog.Attr)`      | nil                                                                                  |
| `WithAttrFromContext(...func(context.Context) []slog.Attr)` | none                                                                                 |
| `WithExtraFields(map[string]any)`                           | none — merged into every record (Erlang `extra_fields`, Java `appName`)              |
| `WithFilter(func(context.Context, slog.Record) bool)`       | none — send-time predicate, **true means send** (Ruby `filter_sent_to_better_stack`) |

**[amended]** Extra fields are merged into the **attribute tree**, at its root, not written as top-level payload keys. So with the default context key they appear inside `context`, and `WithContextKey("")` flattens them with everything else. Three consequences, all of which were the reason:

- `Converter` and `ConvertOptions` are untouched. Had they gone in at the top level the converter would have had to place them, and every custom converter would silently drop them.
- They go through `appendAttr` like any other attribute, so they get the same value mapping (`error` → `{message, type}`, `time.Duration` → string) and the same `ReplaceAttr` treatment, rather than a second, subtly different path for the same job.
- Precedence falls out of placement: they are applied last and yield to any key already taken, so a record attribute beats a `With(...)` chain attribute beats a context extractor beats an extra field. That is PARITY §3's "applies before any `With` chain" read as *least specific loses*, which is the only reading that stays coherent once groups are involved.

The map is copied at option time; a caller mutating theirs afterwards would otherwise race every `Handle`.

A record rejected by `WithFilter` never reaches `Enqueue`, so it appears nowhere in `Stats`. It was not dropped — it was never sent for. This is distinct from `WithLevel`, which decides whether slog builds a record at all; the filter runs on one that already exists, with its context and attributes available.
| `WithConverter(Converter)` | `DefaultConverter` |
| `WithContextKey(string)` | `"context"` — `""` flattens attributes to the top level |

**[amended]** `Converter` is:

```go
type ConvertOptions struct{ ContextKey string } // "" flattens to the top level
type Converter func(r *slog.Record, attrs map[string]any, o ConvertOptions) map[string]any
```

The **handler** owns the `log/slog` contract — group nesting, `Value.Resolve()`, `ReplaceAttr`, empty-attr elision — and hands the converter a finished attribute tree. The **converter** owns only the record shape: which top-level keys exist and where `attrs` is hung. Upstream's signature handed the converter the raw attrs, groups and `ReplaceAttr`, which meant every custom converter had to re-implement the slog contract and could silently break conformance. Splitting them makes `slogtest` a property of the handler, not of whichever converter is installed.

Default level is `Info`, not upstream's `Debug`. Shipping debug logs to a paid ingestion endpoint by default is a billing surprise.

Option names track the sibling clients (`BatchSize`, `BatchInterval`, `MaxQueueSize`, `MaxRetries`) so a user moving from Java or JS recognises the knobs (PARITY §7.3).

## 3. Concurrency model

```
Handle (caller goroutine)      queue          sender goroutine        upload workers
  convert → encode  ──────► chan []byte ─────► accumulate batch ─────► gzip → POST → retry
  (errors returned)         (bounded,          (flush on count /       (≤ MaxInFlight)
                             drop on full)      bytes / interval)
```

**Encoding happens on the caller's goroutine**, inside `Enqueue`, before the record enters the queue. Three things fall out of that:

1. Encoding errors are returned synchronously from `Enqueue` and therefore from `Handle`, which is exactly the synchronous/local error class in PARITY §5. `slog.JSONHandler` formats on the caller's goroutine too, so this is not a novel cost.
2. Byte accounting is exact and free — the batch is a `[]byte` that grows by `len(encoded)`. No second marshal, no parallel `raws [][]byte` bookkeeping (PARITY §4).
3. The queue is `chan []byte`, so no record data is shared between goroutines and there is nothing to alias.

**The queue is bounded and drops on overflow.** A non-blocking send with a `default:` branch that increments a drop counter. Nothing in the wild blocks the application on a logging outage (PARITY §5) — blocking inside `Handle` converts a Better Stack outage into an application outage, and `Handle` runs in the caller's critical path. Per the amendment below, this is the *only* place records are shed under load.

`MaxQueueSize` is counted in records, matching Java. The queue holds encoded bytes, so a records-based bound is a loose memory bound; `MaxBatchBytes` provides the hard per-request one.

**One sender, many uploaders.** The sender goroutine owns the accumulation buffer exclusively — no lock, and the gzip writer can be reused across batches because there is a single writer. Completed batches go to a worker pool of at most `MaxInFlight` uploads, so one slow request does not serialise the next (the fork's single synchronous send is what let its buffer back up until `enqueue` blocked, PARITY §4).

**[amended] The sender blocks handing a completed batch to the pool; it does not drop.** The original text argued the opposite — that a stalled upload "does not stop batch assembly" — with the pool hand-off dropping batches when every worker was busy. Implementing it showed that policy is wrong, and the test that caught it is now in the suite: enqueue 1000 records against a *healthy* recorder and ~20% of them evaporate, because any application that logs a burst momentarily fills `MaxInFlight` slots and whole assembled batches are thrown away while nothing is wrong.

Blocking instead propagates backpressure to the queue, which is where records are meant to be shed: it is explicitly sized by the caller (`MaxQueueSize`), it drops with an accurate count, and `Enqueue` still never blocks the application. Assembling batches that have nowhere to go only moves unbounded memory from the queue, where it is bounded and accounted, into a backlog where it is neither. There is exactly one shedding point, and the user chooses its size.

The wait is bounded by a context — the caller's for an explicit `Flush`, the client's worker context otherwise — so `Close` can cancel a sender parked on a stalled upload. `DroppedBacklog` survives as the counter for that cancellation path.

**[amended] `WithBurstProtection` adds a second place records are shed, and does not contradict the rule above.** That rule is about **backpressure**: shedding because the downstream cannot keep up. There is still exactly one of those, the queue. Burst protection is an **admission ceiling** — a rate the operator declares acceptable, enforced whether or not delivery is keeping up at all, and off unless asked for. The two answer different questions: the queue asks "is the sender behind?", the limiter asks "is this more than I agreed to send?". A client with the limiter disabled, which is every client that has not opted in, behaves exactly as described above.

It sits in `Enqueue`, **before the encode**, which is the entire reason for having it. The queue only fills once a burst has already been converted and marshalled on the calling goroutine, so it bounds memory but not the CPU that a runaway loop inside a hot path burns. Refusing at the gate costs one atomic load. The limiter is a token bucket held as a single monotone timestamp — the theoretical arrival time, with the emission interval `window/max` and the burst tolerance `window` — so its whole state is one `atomic.Int64`, its refusal path performs no write at all, and it needs no array of window slots (the JS client's shape, PARITY §2). Admitting `k` records back to back requires `k·interval ≤ window`, i.e. exactly `max` of them from a full bucket, which is the steady state the JS numbers describe.

Records refused here are counted `Enqueued` and `DroppedBurst`, exactly as a record refused after `Close` is counted `Enqueued` and `DroppedClosed`, so §5's identity is unaffected. The refusal is not an error return, for the reason in §5: one error per lost record is the storm the aggregation exists to prevent.

It is deliberately not in `Handle`. The handler depends on the unexported `enqueuer` interface rather than on `*Client`, and pushing admission policy behind that interface would put client policy inside the `log/slog` contract surface that §2's converter split exists to keep clean. The cost is that a refused record has still paid for attribute conversion; encoding is the larger half, and this saves it.

**[amended] Compression therefore runs on the sender, before dispatch** — not in `transport.go` on the upload workers, as §7's file layout implies. The reusable-`gzip.Writer` justification above is only true where there is a single writer; with ≤`MaxInFlight` concurrent uploads a shared writer is a data race. Compressing first also shrinks the queued-batch memory bound ~8× and means a retry re-sends bytes rather than recompressing them. Level is `gzip.BestSpeed`: log JSON still compresses 8–12× at level 1, and CPU inside the customer's process is the scarce resource.

**[amended] with one exception: the 413 split path, where a worker compresses with its own writer.** Splitting a rejected batch (§5) means framing and compressing two halves, and it happens on the worker that got the 413 — handing the halves back to the sender would deadlock, since the sender's dispatch blocks on the very pool this worker belongs to. The rule was never "one goroutine compresses"; it was "no `gzip.Writer` is shared". So the buffers move into a `packer` that a single goroutine owns: the sender has one from the start, and each worker builds its own lazily, the first time it actually has to split. Most workers, in most processes, never build one.

**[amended] A batch keeps its records unframed and their boundaries**, alongside the finished body, so that it *can* be split. Neither is recoverable after the fact: the body is framed and compressed, and where one record ends inside it is knowable only to the `Encoder`. Two extra slices and one `int` per record is what that costs in steady state, measured at ~90 bytes per record with no change in allocation count or throughput. The alternative — asking the `Encoder` to re-derive boundaries from a compressed body — would put the hardest possible requirement on the one interface third parties implement.

**[amended] A batch handed to an uploader must own its bytes.** The sender reuses both the accumulation buffer and the gzip output buffer, so it copies (or hands over and reallocates) at dispatch. The fork could pass its reused output buffer straight into a send only because that send was synchronous; with concurrent uploads the same code silently corrupts in-flight request bodies. This is the highest-probability silent-corruption bug in the module.

Concurrent uploads mean batches can land out of order. That is fine: Better Stack orders by `dt`, which the client sets.

**Flush triggers**, evaluated by the sender: record count ≥ `BatchSize`, buffered bytes ≥ `MaxBatchBytes`, or `BatchInterval` elapsed since the batch's first record. One interval, not the fork's two-timer min/max debounce — no official client has two, and the min-timer goes inert precisely under sustained load (PARITY §4).

The interval timer is armed when a batch becomes non-empty and stopped when it flushes, so an idle client does no work. Go 1.21 timer semantics mean a fired timer cannot be drained race-free, so the sender uses a single long-lived `time.Timer` reset only from its own goroutine, and treats a spurious fire on an empty batch as a no-op.

**`WithAttrs`/`WithGroup` clone the handler** but share the same `*Client` pointer, so every derived handler feeds one queue.

**[amended]** The clone appends one entry to a `[]groupOrAttrs` accumulation list — the representation from the official [slog handler guide](https://go.dev/s/slog-handler-guide) — allocated at exactly the required length with `make`+`copy`. No `append` ever targets a parent's slice, so two derived handlers cannot write into a shared backing array. That is the live data race in the current code (PARITY §4), fixed structurally rather than defended against.

The original text prescribed `slices.Clip`, which is wrong twice over: `slices` *is* in the Go 1.21 standard library, so the "built by explicit allocation for the 1.21 floor" caveat was unnecessary; and `Clip` is not the right fix anyway, since it only caps capacity so that a *later* append reallocates, still sharing the array for reads and costing a re-slice per derivation. With the accumulation list, `Clip` is simply not needed.

## 4. Wire format

Default body encoding is **NDJSON** (`application/x-ndjson`), a documented Better Stack encoding (PARITY §1). A batch is the concatenation of encoded records; assembly is a single `append`. There is no JSON-array framing step, no separator bookkeeping, and no assemble pass.

```go
type Encoder interface {
    ContentType() string
    AppendRecord(dst []byte, v map[string]any) ([]byte, error)
    Frame(batch []byte, n int) []byte // NDJSON: returns batch unchanged
}
```

**[amended]** `AppendRecord` does **not** take the record's index within the batch. It did, on the argument that the JSON-array encoder needs one to know when to emit a leading comma — but that argument assumed an encoder driven by a batch assembler, and §3 puts encoding in `Enqueue`, one record at a time, on the caller's goroutine. A record is encoded before it is known which batch it will join or what position it will take, so the index passed was always `0` and could never have been anything else.

The array encoder does not need it. Every record carries a **leading** comma, and `Frame` overwrites the first one with the opening bracket:

```
,{"a":1} ,{"b":2} ,{"c":3}      three records, as queued
[{"a":1} ,{"b":2} ,{"c":3}]     after Frame
```

One byte written and one appended, whatever the batch size. The real constraint this exposes is stronger than positional framing and is now stated on the interface: **a record's encoding must be self-delimiting and independent of its position**, because the same bytes may be re-framed into a different batch entirely when an oversized one is split (§5). The comma scheme satisfies it for any contiguous run of records; a "comma before every record but the first" scheme would not.

`Frame` exists so a JSON-array encoder (`[` … `]`) and, later, MessagePack (array header prefix) fit the same interface without special-casing the sender. `ContentType` travels with the encoder, which is what upstream's bare `Marshaler func(any) ([]byte, error)` option could not express. The exported constructors are `NDJSON()`, `JSONArray()` and `MsgPack(Marshaler)`.

**[amended] MessagePack ships as `MsgPack(marshal Marshaler)`, which bundles no codec.** This document previously said it would be written in-module — "implementing it in-module keeps the zero-dependency promise; it is a bounded amount of encoder code for the subset of types a log payload contains". Both halves of that were wrong.

The subset is not bounded. `attr.go`'s attribute handling resolves a `slog.Value` into a closed set of types for every `Kind` *except* `KindAny`, which keeps `v.Any()` as-is — so an arbitrary user value reaches the encoder, and matching what `encoding/json` does with it means reimplementing reflection over struct tags, `json.Marshaler`, `encoding.TextMarshaler` and embedded fields. Anything less makes the same record serialise differently depending on which encoder is selected.

And the promise is not worth what it would cost here. A hand-rolled serialiser in a client whose whole pitch is auditability is a liability: nobody reads it, and everybody has to trust it. Zero dependencies is a means to being safe to adopt, not an end that outranks it.

Taking a dependency instead was measured and rejected too. Dead-code elimination does not remove an unused codec — with a `MsgPack()` constructor present in the package but never called, `ugorji/go/codec` still cost **+8.73 MB** of stripped binary (+453%) and 5.5× the cold build, because its `init()`-time type registration defeats the linker. `vmihailenco/msgpack/v5` costs +123 KB but has had no commit since October 2023. Every user would pay for a format most of them do not select.

What is actually format-specific here is the framing, and it is a length prefix: a one-, three- or five-byte MessagePack array header. So the split is **the caller brings the codec, this package brings the framing**. `Marshaler` is `func(any) ([]byte, error)`, which is deliberately the signature the common libraries already expose, so most can be passed directly with no adapter; the handle-based ones need a three-line closure. Users tend to have a codec in the build already, and the ones who care about `str`-versus-`bin` or timestamp representation get to decide rather than inherit.

**[amended] The JSON record encoder has two implementations, chosen by build tag: `encoding/json` everywhere, and `encoding/json/v2` from Go 1.27.** Both are `appendJSONObject(dst []byte, m map[string]any) ([]byte, error)`, which the two JSON `Encoder`s are written in terms of; the tag is `go1.27 && goexperiment.jsonv2`, naming the experiment as well as the release because json/v2 ships enabled-by-default in 1.27 but can still be turned off with `GOEXPERIMENT=nojsonv2`.

The cost being addressed is the `map[string]any` waypoint. Every value in the payload is an interface, so `encoding/json` reflects over each one and boxes it again on the way out — 13 allocations and ~1.3 µs for the default record shape, which is the largest single line item in what a log call costs the calling goroutine. On one 1.27 toolchain, holding everything else equal:

| record encoder                                    | `Handle` | `AppendRecord` | B/op |
|---------------------------------------------------|----------|----------------|------|
| `encoding/json`, pre-v2 engine (`nojsonv2`)        | 27 allocs | 14 allocs      | 608  |
| `encoding/json`, v2 engine (1.27's default v1 API) | 25 allocs | 11 allocs      | —    |
| `encoding/json/v2` directly (`json_v2.go`)         | **21 allocs** | **9 allocs** | 328  |

So the tagged file is worth about four allocations per record over doing nothing on 1.27, and six over 1.26. It does not approach zero, and nothing in v2 will: reflection over `any` is the cost, and v2's gains are in struct-shaped encoding and in decoding. **A hand-written encoder does reach zero** — 0 allocations and ~250 ns, `Handle` at 16 — and that was implemented, measured and then set aside on the `json-hand-rolled` branch. What it costs is ~80 lines of format rules (string escaping, float formatting, RFC 3339 guards) that have to track `encoding/json`'s bytes forever, in a library whose pitch is auditability. The same judgement that rejected an in-module MessagePack codec above applies, and lands the same way: the speed is real, and it is not worth being the thing that has to be trusted. The branch remains as the measured record.

**v2 is not a drop-in, and the two defaults it changes are the reason `json_contract_test.go` exists.** Both would have altered this library's behaviour on 1.27 only, silently:

- **Invalid UTF-8 is an error in v2**, where v1 substitutes U+FFFD. A log message carries whatever bytes the application put in it; a stray byte must not be why the line never arrives. `jsontext.AllowInvalidUTF8(true)` restores substitution.
- **`time.Duration` cannot be encoded at all in v2** — "no default representation" — where v1 writes the nanosecond count. slog's own durations never arrive as `Duration` values (`attr.go` renders them as strings), but a struct field, a nested map or a direct `Enqueue` can carry one. `FormatDurationAsNano(true)` restores it.

`Deterministic` is deliberately *not* set: v1 sorts an object's keys and v2 does not, so key order differs between the two builds. JSON defines no order for object members, so payload key order is **unspecified** here — do not write a test that depends on it, and note that this is now observable as a difference between toolchains rather than only in principle.

**Both halves of the tag are needed**, and each was verified. `goexperiment.jsonv2` because `encoding/json/v2`'s own files carry that constraint, so `GOEXPERIMENT=nojsonv2` removes the package and a bare `go1.27` tag breaks those builds. `go1.27` because the v2 API is version-stamped there and a build tag sets a file's language version — without it `go vet` fails with "requires go1.27 or later", and vet is not optional here (§0's 1.21 floor). That is also why the tag is not widened to `go1.26` to pick up 1.26 builds that opt into the experiment: `go1.26` vets on 1.26 and fails on 1.27, so those builds take `json_stdlib.go` — correct, merely not the fastest.

**Graduation is the case no tag can express.** When json/v2 graduates the flag is deleted, and a tag naming an experiment that does not exist is silently false rather than an error — so `json_v2.go` would stop being compiled with nothing failing. The condition cannot be written in advance, because it needs the release number that ends the experiment, and guessing (`|| go1.28`) breaks `nojsonv2` builds outright if the experiment is still live there. `runtime.Version()` resolves it after the fact: it reports non-default experiments (`go1.27rc3-X:nojsonv2`), so an opt-out is distinguishable from a graduation at run time. `TestJSONImplementation` asserts on that and names the edit to make. Verified against a simulated graduation.

`json_contract_test.go` is what keeps the two honest. It asserts behaviour, never bytes — invalid UTF-8 survives, a `Duration` encodes as nanoseconds, HTML is unescaped, `dt` is RFC 3339 nano, marshalers are honoured, an unencodable value errors and leaves the buffer holding exactly the records already in it — and it compiles under both tags, so it is the same test on both paths. Verified to have teeth: removing the options above makes it fail on 1.27 with exactly those two errors. The corollary is a testing obligation, recorded in CLAUDE.md — **the suite must be run on a pre-1.27 and a 1.27+ toolchain**, because either alone exercises one implementation and reports success for a library that ships both.

Two consequences worth recording. `AppendRecord` checks that the marshaller returned a map (`0x80`–`0x8f`, `0xde`, `0xdf`), because `MsgPack(json.Marshal)` compiles and would otherwise be diagnosed only as a 406 on every batch. And `Frame` must **shift** the batch rather than return a re-sliced `batch[k:]` with a right-aligned header: `pack` calls it on a buffer it reuses, so a moved start creeps forward by the header width on every batch and grows without bound. The shift measures 285 ns against a 16 KiB batch, versus 67 µs for the gzip pass that immediately follows it.

### Record shape

```json
{
  "dt": "2026-08-06T10:11:12.123456789Z",
  "level": "ERROR",
  "message": "a message",
  "context": {
    "user": {"id": "user-123"},
    "source": {"function": "main.main", "file": "main.go", "line": 42}
  }
}
```

Decisions this settles from PARITY §3:

- **Attributes nest under `"context"`**, not upstream's `"extra"`. It matches upstream's own README, the .NET clients' `context.properties`, and the scoped-context vocabulary the Ruby and Python clients use. `WithContextKey("")` flattens to the top level for anyone who wants that; reserved keys still win on collision.
- **Source location is `context.source`**, using slog's own `source` key rather than upstream's `runtime`. It yields to an attribute the caller keyed `source` themselves. That is what the standard library's ordering amounts to: `JSONHandler` writes its built-in source before the record's attrs, so the caller's value comes second and is the one a last-wins consumer keeps. Emitting both, as it does, is not open to us — see "Duplicate keys" below.
- **`logger.name` / `logger.version` are dropped from the payload.** Dotted top-level keys are unusual, and the same information already travels in `User-Agent: logs-client-go/<version>` where the server can read it without billing the customer for the bytes. Flagged for Better Stack in case they want it in-band.
- **`dt` is RFC 3339 with nanosecond precision, UTC.** Valid per the API. **[amended]** A zero `record.Time` **omits `dt` entirely** rather than substituting send time. Three reasons, in order of weight: it is the documented handler contract (`log/slog/handler.go`: *"If `r.Time` is the zero time, ignore the time"* — `JSONHandler` does not even call `ReplaceAttr` for time in that case); the server then stamps reception time (PARITY §1), which is *more* accurate than a client-side send-time stamp assigned at batch assembly and skewed by `BatchInterval` plus retry backoff on an unsynchronised clock; and send-time substitution makes `slogtest`'s `zero-time` case unsatisfiable by any results-mapper, since a mapper cannot distinguish a substituted timestamp from a real one — which would put §10's "passes `slogtest`" in direct conflict with this bullet. `record.Time.Round(0)` strips the monotonic reading before formatting, matching `JSONHandler`.
- **Errors** are formatted into `{message, type}`. Leaving them to `encoding/json` is badly broken: most error types have no exported fields, so they render as `{}` and the message is lost — on what is the commonest attribute in an error-level line. Detection is **by type**, not by attribute key, so `slog.Any("cause", err)` is handled as well as `slog.Any("error", err)`. There is no `stack` field: Go errors do not carry one, and synthesising a stack at handler time would record where the record was built rather than where the error was raised.

`User-Agent` is `logs-client-go/<version>`, the `<lib>/<version>` convention (cf. `logtail-js(node)`), with the version read from `runtime/debug.ReadBuildInfo()` — no `sed`-rewritten `VERSION` placeholder (PARITY §7.6).

### Duplicate keys

`slog` permits a record to carry the same key twice — `logger.With("a", 1).Info("m", "a", 2)` — and `slog.JSONHandler` emits both, which is [golang/go#59365](https://github.com/golang/go/issues/59365), still open and milestoned Go 1.27. **This package never emits a duplicate key.** The attribute tree is a `map[string]any` and `attr.go`'s `appendAttr` assigns into it, so a repeated key overwrites: **last write wins**, which given the order attributes are applied in means the more specific statement wins — a record attribute over one from the `With(...)` chain that produced the logger, those over the built-in source location, that over a context extractor, and that over `WithExtraFields`.

That is a decision, not an accident of the data structure, and three things back it:

- **Every consumer already resolves duplicates the same way.** `encoding/json` takes the last for both maps and structs, as do the JavaScript and Python parsers, as does Better Stack — a live `POST` of `{"extra":{"foo":1,"foo":2}}` against the ingestion endpoint stores `{"foo": 2}`. Collapsing client-side and forwarding both therefore produce a byte-identical *stored* record, so nothing is taken away from the operator.
- **It is the only answer that is well-defined for every encoder.** The MessagePack spec says nothing whatever about duplicate keys — the map family is a count followed by `N*2` alternating objects, so duplicates are representable but their resolution is decoder-defined. Forwarding them would mean the same record resolving differently depending on which `Encoder` was selected, which §4 already refuses elsewhere.
- **The types depend on it.** `Encoder.AppendRecord(dst []byte, v map[string]any)` and `Converter` are both public and both maps. Preserving duplicates needs an ordered pairs type, which breaks both signatures and kills `MsgPack` outright: a slice of pairs marshals as an *array* in every codec, so `checkMsgPackMap` would reject it, and "the caller brings the codec" only works because the payload is a plain map every codec already knows.

The counter-argument — that duplicate resolution is the server's business and a client that collapses has destroyed data the server might one day want — was considered and rejected. It rests on there being a raw `slog` byte stream to forward faithfully, and there is not: `slog` has no serialization, only a `Handler` contract, so whatever this handler emits *is* the raw output. Forwarding duplicates is not neutrality; it is adopting `JSONHandler`'s behaviour, which is an open bug report.

Note also that the failure mode #59365 actually describes — a user attribute colliding with `msg` or `time` and corrupting the reserved fields — cannot occur here at all. Attributes nest under `"context"` by default, so a user key never reaches `dt`/`level`/`message`; with `WithContextKey("")` the `DefaultConverter` writes the reserved keys after the attributes, so they win.

Anyone who needs the shadowed value kept rather than discarded wants a strategy like `slog-dedup`'s increment (`foo`, `foo#01`) or append (`foo: [1, 2]`). Both are compatible with the map and with every encoder, and neither can be built on `Converter`, which receives `attrs` already collapsed — the seam would have to be in `appendAttr`. Not implemented: a collision is almost always an intentional override, and turning that into an array would break the common case to serve the rare one.

## 5. Delivery, retry, and failure

Every response's status is classified — the single most important thing missing today, since the current code never reads `resp.StatusCode` and cannot distinguish a bad token from success.

| Status                                  | Action                                                                                                                                                                                   |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `2xx`                                   | done                                                                                                                                                                                     |
| `413`                                   | **split** the batch in half and resend both, recursively; a single record over the limit is dropped, counted `DroppedOversize`, and reported once naming `WithMaxBatchBytes` as the knob |
| `401`, `402`, `403`, `406`, other `4xx` | **terminal** — drop the batch, report once. Retrying a bad token burns quota forever                                                                                                     |
| `429`, `5xx`, network/timeout           | retry with backoff                                                                                                                                                                       |

The docs name `403` for a bad token, but the live endpoint answers **`401`** with `{"error": "Unauthorized"}` — verified 2026-08-06 with both a missing and a bogus token, on HTTP/2 and HTTP/1.1. Classifying only the documented codes would leave the single most common misconfiguration in the retry path, so the rule is *terminal by default*: retry only the explicitly retryable set, drop everything else. Auth is also checked before the body is parsed (a bogus token with malformed JSON still returns 401), so `406` is only reachable once the token is valid.

**[amended]** §5's `413` row and §10's milestone split contradicted each other: splitting was a v0.2 item, so v0.1 could not silently retry an over-limit body that is guaranteed to fail again unchanged. Resolved in v0.2, where splitting landed.

413 stays **out of the retryable set**. Retrying means resending the same bytes, which would fail identically; splitting is a different mechanism that happens to be triggered by the same status. Keeping them separate is what stops "recoverable" from quietly becoming "retryable" for a status where that would be a loop.

**[amended] The local hard-limit check splits too, rather than dropping.** The sender refuses to dispatch a body over the API's documented 10 MiB, to save a request and the retry budget behind it. In v0.1 that was a drop; it is now the same halving, through the same helper, since throwing records away locally that the server would have let us rescue is indefensible. The two paths remain distinct for a reason: `MaxBatchBytes` is measured before compression and the server's limit applies after it, so how much actually fits is not knowable locally — the local check catches the hopeless case, and the 413 catches everything else.

Splitting recurses at most log2(`BatchSize`) deep, and each half restarts its attempt count but **inherits the parent's `RetryCeiling` deadline**, so halving cannot buy a batch more time than the original was granted. The halves are sent one after the other on the worker that got the 413, never handed back to the pool: the pool's dispatch blocks when every worker is busy, and a worker waiting on a pool it occupies is a deadlock.

Retry is exponential from `RetryBackoff` with full jitter, capped at `MaxRetries` and at a total elapsed ceiling. A `Retry-After` header overrides the computed delay — server-supplied backoff wins over local config, OTel's rule (PARITY §6.4) — capped so a hostile header cannot stall shutdown.

**[amended]** `MaxRetries` counts retries **after** the initial attempt, so the default of 5 means at most 6 requests and `WithMaxRetries(0)` means "send once, never retry". This matches Java's `maxRetries` and is stated verbatim in the option's doc comment; it is exactly the sort of off-by-one that otherwise differs silently between clients. The total elapsed ceiling is 60 s (OTel `otlploghttp`'s number), an unexported constant in v0.1 and `WithRetryCeiling` from v0.2. It is a second limit alongside `MaxRetries` and the tighter of the two wins, so a generous retry count cannot keep a batch alive against a slow server or a `Retry-After` that keeps asking for more. During shutdown it is bounded by the shutdown context rather than by arithmetic: `Close` cancels in-flight uploads when `ShutdownTimeout` expires, so a batch parked in a backoff aborts.

**[amended] A `Retry-After` at or above the ceiling is terminal, not slow, and that is the wanted answer.** The deadline check runs *before* the wait — a delay that would end past the deadline is never taken — so at the defaults a first-attempt `429` carrying `Retry-After: 60` drops the batch immediately, and `Retry-After: 30` buys exactly one retry. The alternative considered was letting one honoured `Retry-After` extend the deadline, on the belief that OTel treats a server-requested delay as authoritative outside the elapsed budget. It does not: `otlploghttp`'s retry loop computes `elapsed + throttle` and returns *"max retry time would elapse"* when it exceeds `MaxElapsedTime`, which is this behaviour exactly. Independently of the precedent, honouring such a throttle would pin one of the five `MaxInFlight` slots for the whole window while the queue backed up behind it — trading a bounded, counted drop for an unbounded one further upstream. A server asking for the entire budget has declined the batch; an operator who disagrees raises the ceiling. Stated on `WithRetryCeiling`, held by `TestRetryAfterBeyondCeilingGivesUpAtOnce`.

### Connection reuse

One `*http.Client` for the lifetime of the `Client`, shared by every upload. The current code builds a fresh `http.Client` inside `send` for every single record, which means a fresh `Transport`, a fresh (empty) idle-connection pool, and therefore a full TCP + TLS handshake per log line — roughly 3–4 round trips of latency and a new socket, per record.

```go
&http.Transport{
    Proxy:               http.ProxyFromEnvironment,
    DialContext:         (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext,
    MaxIdleConns:        maxInFlight,
    MaxIdleConnsPerHost: maxInFlight, // default is 2 — must be raised
    IdleConnTimeout:     90 * time.Second,
    TLSHandshakeTimeout: 10 * time.Second,
    ForceAttemptHTTP2:   true, // load-bearing — see below
}
```

Four details that silently defeat connection reuse if missed:

- **`ForceAttemptHTTP2` must be set explicitly.** Go only auto-configures HTTP/2 on a `Transport` it recognises as unmodified; setting a custom `DialContext` — which the `ConnectTimeout` option requires — disables that, and the transport silently falls back to HTTP/1.1. Measured against the live endpoint: the same config negotiates `HTTP/1.1` with the flag off and `HTTP/2.0` with it on.
- **`MaxIdleConnsPerHost` defaults to 2** (`http.DefaultMaxIdleConnsPerHost`). All traffic here goes to one host, so with `MaxInFlight: 5` the default would close three connections after every flush and re-handshake them on the next one. It must track `MaxInFlight`.
- **The response body must be drained before `Close`** — `io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))`. A body closed while unread makes `Transport` drop the connection instead of returning it to the pool. This is invisible in tests that only assert status codes.
- **Every error path must still close the body.** An early `return` on a non-2xx that skips the drain leaks the connection just as effectively as never closing it.

**`in.logs.betterstack.com` speaks HTTP/2** — verified 2026-08-06: TLS 1.3, ALPN negotiates `h2`, and real POSTs come back as `HTTP/2.0`. So all `MaxInFlight` uploads multiplex over a **single** TCP connection, and the idle-pool sizing is a fallback for the HTTP/1.1 path rather than the steady state. Concurrency costs streams, not sockets or handshakes.

That makes `MaxInFlight: 5` cheap — it buys pipelining during a slow response without five connections' worth of handshakes — and it means the `MaxIdleConnsPerHost` fix above matters mostly when h2 is unavailable (a proxy, an on-prem ingest host). Keep both; they are correct in either mode.

### Error surfacing

- **Synchronous errors** — encode failure, closed client — are returned from `Enqueue` and from `Handle`. `log/slog` discards `Handle`'s error, but middleware (`slogmulti.RecoverHandlerError`, `Failover`) does not, so returning it keeps the handler composable at no cost.
- **Asynchronous errors** — non-2xx, retry exhaustion, queue overflow — go to `OnError`, defaulting to a single line on stderr. By the time these occur, `Handle` has returned and the failing record is not on the stack.

Two rules from PARITY §5, both enforced by design rather than documentation:

- **Never report a logging error through the logger.** `OnError` writes to a sink the client owns; the client never calls into `slog`.
- **Aggregate, don't fire per record.** Drops increment counters and are reported as a periodic summary (and once on `Close`), not one callback per lost record. During an outage the per-record shape is an error storm that is itself a denial of service.

**[amended]** `Stats` needs two more fields, because §3 and §6 each promise to count something the original struct had nowhere to put: records dropped because every uploader was busy, and records dropped because the client was already closed.

```go
type Stats struct {
    Enqueued, Sent, Retries uint64

    DroppedQueueFull uint64 // queue was full: the app outran the sender
    DroppedBurst     uint64 // over the WithBurstProtection rate ceiling
    DroppedBacklog   uint64 // all MaxInFlight uploads busy and the hand-off full
    DroppedRejected  uint64 // terminal status, or retry budget exhausted
    DroppedOversize  uint64 // over the hard request limit, or 413
    DroppedClosed    uint64 // enqueued after Close, or still queued when Close returned
}
```

with the documented identity, which is a test rather than a comment:

```
Enqueued == Sent + DroppedQueueFull + DroppedBurst + DroppedBacklog +
            DroppedRejected + DroppedOversize + DroppedClosed
                                                   (once Close has returned)
```

For that to hold, `Enqueued` counts records **offered** to `Enqueue`, including those refused with `ErrClosed` after `Close` — a record the caller handed over and that never arrived is a drop whether it was refused at the door or lost later. Records rejected with an encoding error are outside the accounting entirely: nothing was ever handed over, and the caller was told synchronously.

**[amended]** The identity holds *once `Close` has returned* and *provided no `Enqueue` is still running when `Close` is called* — the second half was missing above. `Close` counts the queue's leftovers after the sender has stopped; an `Enqueue` that passed its `done` check just before `Close` closed it can complete its send after that count, leaving one record `Enqueued` and in no other bucket. Closing the identity would mean tracking in-flight `Enqueue` calls — two atomic read-modify-writes on the hot path, on every record, to describe a caller that is already racing its own logging against its own shutdown and losing the record either way. The guarantee is stated with its precondition instead, on `Stats`; `TestStatsBalance` cannot reach the window and does not try.

`Stats()` is cheap (atomics — `sync/atomic.Uint64` fields internally, not bare `uint64`, so there is no 64-bit alignment hazard on 32-bit platforms) and is what makes the client observable from the host application's own metrics — every drop is counted, and no drop is silent.

Typed errors so callers can branch: `*StatusError{StatusCode, Status, Body, Records}`, `*DropError{Records, Reason}`, `ErrClosed`.

**[amended]** `*DropError` is an `OnError` payload, never an `Enqueue` return value. `Enqueue` returns non-nil **only** for an encode failure and for `ErrClosed`; a queue-full drop increments a counter and returns `nil`. Returning an error there would make `Handle` return non-nil for every dropped record, so `slogmulti.RecoverHandlerError` would fire per lost record — reintroducing, through a different door, exactly the per-record error storm the aggregation rule above exists to prevent.

## 6. Shutdown

```go
func (c *Client) Flush(ctx context.Context) error // drain queue + in-flight, return first error
func (c *Client) Close() error                    // Flush with ShutdownTimeout, then stop
```

`Close` is idempotent and safe to call concurrently; after it, `Enqueue` returns `ErrClosed` and drops are counted rather than panicking on a closed channel. `Close` is the one place a delivery error can be returned meaningfully, because the caller has a stack to receive it — so it does, in addition to a final drop summary through `OnError`.

`defer client.Close()` in `main` is the documented pattern, matching `Log.CloseAndFlush()` / `logtail.flush()` / `logger.close()` in the sibling clients.

## 7. Repository layout

```
go.mod            module github.com/prochac/logs-client-go   (go 1.21)
LICENSE           ISC, Copyright (c) 2026, Tomáš Procházka
README.md         install, quickstart, options tables, flush-before-exit
DESIGN.md         this file
PARITY.md         research, contract, defaults, gap checklist
client.go         Client, ClientOption, queue, Stats, batch, packer, gzip
sender.go         sender goroutine, batch assembly, uploadPool
transport.go      http.Client, status classification, retry, 413 split
encoder.go        Encoder, NDJSON, JSONArray, Marshaler, MsgPack
handler.go        Handler, HandlerOption, slog.Handler implementation
converter.go      Converter, DefaultConverter, record shape
attr.go           attribute/group/ReplaceAttr helpers (original, see §9)
errors.go         StatusError, DropError, ErrClosed, OnError plumbing
version.go        ReadBuildInfo
example/          runnable example, no time.Sleep
```

**[amended]** Marked the entries §10 defers, since this table is what someone implements from; the v0.2 markers are cleared now that those landed. Note also that gzip lives in `client.go`'s `packer`, driven by the sender, not in `transport.go` — see §3. `sender.go` was split out of `client.go` during implementation, which the original table did not anticipate.

One package. Nothing exported that isn't in §2 plus the types those signatures name.

## 8. Testing

Real tests are possible from day one because `Flush`/`Close` exist — the current repo has none, and `example/example.go` waits with `time.Sleep`.

- **`httptest.Server` recorder** as the fixture: capture bodies, assert NDJSON and JSON-array framing, headers, gzip round-trip, and payload shape. **[amended]** It also answers `413` on size, not only to a script. That is what drives splitting to convergence rather than to a fixed number of steps, and it lets the assertions be about the outcome — every record delivered — instead of about a request count that just restates the algorithm. It records the status it gave, since once retries or splitting are in play a refused request carried its records too, and counting those makes every record look duplicated.
- **Each flush trigger independently**: count, bytes, interval, explicit `Flush`, `Close`.
- **Failure paths**, which is where the fork's suite stopped: 5xx → retry then success, 403 → terminal with no retry, 413 → split, network error → retry, retry exhaustion → `OnError` + drop counted, queue full → drop counted and `Handle` does not block.
- **Concurrency**: `-race` with concurrent `Handle` across handlers derived by `WithAttrs`/`WithGroup`, concurrent `Close`, `Handle` after `Close`.
- **`goleak.VerifyTestMain`** stays. It is the reason the sender and workers must terminate deterministically on `Close` rather than leaking until process exit — a useful constraint to keep enforced. `goleak` is the one test-only dependency worth its cost; production dependencies stay at zero.

  **[amended]** There are now two. `MsgPack` takes its codec from the caller (§4), so the tests have to supply one, and supplying an independent implementation is the point: it is what makes them evidence that the framing is interoperable MessagePack rather than evidence that we can read back what we wrote. `shamaton/msgpack` was chosen for it — zero transitive dependencies, and its outstanding CVE is a decoder out-of-bounds read on malformed input, which is not the risk profile of a test decoding bytes it just produced. Verified that it reaches importers exactly as `goleak` does: recorded in their `go.sum`, never built. Production dependencies still stay at zero.
- Time-dependent tests use short real intervals rather than a fake clock; the 1.21 floor rules out `testing/synctest`.
- Benchmarks for `Handle` (the caller-goroutine cost: convert + encode + channel send) and for batch assembly.

## 9. Provenance

The greenfield rewrite only buys clean licensing if no source is copied.

- **`samber/slog-betterstack`** (MIT © 2023 Samuel Berthe) and the `alistairjevans` fork are prior art to read, not code to move. Reimplementing an idea — shared batcher, response drain, reused gzip writer — is fine; ideas aren't copyrightable. Copying the fork's 478-line `handler_test.go`, or lifting `DefaultConverter` line for line, makes the result a derivative work and re-triggers MIT attribution.
- **`samber/slog-common` is MIT too.** The eight helpers being dropped for the zero-dependency goal (`ContextExtractor`, `AppendAttrsToGroup`, `AppendRecordAttrsToAttrs`, `ReplaceError`, `Source`, `ReplaceAttrs`, `RemoveEmptyAttrs`, `AttrsToMap`) must be **rewritten**, not vendored. `attr.go` is written against `log/slog`'s documented handler contract and the `slogtest` conformance suite, not against slog-common's source.
- **Conformance**: the handler must pass `testing/slogtest`. That is the specification `attr.go` is written to, and it makes the reimplementation defensible on its own terms rather than as a paraphrase.
- New repo, new history. The `slog-betterstack` repo keeps its MIT `LICENSE` for its own contents; nothing from it is carried here, so nothing follows it here.

## 10. Milestones

**v0.1 — the blockers.** Client + Handler, NDJSON, batching on all three triggers, bounded queue with drop accounting, `Flush`/`Close`, status classification, retry with backoff, connection reuse, `OnError`, `Stats`, gzip. Everything in PARITY §3's blocker list, plus gzip because the 10 MiB limit is measured on compressed bytes. Passes `slogtest`.

**v0.2 — parity.** 413 splitting, `ExtraFields`, send-time filter, dry-run, separate connect/request timeouts, JSON-array encoder, README with the full options table. **[amended]** Also `WithRetryCeiling`, which §5 assigned here without §10 listing it. Separate connect/request timeouts in fact shipped in v0.1, with the transport.

**v0.3 — polish.** MessagePack and burst protection, plus `example/` covering context extraction and graceful shutdown. All three have landed; v0.3 is complete.

**[amended]** MessagePack did not land in the shape §4 described. It ships as `MsgPack(Marshaler)` — the caller's codec, this package's framing — after both alternatives were measured and rejected. §4 above carries the reasoning and the numbers. The rule this milestone illustrates is worth keeping: a zero-dependency promise is a means to being safe to adopt, and where the two conflict it is the promise that gives way, not the safety.

**[amended]** Burst protection arrived without a spec — §10 named it and PARITY §2 quoted JavaScript's two numbers, and nothing else in this document said what it should do. §2 and §3 above now carry that spec: opt-in rather than defaulted, one option rather than two, a token bucket in `Enqueue` ahead of the encode, and `DroppedBurst` in the identity. PARITY §3 ranked it "lowest priority — the bounded queue covers most of the same failure", which is right about *most*: the queue bounds memory but only after every record it drops has been encoded, and it never engages at all while delivery is healthy.

**[amended]** Benchmarks were listed here but shipped with v0.1: `bench_test.go` covers `Handle` (bare, attrs, groups, source, error, disabled, parallel), `Enqueue`, `AppendRecord`, batch assembly, `compress` and `Flush`. Same case as v0.2's connect/request timeouts — an item ticked before the milestone that claimed it.

**[amended] Mirroring to a second `slog.Handler` is struck, not deferred.** It was to be a handler option answering the JavaScript client's `sendLogsToConsoleOutput` (PARITY §5). It is composition, and it belongs outside this library:

- `slog.MultiHandler` (Go 1.26, proposal #65954) does exactly this, and `samber/slog-multi` has done it since before that. Neither is a dependency *this* module takes; both are one line in the user's `main`. An option here would duplicate them and nothing more.
- Duplicating them is not free. A mirror inside the handler has to answer what `Enabled` reports when the two disagree on level, whether a failing mirror suppresses the Better Stack record, and how `WithAttrs`/`WithGroup` fan out — the same `log/slog` contract surface `attr.go` and §8's `slogtest` suite exist to get right *once*. Getting it right twice, in a second place, for no new capability, is a bad trade against §2's small exported surface.
- The one thing composition cannot give is the *bytes actually sent*, as opposed to the same record independently formatted. That is a debugging need, and `example/`'s `-endpoint` covers it by pointing the client at a local sink — no API at all.

The kill-switch half of the same PARITY line, JavaScript's `sendLogsToBetterStack`, is unaffected: it shipped in v0.2 as `WithDryRun`.

Instead, README documents the composition, since "also print to the console in development" is a real need and the answer should be written down rather than merely implied.

Open questions for Better Stack, none blocking: the `context` vs `extra` key, whether library identity belongs in the payload or only in `User-Agent`, and confirmation that undocumented `Content-Encoding: gzip` is contractual rather than incidental.
