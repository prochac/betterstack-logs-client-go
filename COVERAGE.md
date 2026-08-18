# Coverage

Snapshot of `go test -race -coverprofile=cover.out ./...`, taken on go1.26.6.

```
github.com/prochac/betterstack-logs-client-go            98.5%
github.com/prochac/betterstack-logs-client-go/fastjson  100.0%
github.com/prochac/betterstack-logs-client-go/example     0.0%   (package main, no tests)
```

Per file, statements covered:

| file | covered | % |
|---|---|---|
| `attr.go` | 147/147 | 100.0 |
| `converter.go` | 20/20 | 100.0 |
| `encoder.go` | 27/27 | 100.0 |
| `errors.go` | 30/30 | 100.0 |
| `handler.go` | 71/71 | 100.0 |
| `json.go` | 14/14 | 100.0 |
| `limiter.go` | 14/14 | 100.0 |
| `fastjson/fastjson.go` | 114/114 | 100.0 |
| `client.go` | 201/202 | 99.5 |
| `sender.go` | 154/155 | 99.4 |
| `transport.go` | 116/117 | 99.1 |
| `version.go` | 7/16 | 43.8 |
| `example/main.go` | 0/103 | 0.0 |

The headline number is not the interesting part. What follows is every block
still uncovered in the two library packages, and every one of them is
unreachable rather than untested. `example/` is excluded throughout: it is
`package main` with no tests, and `go build ./...` / `go vet ./...` / the
`go tool nm` link check are what keep it honest.

## What remains, and why it stays

- **`client.go:1111-1113`** — `newCompressor`'s panic on an invalid gzip level.
  The level is a constant, so this is unreachable by design.
- **`sender.go:66-68`** — `newStoppedTimer`'s drain. Structurally unreachable: a
  freshly created one-hour timer has not fired, so `Stop` always reports true.
  The receive is defensive by construction. `disarm`'s drain, which used to sit
  beside it and *is* reachable, is now covered — see below.
- **`transport.go:271-273`** — `http.NewRequestWithContext` failing. The
  endpoint is validated at construction and the method is a constant.
- **`version.go` (43.8%)** — `moduleVersion`'s `bi.Deps` scan, the `Replace`
  branch and the `bi.Main` fallback. Only reachable when this module is built
  *as a dependency of something else*, which no in-tree test can be. The
  fallback to `"dev"` is what the tests do exercise, and `isRealVersion` is
  fully covered. Testable only by factoring the `*debug.BuildInfo` in as a
  parameter — worth it only if the `User-Agent` version ever starts mattering.

## The seams that closed the rest

Three test-only fields on `clientConfig`, none with an option, each documented
at its declaration. They follow `dropReportInterval`, which established the
pattern: production sees one value, and the tests get at a path that is
otherwise reachable only by luck or by waiting.

- **`newCompressSink`** builds the buffer a `compressor` writes into —
  `*sliceWriter` in production. A `gzip.Writer` cannot fail on well-formed
  input; every error it returns comes from its sink, so a sink that fails is
  the only way to make compression fail. This one seam covers `compress`'s two
  error returns, `pack`'s, both of `split`'s, and, hanging off those, the
  sender's pack-failure and split-failure accounting and the worker's failure
  to split a batch the server called too large — the family that used to be the
  whole critical section here. `compress_test.go` holds it and the tests.
- **`hardMaxBytes`** is the request size past which a body is split rather than
  sent, `hardMaxRequestBytes` in production. The limit belongs to the ingestion
  API and not to the caller, so it stays optionless; as a field it lets the
  splitting paths be reached with bodies of a few dozen bytes rather than ten
  megabytes. (Incompressible test data large enough to trip the real limit
  measured at 14 MiB in and 110 ms of gzip per run, before `-race` and before
  `-count=5`.)
- **`beforeLeftoverCount`** runs inside `Close`, between the sender's exit and
  the count of what is still in the queue. That window is the Stats identity's
  one documented exception — an `Enqueue` whose record lands after the count —
  and it is a few statements wide, far too narrow to be occupied from another
  goroutine with any reliability.

Two holes closed without a seam, both by building the state directly rather
than waiting for it:

- **`Enqueue`'s authoritative queue-full drop** (`client.go:768-778`). The cheap
  `len == cap` pre-check used to win every race in the suite, leaving the branch
  that actually fires under load untested. `TestQueueFullDropAfterTheEncode`
  holds two producers inside the encode at once, behind a wedged sender and a
  queue with one slot, so both are past the pre-check and the second send must
  fail. The encoder's call count is what proves which drop happened: a
  pre-check drop never reaches the encoder.
- **`disarm`'s timer drain** (`sender.go:89-102`). Its comment argues the receive
  cannot block; if that ever stops holding, the sender parks forever and `Close`
  hangs. `TestDisarmDrainsAFiredTimer` builds the state on a bare `sender` —
  timer fired, value unconsumed, which is the window a running sender closes as
  fast as it can — using `len` on the timer channel to see the fire without
  taking it, and runs `disarm` in a goroutine so a regression fails the test
  instead of hanging the binary.

  Writing it turned up which timer semantics this module actually gets, which
  is the toolchain's call and not the `go 1.21` floor's: Go 1.23 made timer
  channels synchronous, a `go 1.21` module kept the old buffered ones through
  GODEBUG `asynctimerchan` up to go1.26, and **go1.27 removed that setting**
  (`GODEBUG=asynctimerchan=1` is now a fatal error there). Under the new
  semantics `Stop` clears a fire nobody has taken, so it cannot report false
  with a value pending and the drain is unreachable rather than wrong — sound
  either way, and still needed while either is in reach. The test skips on the
  synchronous semantics, keying off `cap(timer.C)`, so `go1.27rc3 test -race
  ./...` stays green; coverage is measured on go1.26.6, where the branch runs.

## Two assertions that were vacuous, and are not now

Coverage found one and the other came with it. Both are the same family as the
`check` content-type default that CLAUDE.md records: a test that passes whatever
the code does.

- **`converter_test.go`'s "empty group is ignored"** asserted a rule
  `slog.Record.Add` had already applied before `Handle` was ever called
  (`log/slog/record.go`, "It omits empty groups"). The elision is top-level
  only, so the handler's own guard is reached by a group nested in another and
  by one arriving through `WithAttrs`; both cases are now there beside it.
- **`TestExtraFieldsYieldToRealAttrs`** checked the precedence rule only for
  extra fields that `prepareExtraFields` hoists ahead of the record. The
  dynamic half — a value that cannot be converted once, such as an error —
  goes down a separate path with its own copy of the rule, and nothing checked
  it.
