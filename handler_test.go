package betterstack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/slogtest"
	"time"
)

// stubSink stands in for *Client so the handler can be tested without a network
// stack. It is the whole reason Handler depends on the enqueuer interface.
type stubSink struct {
	mu     sync.Mutex
	events []map[string]any
	err    error // returned from Enqueue when set
}

func (s *stubSink) Enqueue(event map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.events = append(s.events, event)
	return nil
}

func (s *stubSink) all() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]map[string]any(nil), s.events...)
}

func (s *stubSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// last returns the most recently enqueued payload.
func (s *stubSink) last(t *testing.T) map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		t.Fatal("no records were enqueued")
	}
	return s.events[len(s.events)-1]
}

// TestSlogtest runs the standard library's handler conformance suite.
//
// This is the specification attr.go is written to, and the reason the
// attribute plumbing here is an independent implementation rather than a
// paraphrase of somebody else's: it is checkable against the contract itself.
//
// slogtest.Run is Go 1.22; the module floor is 1.21, so TestHandler it is.
func TestSlogtest(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	// AddSource is on so that the empty-PC case ("a Handler should not output
	// SourceKey if the PC is zero") is actually exercised rather than passing
	// vacuously because the handler never emits source at all.
	h := newHandler(sink, WithAddSource(true))

	err := slogtest.TestHandler(h, func() []map[string]any {
		return slogtestResults(t, sink.all())
	})
	if err != nil {
		t.Error(err)
	}
}

// slogtestResults maps the Better Stack payload onto the shape slogtest checks.
//
// Three differences have to be bridged, and TestHandler's own documentation
// blesses doing so ("If a Handler intentionally drops an attribute that is
// checked by a test, then the results function should check for its absence and
// add it to the map it returns"):
//
//   - key names: dt -> time, message -> msg;
//   - nesting: attributes live under "context", slogtest expects them at the
//     top level;
//   - dt is absent, not null, when the record carried no timestamp, which is
//     precisely what the zero-time case checks.
//
// Payloads are round-tripped through the real encoder rather than inspected as
// maps, so this test covers the JSON encoding too — a group that survives the
// tree builder but cannot be marshalled would otherwise pass.
func slogtestResults(t *testing.T, events []map[string]any) []map[string]any {
	t.Helper()

	var buf []byte
	for i, ev := range events {
		var err error
		buf, err = NDJSON().AppendRecord(buf, ev)
		if err != nil {
			t.Fatalf("encoding record %d: %v", i, err)
		}
	}

	out := make([]map[string]any, 0, len(events))
	for _, line := range bytes.Split(bytes.TrimSuffix(buf, []byte("\n")), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			t.Fatalf("record is not valid JSON: %v (%q)", err, line)
		}

		m := map[string]any{}
		if ctx, ok := raw[DefaultContextKey].(map[string]any); ok {
			for k, v := range ctx {
				m[k] = v
			}
		}
		if dt, ok := raw[KeyTime]; ok {
			m[slog.TimeKey] = dt
		}
		m[slog.LevelKey] = raw[KeyLevel]
		m[slog.MessageKey] = raw[KeyMessage]
		out = append(out, m)
	}
	return out
}

func TestHandlerEnabled(t *testing.T) {
	t.Parallel()

	t.Run("defaults to Info", func(t *testing.T) {
		t.Parallel()
		h := newHandler(&stubSink{})
		// Debug records must not be shipped to a metered endpoint by default.
		if h.Enabled(context.Background(), slog.LevelDebug) {
			t.Error("Debug is enabled by default; it should not be")
		}
		for _, l := range []slog.Level{slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
			if !h.Enabled(context.Background(), l) {
				t.Errorf("%v is not enabled by default", l)
			}
		}
	})

	t.Run("honours WithLevel", func(t *testing.T) {
		t.Parallel()
		h := newHandler(&stubSink{}, WithLevel(slog.LevelWarn))
		if h.Enabled(context.Background(), slog.LevelInfo) {
			t.Error("Info is enabled at LevelWarn")
		}
		if !h.Enabled(context.Background(), slog.LevelWarn) {
			t.Error("Warn is not enabled at LevelWarn")
		}
	})

	t.Run("re-reads a LevelVar", func(t *testing.T) {
		t.Parallel()
		var lv slog.LevelVar
		lv.Set(slog.LevelError)
		h := newHandler(&stubSink{}, WithLevel(&lv))

		if h.Enabled(context.Background(), slog.LevelInfo) {
			t.Error("Info enabled while the var says Error")
		}
		lv.Set(slog.LevelDebug)
		if !h.Enabled(context.Background(), slog.LevelInfo) {
			t.Error("level change was not observed; the Leveler is being read once")
		}
	})
}

// Handlers derived from a common parent must not see each other's attributes.
// Growing a shared backing array is the failure mode this guards.
func TestWithAttrsIsolation(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	base := newHandler(sink)
	logger := slog.New(base)

	logger.With("branch", "a").Info("one")
	logger.With("branch", "b").Info("two")

	events := sink.all()
	if len(events) != 2 {
		t.Fatalf("got %d records, want 2", len(events))
	}
	for i, want := range []string{"a", "b"} {
		ctx := events[i][DefaultContextKey].(map[string]any)
		if got := ctx["branch"]; got != want {
			t.Errorf("record %d: branch = %v, want %q", i, got, want)
		}
		if len(ctx) != 1 {
			t.Errorf("record %d: leaked attributes from the sibling handler: %v", i, ctx)
		}
	}
}

// The data race that is live in the library this one replaces: with a context
// extractor configured, concurrent Handle calls through handlers derived from
// one parent appended into a shared array. Run with -race.
func TestConcurrentHandleAcrossDerivedHandlers(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	base := newHandler(sink, WithAttrFromContext(func(context.Context) []slog.Attr {
		return []slog.Attr{slog.String("trace", "t-1")}
	}))

	// Derive several handlers from one parent, as an application does.
	logger := slog.New(base).With("service", "api")

	const goroutines, perGoroutine = 8, 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			l := logger.WithGroup("req").With("worker", g)
			for i := 0; i < perGoroutine; i++ {
				l.Info("working", "i", i)
			}
		}(g)
	}
	wg.Wait()

	if got, want := sink.len(), goroutines*perGoroutine; got != want {
		t.Errorf("got %d records, want %d", got, want)
	}
}

// --- extra fields -----------------------------------------------------------

func TestWithExtraFields(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	logger := slog.New(newHandler(sink, WithExtraFields(map[string]any{
		"service": "checkout",
		"env":     "prod",
	})))

	logger.Info("one")
	logger.WithGroup("req").With("id", "r-1").Info("two")

	for i, ev := range sink.all() {
		ctx, ok := ev[DefaultContextKey].(map[string]any)
		if !ok {
			t.Fatalf("record %d has no context block: %v", i, ev)
		}
		if got := ctx["service"]; got != "checkout" {
			t.Errorf("record %d: service = %v, want %q", i, got, "checkout")
		}
		if got := ctx["env"]; got != "prod" {
			t.Errorf("record %d: env = %v, want %q", i, got, "prod")
		}
	}

	// They belong to the record, not to whatever group happened to be open.
	second := sink.all()[1][DefaultContextKey].(map[string]any)
	if _, nested := second["req"].(map[string]any)["service"]; nested {
		t.Error("an extra field was placed inside an open group")
	}
}

// Extra fields are the most general thing said about a record, so anything more
// specific about this particular one wins.
func TestExtraFieldsYieldToRealAttrs(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newHandler(sink,
		WithExtraFields(map[string]any{"service": "default", "env": "prod"}),
		WithAttrFromContext(func(context.Context) []slog.Attr {
			return []slog.Attr{slog.String("env", "from-context")}
		}),
	)
	slog.New(h).With("service", "explicit").Info("hello")

	ctx := sink.last(t)[DefaultContextKey].(map[string]any)
	if got := ctx["service"]; got != "explicit" {
		t.Errorf("service = %v, want %q: an extra field beat a real attribute", got, "explicit")
	}
	if got := ctx["env"]; got != "from-context" {
		t.Errorf("env = %v, want %q: an extra field beat a context extractor", got, "from-context")
	}
}

// A context extractor states a fact about the request; a record or With(...)
// attribute states one about this single line. The more specific wins.
func TestContextAttrsYieldToRealAttrs(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newHandler(sink, WithAttrFromContext(func(context.Context) []slog.Attr {
		return []slog.Attr{
			slog.String("trace", "t-1"),
			slog.String("user", "from-context"),
			slog.String("region", "from-context"),
		}
	}))
	slog.New(h).With("user", "from-with").Info("hello", slog.String("region", "from-record"))

	ctx := sink.last(t)[DefaultContextKey].(map[string]any)
	if got := ctx["user"]; got != "from-with" {
		t.Errorf("user = %v, want %q: a context extractor beat a With attribute", got, "from-with")
	}
	if got := ctx["region"]; got != "from-record" {
		t.Errorf("region = %v, want %q: a context extractor beat a record attribute", got, "from-record")
	}
	if got := ctx["trace"]; got != "t-1" {
		t.Errorf("trace = %v, want %q: an uncontested extracted attribute was lost", got, "t-1")
	}
}

// slog permits a key to repeat within one record, and slog.JSONHandler emits it
// twice (golang/go#59365). Here the tree is a map, so the collision collapses to
// the last write — which is what makes a call-site attribute override the
// With(...) chain that produced the logger, and is the resolution every consumer
// of the payload applies anyway. DESIGN §4, "Duplicate keys".
func TestDuplicateKeysCollapseToTheLastWrite(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	log := slog.New(newHandler(sink)).With("k", "from-with")
	log.Info("hello", slog.String("k", "from-record"), slog.String("k", "from-record-2"))

	ctx := sink.last(t)[DefaultContextKey].(map[string]any)
	if got := ctx["k"]; got != "from-record-2" {
		t.Errorf("k = %v, want %q", got, "from-record-2")
	}

	// And the wire carries it once, whatever the encoder: nothing downstream is
	// ever asked to resolve a duplicate.
	line, err := NDJSON().AppendRecord(nil, sink.last(t))
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	if n := bytes.Count(line, []byte(`"k"`)); n != 1 {
		t.Errorf(`"k" appears %d times on the wire, want 1: %s`, n, line)
	}
}

func TestExtraFieldsFlattenWithEmptyContextKey(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newHandler(sink,
		WithContextKey(""),
		WithExtraFields(map[string]any{"service": "checkout"}),
	)
	slog.New(h).Info("hello")

	ev := sink.last(t)
	if got := ev["service"]; got != "checkout" {
		t.Errorf("service = %v, want it at the top level: %v", got, ev)
	}
	if _, nested := ev[DefaultContextKey]; nested {
		t.Errorf("a context block was created despite an empty context key: %v", ev)
	}
}

// The caller's map must not be live: mutating it after construction cannot be
// allowed to race every Handle call.
func TestExtraFieldsAreCopied(t *testing.T) {
	t.Parallel()

	fields := map[string]any{"service": "checkout"}
	sink := &stubSink{}
	h := newHandler(sink, WithExtraFields(fields))

	fields["service"] = "mutated"
	fields["late"] = "addition"
	slog.New(h).Info("hello")

	ctx := sink.last(t)[DefaultContextKey].(map[string]any)
	if got := ctx["service"]; got != "checkout" {
		t.Errorf("service = %v, want %q: the caller's map was retained", got, "checkout")
	}
	if _, ok := ctx["late"]; ok {
		t.Errorf("a key added after construction appeared: %v", ctx)
	}
}

// Extra fields go through the same attribute path as everything else, so they
// get the same value mapping.
func TestExtraFieldsGetValueMapping(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newHandler(sink, WithExtraFields(map[string]any{
		"boot": 90 * time.Second,
	}))
	slog.New(h).Info("hello")

	ctx := sink.last(t)[DefaultContextKey].(map[string]any)
	if got := ctx["boot"]; got != "1m30s" {
		t.Errorf("boot = %v (%T), want the mapped duration string", got, got)
	}
}

// --- filter -----------------------------------------------------------------

func TestWithFilter(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newHandler(sink, WithFilter(func(_ context.Context, r slog.Record) bool {
		return !strings.Contains(r.Message, "health")
	}))
	logger := slog.New(h)

	logger.Info("health check ok")
	logger.Info("real work")
	logger.Info("health check ok again")

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("got %d records, want 1: %v", len(events), events)
	}
	if got := events[0][KeyMessage]; got != "real work" {
		t.Errorf("the wrong record survived: %v", got)
	}
}

// The predicate sees the context, which is most of the point: it can ask
// questions about the request that the record alone cannot answer.
func TestFilterSeesTheContext(t *testing.T) {
	t.Parallel()

	type key struct{}
	sink := &stubSink{}
	h := newHandler(sink, WithFilter(func(ctx context.Context, _ slog.Record) bool {
		return ctx.Value(key{}) == "keep"
	}))

	logger := slog.New(h)
	logger.InfoContext(context.WithValue(context.Background(), key{}, "keep"), "kept")
	logger.InfoContext(context.WithValue(context.Background(), key{}, "drop"), "dropped")

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("got %d records, want 1: %v", len(events), events)
	}
	if got := events[0][KeyMessage]; got != "kept" {
		t.Errorf("the wrong record survived: %v", got)
	}
}

// A filtered record was never sent for, so it is not a drop and must not show
// up in the accounting.
func TestFilteredRecordsAreNotCounted(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t)
	c, _ := newTestClient(t, rec, WithBatchSize(1000))
	defer c.Close()

	logger := slog.New(NewHandler(c, WithFilter(func(context.Context, slog.Record) bool {
		return false
	})))
	for i := 0; i < 10; i++ {
		logger.Info("dropped by the filter")
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := c.Stats().Enqueued; got != 0 {
		t.Errorf("Stats().Enqueued = %d, want 0", got)
	}
	if got := rec.count(); got != 0 {
		t.Errorf("got %d requests, want 0", got)
	}
}

// A filter that keeps everything must not perturb the normal path.
func TestFilterKeepingEverything(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newHandler(sink, WithFilter(func(context.Context, slog.Record) bool { return true }))
	slog.New(h).Info("hello", "a", 1)

	ev := sink.last(t)
	if got := ev[KeyMessage]; got != "hello" {
		t.Errorf("message = %v, want %q", got, "hello")
	}
	if got := ev[DefaultContextKey].(map[string]any)["a"]; got != int64(1) {
		t.Errorf("a = %v (%T), want 1", got, got)
	}
}

// Every handler derived by WithAttrs or WithGroup must feed the same client.
func TestDerivedHandlersShareTheClient(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	base := newHandler(sink)

	slog.New(base).Info("root")
	slog.New(base.WithAttrs([]slog.Attr{slog.String("a", "1")})).Info("with-attrs")
	slog.New(base.WithGroup("g")).Info("with-group")
	slog.New(base.WithGroup("g").WithAttrs([]slog.Attr{slog.String("b", "2")})).Info("both")

	if got := sink.len(); got != 4 {
		t.Errorf("got %d records in the shared sink, want 4", got)
	}
}

func TestWithGroupEmptyNameReturnsReceiver(t *testing.T) {
	t.Parallel()
	h := newHandler(&stubSink{})
	if got := h.WithGroup(""); got != slog.Handler(h) {
		t.Error(`WithGroup("") returned a new handler; the contract says it is a no-op`)
	}
}

func TestWithAttrsEmptyReturnsReceiver(t *testing.T) {
	t.Parallel()
	h := newHandler(&stubSink{})
	if got := h.WithAttrs(nil); got != slog.Handler(h) {
		t.Error("WithAttrs(nil) allocated a new handler unnecessarily")
	}
}

// WithAttrs is documented as owning the slice, but callers reuse theirs.
func TestWithAttrsCopiesTheSlice(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	attrs := []slog.Attr{slog.String("k", "original")}
	h := newHandler(sink).WithAttrs(attrs)

	attrs[0] = slog.String("k", "mutated-by-caller")
	slog.New(h).Info("msg")

	ctx := sink.last(t)[DefaultContextKey].(map[string]any)
	if got := ctx["k"]; got != "original" {
		t.Errorf("k = %v, want %q: the caller's slice was retained, not copied", got, "original")
	}
}

func TestContextKey(t *testing.T) {
	t.Parallel()

	t.Run("nests under context by default", func(t *testing.T) {
		t.Parallel()
		sink := &stubSink{}
		slog.New(newHandler(sink)).Info("msg", "k", "v")

		payload := sink.last(t)
		ctx, ok := payload[DefaultContextKey].(map[string]any)
		if !ok {
			t.Fatalf("no %q object in the payload: %v", DefaultContextKey, payload)
		}
		if ctx["k"] != "v" {
			t.Errorf("context.k = %v, want %q", ctx["k"], "v")
		}
	})

	t.Run("empty key flattens", func(t *testing.T) {
		t.Parallel()
		sink := &stubSink{}
		slog.New(newHandler(sink, WithContextKey(""))).Info("msg", "k", "v")

		payload := sink.last(t)
		if _, nested := payload[DefaultContextKey]; nested {
			t.Error("attributes are still nested despite an empty context key")
		}
		if payload["k"] != "v" {
			t.Errorf("k = %v, want %q at the top level", payload["k"], "v")
		}
	})

	t.Run("reserved keys win when flattened", func(t *testing.T) {
		t.Parallel()
		sink := &stubSink{}
		// An attribute named "message" must not displace the record's message:
		// the result still has to be a payload the ingestion API can read.
		slog.New(newHandler(sink, WithContextKey(""))).Info("the real message", "message", "impostor")

		if got := sink.last(t)[KeyMessage]; got != "the real message" {
			t.Errorf("%s = %v, want the record's message", KeyMessage, got)
		}
	})
}

func TestAddSource(t *testing.T) {
	t.Parallel()

	t.Run("reports the call site", func(t *testing.T) {
		t.Parallel()
		sink := &stubSink{}
		slog.New(newHandler(sink, WithAddSource(true))).Info("msg")

		ctx := sink.last(t)[DefaultContextKey].(map[string]any)
		src, ok := ctx[slog.SourceKey].(map[string]any)
		if !ok {
			t.Fatalf("no %q object in the context: %v", slog.SourceKey, ctx)
		}
		if fn, _ := src["function"].(string); !strings.Contains(fn, "TestAddSource") {
			t.Errorf("function = %v, want this test's own frame", src["function"])
		}
		if file, _ := src["file"].(string); !strings.HasSuffix(file, "handler_test.go") {
			t.Errorf("file = %v, want handler_test.go", src["file"])
		}
		if line, _ := src["line"].(int); line <= 0 {
			t.Errorf("line = %v, want a positive line number", src["line"])
		}
	})

	t.Run("omitted by default", func(t *testing.T) {
		t.Parallel()
		sink := &stubSink{}
		slog.New(newHandler(sink)).Info("msg")

		if ctx, ok := sink.last(t)[DefaultContextKey].(map[string]any); ok {
			if _, present := ctx[slog.SourceKey]; present {
				t.Error("source is present without WithAddSource")
			}
		}
	})

	t.Run("omitted for a zero PC", func(t *testing.T) {
		t.Parallel()
		sink := &stubSink{}
		h := newHandler(sink, WithAddSource(true))

		// A Record built directly carries no program counter.
		r := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
		if err := h.Handle(context.Background(), r); err != nil {
			t.Fatalf("Handle: %v", err)
		}

		if ctx, ok := sink.last(t)[DefaultContextKey].(map[string]any); ok {
			if _, present := ctx[slog.SourceKey]; present {
				t.Error("source is present for a zero PC")
			}
		}
	})

	// JSONHandler writes its built-in source before the record's attrs, so a
	// caller who keys an attribute "source" is the one a last-wins consumer
	// keeps. Emitting both is not open to us, so source yields to reach the same
	// outcome.
	t.Run("yields to the caller's own source attribute", func(t *testing.T) {
		t.Parallel()
		sink := &stubSink{}
		h := newHandler(sink, WithAddSource(true))

		slog.New(h).Info("msg", slog.String(slog.SourceKey, "mine"))

		ctx := sink.last(t)[DefaultContextKey].(map[string]any)
		if got := ctx[slog.SourceKey]; got != "mine" {
			t.Errorf("source = %v, want %q: the call site beat the caller's own attribute", got, "mine")
		}
	})

	// It is still more specific than the two root-placed things below it.
	t.Run("beats a context extractor and an extra field", func(t *testing.T) {
		t.Parallel()
		sink := &stubSink{}
		h := newHandler(sink,
			WithAddSource(true),
			WithAttrFromContext(func(context.Context) []slog.Attr {
				return []slog.Attr{slog.String(slog.SourceKey, "from-context")}
			}),
			WithExtraFields(map[string]any{slog.SourceKey: "from-extra"}),
		)

		slog.New(h).Info("msg")

		ctx := sink.last(t)[DefaultContextKey].(map[string]any)
		if _, ok := ctx[slog.SourceKey].(map[string]any); !ok {
			t.Errorf("source = %v, want the resolved call site", ctx[slog.SourceKey])
		}
	})
}

// Extracted context attributes belong at the root, not inside whatever group
// the emitting logger happened to have open.
func TestAttrFromContextIsRooted(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newHandler(sink, WithAttrFromContext(func(context.Context) []slog.Attr {
		return []slog.Attr{slog.String("trace", "t-1")}
	}))

	slog.New(h).WithGroup("req").Info("msg", "path", "/x")

	ctx := sink.last(t)[DefaultContextKey].(map[string]any)
	if ctx["trace"] != "t-1" {
		t.Errorf("trace is not at the root of the attribute tree: %v", ctx)
	}
	req, ok := ctx["req"].(map[string]any)
	if !ok {
		t.Fatalf("no %q group: %v", "req", ctx)
	}
	if req["path"] != "/x" {
		t.Errorf("req.path = %v, want %q", req["path"], "/x")
	}
}

func TestHandlePropagatesSinkError(t *testing.T) {
	t.Parallel()

	sink := &stubSink{err: ErrClosed}
	err := newHandler(sink).Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0))
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Handle returned %v, want ErrClosed", err)
	}
}

func TestNewHandlerNilClientPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewHandler(nil) did not panic")
		}
		if msg, _ := r.(string); !strings.Contains(msg, "nil *Client") {
			t.Errorf("panic message %q does not name the mistake", r)
		}
	}()
	NewHandler(nil)
}
