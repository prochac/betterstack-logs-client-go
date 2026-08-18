package betterstack

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// logRecord drives one record through a handler and returns the payload.
func logRecord(t *testing.T, opts []HandlerOption, f func(*slog.Logger)) map[string]any {
	t.Helper()
	sink := &stubSink{}
	f(slog.New(newHandler(sink, opts...)))
	return sink.last(t)
}

// contextOf returns the attribute tree from a payload.
func contextOf(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	ctx, ok := payload[DefaultContextKey].(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return ctx
}

func TestDefaultConverterShape(t *testing.T) {
	t.Parallel()

	when := time.Date(2026, 8, 6, 10, 11, 12, 123456789, time.UTC)
	r := slog.NewRecord(when, slog.LevelError, "a message", 0)

	payload := DefaultConverter(&r, map[string]any{"k": "v"}, ConvertOptions{ContextKey: DefaultContextKey})

	if got := payload[KeyLevel]; got != "ERROR" {
		t.Errorf("%s = %v, want %q", KeyLevel, got, "ERROR")
	}
	if got := payload[KeyMessage]; got != "a message" {
		t.Errorf("%s = %v, want %q", KeyMessage, got, "a message")
	}
	if got, ok := payload[KeyTime].(time.Time); !ok || !got.Equal(when) {
		t.Errorf("%s = %v, want %v", KeyTime, payload[KeyTime], when)
	}
	if got := contextOf(t, payload)["k"]; got != "v" {
		t.Errorf("context.k = %v, want %q", got, "v")
	}

	// The library's identity travels in the User-Agent, not in every record.
	for _, unwanted := range []string{"logger.name", "logger.version"} {
		if _, present := payload[unwanted]; present {
			t.Errorf("payload carries %q; that belongs in the User-Agent", unwanted)
		}
	}
}

// A zero Record.Time means "this record has no timestamp", which the slog
// contract says to ignore. Omitting dt lets the ingestion API stamp its own
// reception time, which beats a client-side stamp applied a batch interval and
// several backoffs later.
func TestZeroTimeOmitsTimestamp(t *testing.T) {
	t.Parallel()

	r := slog.NewRecord(time.Time{}, slog.LevelInfo, "msg", 0)
	payload := DefaultConverter(&r, nil, ConvertOptions{ContextKey: DefaultContextKey})

	if v, present := payload[KeyTime]; present {
		t.Errorf("%s is present for a zero record time: %v", KeyTime, v)
	}
	// Everything else still has to be there.
	if payload[KeyMessage] != "msg" {
		t.Errorf("%s = %v", KeyMessage, payload[KeyMessage])
	}
}

func TestConverterOmitsEmptyContext(t *testing.T) {
	t.Parallel()

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	payload := DefaultConverter(&r, map[string]any{}, ConvertOptions{ContextKey: DefaultContextKey})

	if _, present := payload[DefaultContextKey]; present {
		t.Error("an empty context object was emitted; it should be omitted entirely")
	}
}

// Most error types have no exported fields, so encoding/json renders them as
// {} and the message — the single most useful thing in an error-level record —
// is lost. Detection is by type, so any error-valued attribute is covered, not
// only the ones named "error".
func TestErrorFormatting(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")

	for _, key := range []string{"error", "err", "cause"} {
		key := key
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			payload := logRecord(t, nil, func(l *slog.Logger) {
				l.Error("failed", key, sentinel)
			})

			got, ok := contextOf(t, payload)[key].(map[string]any)
			if !ok {
				t.Fatalf("%s is not an object: %#v", key, contextOf(t, payload)[key])
			}
			if got["message"] != "boom" {
				t.Errorf("message = %v, want %q", got["message"], "boom")
			}
			if got["type"] != fmt.Sprintf("%T", sentinel) {
				t.Errorf("type = %v, want %v", got["type"], fmt.Sprintf("%T", sentinel))
			}
		})
	}
}

func TestErrorFormattingWrapped(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("outer: %w", errors.New("inner"))
	payload := logRecord(t, nil, func(l *slog.Logger) { l.Error("failed", "error", err) })

	got := contextOf(t, payload)["error"].(map[string]any)
	if got["message"] != "outer: inner" {
		t.Errorf("message = %v, want the fully wrapped text", got["message"])
	}
}

func TestValueKinds(t *testing.T) {
	t.Parallel()

	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	payload := logRecord(t, nil, func(l *slog.Logger) {
		l.Info("msg",
			slog.String("s", "str"),
			slog.Int64("i", -7),
			slog.Uint64("u", 7),
			slog.Float64("f", 1.5),
			slog.Bool("b", true),
			slog.Duration("d", 1500*time.Millisecond),
			slog.Time("t", when),
		)
	})
	ctx := contextOf(t, payload)

	want := map[string]any{
		"s": "str",
		"i": int64(-7),
		"u": uint64(7),
		"f": 1.5,
		"b": true,
		// A raw Duration marshals as an integer nanosecond count, which is
		// unreadable in a log viewer.
		"d": "1.5s",
	}
	for k, v := range want {
		if got := ctx[k]; !reflect.DeepEqual(got, v) {
			t.Errorf("%s = %#v, want %#v", k, got, v)
		}
	}
	if got, ok := ctx["t"].(time.Time); !ok || !got.Equal(when) {
		t.Errorf("t = %#v, want %v", ctx["t"], when)
	}
}

func TestGroups(t *testing.T) {
	t.Parallel()

	t.Run("nested", func(t *testing.T) {
		t.Parallel()
		payload := logRecord(t, nil, func(l *slog.Logger) {
			l.Info("msg", slog.Group("outer", slog.Group("inner", slog.String("k", "v"))))
		})
		outer := contextOf(t, payload)["outer"].(map[string]any)
		inner := outer["inner"].(map[string]any)
		if inner["k"] != "v" {
			t.Errorf("outer.inner.k = %v, want %q", inner["k"], "v")
		}
	})

	t.Run("empty group key inlines", func(t *testing.T) {
		t.Parallel()
		payload := logRecord(t, nil, func(l *slog.Logger) {
			l.Info("msg", slog.Group("", slog.String("k", "v")))
		})
		if got := contextOf(t, payload)["k"]; got != "v" {
			t.Errorf("k = %v, want the group to have been inlined", got)
		}
	})

	t.Run("empty group is ignored", func(t *testing.T) {
		t.Parallel()
		payload := logRecord(t, nil, func(l *slog.Logger) {
			l.Info("msg", slog.Group("g"), slog.String("k", "v"))
		})
		if _, present := contextOf(t, payload)["g"]; present {
			t.Error("an empty group was emitted")
		}
	})

	t.Run("WithGroup scopes later attrs", func(t *testing.T) {
		t.Parallel()
		payload := logRecord(t, nil, func(l *slog.Logger) {
			l.WithGroup("g").With("a", 1).Info("msg", "b", 2)
		})
		g := contextOf(t, payload)["g"].(map[string]any)
		if g["a"] != int64(1) || g["b"] != int64(2) {
			t.Errorf("g = %#v, want both a and b inside it", g)
		}
	})
}

func TestReplaceAttr(t *testing.T) {
	t.Parallel()

	t.Run("receives the open group path", func(t *testing.T) {
		t.Parallel()
		var seen [][]string
		var keys []string
		opts := []HandlerOption{WithReplaceAttr(func(groups []string, a slog.Attr) slog.Attr {
			// Copied because that is what the log/slog contract asks of a
			// ReplaceAttr, not because this handler needs it — see the
			// sibling subtest below.
			seen = append(seen, append([]string(nil), groups...))
			keys = append(keys, a.Key)
			return a
		})}

		logRecord(t, opts, func(l *slog.Logger) {
			l.Info("msg", slog.Int("a", 1), slog.Group("g", slog.Int("b", 2)), slog.Int("c", 3))
		})

		want := map[string][]string{"a": nil, "b": {"g"}, "c": nil}
		for i, k := range keys {
			if !reflect.DeepEqual(seen[i], want[k]) {
				t.Errorf("groups for %q = %v, want %v", k, seen[i], want[k])
			}
		}
		// The group itself is never passed, only its contents.
		for _, k := range keys {
			if k == "g" {
				t.Error("ReplaceAttr was called for the group attr itself")
			}
		}
	})

	// The group path handed to ReplaceAttr keeps its contents after the call
	// returns, which is more than log/slog promises and more than
	// slog.JSONHandler delivers: its group stack is a pooled slice that is
	// pushed and popped, so a retained one is rewritten by the next sibling
	// group — verified against slog.JSONHandler, which fails this exact test.
	//
	// The guarantee comes from the three-index slices in attr.go. This test is
	// what stops them being simplified back to a plain append: the shape below
	// is the one that exposes the sharing, two sibling groups nested inside a
	// third so that both borrow the same spare slot in the parent's array. A
	// single nested group does not reproduce it, because nothing overwrites it
	// afterwards.
	t.Run("the group path is not shared between siblings", func(t *testing.T) {
		t.Parallel()
		var retained, atCallTime [][]string
		opts := []HandlerOption{WithReplaceAttr(func(groups []string, a slog.Attr) slog.Attr {
			retained = append(retained, groups) // deliberately not copied
			atCallTime = append(atCallTime, append([]string(nil), groups...))
			return a
		})}

		logRecord(t, opts, func(l *slog.Logger) {
			l.WithGroup("w1").WithGroup("w2").Info("msg", slog.Group("g",
				slog.Group("n1", slog.Int("k", 1)),
				slog.Group("n2", slog.Int("k", 2)),
				slog.Group("n3", slog.Int("k", 3)),
			))
		})

		for i := range retained {
			if !reflect.DeepEqual(retained[i], atCallTime[i]) {
				t.Errorf("call %d: groups was %v during the call and reads %v now: a sibling group overwrote it",
					i, atCallTime[i], retained[i])
			}
		}
	})

	t.Run("a zero Attr deletes", func(t *testing.T) {
		t.Parallel()
		opts := []HandlerOption{WithReplaceAttr(func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "secret" {
				return slog.Attr{}
			}
			return a
		})}
		payload := logRecord(t, opts, func(l *slog.Logger) {
			l.Info("msg", "secret", "hunter2", "keep", "yes")
		})
		ctx := contextOf(t, payload)
		if _, present := ctx["secret"]; present {
			t.Error("a zero Attr did not delete the attribute")
		}
		if ctx["keep"] != "yes" {
			t.Errorf("keep = %v, want it untouched", ctx["keep"])
		}
	})

	t.Run("can rename and retype", func(t *testing.T) {
		t.Parallel()
		opts := []HandlerOption{WithReplaceAttr(func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "n" {
				return slog.String("renamed", "as-string")
			}
			return a
		})}
		ctx := contextOf(t, logRecord(t, opts, func(l *slog.Logger) { l.Info("msg", "n", 42) }))
		if ctx["renamed"] != "as-string" {
			t.Errorf("renamed = %v", ctx["renamed"])
		}
		if _, present := ctx["n"]; present {
			t.Error("the original key survived the rename")
		}
	})

	// A group whose every member is deleted is empty after the fact, and the
	// contract says an empty group is ignored. slogtest cannot reach this
	// branch, because it never configures ReplaceAttr.
	t.Run("a group emptied by deletion is pruned", func(t *testing.T) {
		t.Parallel()
		opts := []HandlerOption{WithReplaceAttr(func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 && groups[0] == "doomed" {
				return slog.Attr{}
			}
			return a
		})}
		payload := logRecord(t, opts, func(l *slog.Logger) {
			l.Info("msg", slog.Group("doomed", slog.Int("a", 1), slog.Int("b", 2)), slog.Int("kept", 3))
		})
		ctx := contextOf(t, payload)
		if v, present := ctx["doomed"]; present {
			t.Errorf("group survived with every member deleted: %#v", v)
		}
		if ctx["kept"] != int64(3) {
			t.Errorf("kept = %v, want it untouched", ctx["kept"])
		}
	})

	t.Run("applies to source", func(t *testing.T) {
		t.Parallel()
		opts := []HandlerOption{
			WithAddSource(true),
			WithReplaceAttr(func(_ []string, a slog.Attr) slog.Attr {
				if a.Key == slog.SourceKey {
					return slog.String(slog.SourceKey, "redacted")
				}
				return a
			}),
		}
		ctx := contextOf(t, logRecord(t, opts, func(l *slog.Logger) { l.Info("msg") }))
		if ctx[slog.SourceKey] != "redacted" {
			t.Errorf("source = %#v, want it to have gone through ReplaceAttr", ctx[slog.SourceKey])
		}
	})
}

// replaceValuer is a LogValuer, for checking that resolution happens before
// anything else looks at the value.
type replaceValuer struct{ v string }

func (r replaceValuer) LogValue() slog.Value { return slog.StringValue(r.v) }

func TestResolve(t *testing.T) {
	t.Parallel()

	t.Run("in record attrs", func(t *testing.T) {
		t.Parallel()
		ctx := contextOf(t, logRecord(t, nil, func(l *slog.Logger) {
			l.Info("msg", "k", replaceValuer{"resolved"})
		}))
		if ctx["k"] != "resolved" {
			t.Errorf("k = %#v, want %q", ctx["k"], "resolved")
		}
	})

	t.Run("in WithAttrs", func(t *testing.T) {
		t.Parallel()
		ctx := contextOf(t, logRecord(t, nil, func(l *slog.Logger) {
			l.With("k", replaceValuer{"resolved"}).Info("msg")
		}))
		if ctx["k"] != "resolved" {
			t.Errorf("k = %#v, want %q", ctx["k"], "resolved")
		}
	})

	// Resolution must precede ReplaceAttr, which is documented to receive an
	// already-resolved value.
	t.Run("before ReplaceAttr", func(t *testing.T) {
		t.Parallel()
		var kind slog.Kind
		opts := []HandlerOption{WithReplaceAttr(func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "k" {
				kind = a.Value.Kind()
			}
			return a
		})}
		logRecord(t, opts, func(l *slog.Logger) { l.Info("msg", "k", replaceValuer{"resolved"}) })

		if kind != slog.KindString {
			t.Errorf("ReplaceAttr saw kind %v, want %v: the value was not resolved first", kind, slog.KindString)
		}
	})

	// A LogValuer may resolve to a group, which then has to be treated as one.
	t.Run("to a group", func(t *testing.T) {
		t.Parallel()
		ctx := contextOf(t, logRecord(t, nil, func(l *slog.Logger) {
			l.Info("msg", "k", groupValuer{})
		}))
		g, ok := ctx["k"].(map[string]any)
		if !ok {
			t.Fatalf("k = %#v, want a nested object", ctx["k"])
		}
		if g["inner"] != "v" {
			t.Errorf("k.inner = %v, want %q", g["inner"], "v")
		}
	})
}

type groupValuer struct{}

func (groupValuer) LogValue() slog.Value {
	return slog.GroupValue(slog.String("inner", "v"))
}

// --- conversion hoisted out of the per-record path --------------------------
//
// An attribute accumulated by WithAttrs, and a WithExtraFields value, appear on
// every record the handler produces, so their conversion is done once and the
// boxed result shared (attr.go's preAttr). These tests pin the two halves of
// that: what is hoisted must be indistinguishable from converting per record,
// and what must not be hoisted must not be.

// countingValuer resolves to a different value every time it is asked, which is
// how a per-record resolution is told apart from one done at With time.
type countingValuer struct{ n *atomic.Int64 }

func (c countingValuer) LogValue() slog.Value { return slog.Int64Value(c.n.Add(1)) }

func TestAheadOfTimeConversion(t *testing.T) {
	t.Parallel()

	// The same table as TestValueKinds, through With instead of the record, so
	// the hoisted path cannot drift from the per-record one.
	t.Run("With-chain values are mapped exactly as record attrs", func(t *testing.T) {
		t.Parallel()
		when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		ctx := contextOf(t, logRecord(t, nil, func(l *slog.Logger) {
			l.With(
				slog.String("s", "str"),
				slog.Int64("i", -7),
				slog.Uint64("u", 7),
				slog.Float64("f", 1.5),
				slog.Bool("b", true),
				slog.Duration("d", 1500*time.Millisecond),
				slog.Time("t", when),
			).Info("msg")
		}))

		want := map[string]any{"s": "str", "i": int64(-7), "u": uint64(7), "f": 1.5, "b": true, "d": "1.5s"}
		for k, v := range want {
			if got := ctx[k]; !reflect.DeepEqual(got, v) {
				t.Errorf("%s = %#v, want %#v", k, got, v)
			}
		}
		if got, ok := ctx["t"].(time.Time); !ok || !got.Equal(when) {
			t.Errorf("t = %#v, want %v", ctx["t"], when)
		}
	})

	// appendAttr drops a keyless attribute, and the hoist must not smuggle one
	// past it: a map has nowhere to put it.
	t.Run("a keyless With-chain attr is still dropped", func(t *testing.T) {
		t.Parallel()
		ctx := contextOf(t, logRecord(t, nil, func(l *slog.Logger) {
			l.With(slog.String("", "orphan"), slog.String("k", "v")).Info("msg")
		}))
		if len(ctx) != 1 || ctx["k"] != "v" {
			t.Errorf("context = %#v, want only k", ctx)
		}
	})

	t.Run("a With-chain LogValuer is resolved per record", func(t *testing.T) {
		t.Parallel()
		var n atomic.Int64
		sink := &stubSink{}
		l := slog.New(newHandler(sink)).With("k", countingValuer{&n})
		l.Info("first")
		l.Info("second")

		got := sink.all()
		if len(got) != 2 {
			t.Fatalf("got %d records, want 2", len(got))
		}
		first, second := contextOf(t, got[0])["k"], contextOf(t, got[1])["k"]
		if first != int64(1) || second != int64(2) {
			t.Errorf("k = %v then %v, want 1 then 2: the value was resolved once and reused", first, second)
		}
	})

	t.Run("an extra field LogValuer is resolved per record", func(t *testing.T) {
		t.Parallel()
		var n atomic.Int64
		sink := &stubSink{}
		l := slog.New(newHandler(sink, WithExtraFields(map[string]any{"k": countingValuer{&n}})))
		l.Info("first")
		l.Info("second")

		got := sink.all()
		if len(got) != 2 {
			t.Fatalf("got %d records, want 2", len(got))
		}
		first, second := contextOf(t, got[0])["k"], contextOf(t, got[1])["k"]
		if first != int64(1) || second != int64(2) {
			t.Errorf("k = %v then %v, want 1 then 2: the value was resolved once and reused", first, second)
		}
	})

	// An error converts to a map, and a map in the payload belongs to whoever
	// is handed it — a custom Converter may rewrite it. Sharing one between
	// records would let one record's rewrite show up in the next.
	t.Run("an extra field error keeps its own map per record", func(t *testing.T) {
		t.Parallel()
		sink := &stubSink{}
		l := slog.New(newHandler(sink, WithExtraFields(map[string]any{"boom": errors.New("nope")})))
		l.Info("first")
		l.Info("second")

		got := sink.all()
		if len(got) != 2 {
			t.Fatalf("got %d records, want 2", len(got))
		}
		first, ok := contextOf(t, got[0])["boom"].(map[string]any)
		if !ok {
			t.Fatalf("boom = %#v, want the error map", contextOf(t, got[0])["boom"])
		}
		second, ok := contextOf(t, got[1])["boom"].(map[string]any)
		if !ok {
			t.Fatalf("boom = %#v, want the error map", contextOf(t, got[1])["boom"])
		}
		first["message"] = "rewritten"
		if second["message"] != "nope" {
			t.Error("both records were handed the same error map")
		}
	})

	// Nothing may be hoisted past a ReplaceAttr: it is entitled to see every
	// attribute of every record, and may answer differently each time.
	t.Run("ReplaceAttr still sees With-chain attrs and extra fields", func(t *testing.T) {
		t.Parallel()
		var n atomic.Int64
		opts := []HandlerOption{
			WithExtraFields(map[string]any{"env": "prod"}),
			WithReplaceAttr(func(_ []string, a slog.Attr) slog.Attr {
				if a.Value.Kind() == slog.KindString {
					return slog.Int64(a.Key, n.Add(1))
				}
				return a
			}),
		}
		sink := &stubSink{}
		l := slog.New(newHandler(sink, opts...)).With("service", "api")
		l.Info("first")
		l.Info("second")

		got := sink.all()
		if len(got) != 2 {
			t.Fatalf("got %d records, want 2", len(got))
		}
		for i, want := range [][2]int64{{1, 2}, {3, 4}} {
			ctx := contextOf(t, got[i])
			// The two keys are visited in an unspecified order, so only the
			// pair is asserted: both went through ReplaceAttr, on this record.
			pair := [2]int64{0, 0}
			pair[0], _ = ctx["service"].(int64)
			pair[1], _ = ctx["env"].(int64)
			if pair[0]+pair[1] != want[0]+want[1] {
				t.Errorf("record %d: service=%v env=%v, want the pair %v: a value was hoisted past ReplaceAttr",
					i, ctx["service"], ctx["env"], want)
			}
		}
	})

	// WithExtraFields and WithReplaceAttr may be given in either order, so the
	// split has to happen after every option has been applied.
	t.Run("option order does not defeat the ReplaceAttr check", func(t *testing.T) {
		t.Parallel()
		replace := WithReplaceAttr(func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "env" {
				return slog.String("env", "replaced")
			}
			return a
		})
		for _, opts := range [][]HandlerOption{
			{WithExtraFields(map[string]any{"env": "prod"}), replace},
			{replace, WithExtraFields(map[string]any{"env": "prod"})},
		} {
			ctx := contextOf(t, logRecord(t, opts, func(l *slog.Logger) { l.Info("msg") }))
			if ctx["env"] != "replaced" {
				t.Errorf("env = %#v, want it to have gone through ReplaceAttr", ctx["env"])
			}
		}
	})
}

// The hoist is worth having only if it is actually free per record, so this
// measures exactly that: a handler carrying static attributes must allocate no
// more per record than one carrying none. It compares against itself rather
// than pinning an absolute count, which would only pin the Go version.
func TestHoistedAttrsCostNothingPerRecord(t *testing.T) {
	// No t.Parallel: AllocsPerRun requires the test to have the machine to
	// itself, and panics if it does not.
	r := slog.NewRecord(time.Now(), slog.LevelInfo, "a log message", 0)
	r.AddAttrs(slog.String("k", "v"))

	allocs := func(h slog.Handler) float64 {
		ctx := context.Background()
		return testing.AllocsPerRun(100, func() {
			if err := h.Handle(ctx, r); err != nil {
				t.Fatalf("Handle: %v", err)
			}
		})
	}

	plain := allocs(newHandler(discardSink{}))

	for _, tc := range []struct {
		name string
		h    slog.Handler
	}{
		{"WithAttrs", newHandler(discardSink{}).WithAttrs([]slog.Attr{
			slog.String("service", "api"), slog.String("release", "v1.0.0"),
		})},
		{"WithExtraFields", newHandler(discardSink{}, WithExtraFields(map[string]any{
			"service": "api", "release": "v1.0.0",
		}))},
	} {
		if got := allocs(tc.h); got > plain {
			t.Errorf("%s: %v allocations per record against %v with no attributes at all: "+
				"the conversion is not being hoisted", tc.name, got, plain)
		}
	}
}

// The four standard levels are boxed once, so DefaultConverter does not
// allocate for the level of every record. What matters is that the shortcut
// still answers exactly what Level.String() would.
func TestLevelValue(t *testing.T) {
	t.Parallel()

	for _, l := range []slog.Level{
		slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError,
		slog.LevelInfo + 2, slog.LevelWarn - 1, slog.Level(-8), slog.Level(100),
	} {
		if got := levelValue(l); got != l.String() {
			t.Errorf("levelValue(%v) = %#v, want %q", l, got, l.String())
		}
	}
}

func TestEmptyAttrIsElided(t *testing.T) {
	t.Parallel()

	payload := logRecord(t, nil, func(l *slog.Logger) {
		l.Info("msg", slog.Attr{}, slog.String("k", "v"))
	})
	ctx := contextOf(t, payload)
	if len(ctx) != 1 || ctx["k"] != "v" {
		t.Errorf("context = %#v, want only k", ctx)
	}
}

func TestCustomConverter(t *testing.T) {
	t.Parallel()

	custom := func(r *slog.Record, attrs map[string]any, _ ConvertOptions) map[string]any {
		return map[string]any{"custom": true, "msg": r.Message, "n": len(attrs)}
	}
	sink := &stubSink{}
	slog.New(newHandler(sink, WithConverter(custom))).Info("hello", "a", 1)

	payload := sink.last(t)
	if payload["custom"] != true || payload["msg"] != "hello" || payload["n"] != 1 {
		t.Errorf("custom converter was not used: %#v", payload)
	}
}

// A nil extractor in the list must not panic the caller's logging path.
func TestAttrFromContextTolerantOfNils(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newHandler(sink, WithAttrFromContext(nil, func(context.Context) []slog.Attr {
		return []slog.Attr{slog.String("k", "v")}
	}))
	slog.New(h).Info("msg")

	if got := contextOf(t, sink.last(t))["k"]; got != "v" {
		t.Errorf("k = %v, want %q", got, "v")
	}
}
