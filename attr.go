package betterstack

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
)

// This file implements the attribute half of the log/slog Handler contract. It
// is written against the documented rules in log/slog's Handler doc comment and
// validated by testing/slogtest, not adapted from any existing handler. The
// rules it implements, verbatim from the standard library:
//
//   - Attr's values should be resolved.
//   - If an Attr's key and value are both the zero value, ignore the Attr.
//     This can be tested with attr.Equal(Attr{}).
//   - If a group's key is empty, inline the group's Attrs.
//   - If a group has no Attrs (even if it has a non-empty key), ignore it.
//
// plus, for ReplaceAttr:
//
//   - The attribute's value has been resolved.
//   - If ReplaceAttr returns a zero Attr, the attribute is discarded.
//   - ReplaceAttr is never called for Group attributes, only their contents.
//   - The first argument is a list of currently open groups. It must not be
//     retained or modified.

// groupOrAttrs is one WithGroup or one WithAttrs call, recorded in the order it
// was made. Exactly one field is set.
//
// Keeping the calls as a list instead of eagerly folding them into a nested map
// is what makes handler derivation cheap and, more importantly, safe: see
// (*Handler).withGroupOrAttrs.
type groupOrAttrs struct {
	group string      // non-empty => a WithGroup
	attrs []slog.Attr // non-nil  => a WithAttrs
	pre   []preAttr   // attrs, converted ahead of time; nil => convert per record
}

// preAttr is one accumulated attribute, converted as far ahead of the records
// it will appear in as its kind allows.
//
// The point is that a With(...) chain is walked on every single record, and for
// the overwhelmingly common attribute — a string, a number, a bool — that walk
// re-does work whose answer cannot change: resolve, convert, and box the result
// into an any, one heap allocation each, per attribute, per record. Boxing it
// once at With time and assigning the same interface value into every record's
// tree is indistinguishable downstream, because what staticValue returns is
// immutable.
//
// What it deliberately does not do is precompute a *subtree*. The tree is a
// mutable map that each record's own attributes merge into, so a precomputed
// map would have to be deep-copied per record — which is the allocation being
// avoided. A flat list of already-boxed values has no such problem: the maps
// are still built per record, only their contents are hoisted.
//
// static is false for anything that must be built per record — a group, a
// LogValuer, an error — and such an entry goes through appendAttr exactly as
// before.
type preAttr struct {
	attr   slog.Attr // the original; the path taken when static is false
	value  any       // the converted value, when static
	static bool
}

// preResolve converts what it can of one WithAttrs call.
//
// It is only ever called when the handler has no ReplaceAttr: that function is
// entitled to see every attribute of every record, and may answer differently
// each time, so nothing can be hoisted past it. An attribute is hoisted only if
// it would take appendAttr's plain-assignment path unchanged — a non-empty key,
// a static kind, and therefore neither the zero Attr nor a group.
func preResolve(attrs []slog.Attr) []preAttr {
	out := make([]preAttr, len(attrs))
	for i, a := range attrs {
		out[i].attr = a
		if a.Key == "" {
			continue
		}
		// No Resolve first: Resolve is the identity on every kind staticValue
		// accepts, and a LogValuer is not one of them.
		if v, ok := staticValue(a.Value); ok {
			out[i].value = v
			out[i].static = true
		}
	}
	return out
}

// treeBuilder turns a handler's accumulated state and one Record into the
// nested map that becomes the payload's attribute tree.
type treeBuilder struct {
	replace func(groups []string, a slog.Attr) slog.Attr

	// The two halves of WithExtraFields, split once when the handler is built:
	// values that could be converted ahead of time, and the attrs that could
	// not. See handlerConfig.prepareExtraFields.
	extraStatic  map[string]any
	extraDynamic []slog.Attr

	// recordAttrs is what the record itself contributes to the innermost map,
	// so that map can be sized for it. See treeSize.
	recordAttrs int
}

// build assembles the attribute tree for one record.
//
// Placement follows the slog contract for everything slog defines: attrs from
// WithAttrs and from the record land inside whatever groups are open at that
// point. Two things slog does not define are placed at the root deliberately:
//
//   - source, matching the standard library, where the built-in attributes are
//     unaffected by WithGroup;
//   - attributes extracted from the context, which are ambient facts about the
//     request rather than about the call site. A trace ID should not move
//     around the payload depending on which derived logger happened to emit
//     the line.
//   - the handler's extra fields, for the same reason, one step more global
//     still.
//
// Those three are applied in increasing order of generality, and each of them
// yields to a key already taken. So a record attribute beats source, which
// beats a context extractor, which beats an extra field — the more specific
// statement about this particular line wins.
//
// Source yields for the same reason as the rest, and the standard library
// agrees: JSONHandler writes its built-in source before the record's attrs, so
// an attribute keyed "source" appears second and, under the last-wins rule that
// every JSON consumer applies, is the one that survives. Emitting both is not
// open to us — the tree is a map, so a key appears once — so yielding is how
// the same outcome is reached.
func (b *treeBuilder) build(goas []groupOrAttrs, ctxAttrs []slog.Attr, r *slog.Record, source map[string]any) map[string]any {
	b.recordAttrs = r.NumAttrs()
	root := b.walk(goas, nil, func(dst map[string]any, groups []string) {
		r.Attrs(func(a slog.Attr) bool {
			b.appendAttr(dst, groups, a)
			return true
		})
	})

	// All three route their attributes through appendAttr rather than writing
	// them straight into the map, so they get the same value mapping and the
	// same ReplaceAttr treatment as any other attribute. The one consequence
	// worth knowing: the taken-check runs before appendAttr, so a ReplaceAttr
	// that renames keys can defeat it.
	//
	// extraStatic is the one exception, and only ever populated when there is
	// no ReplaceAttr: those values went through the same conversion, once, when
	// the handler was built. See preAttr.
	if source != nil {
		if _, taken := root[slog.SourceKey]; !taken {
			b.appendAttr(root, nil, slog.Any(slog.SourceKey, source))
		}
	}
	for _, a := range ctxAttrs {
		if _, taken := root[a.Key]; taken {
			continue
		}
		b.appendAttr(root, nil, a)
	}
	for k, v := range b.extraStatic {
		if _, taken := root[k]; taken {
			continue
		}
		root[k] = v
	}
	for _, a := range b.extraDynamic {
		if _, taken := root[a.Key]; taken {
			continue
		}
		b.appendAttr(root, nil, a)
	}
	return root
}

// walk processes the accumulated WithGroup/WithAttrs calls, calling tail once
// inside the innermost open group.
//
// It builds each group into its own map and attaches that map to its parent
// only if something actually landed in it. That is what satisfies the contract
// rule "if a group has no Attrs, ignore it" for groups opened by WithGroup,
// whose contents arrive later and may never arrive at all. Because the check
// unwinds outward, a chain of empty nested groups collapses entirely.
func (b *treeBuilder) walk(goas []groupOrAttrs, groups []string, tail func(map[string]any, []string)) map[string]any {
	dst := make(map[string]any, treeSize(goas, b.recordAttrs))

	for i, goa := range goas {
		if goa.group != "" {
			// Everything after this entry belongs inside the group, so the
			// remainder of the list is consumed by the recursion.
			child := b.walk(goas[i+1:], childPath(groups, goa.group), tail)
			if len(child) > 0 {
				dst[goa.group] = child
			}
			return dst
		}
		b.appendGroupOrAttrs(dst, groups, goa)
	}

	tail(dst, groups)
	return dst
}

// treeSize is how many keys one level of the tree is about to hold: every
// WithAttrs entry up to the first group, plus either that group or, when there
// is none, the record's own attributes.
//
// It is a hint, not a bound — a ReplaceAttr may discard attributes, groups may
// collapse, and the root takes source, context attributes and extra fields on
// top. Getting it approximately right is what matters: a map built with no hint
// at all rehashes once past eight keys, which for a ten-attribute record is an
// extra allocation and half again the bytes (952 B and 5 allocations against
// 664 B and 4). Below eight keys the hint costs and saves nothing.
func treeSize(goas []groupOrAttrs, recordAttrs int) int {
	n := 0
	for _, goa := range goas {
		if goa.group != "" {
			return n + 1
		}
		n += len(goa.attrs)
	}
	return n + recordAttrs
}

// appendGroupOrAttrs writes the attributes of one WithAttrs call into dst,
// taking the hoisted values where preResolve produced them.
func (b *treeBuilder) appendGroupOrAttrs(dst map[string]any, groups []string, goa groupOrAttrs) {
	if goa.pre == nil {
		for _, a := range goa.attrs {
			b.appendAttr(dst, groups, a)
		}
		return
	}
	for _, p := range goa.pre {
		if p.static {
			// Plain assignment, exactly as appendAttr's tail would do it: a
			// hoisted attr has a non-empty key, is not the zero Attr and is not
			// a group, all checked when it was hoisted.
			dst[p.attr.Key] = p.value
			continue
		}
		b.appendAttr(dst, groups, p.attr)
	}
}

// childPath returns groups extended by one level, in an array of its own.
//
// The obvious append(groups, name) is *almost* right: the extended path is
// dead before a sibling at the same depth reuses the slot it borrowed, so
// nothing here observes the sharing. But "dead before" is a property of the
// traversal that has to be re-proved by hand every time this file is touched,
// and it is not a property callers can see — a ReplaceAttr that keeps the
// slice it was handed finds its path rewritten by the next sibling group.
// slog.JSONHandler leaves that edge exposed (its group stack is pooled, pushed
// and popped); allocating exactly len+1 here closes it for one small
// allocation, next to the map allocated by the caller anyway, and measures as
// no change at all in BenchmarkHandleGroups.
//
// This is the same reasoning, and the same exact-size copy, as
// Handler.withGroupOrAttrs.
func childPath(groups []string, name string) []string {
	sub := make([]string, len(groups)+1)
	copy(sub, groups)
	sub[len(groups)] = name
	return sub
}

// appendAttr writes one attribute into dst.
//
// The order of the steps is the specification, not a style choice:
//
//  1. Resolve first, because ReplaceAttr is documented to receive an already
//     resolved value, and because a LogValuer may resolve to a Group.
//  2. ReplaceAttr, never for a Group attr itself. Resolve again afterwards:
//     ReplaceAttr may return a LogValuer.
//  3. Elide the zero Attr, using exactly the test the contract names.
//  4. Groups, which recurse.
func (b *treeBuilder) appendAttr(dst map[string]any, groups []string, a slog.Attr) {
	a.Value = a.Value.Resolve()

	if b.replace != nil && a.Value.Kind() != slog.KindGroup {
		a = b.replace(groups, a)
		a.Value = a.Value.Resolve()
	}

	if a.Equal(slog.Attr{}) {
		return
	}

	if a.Value.Kind() == slog.KindGroup {
		members := a.Value.Group()
		if len(members) == 0 {
			return
		}
		if a.Key == "" {
			// An empty group key inlines the group's members into the parent.
			for _, m := range members {
				b.appendAttr(dst, groups, m)
			}
			return
		}
		child := make(map[string]any, len(members))
		sub := childPath(groups, a.Key)
		for _, m := range members {
			b.appendAttr(child, sub, m)
		}
		// Every member may have been elided, which leaves the group empty
		// after the fact and therefore ignorable.
		if len(child) == 0 {
			return
		}
		dst[a.Key] = child
		return
	}

	if a.Key == "" {
		// A non-group attr with an empty key has no representation in a JSON
		// object. The zero-Attr rule above has already passed, so this is a
		// keyless attr with a real value; dropping it is the only option that
		// does not corrupt the object.
		return
	}

	// Plain assignment, so a repeated key overwrites and the last write wins.
	// That is the whole of this package's duplicate-key policy, and it is
	// deliberate rather than a side effect of the tree being a map. Do not make
	// this yield to the existing value — slog's own semantics are that a
	// call-site attribute overrides one from the With chain that produced the
	// logger, and reversing that here would make this
	// the only handler where it does not.
	dst[a.Key] = b.value(a.Value)
}

// value converts a resolved, non-group slog.Value into something the encoder
// can marshal.
func (b *treeBuilder) value(v slog.Value) any {
	if out, ok := staticValue(v); ok {
		return out
	}
	return anyValue(v.Any())
}

// staticValue converts the kinds whose conversion depends on nothing but the
// value itself, and reports false for the rest.
//
// The split exists so that an attribute accumulated by WithAttrs or given to
// WithExtraFields can be converted once instead of on every record: what it
// returns is immutable, self-contained and identical each time, so one boxed
// copy can be shared by every record that carries it. Everything it declines is
// something that must not be hoisted — a LogValuer, whose whole purpose is to
// be resolved late; a group, which needs a map of its own per record; and
// KindAny, because an error becomes a fresh map (see errorValue) and because
// conversion there is a no-op that saves nothing anyway.
//
// Keeping it a separate function is what keeps the two paths honest: the
// per-record path goes through it too, so a hoisted value cannot drift from
// the one a record would have produced.
func staticValue(v slog.Value) (any, bool) {
	switch v.Kind() {
	case slog.KindString:
		return v.String(), true
	case slog.KindInt64:
		return v.Int64(), true
	case slog.KindUint64:
		return v.Uint64(), true
	case slog.KindFloat64:
		return v.Float64(), true
	case slog.KindBool:
		return v.Bool(), true
	case slog.KindDuration:
		// Durations marshal as an integer nanosecond count otherwise, which is
		// unreadable in a log viewer.
		return v.Duration().String(), true
	case slog.KindTime:
		// Round(0) strips the monotonic reading so the value marshals as a
		// plain wall-clock timestamp.
		return v.Time().Round(0), true
	default:
		return nil, false
	}
}

// anyValue handles the values slog carries as KindAny.
//
// Errors get special treatment because the obvious thing is badly broken: most
// error types have no exported fields, so encoding/json renders them as {} and
// the actual message is lost. That is the single most common attribute in an
// error-level log line. Detection is by type rather than by attribute key, so
// slog.Any("cause", err) is formatted just as well as slog.Any("error", err).
func anyValue(v any) any {
	if err, ok := v.(error); ok {
		return errorValue(err)
	}
	return v
}

func errorValue(err error) map[string]any {
	if err == nil {
		return nil
	}
	return map[string]any{
		"message": err.Error(),
		"type":    fmt.Sprintf("%T", err),
	}
}

// callSite is one symbolized program counter: the three fields the payload
// needs, without the rest of a runtime.Frame, and already boxed into the any
// each one is assigned as.
//
// Boxing here rather than per record is safe where sharing the map is not. A
// string and an int are immutable, so every record's source map may hold the
// same three interface values; the map itself is still built fresh, because it
// lands in the payload where a ReplaceAttr or a custom Converter may legally
// mutate it. That takes three allocations per record off WithAddSource — more
// than the symbolization the cache around it already saves.
type callSite struct {
	function any // string, runtime.Frame.Function
	file     any // string, runtime.Frame.File
	line     any // int, runtime.Frame.Line
}

// sourceCache memoizes symbolization by program counter.
//
// runtime.CallersFrames is what WithAddSource costs — it walks the pclntab and
// allocates both the Frames and the []uintptr behind it — and it answers the
// same question every time, because a program logs from the same call sites
// over and over. Only the resolved triple is cached. The map handed to the tree
// is built fresh per record on purpose: it lands in the payload, where a
// ReplaceAttr or a custom Converter may legally mutate it.
//
// Eviction is two-generation second chance, not a true LRU, and the reason is
// the hot path. A strict LRU has to mutate shared order state — a list, or a
// per-entry timestamp — on every *hit*, which means a lock or a contended
// atomic write on every log call in the process: more than the ~180 ns a hit
// saves, and a point every logging goroutine serialises on. So recency is kept
// at generation granularity instead. Lookups read the hot generation; a hit in
// cold is carried back into hot on the way out. When hot fills — max *newly
// discovered* call sites — it becomes cold and the previous cold is dropped
// whole. The guarantee that buys is the one that matters: **a call site used at
// least once in the time it takes the process to meet max new ones is never
// evicted**, however long it has been cached and however much has been cached
// since. A site that has genuinely fallen out of use costs one re-symbolization
// if it comes back.
//
// The ceiling is therefore 2*max live entries — a full hot generation plus a
// full cold one — which at a measured ~156 B per entry is ~2.5 MB at max=8192,
// and a few tens of KB for the few hundred call sites a normal program has.
// Entries are cheap because Frame.Function and Frame.File point into the
// binary's own tables; the cache copies no string data.
//
// max is a field rather than a constant so the tests can reach the full state,
// and the rotation, cheaply — the same kind of seam as the limiter's clock.
type sourceCache struct {
	max int64

	hot  atomic.Pointer[sync.Map] // uintptr -> callSite, the generation being filled
	cold atomic.Pointer[sync.Map] // the previous generation, still readable
	n    atomic.Int64             // call sites newly discovered since the last rotation

	rotating sync.Mutex // serialises rotation, and nothing on the lookup path
}

// maxCachedSources is a generation's size, so up to twice this many call sites
// are held. A program with more distinct logging call sites than that still
// works, and degrades gently: it rotates more often, and what it keeps is
// whatever it is actually logging from.
const maxCachedSources = 8192

var sources = newSourceCache(maxCachedSources)

func newSourceCache(perGeneration int64) *sourceCache {
	c := &sourceCache{max: perGeneration}
	c.hot.Store(&sync.Map{})
	return c
}

// lookup symbolizes pc, through the cache.
//
// It reports false for a program counter that resolves to nothing, and does not
// cache that: an unresolvable PC comes from a hand-built Record or a stripped
// binary, and caching one would let a stream of junk churn the generations.
func (c *sourceCache) lookup(pc uintptr) (callSite, bool) {
	if v, ok := c.hot.Load().Load(pc); ok {
		return v.(callSite), true
	}
	if cold := c.cold.Load(); cold != nil {
		if v, ok := cold.Load(pc); ok {
			site := v.(callSite)
			// Used in this generation, so it must survive the next rotation.
			c.promote(pc, site, cold)
			return site, true
		}
	}

	f, _ := runtime.CallersFrames([]uintptr{pc}).Next()
	if f.File == "" && f.Function == "" && f.Line == 0 {
		return callSite{}, false
	}
	site := callSite{function: f.Function, file: f.File, line: f.Line}
	c.remember(pc, site)
	return site, true
}

// remember records a newly discovered call site in the hot generation, and
// rotates when that generation is full.
func (c *sourceCache) remember(pc uintptr, site callSite) {
	hot := c.hot.Load()
	if _, loaded := hot.LoadOrStore(pc, site); loaded {
		return
	}
	if c.n.Add(1) >= c.max {
		c.rotate(hot)
	}
}

// promote carries a site that is still in use out of the retiring generation,
// so that it survives the next rotation.
//
// It deliberately does not count against the generation: n measures call sites
// newly *discovered*, not ones carried over. Counting them was the first cut,
// and it made the guarantee false — a site touched once per generation could
// have its own promotion fill the generation, rotate, and then be dropped by
// the next one, which is exactly the case the caller thinks it is protected
// from. TestSourceCache/a_site_used_every_generation_is_never_evicted fails
// against that version.
//
// Deleting from cold is what keeps the ceiling at 2*max: without it a promoted
// site is held in both generations at once.
func (c *sourceCache) promote(pc uintptr, site callSite, cold *sync.Map) {
	c.hot.Load().LoadOrStore(pc, site)
	cold.Delete(pc)
}

// rotate retires the full generation and starts an empty one.
//
// A store racing a rotation can land in the generation being retired, which
// costs that call site nothing worse than an earlier eviction, and the count
// can drift by the number of goroutines racing here — both are why the
// generation boundary is approximate by design rather than by accident.
func (c *sourceCache) rotate(full *sync.Map) {
	c.rotating.Lock()
	defer c.rotating.Unlock()

	// Another goroutine already retired this generation.
	if c.hot.Load() != full {
		return
	}
	c.cold.Store(full)
	c.hot.Store(&sync.Map{})
	c.n.Store(0)
}

// sourceValue resolves a Record's program counter to a call site.
//
// It reports false for a zero PC, which the contract requires be ignored, and
// which happens whenever a Record is constructed directly rather than through
// slog.Logger.
func sourceValue(pc uintptr) (map[string]any, bool) {
	if pc == 0 {
		return nil, false
	}
	site, ok := sources.lookup(pc)
	if !ok {
		return nil, false
	}
	return map[string]any{
		"function": site.function,
		"file":     site.file,
		"line":     site.line,
	}, true
}

// attrsFromContext runs the configured context extractors.
//
// The result is always a freshly allocated slice. Appending extracted
// attributes onto a slice owned by the handler is a data race the moment two
// goroutines log through handlers derived from a common parent, because the
// derived handlers share that slice's backing array.
func attrsFromContext(ctx context.Context, extractors []func(context.Context) []slog.Attr) []slog.Attr {
	if len(extractors) == 0 || ctx == nil {
		return nil
	}
	var out []slog.Attr
	for _, extract := range extractors {
		if extract == nil {
			continue
		}
		if attrs := extract(ctx); len(attrs) > 0 {
			out = append(out, attrs...)
		}
	}
	return out
}
