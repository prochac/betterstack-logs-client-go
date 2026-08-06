package betterstack

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
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
}

// treeBuilder turns a handler's accumulated state and one Record into the
// nested map that becomes the payload's attribute tree.
type treeBuilder struct {
	replace func(groups []string, a slog.Attr) slog.Attr
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
// open to us — see DESIGN §4 — so yielding is how the same outcome is reached.
func (b *treeBuilder) build(goas []groupOrAttrs, ctxAttrs []slog.Attr, r *slog.Record, source map[string]any, extra map[string]any) map[string]any {
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
	for k, v := range extra {
		if _, taken := root[k]; taken {
			continue
		}
		b.appendAttr(root, nil, slog.Any(k, v))
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
	dst := map[string]any{}

	for i, goa := range goas {
		if goa.group != "" {
			// Everything after this entry belongs inside the group, so the
			// remainder of the list is consumed by the recursion.
			child := b.walk(goas[i+1:], append(groups, goa.group), tail)
			if len(child) > 0 {
				dst[goa.group] = child
			}
			return dst
		}
		for _, a := range goa.attrs {
			b.appendAttr(dst, groups, a)
		}
	}

	tail(dst, groups)
	return dst
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
		sub := append(groups, a.Key)
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
	// deliberate rather than a side effect of the tree being a map: see
	// DESIGN §4. Do not make this yield to the existing value — slog's own
	// semantics are that a call-site attribute overrides one from the With
	// chain that produced the logger, and reversing that here would make this
	// the only handler where it does not.
	dst[a.Key] = b.value(a.Value)
}

// value converts a resolved, non-group slog.Value into something the encoder
// can marshal.
func (b *treeBuilder) value(v slog.Value) any {
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return v.Float64()
	case slog.KindBool:
		return v.Bool()
	case slog.KindDuration:
		// Durations marshal as an integer nanosecond count otherwise, which is
		// unreadable in a log viewer.
		return v.Duration().String()
	case slog.KindTime:
		// Round(0) strips the monotonic reading so the value marshals as a
		// plain wall-clock timestamp.
		return v.Time().Round(0)
	default:
		return anyValue(v.Any())
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

// sourceValue resolves a Record's program counter to a call site.
//
// It reports false for a zero PC, which the contract requires be ignored, and
// which happens whenever a Record is constructed directly rather than through
// slog.Logger.
func sourceValue(pc uintptr) (map[string]any, bool) {
	if pc == 0 {
		return nil, false
	}
	f, _ := runtime.CallersFrames([]uintptr{pc}).Next()
	if f.File == "" && f.Function == "" && f.Line == 0 {
		return nil, false
	}
	return map[string]any{
		"function": f.Function,
		"file":     f.File,
		"line":     f.Line,
	}, true
}

// attrsFromContext runs the configured context extractors.
//
// The result is always a freshly allocated slice. Appending extracted
// attributes onto a slice owned by the handler is a data race the moment two
// goroutines log through handlers derived from a common parent, because the
// derived handlers share that slice's backing array. That bug is live in the
// library this one replaces.
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
