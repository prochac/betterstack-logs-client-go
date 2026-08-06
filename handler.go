package betterstack

import (
	"context"
	"log/slog"
)

// enqueuer is the part of *Client that Handler depends on. Naming it keeps the
// handler testable without a network stack, and documents that conversion does
// no I/O of its own.
type enqueuer interface {
	Enqueue(event map[string]any) error
}

// Handler is a slog.Handler that converts records and hands them to a Client.
//
// It does no I/O. Everything with a lifecycle — batching, HTTP, retry, Flush
// and Close — belongs to the Client, which the caller constructs and owns.
type Handler struct {
	client enqueuer
	cfg    handlerConfig

	// goas records the WithGroup and WithAttrs calls that produced this
	// handler, in order.
	goas []groupOrAttrs
}

var _ slog.Handler = (*Handler)(nil)

type handlerConfig struct {
	level           slog.Leveler
	addSource       bool
	replaceAttr     func(groups []string, a slog.Attr) slog.Attr
	attrFromContext []func(context.Context) []slog.Attr
	converter       Converter
	contextKey      string
}

// HandlerOption configures a Handler.
type HandlerOption func(*handlerConfig)

// WithLevel sets the minimum level the handler reports as enabled.
//
// The default is slog.LevelInfo, not Debug. Shipping debug records to a metered
// ingestion endpoint unless the user opts out is a billing surprise, and the
// user who wants them can always ask.
//
// A slog.LevelVar may be passed to change the level at runtime; it is read on
// every record.
func WithLevel(l slog.Leveler) HandlerOption {
	return func(c *handlerConfig) {
		if l != nil {
			c.level = l
		}
	}
}

// WithAddSource includes the call site of each record, under the "source" key,
// as function, file and line. Records with no program counter are unaffected.
func WithAddSource(add bool) HandlerOption {
	return func(c *handlerConfig) { c.addSource = add }
}

// WithReplaceAttr sets a function to rewrite attributes before they are
// encoded, with the semantics documented on slog.HandlerOptions.ReplaceAttr:
// the value is already resolved, returning a zero Attr discards the attribute,
// and it is never called for a group itself, only for the group's contents.
//
// It applies to attributes, including "source". It does not apply to the
// reserved top-level fields dt, level and message: those have fixed meanings in
// the ingestion API, and rewriting them would produce a payload the server
// cannot read. Use WithConverter to change the record shape.
func WithReplaceAttr(f func(groups []string, a slog.Attr) slog.Attr) HandlerOption {
	return func(c *handlerConfig) { c.replaceAttr = f }
}

// WithAttrFromContext adds extractors that pull attributes out of the context
// of each record — a request or trace ID, say.
//
// Extracted attributes are placed at the root of the attribute tree, outside
// any groups opened with WithGroup, because they describe the ambient request
// rather than the call site. A trace ID stays in the same place regardless of
// which derived logger emitted the line.
func WithAttrFromContext(extractors ...func(context.Context) []slog.Attr) HandlerOption {
	return func(c *handlerConfig) {
		c.attrFromContext = append(c.attrFromContext, extractors...)
	}
}

// WithConverter replaces the function that builds each record's payload. It is
// the supported way to change the wire shape. See Converter.
func WithConverter(conv Converter) HandlerOption {
	return func(c *handlerConfig) {
		if conv != nil {
			c.converter = conv
		}
	}
}

// WithContextKey sets the payload key that attributes are nested under. The
// default is "context". An empty string flattens attributes to the top level,
// where the reserved keys dt, level and message win on collision.
func WithContextKey(key string) HandlerOption {
	return func(c *handlerConfig) { c.contextKey = key }
}

// NewHandler returns a slog.Handler that sends records to c.
//
// It panics if c is nil. That is a programmer error either way — the nil would
// otherwise be dereferenced inside Handle, on whichever goroutine happened to
// log first — and failing at construction points at the actual mistake.
func NewHandler(c *Client, opts ...HandlerOption) *Handler {
	if c == nil {
		panic("betterstack: NewHandler called with a nil *Client")
	}
	return newHandler(c, opts...)
}

func newHandler(sink enqueuer, opts ...HandlerOption) *Handler {
	cfg := handlerConfig{
		level:      slog.LevelInfo,
		converter:  DefaultConverter,
		contextKey: DefaultContextKey,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &Handler{client: sink, cfg: cfg}
}

// Enabled reports whether records at the given level are handled.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.cfg.level.Level()
}

// Handle converts the record and enqueues it.
//
// Conversion and encoding happen here, on the caller's goroutine, and delivery
// does not: Enqueue hands the encoded bytes to a bounded queue and returns.
// Handle therefore never blocks on the network, and a Better Stack outage
// cannot become an outage in the calling application.
//
// The returned error is only ever a local one — an encoding failure, or
// ErrClosed. Delivery failures happen long after Handle has returned and are
// reported through the client's OnError callback instead. slog.Logger discards
// this error, but middleware such as slogmulti.RecoverHandlerError does not, so
// returning it keeps the handler composable at no cost.
func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	b := treeBuilder{replace: h.cfg.replaceAttr}

	var source map[string]any
	if h.cfg.addSource {
		if s, ok := sourceValue(r.PC); ok {
			source = s
		}
	}

	attrs := b.build(h.goas, attrsFromContext(ctx, h.cfg.attrFromContext), &r, source)
	payload := h.cfg.converter(&r, attrs, ConvertOptions{ContextKey: h.cfg.contextKey})

	return h.client.Enqueue(payload)
}

// WithAttrs returns a handler that also includes attrs, sharing the same
// Client so that every derived handler feeds one queue.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	// The interface says the handler owns the slice, but callers pass slices
	// that slog may reuse, so copy rather than retain.
	owned := make([]slog.Attr, len(attrs))
	copy(owned, attrs)
	return h.withGroupOrAttrs(groupOrAttrs{attrs: owned})
}

// WithGroup returns a handler that qualifies all subsequent attributes with
// name, sharing the same Client.
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h.withGroupOrAttrs(groupOrAttrs{group: name})
}

// withGroupOrAttrs derives a handler with one more accumulated call.
//
// The new list is allocated at exactly the length required, so no append ever
// writes into a parent's backing array. Deriving two handlers from one parent
// and logging through both concurrently is therefore safe by construction —
// growing a shared slice in place is a live data race in the library this one
// replaces, and it needs no synchronisation to avoid, only an exact-size copy.
func (h *Handler) withGroupOrAttrs(goa groupOrAttrs) *Handler {
	clone := *h
	clone.goas = make([]groupOrAttrs, len(h.goas)+1)
	copy(clone.goas, h.goas)
	clone.goas[len(h.goas)] = goa
	return &clone
}
