package betterstack

import "log/slog"

// Reserved top-level keys in the Better Stack payload.
const (
	// KeyTime is the timestamp field the ingestion API reads. It accepts UNIX
	// seconds, milliseconds or nanoseconds, or an RFC 3339 / ISO 8601 string;
	// an unparseable value is stored as a plain string and the server's
	// reception time is used instead.
	KeyTime = "dt"
	// KeyLevel is the severity field.
	KeyLevel = "level"
	// KeyMessage is the log message field.
	KeyMessage = "message"
)

// DefaultContextKey is where attributes are nested by default.
//
// "context" rather than "extra": it matches the vocabulary the other official
// Better Stack clients use, several of which nest structured data under a
// "context" block of their own.
const DefaultContextKey = "context"

// ConvertOptions carries the record-shape settings a Converter needs.
type ConvertOptions struct {
	// ContextKey is the payload key that attributes are nested under. An empty
	// string flattens them to the top level, where the reserved keys above win
	// on collision.
	ContextKey string
}

// Converter builds the payload for one log record.
//
// The handler has already done everything the log/slog contract requires:
// attrs holds the finished attribute tree, with groups nested, LogValuers
// resolved, ReplaceAttr applied and empty attributes elided. A Converter only
// decides the shape of the record around it — which top-level keys exist and
// where attrs is hung.
//
// That split is deliberate. Handing a Converter the raw attributes, open groups
// and ReplaceAttr function would make every custom converter re-implement the
// slog contract, and a subtle mistake there would silently break conformance
// for the whole handler. As it stands, conformance is a property of the
// handler and holds whatever Converter is installed.
//
// r must not be retained: the handler reuses it.
type Converter func(r *slog.Record, attrs map[string]any, o ConvertOptions) map[string]any

// DefaultConverter produces the documented Better Stack record shape:
//
//	{
//	  "dt":      "2026-08-06T10:11:12.123456789Z",
//	  "level":   "ERROR",
//	  "message": "a message",
//	  "context": { ... attributes ... }
//	}
//
// Two decisions worth knowing about:
//
// The library's own name and version are not in the payload. They already
// travel in the User-Agent header, where the server can read them without
// charging the customer for the bytes on every single record.
//
// A record with no timestamp omits dt entirely rather than sending the zero
// time or substituting the current one. The slog contract says a zero
// Record.Time means "ignore the time", and the ingestion API stamps its own
// reception time when dt is absent — which is more accurate than a
// client-side stamp applied at batch-assembly time, potentially a batch
// interval and several retry backoffs after the event, from an unsynchronised
// clock.
func DefaultConverter(r *slog.Record, attrs map[string]any, o ConvertOptions) map[string]any {
	payload := make(map[string]any, len(attrs)+3)

	if o.ContextKey == "" {
		// Flattened: attributes go to the top level, and the reserved keys are
		// written afterwards so they win on collision. A record shaped like a
		// Better Stack payload must still be a valid Better Stack payload.
		for k, v := range attrs {
			payload[k] = v
		}
	} else if len(attrs) > 0 {
		payload[o.ContextKey] = attrs
	}

	if !r.Time.IsZero() {
		// Round(0) strips the monotonic reading; UTC and RFC 3339 with
		// nanoseconds is what encoding/json produces for a time.Time and is
		// accepted by the API.
		payload[KeyTime] = r.Time.Round(0).UTC()
	}
	payload[KeyLevel] = r.Level.String()
	payload[KeyMessage] = r.Message

	return payload
}

// compile-time proof that DefaultConverter satisfies Converter.
var _ Converter = DefaultConverter
