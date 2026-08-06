package betterstack

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const testToken = "src_test_token"

// recorder is an httptest.Server that captures every upload and asserts the
// wire-level invariants on all of them.
//
// Putting the header, compression and framing checks here rather than in
// individual tests means every test in the package gets them, and a regression
// in the request shape fails everywhere rather than nowhere.
type recorder struct {
	srv *httptest.Server
	t   *testing.T

	mu       sync.Mutex
	requests []recordedRequest
	statuses []int       // consumed in order; 202 once exhausted
	headers  http.Header // injected into responses
	delay    time.Duration
	gate     chan struct{} // when non-nil, handlers block on it
	newConns int
}

type recordedRequest struct {
	header  http.Header
	body    []byte // decompressed
	lines   [][]byte
	records []map[string]any
	at      time.Time
}

type recorderOption func(*recorder)

// withStatuses scripts the response codes, in order. After the script is
// exhausted every request gets 202.
func withStatuses(codes ...int) recorderOption {
	return func(r *recorder) { r.statuses = codes }
}

func withResponseHeader(key, value string) recorderOption {
	return func(r *recorder) {
		if r.headers == nil {
			r.headers = http.Header{}
		}
		r.headers.Set(key, value)
	}
}

func withDelay(d time.Duration) recorderOption {
	return func(r *recorder) { r.delay = d }
}

// withGate blocks every handler until release is called, for testing what the
// client does while uploads are stalled.
func withGate() recorderOption {
	return func(r *recorder) { r.gate = make(chan struct{}) }
}

func newRecorder(t *testing.T, opts ...recorderOption) *recorder {
	t.Helper()

	rec := &recorder{t: t}
	for _, opt := range opts {
		opt(rec)
	}

	// Unstarted, so ConnState is installed before the serve loop reads it.
	// NewServer starts serving immediately and setting Config afterwards is a
	// data race on the Server itself.
	rec.srv = httptest.NewUnstartedServer(http.HandlerFunc(rec.serve))
	rec.srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			rec.mu.Lock()
			rec.newConns++
			rec.mu.Unlock()
		}
	}
	rec.srv.Start()
	// The client must always be closed before the server: Server.Close waits
	// for outstanding requests, and a client still retrying into it would
	// deadlock the cleanup.
	t.Cleanup(rec.srv.Close)
	return rec
}

func (r *recorder) serve(w http.ResponseWriter, req *http.Request) {
	if r.gate != nil {
		<-r.gate
	}
	if r.delay > 0 {
		time.Sleep(r.delay)
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.t.Errorf("reading request body: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	captured := r.check(req.Header, body)

	r.mu.Lock()
	r.requests = append(r.requests, captured)
	status := http.StatusAccepted
	if len(r.statuses) > 0 {
		status, r.statuses = r.statuses[0], r.statuses[1:]
	}
	headers := r.headers.Clone()
	r.mu.Unlock()

	for k, vs := range headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(status)
	// A body on the error paths, so the drain-before-close discipline is
	// actually exercised rather than trivially satisfied by an empty body.
	_, _ = io.WriteString(w, `{"status":"recorded"}`)
}

// check asserts the invariants that must hold for every single upload.
func (r *recorder) check(header http.Header, raw []byte) recordedRequest {
	r.t.Helper()

	if got, want := header.Get("Authorization"), "Bearer "+testToken; got != want {
		r.t.Errorf("Authorization = %q, want %q", got, want)
	}
	if got := header.Get("User-Agent"); got == "" {
		r.t.Error("User-Agent is empty")
	}

	body := raw
	if header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			r.t.Fatalf("body is not valid gzip despite Content-Encoding: %v", err)
		}
		if body, err = io.ReadAll(zr); err != nil {
			r.t.Fatalf("decompressing body: %v", err)
		}
		if err := zr.Close(); err != nil {
			r.t.Errorf("closing gzip reader: %v", err)
		}
	}

	captured := recordedRequest{header: header.Clone(), body: body, at: time.Now()}

	if header.Get("Content-Type") == "application/x-ndjson" {
		if len(body) == 0 {
			r.t.Error("empty NDJSON body")
		} else if body[len(body)-1] != '\n' {
			r.t.Errorf("NDJSON body is not newline-terminated: %q", body)
		}
		for i, line := range bytes.Split(bytes.TrimSuffix(body, []byte("\n")), []byte("\n")) {
			if len(line) == 0 {
				r.t.Errorf("blank line at index %d in NDJSON body", i)
				continue
			}
			var m map[string]any
			if err := json.Unmarshal(line, &m); err != nil {
				r.t.Errorf("line %d is not valid JSON: %v (%q)", i, err, line)
				continue
			}
			captured.lines = append(captured.lines, line)
			captured.records = append(captured.records, m)
		}
	}
	return captured
}

func (r *recorder) endpoint() string { return r.srv.URL }

func (r *recorder) release() { close(r.gate) }

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

func (r *recorder) connections() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.newConns
}

func (r *recorder) all() []recordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedRequest(nil), r.requests...)
}

// records flattens every record from every request, in arrival order.
func (r *recorder) records() []map[string]any {
	var out []map[string]any
	for _, req := range r.all() {
		out = append(out, req.records...)
	}
	return out
}

// waitFor blocks until the predicate holds, polling rather than sleeping so the
// common case is fast and a loaded CI machine does not turn a timing assumption
// into a flake.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitForRequests waits until exactly n requests have arrived.
func (r *recorder) waitForRequests(t *testing.T, n int) {
	t.Helper()
	waitFor(t, "requests", func() bool { return r.count() >= n })
}

// newTestClient builds a client pointed at the recorder, with the settings
// every deterministic test wants: a batch interval long enough that only the
// trigger under test can fire, and errors collected rather than printed.
func newTestClient(t *testing.T, rec *recorder, opts ...ClientOption) (*Client, *errorSink) {
	t.Helper()

	errs := &errorSink{}
	base := []ClientOption{
		WithEndpoint(rec.endpoint()),
		WithBatchInterval(time.Hour),
		WithOnError(errs.add),
	}
	c, err := NewClient(testToken, append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c, errs
}

// errorSink collects OnError callbacks. It must be safe for concurrent use:
// OnError is invoked from the sender and from every upload worker.
type errorSink struct {
	mu   sync.Mutex
	errs []error
}

func (s *errorSink) add(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
}

func (s *errorSink) all() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}

func (s *errorSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.errs)
}
