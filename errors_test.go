package betterstack

import (
	"strings"
	"testing"
)

// The operator reading a StatusError is not looking at our source, and the
// endpoint's own body is terse ({"error": "Unauthorized"}). The message has to
// name the thing to go fix.
func TestStatusErrorMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      *StatusError
		contains []string
	}{
		{
			name:     "401 names the source token",
			err:      &StatusError{StatusCode: 401, Body: `{"error": "Unauthorized"}`, Records: 7},
			contains: []string{"401", "7 record", "source token", "Unauthorized"},
		},
		{
			// The docs name 403; the live endpoint answers 401 (PARITY §1).
			// Both must read the same way.
			name:     "403 names the source token too",
			err:      &StatusError{StatusCode: 403, Records: 1},
			contains: []string{"403", "source token"},
		},
		{
			name:     "402 names the quota",
			err:      &StatusError{StatusCode: 402, Records: 3},
			contains: []string{"402", "quota"},
		},
		{
			name:     "406 admits it is our bug",
			err:      &StatusError{StatusCode: 406, Records: 3},
			contains: []string{"406", "bug in this client"},
		},
		{
			name:     "413 names the knob to turn",
			err:      &StatusError{StatusCode: 413, Records: 1000},
			contains: []string{"413", "WithMaxBatchBytes"},
		},
		{
			name:     "an unknown status still reports cleanly",
			err:      &StatusError{StatusCode: 418, Records: 2},
			contains: []string{"418", "2 record"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.err.Error()
			for _, want := range tt.contains {
				if !strings.Contains(got, want) {
					t.Errorf("Error() = %q, want it to contain %q", got, want)
				}
			}
		})
	}
}

func TestDropErrorMessage(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		reason DropReason
		want   string
	}{
		{DropQueueFull, "queue full"},
		{DropBacklog, "upload backlog full"},
		{DropRejected, "rejected by ingest"},
		{DropOversize, "over the size limit"},
		{DropClosed, "client closed"},
	} {
		err := &DropError{Records: 5, Reason: tt.reason}
		got := err.Error()
		if !strings.Contains(got, tt.want) {
			t.Errorf("Error() = %q, want it to contain %q", got, tt.want)
		}
		if !strings.Contains(got, "5 record") {
			t.Errorf("Error() = %q, want it to report the record count", got)
		}
	}
}

func TestDropReasonStringIsTotal(t *testing.T) {
	t.Parallel()
	// An unnamed reason must still format rather than returning "".
	if got := DropReason(99).String(); got == "" {
		t.Error("DropReason(99).String() is empty")
	}
}

// A panic inside a user callback must not escape. safeReport runs on the sender
// goroutine, where an unrecovered panic would take down the host process.
func TestSafeReportContainsPanics(t *testing.T) {
	t.Parallel()

	called := false
	safeReport(func(error) {
		called = true
		panic("user callback blew up")
	}, ErrClosed)

	if !called {
		t.Error("OnError was not called")
	}
	// Reaching here at all is the assertion: the panic did not propagate.
}

func TestSafeReportIgnoresNils(t *testing.T) {
	t.Parallel()

	safeReport(nil, ErrClosed) // must not dereference a nil callback

	called := false
	safeReport(func(error) { called = true }, nil)
	if called {
		t.Error("OnError was called for a nil error")
	}
}
