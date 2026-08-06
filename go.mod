module github.com/prochac/logs-client-go

go 1.21

// The client itself has no production dependencies: standard library only.
// goleak is test-only, and enforces that the sender goroutine and the upload
// workers terminate deterministically on Close rather than leaking until exit.
require go.uber.org/goleak v1.3.0
