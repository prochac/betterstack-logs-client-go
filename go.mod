module github.com/prochac/logs-client-go

go 1.21

// The client itself has no production dependencies: standard library only. Both
// requires below are test-only, and neither is built by anything that imports
// this module.
//
// goleak enforces that the sender goroutine and the upload workers terminate
// deterministically on Close rather than leaking until exit.
require go.uber.org/goleak v1.3.0
