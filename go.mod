module github.com/prochac/logs-client-go

go 1.21

// The client itself has no production dependencies: standard library only. Both
// requires below are test-only, and neither is built by anything that imports
// this module.
//
// goleak enforces that the sender goroutine and the upload workers terminate
// deterministically on Close rather than leaking until exit.
//
// shamaton/msgpack supplies a MessagePack codec to the tests. MsgPack ships none
// — it takes a Marshaler from the caller — so the tests have to bring one, and
// bringing an independent implementation is what makes them evidence of
// interoperability rather than of agreeing with ourselves.
require (
	github.com/shamaton/msgpack/v2 v2.4.1
	go.uber.org/goleak v1.3.0
)
