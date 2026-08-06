package betterstack

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any goroutine outlives the tests.
//
// There are deliberately no Ignore options. This is a design constraint on the
// library, not a hygiene check on the tests: it is what forces the sender
// goroutine and the upload workers to terminate on Close rather than leaking
// until process exit, and it is why NewClient validates its configuration
// before starting anything. The usual reflex when this fails — adding an
// IgnoreTopFunction for net/http's connection loops — would paper over a
// client that does not release its transport on Close.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
