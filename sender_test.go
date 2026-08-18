package betterstack

import (
	"testing"
	"time"
)

// disarm's drain is the branch the sender's timer state machine is built
// around, and it is the one the rest of the suite structurally cannot reach:
// every deterministic test sets WithBatchInterval(time.Hour) precisely so the
// timer never fires. It runs when the interval expires and a size-, byte-cap-,
// Flush- or Close-triggered flush arrives before the sender has consumed the
// fire.
//
// Its comment argues the receive cannot block. If that argument ever stops
// holding, the sender parks forever, Close hangs, and goleak reports it on
// whichever machine happened to interleave that way — so it is worth asserting
// rather than reasoning about.
//
// The state is built directly instead of being provoked through a client: the
// fire has to be pending and unconsumed, which is exactly the window a running
// sender closes as fast as it can. len on a timer channel is what makes it
// observable — it says the value has arrived without taking it.
func TestDisarmDrainsAFiredTimer(t *testing.T) {
	t.Parallel()

	s := &sender{timer: newStoppedTimer(), armed: true}
	defer s.timer.Stop()

	// Which timer semantics apply is the toolchain's call and not this module's.
	// Go 1.23 made timer channels synchronous, and there a Stop clears any fire
	// that has not been received, so it cannot report false with a value left
	// to take: the drain is unreachable rather than untested. A go1.21 module
	// still got the old buffered channels through GODEBUG asynctimerchan up to
	// go1.26, and go1.27 removed that setting, so both worlds are live. cap is
	// the discriminator — 1 under the old semantics, 0 under the new.
	if cap(s.timer.C) == 0 {
		t.Skip("synchronous timer channels: Stop clears a pending fire itself, so disarm never drains one")
	}

	s.timer.Reset(time.Millisecond)
	waitFor(t, "the batch timer to fire", func() bool { return len(s.timer.C) == 1 })

	// In a goroutine, so that a disarm that blocks fails the test instead of
	// hanging the whole binary until the package timeout.
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.disarm()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("disarm blocked on the receive its comment says cannot block")
	}

	if s.armed {
		t.Error("armed is still set after disarm")
	}
	// The next arm calls Reset, which is only correct on a stopped timer with an
	// empty channel. A leftover fire here would flush the following batch
	// immediately, whatever the batch interval says.
	if got := len(s.timer.C); got != 0 {
		t.Errorf("the timer channel holds %d values after disarm, want 0", got)
	}
}
