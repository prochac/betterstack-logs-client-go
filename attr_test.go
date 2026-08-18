package betterstack

import (
	"runtime"
	"sync"
	"testing"
)

// callerPC returns this function's caller's program counter, shaped like the one
// slog.Logger records: a return address, which CallersFrames adjusts itself.
func callerPC() uintptr {
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])
	return pcs[0]
}

// cached reports whether pc is held in either generation. Membership is checked
// directly rather than through lookup, so that eviction tests do not depend on
// what the runtime makes of a synthetic PC.
func cached(c *sourceCache, pc uintptr) bool {
	if _, ok := c.hot.Load().Load(pc); ok {
		return true
	}
	if cold := c.cold.Load(); cold != nil {
		if _, ok := cold.Load(pc); ok {
			return true
		}
	}
	return false
}

func liveEntries(c *sourceCache) int {
	n := 0
	count := func(m *sync.Map) {
		if m == nil {
			return
		}
		m.Range(func(any, any) bool { n++; return true })
	}
	count(c.hot.Load())
	count(c.cold.Load())
	return n
}

// site returns a distinct synthetic entry, so the eviction policy can be
// exercised without a real call site per case.
func site(i int) (uintptr, callSite) {
	return uintptr(0x400000 + i*16), callSite{function: "fn", file: "f.go", line: i}
}

// A program counter the runtime cannot symbolize yields no source key at all.
// The alternative — a zero callSite — would put
// {"function":"","file":"","line":0} on the record, which reads as a real
// answer and is not one. Reachable in the wild: a stripped binary has nothing
// to resolve against.
func TestSourceValueOfAnUnresolvablePC(t *testing.T) {
	t.Parallel()

	// Not a return address of anything: no text segment starts here.
	if got, ok := sourceValue(uintptr(1)); ok {
		t.Errorf("sourceValue of an unresolvable PC = %v, true; want it omitted", got)
	}
	// The zero PC, which is what a Record built by hand rather than through
	// slog.Logger carries, is the same answer by a different route.
	if got, ok := sourceValue(0); ok {
		t.Errorf("sourceValue(0) = %v, true; want it omitted", got)
	}
}

func TestSourceCache(t *testing.T) {
	t.Parallel()

	t.Run("a hit resolves to what the miss did", func(t *testing.T) {
		t.Parallel()
		c := newSourceCache(maxCachedSources)
		pc := callerPC()

		miss, ok := c.lookup(pc)
		if !ok {
			t.Fatalf("lookup(%#x) failed on a live PC", pc)
		}
		hit, ok := c.lookup(pc)
		if !ok {
			t.Fatalf("lookup(%#x) failed on the second call", pc)
		}
		if hit != miss {
			t.Errorf("cached %+v, resolved %+v", hit, miss)
		}
		if c.n.Load() != 1 {
			t.Errorf("stored %d entries for one PC", c.n.Load())
		}
	})

	// The triple is shared; the map that carries it into the payload is not,
	// because a ReplaceAttr or a custom Converter may mutate what it is handed.
	t.Run("the map is fresh per call", func(t *testing.T) {
		t.Parallel()
		pc := callerPC()

		first, ok := sourceValue(pc)
		if !ok {
			t.Fatalf("sourceValue(%#x) failed on a live PC", pc)
		}
		first["function"] = "clobbered"

		second, ok := sourceValue(pc)
		if !ok {
			t.Fatalf("sourceValue(%#x) failed on the second call", pc)
		}
		if second["function"] == "clobbered" {
			t.Error("the second record got the first record's map")
		}
	})

	// The guarantee the two generations exist to give.
	t.Run("a site used every generation is never evicted", func(t *testing.T) {
		t.Parallel()
		const perGen = 4
		c := newSourceCache(perGen)

		pc, s := site(0)
		c.remember(pc, s)

		for gen := 1; gen <= 5; gen++ {
			got, ok := c.lookup(pc)
			if !ok {
				t.Fatalf("generation %d: the used site was evicted", gen)
			}
			if got != s {
				t.Fatalf("generation %d: got %+v, want %+v", gen, got, s)
			}
			for i := 0; i < perGen; i++ { // fill a whole generation with strangers
				c.remember(site(gen*100 + i))
			}
		}

		if !cached(c, pc) {
			t.Error("the used site is gone after five generations")
		}
	})

	// The other half: what is not used goes away, which is what makes the cache
	// usable in a process with more call sites than a generation holds.
	t.Run("an unused site is evicted within two generations", func(t *testing.T) {
		t.Parallel()
		const perGen = 4
		c := newSourceCache(perGen)

		cold, s := site(0)
		c.remember(cold, s)

		for i := 1; i <= 2*perGen; i++ { // two full generations, never touching it
			c.remember(site(i))
		}

		if cached(c, cold) {
			t.Error("an untouched site survived two rotations")
		}
	})

	t.Run("live entries stay under two generations", func(t *testing.T) {
		t.Parallel()
		const perGen = 8
		c := newSourceCache(perGen)

		for i := 0; i < 40*perGen; i++ {
			c.remember(site(i))
			if live := liveEntries(c); live > 2*perGen {
				t.Fatalf("after %d inserts: %d live entries, ceiling is %d", i+1, live, 2*perGen)
			}
		}
	})

	// A hand-built Record carries no PC, and a junk one must not be memoized —
	// caching misses is how a stream of garbage would churn the generations.
	t.Run("unresolvable PCs are neither reported nor cached", func(t *testing.T) {
		t.Parallel()
		c := newSourceCache(maxCachedSources)

		if _, ok := c.lookup(1); ok {
			t.Error("lookup(1) resolved a junk PC")
		}
		if n := liveEntries(c); n != 0 {
			t.Errorf("%d entries cached for an unresolvable PC", n)
		}
		if _, ok := sourceValue(0); ok {
			t.Error("sourceValue(0) resolved a zero PC")
		}
	})

	// Handle runs on the caller's goroutine, so every one of these is concurrent
	// in any real program, rotations included. Run under -race.
	t.Run("concurrent lookups rotate safely", func(t *testing.T) {
		t.Parallel()
		const (
			perGen     = 16
			goroutines = 8
			each       = 500
		)
		c := newSourceCache(perGen)
		live := callerPC()

		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < each; i++ {
					if _, ok := c.lookup(live); !ok {
						t.Errorf("goroutine %d: the live PC stopped resolving", g)
						return
					}
					c.remember(site(g*each + i)) // force rotations underneath it
				}
			}(g)
		}
		wg.Wait()

		if live := liveEntries(c); live > 2*perGen {
			t.Errorf("%d live entries, ceiling is %d", live, 2*perGen)
		}
	})
}
