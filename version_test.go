package betterstack

import (
	"strings"
	"testing"
)

func TestUserAgent(t *testing.T) {
	t.Parallel()

	got := userAgent()
	if !strings.HasPrefix(got, clientName+"/") {
		t.Errorf("userAgent() = %q, want the %q/<version> convention", got, clientName)
	}
	if strings.HasSuffix(got, "/") {
		t.Errorf("userAgent() = %q, version half is empty", got)
	}
	// Whatever the resolution path, the result must never leak a placeholder.
	// This is the failure mode the fork shipped: a literal "VERSION" in the
	// header of every request built from source.
	for _, bad := range []string{"VERSION", "(devel)", "%!"} {
		if strings.Contains(got, bad) {
			t.Errorf("userAgent() = %q, contains the placeholder %q", got, bad)
		}
	}
}

func TestUserAgentIsStable(t *testing.T) {
	t.Parallel()
	if a, b := userAgent(), userAgent(); a != b {
		t.Errorf("userAgent() is not memoised: %q then %q", a, b)
	}
}

// Under `go test` this module is the main module, so it is absent from bi.Deps
// and bi.Main.Version is "(devel)". Falling through to "dev" is the correct
// outcome, and pins the behaviour that must not regress into reporting the host
// application's version.
func TestModuleVersionDegradesToDev(t *testing.T) {
	t.Parallel()
	if got := moduleVersion(); got != "dev" {
		t.Logf("moduleVersion() = %q (not %q); acceptable if this binary carries "+
			"real build info for %s", got, "dev", modulePath)
		if !isRealVersion(got) {
			t.Errorf("moduleVersion() = %q, which is neither a real version nor %q", got, "dev")
		}
	}
}

func TestIsRealVersion(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		in   string
		want bool
	}{
		{"v1.2.3", true},
		{"v0.0.0-20260806120000-abcdef123456", true},
		{"", false},
		{"(devel)", false},
	} {
		if got := isRealVersion(tt.in); got != tt.want {
			t.Errorf("isRealVersion(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
