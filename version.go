package betterstack

import (
	"runtime/debug"
	"sync"
)

// modulePath must match the module directive in go.mod. It is how this module
// finds its own entry in the importing binary's build info.
const modulePath = "github.com/prochac/betterstack-logs-client-go"

// clientName is the <lib> half of the User-Agent, matching the convention the
// sibling official clients follow. It names the client, not the repository, so
// it stays unprefixed even though modulePath carries a "betterstack-" prefix
// that a personal namespace needs and Better Stack's own would not.
const clientName = "logs-client-go"

// devVersion is the version reported when build info names no real one: the
// module is the main module, was replaced by a directory, or was built without
// VCS information.
const devVersion = "dev"

// userAgent returns the User-Agent sent with every request, e.g.
// "logs-client-go/v0.1.0".
//
// The version comes from the importing binary's build info rather than from a
// constant rewritten at tag time, so a module built from source reports a real
// version instead of a placeholder string.
var userAgent = sync.OnceValue(func() string {
	return clientName + "/" + moduleVersion()
})

// moduleVersion resolves this module's own version from build info.
//
// debug.ReadBuildInfo reports the *main* module, so our version lives in
// bi.Deps, not bi.Main — reading bi.Main.Version is the classic mistake here and
// yields the host application's version. Three cases have to be handled:
//
//   - imported normally: found in bi.Deps with a real version;
//   - this module is the main module (its own tests, its own example binaries):
//     absent from bi.Deps, and bi.Main.Version is "(devel)" or empty;
//   - replaced by a directory, or built without VCS info: the dep entry exists
//     but carries an empty version.
//
// Every one of those degrades to "dev" rather than to a misleading number.
func moduleVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return devVersion
	}
	for _, dep := range bi.Deps {
		if dep == nil || dep.Path != modulePath {
			continue
		}
		// A replace directive points Module.Replace at the effective module.
		if dep.Replace != nil && isRealVersion(dep.Replace.Version) {
			return dep.Replace.Version
		}
		if isRealVersion(dep.Version) {
			return dep.Version
		}
		return devVersion
	}
	if bi.Main.Path == modulePath && isRealVersion(bi.Main.Version) {
		return bi.Main.Version
	}
	return devVersion
}

func isRealVersion(v string) bool {
	return v != "" && v != "(devel)"
}
