package version

import (
	"runtime/debug"
)

// Version can be overridden at link time via:
//
//	-ldflags "-X github.com/spetr/icecluster/internal/version.Version=v1.2.3"
var Version = ""

// Get returns the build version. If not set via ldflags, it falls back to
// dev-<short-commit> using VCS info embedded by the Go toolchain.
func Get() string {
	if Version != "" {
		return Version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi != nil {
		var rev string
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" {
				rev = s.Value
				break
			}
		}
		if rev != "" {
			if len(rev) > 12 {
				rev = rev[:12]
			}
			return "dev-" + rev
		}
	}
	return "dev-unknown"
}
