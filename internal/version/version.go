// Package version reports build information for the parkana binary.
package version

import (
	"cmp"
	"runtime"
	"runtime/debug"
)

// LokiCompat is the Loki version Parkana reports to clients such as Grafana,
// which gate features on the version returned by /loki/api/v1/status/buildinfo.
const LokiCompat = "3.0.0"

// Set at link time:
//
//	go build -ldflags "-X github.com/nathabonfim59/parkana/internal/version.Version=v0.1.0"
var (
	Version   = ""
	Revision  = ""
	Branch    = ""
	BuildDate = ""
)

// Info describes the running binary.
type Info struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	Branch    string `json:"branch"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

// Get returns build information, falling back to the module and VCS data the
// Go toolchain embeds when the link-time variables are unset.
func Get() Info {
	info := Info{
		Version:   Version,
		Revision:  Revision,
		Branch:    Branch,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		info.Version = cmp.Or(info.Version, bi.Main.Version)
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				info.Revision = cmp.Or(info.Revision, s.Value)
			case "vcs.time":
				info.BuildDate = cmp.Or(info.BuildDate, s.Value)
			}
		}
	}
	info.Version = cmp.Or(info.Version, "dev")
	return info
}
