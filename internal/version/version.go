// Package version provides version information for the Gorilla DataFrame library.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// Constants for magic numbers and repeated strings
const (
	unknownValue     = "unknown"
	commitHashLength = 7
	semVerPartsCount = 3
)

// Build-time variables set by ldflags
var (
	Version   = "dev"
	BuildDate = unknownValue
	GitCommit = unknownValue
	GitTag    = unknownValue
	GoVersion = runtime.Version()
)

// BuildInfo contains detailed build information
type BuildInfo struct {
	Version   string    `json:"version"`
	BuildDate string    `json:"build_date"`
	GitCommit string    `json:"git_commit"`
	GitTag    string    `json:"git_tag"`
	GoVersion string    `json:"go_version"`
	BuildTime time.Time `json:"build_time"`
	Dirty     bool      `json:"dirty"`
	Main      Module    `json:"main"`
	Deps      []Module  `json:"deps"`
	Settings  []Setting `json:"settings"`
}

// Module represents a Go module with version information
type Module struct {
	Path    string `json:"path"`
	Version string `json:"version"`
	Sum     string `json:"sum"`
}

// Setting represents a build setting
type Setting struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Info returns detailed build information
func Info() BuildInfo {
	buildTime, _ := time.Parse(time.RFC3339, BuildDate)
	if buildTime.IsZero() {
		buildTime = time.Now()
	}

	info := BuildInfo{
		Version:   Version,
		BuildDate: BuildDate,
		GitCommit: GitCommit,
		GitTag:    GitTag,
		GoVersion: GoVersion,
		BuildTime: buildTime,
		Dirty:     strings.Contains(GitCommit, "-dirty"),
	}

	// Get build info from runtime
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		info.Main = Module{
			Path:    buildInfo.Main.Path,
			Version: buildInfo.Main.Version,
			Sum:     buildInfo.Main.Sum,
		}

		for _, dep := range buildInfo.Deps {
			info.Deps = append(info.Deps, Module{
				Path:    dep.Path,
				Version: dep.Version,
				Sum:     dep.Sum,
			})
		}

		for _, setting := range buildInfo.Settings {
			info.Settings = append(info.Settings, Setting{
				Key:   setting.Key,
				Value: setting.Value,
			})
		}
	}

	return info
}

// String returns a formatted version string
func (b BuildInfo) String() string {
	var sb strings.Builder
	sb.WriteString("Gorilla DataFrame Library\n")
	sb.WriteString(fmt.Sprintf("Version: %s", b.Version))

	if b.GitTag != unknownValue && b.GitTag != b.Version {
		sb.WriteString(fmt.Sprintf(" (%s)", b.GitTag))
	}

	if b.Dirty {
		sb.WriteString(" (dirty)")
	}
	sb.WriteString("\n")

	if b.BuildDate != unknownValue {
		sb.WriteString(fmt.Sprintf("Build Date: %s\n", b.BuildDate))
	}

	if b.GitCommit != unknownValue {
		commit := b.GitCommit
		if len(commit) > commitHashLength {
			commit = commit[:commitHashLength]
		}
		sb.WriteString(fmt.Sprintf("Git Commit: %s\n", commit))
	}

	sb.WriteString(fmt.Sprintf("Go Version: %s\n", b.GoVersion))

	if b.Main.Path != "" {
		sb.WriteString(fmt.Sprintf("Module: %s\n", b.Main.Path))
	}

	return sb.String()
}

// UserAgent returns a user agent string for HTTP requests
func UserAgent() string {
	return fmt.Sprintf("gorilla-dataframe/%s", Version)
}

// IsRelease returns true if this is a release version (not dev)
func IsRelease() bool {
	return Version != "dev" && !strings.Contains(Version, "-")
}

// IsPreRelease returns true if this is a pre-release version
func IsPreRelease() bool {
	return strings.Contains(Version, "-alpha") ||
		strings.Contains(Version, "-beta") ||
		strings.Contains(Version, "-rc")
}

// SemVer represents semantic version components
type SemVer struct {
	Major      int
	Minor      int
	Patch      int
	PreRelease string
	Build      string
}

// ParseSemVer parses a semantic version string using a simplified parser.
//
// This implementation handles basic semantic version parsing for build information
// and version comparison. For production applications requiring strict SemVer
// compliance, consider using a dedicated library like github.com/Masterminds/semver.
//
// Supported format: [v]MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]
//
// Examples:
//   - "1.0.0" -> SemVer{Major: 1, Minor: 0, Patch: 0}
//   - "v2.1.3-alpha.1" -> SemVer{Major: 2, Minor: 1, Patch: 3, PreRelease: "alpha.1"}
//   - "1.0.0+build.1" -> SemVer{Major: 1, Minor: 0, Patch: 0, Build: "build.1"}
func ParseSemVer(version string) (*SemVer, error) {
	if version == "" {
		return nil, fmt.Errorf("version string cannot be empty")
	}

	version = strings.TrimPrefix(version, "v")

	var major, minor, patch int
	var preRelease, build string

	// Split off build metadata first (+)
	if idx := strings.Index(version, "+"); idx != -1 {
		build = version[idx+1:]
		version = version[:idx]
	}

	// Split off pre-release (-)
	if idx := strings.Index(version, "-"); idx != -1 {
		preRelease = version[idx+1:]
		version = version[:idx]
	}

	// Now parse major.minor.patch
	parts := strings.Split(version, ".")
	if len(parts) != semVerPartsCount {
		return nil, fmt.Errorf("invalid version format: %s", version)
	}

	if _, err := fmt.Sscanf(parts[0], "%d", &major); err != nil {
		return nil, fmt.Errorf("invalid major version: %s", parts[0])
	}

	if _, err := fmt.Sscanf(parts[1], "%d", &minor); err != nil {
		return nil, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	if _, err := fmt.Sscanf(parts[2], "%d", &patch); err != nil {
		return nil, fmt.Errorf("invalid patch version: %s", parts[2])
	}

	return &SemVer{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: preRelease,
		Build:      build,
	}, nil
}

// String returns the semantic version as a string
func (s *SemVer) String() string {
	version := fmt.Sprintf("%d.%d.%d", s.Major, s.Minor, s.Patch)
	if s.PreRelease != "" {
		version += "-" + s.PreRelease
	}
	if s.Build != "" {
		version += "+" + s.Build
	}
	return version
}

// Compare compares two semantic versions
// Returns: -1 if s < other, 0 if s == other, 1 if s > other
func (s *SemVer) Compare(other *SemVer) int {
	// Compare major.minor.patch
	if s.Major != other.Major {
		if s.Major > other.Major {
			return 1
		}
		return -1
	}

	if s.Minor != other.Minor {
		if s.Minor > other.Minor {
			return 1
		}
		return -1
	}

	if s.Patch != other.Patch {
		if s.Patch > other.Patch {
			return 1
		}
		return -1
	}

	// Compare pre-release versions
	if s.PreRelease == "" && other.PreRelease != "" {
		return 1 // Release version > pre-release
	}
	if s.PreRelease != "" && other.PreRelease == "" {
		return -1 // Pre-release < release version
	}

	if s.PreRelease != other.PreRelease {
		return strings.Compare(s.PreRelease, other.PreRelease)
	}

	return 0
}
