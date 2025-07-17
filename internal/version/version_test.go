package version

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInfo(t *testing.T) {
	info := Info()

	assert.NotEmpty(t, info.Version)
	assert.NotEmpty(t, info.GoVersion)
	assert.NotZero(t, info.BuildTime)

	// Should contain some build information
	assert.Contains(t, info.String(), "Gorilla DataFrame Library")
	assert.Contains(t, info.String(), "Version:")
	assert.Contains(t, info.String(), "Go Version:")
}

func TestBuildInfoString(t *testing.T) {
	info := BuildInfo{
		Version:   "v1.0.0",
		BuildDate: "2024-01-01T00:00:00Z",
		GitCommit: "abc123def456",
		GitTag:    "v1.0.0",
		GoVersion: "go1.21.0",
		BuildTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Dirty:     false,
	}

	str := info.String()
	assert.Contains(t, str, "Gorilla DataFrame Library")
	assert.Contains(t, str, "Version: v1.0.0")
	assert.Contains(t, str, "Build Date: 2024-01-01T00:00:00Z")
	assert.Contains(t, str, "Git Commit: abc123d") // Should be truncated
	assert.Contains(t, str, "Go Version: go1.21.0")
}

func TestBuildInfoStringDirty(t *testing.T) {
	info := BuildInfo{
		Version:   "v1.0.0",
		GitCommit: "abc123-dirty",
		Dirty:     true,
		GitTag:    "v1.0.0",
	}

	str := info.String()
	assert.Contains(t, str, "Version: v1.0.0")
	assert.Contains(t, str, "(dirty)")
}

func TestUserAgent(t *testing.T) {
	// Save original version
	originalVersion := Version
	defer func() { Version = originalVersion }()

	Version = "v1.0.0"
	userAgent := UserAgent()
	assert.Equal(t, "gorilla-dataframe/v1.0.0", userAgent)

	Version = "dev"
	userAgent = UserAgent()
	assert.Equal(t, "gorilla-dataframe/dev", userAgent)
}

func TestIsRelease(t *testing.T) {
	// Save original version
	originalVersion := Version
	defer func() { Version = originalVersion }()

	tests := []struct {
		version  string
		expected bool
	}{
		{"v1.0.0", true},
		{"1.0.0", true},
		{"dev", false},
		{"v1.0.0-alpha.1", false},
		{"v1.0.0-beta.1", false},
		{"v1.0.0-rc.1", false},
		{"v1.0.0-dirty", false},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			Version = tt.version
			assert.Equal(t, tt.expected, IsRelease())
		})
	}
}

func TestIsPreRelease(t *testing.T) {
	// Save original version
	originalVersion := Version
	defer func() { Version = originalVersion }()

	tests := []struct {
		version  string
		expected bool
	}{
		{"v1.0.0", false},
		{"1.0.0", false},
		{"dev", false},
		{"v1.0.0-alpha.1", true},
		{"v1.0.0-beta.1", true},
		{"v1.0.0-rc.1", true},
		{"v1.0.0-dirty", false},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			Version = tt.version
			assert.Equal(t, tt.expected, IsPreRelease())
		})
	}
}

func TestParseSemVer(t *testing.T) {
	tests := []struct {
		version  string
		expected *SemVer
		hasError bool
	}{
		{
			version: "1.0.0",
			expected: &SemVer{
				Major: 1,
				Minor: 0,
				Patch: 0,
			},
		},
		{
			version: "v2.1.3",
			expected: &SemVer{
				Major: 2,
				Minor: 1,
				Patch: 3,
			},
		},
		{
			version: "1.0.0-alpha.1",
			expected: &SemVer{
				Major:      1,
				Minor:      0,
				Patch:      0,
				PreRelease: "alpha.1",
			},
		},
		{
			version: "1.0.0+build.1",
			expected: &SemVer{
				Major: 1,
				Minor: 0,
				Patch: 0,
				Build: "build.1",
			},
		},
		{
			version: "1.0.0-beta.1+build.2",
			expected: &SemVer{
				Major:      1,
				Minor:      0,
				Patch:      0,
				PreRelease: "beta.1",
				Build:      "build.2",
			},
		},
		{
			version:  "invalid",
			hasError: true,
		},
		{
			version:  "1.0",
			hasError: true,
		},
		{
			version:  "a.b.c",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			result, err := ParseSemVer(tt.version)

			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSemVerString(t *testing.T) {
	tests := []struct {
		semver   *SemVer
		expected string
	}{
		{
			semver:   &SemVer{Major: 1, Minor: 0, Patch: 0},
			expected: "1.0.0",
		},
		{
			semver:   &SemVer{Major: 2, Minor: 1, Patch: 3},
			expected: "2.1.3",
		},
		{
			semver:   &SemVer{Major: 1, Minor: 0, Patch: 0, PreRelease: "alpha.1"},
			expected: "1.0.0-alpha.1",
		},
		{
			semver:   &SemVer{Major: 1, Minor: 0, Patch: 0, Build: "build.1"},
			expected: "1.0.0+build.1",
		},
		{
			semver:   &SemVer{Major: 1, Minor: 0, Patch: 0, PreRelease: "beta.1", Build: "build.2"},
			expected: "1.0.0-beta.1+build.2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.semver.String())
		})
	}
}

func TestSemVerCompare(t *testing.T) {
	tests := []struct {
		v1       string
		v2       string
		expected int
	}{
		{"1.0.0", "1.0.0", 0},
		{"1.0.0", "1.0.1", -1},
		{"1.0.1", "1.0.0", 1},
		{"1.0.0", "1.1.0", -1},
		{"1.1.0", "1.0.0", 1},
		{"1.0.0", "2.0.0", -1},
		{"2.0.0", "1.0.0", 1},
		{"1.0.0", "1.0.0-alpha", 1},  // Release > pre-release
		{"1.0.0-alpha", "1.0.0", -1}, // Pre-release < release
		{"1.0.0-alpha", "1.0.0-beta", -1},
		{"1.0.0-beta", "1.0.0-alpha", 1},
		{"1.0.0-alpha.1", "1.0.0-alpha.2", -1},
	}

	for _, tt := range tests {
		t.Run(tt.v1+"_vs_"+tt.v2, func(t *testing.T) {
			v1, err := ParseSemVer(tt.v1)
			require.NoError(t, err)

			v2, err := ParseSemVer(tt.v2)
			require.NoError(t, err)

			result := v1.Compare(v2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRoundTripSemVer(t *testing.T) {
	versions := []string{
		"1.0.0",
		"2.1.3",
		"1.0.0-alpha.1",
		"1.0.0+build.1",
		"1.0.0-beta.1+build.2",
	}

	for _, version := range versions {
		t.Run(version, func(t *testing.T) {
			parsed, err := ParseSemVer(version)
			require.NoError(t, err)

			result := parsed.String()
			assert.Equal(t, version, result)
		})
	}
}

func TestBuildInfoWithDifferentVersions(t *testing.T) {
	// Save original values
	originalVersion := Version
	originalGitTag := GitTag
	defer func() {
		Version = originalVersion
		GitTag = originalGitTag
	}()

	// Test with different version and tag
	Version = "v1.0.0"
	GitTag = "v1.0.0-rc.1"

	info := Info()
	str := info.String()

	assert.Contains(t, str, "Version: v1.0.0 (v1.0.0-rc.1)")
}

func TestModuleDependencies(t *testing.T) {
	info := Info()

	// Should have main module info (might be empty in tests)
	// Just check that it doesn't panic
	assert.NotNil(t, info.Main)

	// Dependencies might be empty in test environment, that's OK
	t.Logf("Main module: %s", info.Main.Path)
	t.Logf("Number of dependencies: %d", len(info.Deps))
}
