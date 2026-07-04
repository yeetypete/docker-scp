package main

import (
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

var (
	linuxAmd64 = ocispec.Platform{OS: "linux", Architecture: "amd64"}
	linuxArm64 = ocispec.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}
)

func TestAnyMatches(t *testing.T) {
	candidates := []ocispec.Platform{linuxAmd64, linuxArm64}
	if !anyMatches(linuxAmd64, candidates) {
		t.Error("expected linux/amd64 to match")
	}
	if anyMatches(ocispec.Platform{OS: "linux", Architecture: "riscv64"}, candidates) {
		t.Error("expected linux/riscv64 not to match")
	}
}

func TestFormatPlatforms(t *testing.T) {
	got := formatPlatforms([]ocispec.Platform{linuxAmd64, linuxArm64})
	want := "linux/amd64, linux/arm64/v8"
	if got != want {
		t.Errorf("formatPlatforms = %q, want %q", got, want)
	}
}
