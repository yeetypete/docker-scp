package main

import (
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestMatchersForEmptyMatchesAll(t *testing.T) {
	ms := matchersFor(nil)
	if len(ms) != 1 {
		t.Fatalf("matchersFor(nil) returned %d matchers, want 1", len(ms))
	}
	if !ms[0].Match(linuxAmd64) || !ms[0].Match(linuxArm64) {
		t.Error("matchersFor(nil) should match every platform")
	}
}

func TestMatchersForOnePerPlatform(t *testing.T) {
	ms := matchersFor([]ocispec.Platform{linuxAmd64, linuxArm64})
	if len(ms) != 2 {
		t.Fatalf("got %d matchers, want 2", len(ms))
	}
	if !ms[0].Match(linuxAmd64) || ms[0].Match(linuxArm64) {
		t.Error("first matcher should match only linux/amd64")
	}
	if !ms[1].Match(linuxArm64) || ms[1].Match(linuxAmd64) {
		t.Error("second matcher should match only linux/arm64/v8")
	}
}

func TestShort(t *testing.T) {
	d := digest.FromString("x")
	got := short(d)
	if len(got) != 12 || !strings.HasPrefix(d.Encoded(), got) {
		t.Errorf("short(%s) = %q, want first 12 chars of encoded digest", d, got)
	}
}
