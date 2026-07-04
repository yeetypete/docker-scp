package main

import "testing"

func TestParseTarget(t *testing.T) {
	tests := []struct {
		in               string
		user, host, port string
	}{
		{"host", "", "host", ""},
		{"user@host", "user", "host", ""},
		{"user@host:2222", "user", "host", "2222"},
		{"host:2222", "", "host", "2222"},
		{"user@[::1]:2222", "user", "::1", "2222"},
		{"::1", "", "::1", ""},
		{"@host", "", "host", ""},
		{"", "", "", ""},
	}
	for _, tt := range tests {
		user, host, port := parseTarget(tt.in)
		if user != tt.user || host != tt.host || port != tt.port {
			t.Errorf("parseTarget(%q) = (%q, %q, %q), want (%q, %q, %q)",
				tt.in, user, host, port, tt.user, tt.host, tt.port)
		}
	}
}
