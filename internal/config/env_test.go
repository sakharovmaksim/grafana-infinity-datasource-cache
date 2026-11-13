package config

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"7d": 7 * 24 * time.Hour, "1d12h": 36 * time.Hour, "90s": 90 * time.Second, "1h30m": 90 * time.Minute,
	} {
		if got, err := ParseDuration(in); err != nil || got != want {
			t.Errorf("%s: got %v %v", in, got, err)
		}
	}
	if _, err := ParseDuration("xd"); err == nil {
		t.Error("want error")
	}
}
