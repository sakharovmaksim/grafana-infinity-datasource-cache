// Package config builds the service configuration from defaults, a YAML file and ENV overrides.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Env reads typed values and collects parse errors, so all mistakes are reported at once.
type Env struct {
	lookup func(string) (string, bool)
	errs   []error
}

func NewEnv(lookup func(string) (string, bool)) *Env {
	return &Env{lookup: lookup}
}

func (e *Env) Err() error { return errors.Join(e.errs...) }

func (e *Env) fail(key string, err error) {
	e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
}

func (e *Env) get(key string) (string, bool) {
	v, ok := e.lookup(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (e *Env) String(key, def string) string {
	if v, ok := e.get(key); ok {
		return v
	}
	return def
}

// Parse reads key with parse; an unset key gives def, an invalid one gives def and is reported by Err.
func (e *Env) Parse[T any](key string, def T, parse func(string) (T, error)) T {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	x, err := parse(v)
	if err != nil {
		e.fail(key, err)
		return def
	}
	return x
}

func (e *Env) Int(key string, def int) int { return e.Parse(key, def, strconv.Atoi) }

// List splits on whitespace: URLs in lists may contain commas.
func (e *Env) List(key string, def []string) []string {
	if v, ok := e.get(key); ok {
		return strings.Fields(v)
	}
	return def
}

// Names splits on commas and whitespace.
func (e *Env) Names(key string, def []string) []string {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	return strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\t' })
}

// ParseDuration extends time.ParseDuration with a "d" (24h) unit: "7d", "1d12h".
func ParseDuration(s string) (time.Duration, error) {
	if i := strings.IndexByte(s, 'd'); i > 0 {
		days, err := strconv.Atoi(s[:i])
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		rest := time.Duration(0)
		if s[i+1:] != "" {
			if rest, err = time.ParseDuration(s[i+1:]); err != nil {
				return 0, err
			}
		}
		return time.Duration(days)*24*time.Hour + rest, nil
	}
	return time.ParseDuration(s)
}

// ParseBytes accepts plain numbers and KiB/MiB/GiB suffixes.
func ParseBytes(s string) (int64, error) {
	mult := int64(1)
	for suffix, m := range map[string]int64{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30} {
		if before, ok := strings.CutSuffix(s, suffix); ok {
			s, mult = before, m
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return n * mult, nil
}
