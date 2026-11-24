package roster

import (
	"fmt"
	"strings"
)

// MaxSegmentLen caps a service or an owner ID when it is used as a path segment of a storage key.
const MaxSegmentLen = 255

// Caps on the fields that are not storage key segments. They are generous: the point is that every field has
// a bound, so no publisher can write a document that is unbounded in a dimension nobody checked.
const (
	MaxLogicalNameLen = 512
	MaxRegionLen      = 64
	MaxZoneLen        = 64
)

// ValidText reports whether s is usable as a plain text field value: at most max bytes, and free of control
// characters. kind names what s is, for the error.
//
// Control characters are refused for two reasons. They have no meaning in any of these fields, and a NUL in
// particular used to make two different records produce the same input to ContentHash, so a real change could
// look like no change. ContentHash no longer depends on this, because it length prefixes every value, but a
// value that cannot be logged, compared or written into a proxy configuration is still not worth storing.
//
// Unlike ValidSegment this allows "/", ":" and wildcards, because these values never form part of a storage
// key or an IAM pattern and identifiers like an ARN legitimately contain them. An empty s is allowed: a caller
// that requires a value checks for that separately.
func ValidText(kind, s string, max int) error {
	if len(s) > max {
		return fmt.Errorf("%s is %d bytes, at most %d are allowed", kind, len(s), max)
	}
	if i := strings.IndexFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }); i >= 0 {
		return fmt.Errorf("%s %q has a control character (%q) at byte %d", kind, s, s[i], i)
	}
	return nil
}

// ValidSegment reports whether s can be one segment of a storage key and of an IAM resource pattern. kind
// names what s is, for the error ("service", "owner ID"). It must be non empty and at most MaxSegmentLen
// bytes, with no "/" (it would make the key ambiguous), no wildcard characters, no "$" (policy variables), no
// whitespace or control characters, and it must not be "." or "..".
func ValidSegment(kind, s string) error {
	if s == "" {
		return fmt.Errorf("a %s is required", kind)
	}
	if len(s) > MaxSegmentLen {
		return fmt.Errorf("%s is %d bytes, at most %d are allowed", kind, len(s), MaxSegmentLen)
	}
	if s == "." || s == ".." {
		return fmt.Errorf("%s %q is not allowed", kind, s)
	}
	if i := strings.IndexFunc(s, func(r rune) bool { return r < 0x21 || r == 0x7f || strings.ContainsRune(`/*?$`, r) }); i >= 0 {
		return fmt.Errorf("%s %q has a character that cannot be part of a key (%q): no whitespace, control characters, or any of / * ? $", kind, s, s[i])
	}
	return nil
}
