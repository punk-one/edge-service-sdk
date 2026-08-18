package driver

// NormalizeStringForWrite converts a configured fixed-width String property
// into the value expected by a protocol driver.
//
// maxLength is measured in bytes, matching the String point contract and the
// MC raw-byte representation. Values shorter than maxLength are zero-padded;
// values longer than maxLength are truncated before they reach the driver.
// A non-positive maxLength means that no fixed-width normalization is applied.
//
// The function deliberately operates on []byte rather than []rune. This keeps
// the SDK contract consistent with byte-oriented PLC String fields. Protocols
// that use a character-oriented encoding (for example WString) must keep
// their own encoding rules and should not use this helper for those values.
func NormalizeStringForWrite(value string, maxLength int) string {
	if maxLength <= 0 {
		return value
	}

	buf := make([]byte, maxLength)
	copy(buf, []byte(value))
	return string(buf)
}
