package driver

import "testing"

func TestNormalizeStringForWrite(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		maxLength int
		want      string
	}{
		{name: "pads short value", value: "ABC", maxLength: 6, want: "ABC\x00\x00\x00"},
		{name: "keeps value at limit", value: "ABCDEF", maxLength: 6, want: "ABCDEF"},
		{name: "truncates value over limit", value: "ABCDEFG", maxLength: 6, want: "ABCDEF"},
		{name: "pads empty value", value: "", maxLength: 3, want: "\x00\x00\x00"},
		{name: "does not normalize without limit", value: "ABC", maxLength: 0, want: "ABC"},
		{name: "does not normalize negative limit", value: "ABC", maxLength: -1, want: "ABC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeStringForWrite(tt.value, tt.maxLength); got != tt.want {
				t.Fatalf("NormalizeStringForWrite(%q, %d) = %q, want %q", tt.value, tt.maxLength, got, tt.want)
			}
		})
	}
}

func TestNormalizeStringForWriteUsesByteLength(t *testing.T) {
	value := "éABC"
	got := NormalizeStringForWrite(value, 4)
	if len([]byte(got)) != 4 {
		t.Fatalf("normalized byte length = %d, want 4", len([]byte(got)))
	}
	if got != string([]byte(value)[:4]) {
		t.Fatalf("normalized value = %q, want byte-truncated value %q", got, string([]byte(value)[:4]))
	}
}
