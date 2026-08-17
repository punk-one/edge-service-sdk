package bus

import "testing"

func TestFixedSubjects(t *testing.T) {
	tests := []struct {
		messageType MessageType
		identifier  string
		subject     string
	}{
		{TelemetryReport, "", SubjectTelemetryReport},
		{PropertySet, "", SubjectPropertySet},
		{CommandCall, "start_machine", "edge.v1.command.call.start_machine"},
	}
	for _, test := range tests {
		subject, err := SubjectFor(test.messageType, test.identifier)
		if err != nil {
			t.Fatalf("SubjectFor(%s): %v", test.messageType, err)
		}
		if subject != test.subject {
			t.Fatalf("SubjectFor(%s) = %s, want %s", test.messageType, subject, test.subject)
		}
		parsedType, identifier, ok := ParseSubject(subject)
		if !ok || parsedType != test.messageType || identifier != test.identifier {
			t.Fatalf("ParseSubject(%s) = (%s, %s, %t)", subject, parsedType, identifier, ok)
		}
	}
}

func TestCommandIdentifierRejectsSubjectTokens(t *testing.T) {
	for _, identifier := range []string{"", "a.b", "*", "has space"} {
		if _, err := SubjectFor(CommandCall, identifier); err == nil {
			t.Fatalf("SubjectFor(CommandCall, %q) succeeded", identifier)
		}
	}
}
