package configsvc

import (
	"testing"

	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

func TestResolveRuleMatchesMultipleWildcards(t *testing.T) {
	rule, ok := resolveRule(
		DefaultValidationRules(),
		"profile",
		"telemetry.groups.analog.points.temperature.precision",
	)
	if !ok {
		t.Fatal("resolveRule() did not match telemetry group point precision")
	}
	if err := rule.Validate(outevent.MaxDecimalPrecision); err != nil {
		t.Fatalf("maximum precision rejected: %v", err)
	}
	if err := rule.Validate(outevent.MaxDecimalPrecision + 1); err == nil {
		t.Fatal("precision above maximum was accepted")
	}
}
