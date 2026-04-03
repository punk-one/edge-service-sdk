package driver

import "testing"

func TestPointConfigToCommandRequestDefaultsReadWriteToRead(t *testing.T) {
	req, err := (PointConfig{
		Name:      "alarm",
		ValueType: "Bool",
		NodeName:  "DB1.DBX0.0",
	}).ToCommandRequest("")
	if err != nil {
		t.Fatalf("ToCommandRequest() error = %v", err)
	}
	if req.Properties.ReadWrite != "R" {
		t.Fatalf("expected default readWrite R, got %q", req.Properties.ReadWrite)
	}
}
