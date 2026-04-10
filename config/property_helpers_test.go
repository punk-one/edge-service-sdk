package config

import (
	"testing"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

func testDeviceConfig() contracts.DeviceConfig {
	return contracts.DeviceConfig{
		Name:        "acm006",
		ProductCode: "acm",
		Property: contracts.PropertyConfig{
			Points: []contracts.PointConfig{
				{
					Name:      "status_text",
					ValueType: "String",
					NodeName:  "DB200.DBB0",
					MaxLength: 20,
				},
				{
					Name:             "a",
					ValueType:        "Int16",
					NodeNameTemplate: "DB200.DBW{index}",
				},
			},
			Structs: []contracts.PropertyStruct{
				{
					Name:      "wheels",
					Kind:      "struct_array",
					IndexBase: 1,
					MaxItems:  450,
					Address: contracts.PropertyStructAddress{
						DBNumber:    200,
						BaseOffset:  20,
						IndexStride: 20,
						Unit:        "word",
					},
					Fields: []contracts.PropertyStructField{
						{Name: "diameter", ValueType: "Int16", FieldOffset: 0},
						{Name: "height", ValueType: "Int16", FieldOffset: 2},
					},
				},
			},
		},
	}
}

func TestBuildPropertyWriteRequestsSupportsPointArrayAndStruct(t *testing.T) {
	device := testDeviceConfig()

	reqs, params, err := buildPropertyWriteRequests(device, map[string]interface{}{
		"status_text": "READY",
		"a[2]":        10,
		"wheels": map[string]interface{}{
			"2": map[string]interface{}{
				"diameter": 40,
				"height":   42,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildPropertyWriteRequests() error = %v", err)
	}
	if len(reqs) != 4 || len(params) != 4 {
		t.Fatalf("expected 4 requests and params, got %d and %d", len(reqs), len(params))
	}
	if got := reqs[0].Attributes["NodeName"]; got == "" {
		t.Fatalf("expected first request node name, got empty")
	}

	foundArray := false
	foundStruct := false
	for _, req := range reqs {
		nodeName := req.Attributes["NodeName"]
		if nodeName == "DB200.DBW2" {
			foundArray = true
		}
		if nodeName == "DB200.DBW40" || nodeName == "DB200.DBW42" {
			foundStruct = true
		}
	}
	if !foundArray {
		t.Fatal("expected array write request for a[2]")
	}
	if !foundStruct {
		t.Fatal("expected struct write requests for wheels[2]")
	}
}

func TestBuildPropertyReadRequestsBuildsNestedBindings(t *testing.T) {
	device := testDeviceConfig()

	reqs, bindings, err := buildPropertyReadRequests(device, map[string]interface{}{
		"wheels": map[string]interface{}{
			"1": map[string]interface{}{},
			"2": map[string]interface{}{
				"height": true,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildPropertyReadRequests() error = %v", err)
	}
	if len(reqs) != 3 || len(bindings) != 3 {
		t.Fatalf("expected 3 read requests and bindings, got %d and %d", len(reqs), len(bindings))
	}

	values := []*contracts.CommandValue{
		{DeviceResourceName: "wheels.1.diameter", Type: "Int16", Value: int16(20)},
		{DeviceResourceName: "wheels.1.height", Type: "Int16", Value: int16(22)},
		{DeviceResourceName: "wheels.2.height", Type: "Int16", Value: int16(42)},
	}
	response := buildPropertyResponse(values, bindings)
	wheels := response["wheels"].(map[string]interface{})
	first := wheels["1"].(map[string]interface{})
	second := wheels["2"].(map[string]interface{})

	if first["diameter"] != int16(20) || first["height"] != int16(22) {
		t.Fatalf("unexpected wheels[1] payload: %#v", first)
	}
	if second["height"] != int16(42) {
		t.Fatalf("unexpected wheels[2] payload: %#v", second)
	}
}

func TestParseStructIndexRejectsOutOfRange(t *testing.T) {
	device := testDeviceConfig()
	structDef := device.Property.Structs[0]

	if _, err := parseStructIndex(structDef, "451"); err == nil {
		t.Fatal("expected parseStructIndex to reject 451")
	}
}

func TestBuildPropertyReadSelectionUsesWriteShape(t *testing.T) {
	selection := BuildPropertyReadSelection(map[string]interface{}{
		"status_text": "READY",
		"wheels": map[string]interface{}{
			"2": map[string]interface{}{
				"diameter": 40,
				"height":   42,
			},
		},
	})

	if selection["status_text"] != true {
		t.Fatalf("expected point selection to be true, got %#v", selection["status_text"])
	}
	wheels := selection["wheels"].(map[string]interface{})
	item := wheels["2"].(map[string]interface{})
	if item["diameter"] != true || item["height"] != true {
		t.Fatalf("expected nested struct selection to be true flags, got %#v", item)
	}
}

func TestBuildAutoPropertyReadRequestsSkipsNonAutoReportStructs(t *testing.T) {
	device := testDeviceConfig()
	device.Property.Structs[0].AutoReport = false

	reqs, bindings, err := BuildAutoPropertyReadRequests(device)
	if err != nil {
		t.Fatalf("BuildAutoPropertyReadRequests() error = %v", err)
	}
	if len(reqs) != 1 || len(bindings) != 1 {
		t.Fatalf("expected only direct property points to auto-report, got %d reqs %d bindings", len(reqs), len(bindings))
	}
	if got := reqs[0].DeviceResourceName; got != "status_text" {
		t.Fatalf("unexpected auto-report point %q", got)
	}
}
