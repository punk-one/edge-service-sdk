package config

import (
	"strconv"
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

func TestParseStructNameWithIndices(t *testing.T) {
	tests := []struct {
		raw         string
		wantName    string
		wantIndices []int
		wantErr     bool
	}{
		{"machine_data", "machine_data", nil, false},
		{"machine_data[1]", "machine_data", []int{1}, false},
		{"machine_data[1,3,5]", "machine_data", []int{1, 3, 5}, false},
		{"machine_data[1-5]", "machine_data", []int{1, 2, 3, 4, 5}, false},
		{"machine_data[5-1]", "", nil, true},
		{"machine_data[1,3-5,7]", "machine_data", []int{1, 3, 4, 5, 7}, false},
		{"machine_data[1,1,1]", "machine_data", []int{1}, false},
		{"machine_data[a]", "", nil, true},
		{"machine_data[1-a]", "", nil, true},
		{"machine_data[]", "", nil, true},
		{"[1]", "[1]", nil, false},
		{"", "", nil, false},
	}
	for _, tt := range tests {
		name, indices, err := parseStructNameWithIndices(tt.raw)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseStructNameWithIndices(%q) expected error", tt.raw)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseStructNameWithIndices(%q) unexpected error: %v", tt.raw, err)
			continue
		}
		if name != tt.wantName {
			t.Errorf("parseStructNameWithIndices(%q) structName = %q, want %q", tt.raw, name, tt.wantName)
		}
		if len(indices) != len(tt.wantIndices) {
			t.Errorf("parseStructNameWithIndices(%q) indices = %v, want %v", tt.raw, indices, tt.wantIndices)
			continue
		}
		for i := range indices {
			if indices[i] != tt.wantIndices[i] {
				t.Errorf("parseStructNameWithIndices(%q) indices = %v, want %v", tt.raw, indices, tt.wantIndices)
				break
			}
		}
	}
}

func TestDeduplicateAndSort(t *testing.T) {
	tests := []struct {
		input []int
		want  []int
	}{
		{[]int{3, 1, 2}, []int{1, 2, 3}},
		{[]int{1, 1, 1}, []int{1}},
		{[]int{5, 3, 5, 3, 1}, []int{1, 3, 5}},
		{nil, nil},
		{[]int{1}, []int{1}},
	}
	for _, tt := range tests {
		got := deduplicateAndSort(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("deduplicateAndSort(%v) = %v, want %v", tt.input, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("deduplicateAndSort(%v) = %v, want %v", tt.input, got, tt.want)
				break
			}
		}
	}
}

func TestBuildPropertyReadSelectionFromNamesAllIndices(t *testing.T) {
	device := testDeviceConfig()
	selection, err := BuildPropertyReadSelectionFromNames(device, []string{"wheels"})
	if err != nil {
		t.Fatalf("BuildPropertyReadSelectionFromNames() error = %v", err)
	}
	wheels, ok := selection["wheels"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected wheels to be map, got %T", selection["wheels"])
	}
	if len(wheels) != 450 {
		t.Fatalf("expected 450 wheel indices, got %d", len(wheels))
	}
	// verify first and last
	if _, ok := wheels["1"]; !ok {
		t.Fatal("expected index 1")
	}
	if _, ok := wheels["450"]; !ok {
		t.Fatal("expected index 450")
	}
}

func TestBuildPropertyReadSelectionFromNamesSingleIndex(t *testing.T) {
	device := testDeviceConfig()
	selection, err := BuildPropertyReadSelectionFromNames(device, []string{"wheels[1]"})
	if err != nil {
		t.Fatalf("BuildPropertyReadSelectionFromNames() error = %v", err)
	}
	wheels, ok := selection["wheels"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected wheels to be map, got %T", selection["wheels"])
	}
	if len(wheels) != 1 {
		t.Fatalf("expected 1 wheel index, got %d", len(wheels))
	}
	if _, ok := wheels["1"]; !ok {
		t.Fatal("expected index 1")
	}
}

func TestBuildPropertyReadSelectionFromNamesMultiIndex(t *testing.T) {
	device := testDeviceConfig()
	selection, err := BuildPropertyReadSelectionFromNames(device, []string{"wheels[1,3,5]"})
	if err != nil {
		t.Fatalf("BuildPropertyReadSelectionFromNames() error = %v", err)
	}
	wheels, ok := selection["wheels"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected wheels to be map, got %T", selection["wheels"])
	}
	if len(wheels) != 3 {
		t.Fatalf("expected 3 wheel indices, got %d", len(wheels))
	}
	for _, idx := range []string{"1", "3", "5"} {
		if _, ok := wheels[idx]; !ok {
			t.Fatalf("expected index %s", idx)
		}
	}
}

func TestBuildPropertyReadSelectionFromNamesRange(t *testing.T) {
	device := testDeviceConfig()
	selection, err := BuildPropertyReadSelectionFromNames(device, []string{"wheels[1-5]"})
	if err != nil {
		t.Fatalf("BuildPropertyReadSelectionFromNames() error = %v", err)
	}
	wheels, ok := selection["wheels"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected wheels to be map, got %T", selection["wheels"])
	}
	if len(wheels) != 5 {
		t.Fatalf("expected 5 wheel indices, got %d", len(wheels))
	}
	for i := 1; i <= 5; i++ {
		key := strconv.Itoa(i)
		if _, ok := wheels[key]; !ok {
			t.Fatalf("expected index %d", i)
		}
	}
}

func TestBuildPropertyReadSelectionFromNamesIndexOutOfRange(t *testing.T) {
	device := testDeviceConfig()
	_, err := BuildPropertyReadSelectionFromNames(device, []string{"wheels[451]"})
	if err == nil {
		t.Fatal("expected error for index 451 exceeding maxItems 450")
	}
}

func TestBuildPropertyReadSelectionFromNamesBelowBase(t *testing.T) {
	device := testDeviceConfig()
	_, err := BuildPropertyReadSelectionFromNames(device, []string{"wheels[0]"})
	if err == nil {
		t.Fatal("expected error for index 0 below base 1")
	}
}

func TestBuildPropertyReadSelectionFromNamesInvalidFormat(t *testing.T) {
	device := testDeviceConfig()
	invalidKeys := []string{"wheels[a]", "wheels[1-a]", "wheels[]", "wheels[1-]"}
	for _, key := range invalidKeys {
		_, err := BuildPropertyReadSelectionFromNames(device, []string{key})
		if err == nil {
			t.Errorf("expected error for key %q", key)
		}
	}
}

func TestBuildPropertyReadSelectionFromNamesEndToEnd(t *testing.T) {
	device := testDeviceConfig()
	selection, err := BuildPropertyReadSelectionFromNames(device, []string{"wheels[1,2]"})
	if err != nil {
		t.Fatalf("BuildPropertyReadSelectionFromNames() error = %v", err)
	}
	reqs, bindings, err := BuildPropertyReadRequests(device, selection)
	if err != nil {
		t.Fatalf("BuildPropertyReadRequests() error = %v", err)
	}
	// 2 indices * 2 fields (diameter, height) = 4 requests
	if len(reqs) != 4 || len(bindings) != 4 {
		t.Fatalf("expected 4 read requests and bindings, got %d and %d", len(reqs), len(bindings))
	}
	// verify both fields for wheels[1] and wheels[2]
	fieldCount := make(map[string]int)
	for _, binding := range bindings {
		if len(binding.Path) == 3 && binding.Path[0] == "wheels" {
			fieldCount[binding.Path[1]+"."+binding.Path[2]]++
		}
	}
	if fieldCount["1.diameter"] != 1 || fieldCount["1.height"] != 1 {
		t.Fatalf("expected wheels[1] fields, got %v", fieldCount)
	}
	if fieldCount["2.diameter"] != 1 || fieldCount["2.height"] != 1 {
		t.Fatalf("expected wheels[2] fields, got %v", fieldCount)
	}
}

func TestBuildPropertyReadSelectionFromNamesMultiStruct(t *testing.T) {
	device := testDeviceConfig()
	device.Property.Structs = append(device.Property.Structs, contracts.PropertyStruct{
		Name:      "gears",
		Kind:      "struct_array",
		IndexBase: 1,
		MaxItems:  10,
		Address: contracts.PropertyStructAddress{
			DBNumber:    200,
			BaseOffset:  100,
			IndexStride: 10,
			Unit:        "word",
		},
		Fields: []contracts.PropertyStructField{
			{Name: "teeth", ValueType: "Int16", FieldOffset: 0},
		},
	})

	selection, err := BuildPropertyReadSelectionFromNames(device, []string{"wheels[1-3]", "gears[5]"})
	if err != nil {
		t.Fatalf("BuildPropertyReadSelectionFromNames() error = %v", err)
	}

	wheels, ok := selection["wheels"].(map[string]interface{})
	if !ok || len(wheels) != 3 {
		t.Fatalf("expected 3 wheels, got %d", len(wheels))
	}
	gears, ok := selection["gears"].(map[string]interface{})
	if !ok || len(gears) != 1 {
		t.Fatalf("expected 1 gear, got %d", len(gears))
	}
	if _, ok := gears["5"]; !ok {
		t.Fatal("expected gear index 5")
	}
}

func TestBuildPropertyWriteRequestsWithIndex(t *testing.T) {
	device := testDeviceConfig()
	reqs, params, err := BuildPropertyWriteRequests(device, map[string]interface{}{
		"wheels[2]": map[string]interface{}{
			"diameter": 40,
			"height":   42,
		},
	})
	if err != nil {
		t.Fatalf("BuildPropertyWriteRequests() error = %v", err)
	}
	if len(reqs) != 2 || len(params) != 2 {
		t.Fatalf("expected 2 requests and params, got %d and %d", len(reqs), len(params))
	}
	// Verify node names for wheels[2]: baseOffset=20, indexBase=1, indexStride=20
	// index 2 -> (2-1)*20+20 = 40 -> field offset 0 = DBW40, field offset 2 = DBW42
	for _, req := range reqs {
		nodeName := req.Attributes["NodeName"]
		if nodeName == "DB200.DBW40" || nodeName == "DB200.DBW42" {
			continue
		}
		t.Fatalf("unexpected node name %q", nodeName)
	}
}

func TestBuildPropertyWriteRequestsRejectsMulti(t *testing.T) {
	device := testDeviceConfig()
	_, _, err := BuildPropertyWriteRequests(device, map[string]interface{}{
		"wheels[1,3]": map[string]interface{}{
			"diameter": 40,
		},
	})
	if err == nil {
		t.Fatal("expected error for multi-index write")
	}
}

func TestBuildPropertyWriteRequestsBackwardCompat(t *testing.T) {
	device := testDeviceConfig()
	reqs, params, err := BuildPropertyWriteRequests(device, map[string]interface{}{
		"wheels": map[string]interface{}{
			"2": map[string]interface{}{
				"diameter": 40,
				"height":   42,
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildPropertyWriteRequests() error = %v", err)
	}
	if len(reqs) != 2 || len(params) != 2 {
		t.Fatalf("expected 2 requests and params, got %d and %d", len(reqs), len(params))
	}
}

func TestBuildPropertyReadRequestsWithIndexKey(t *testing.T) {
	device := testDeviceConfig()
	// Simulate the selection map produced by BuildPropertyReadSelectionFromNames
	// with name[index] format keys in the readback payload
	reqs, bindings, err := BuildPropertyReadRequests(device, map[string]interface{}{
		"wheels[2]": map[string]interface{}{
			"height": true,
		},
		"wheels[3]": map[string]interface{}{},
	})
	if err != nil {
		t.Fatalf("BuildPropertyReadRequests() error = %v", err)
	}
	// wheels[2] has 1 field (height), wheels[3] has all 2 fields = 3 total
	if len(reqs) != 3 || len(bindings) != 3 {
		t.Fatalf("expected 3 read requests and bindings, got %d and %d", len(reqs), len(bindings))
	}
	fieldCount := make(map[string]int)
	for _, binding := range bindings {
		if len(binding.Path) == 3 && binding.Path[0] == "wheels" {
			fieldCount[binding.Path[1]+"."+binding.Path[2]]++
		}
	}
	if fieldCount["2.height"] != 1 {
		t.Fatalf("expected wheels[2].height, got %v", fieldCount)
	}
	if fieldCount["3.diameter"] != 1 || fieldCount["3.height"] != 1 {
		t.Fatalf("expected wheels[3] both fields, got %v", fieldCount)
	}
}

func TestBuildPropertyReadRequestsBackwardCompat(t *testing.T) {
	device := testDeviceConfig()
	reqs, bindings, err := BuildPropertyReadRequests(device, map[string]interface{}{
		"wheels": map[string]interface{}{
			"2": map[string]interface{}{
				"height": true,
			},
		},
	})
	if err != nil {
		t.Fatalf("BuildPropertyReadRequests() error = %v", err)
	}
	if len(reqs) != 1 || len(bindings) != 1 {
		t.Fatalf("expected 1 read request and binding, got %d and %d", len(reqs), len(bindings))
	}
	if len(bindings[0].Path) != 3 || bindings[0].Path[0] != "wheels" || bindings[0].Path[1] != "2" || bindings[0].Path[2] != "height" {
		t.Fatalf("unexpected binding path: %v", bindings[0].Path)
	}
}

func testArrayKindDeviceConfig() contracts.DeviceConfig {
	return contracts.DeviceConfig{
		Name:        "acm007",
		ProductCode: "acm",
		Property: contracts.PropertyConfig{
			Points: []contracts.PointConfig{
				{
					Name:      "status_text",
					ValueType: "String",
					NodeName:  "DB200.DBB0",
					MaxLength: 20,
				},
			},
			Structs: []contracts.PropertyStruct{
				{
					Name:      "Name",
					Kind:      "array",
					IndexBase: 1,
					MaxItems:  7,
					Address: contracts.PropertyStructAddress{
						DBNumber:    200,
						BaseOffset:  9040,
						IndexStride: 20,
						Unit:        "byte",
					},
					Fields: []contracts.PropertyStructField{
						{Name: "v", ValueType: "String", FieldOffset: 0, MaxLength: 18},
					},
				},
				{
					Name:      "Temps",
					Kind:      "array",
					IndexBase: 1,
					MaxItems:  4,
					Address: contracts.PropertyStructAddress{
						DBNumber:    200,
						BaseOffset:  100,
						IndexStride: 4,
						Unit:        "dword",
					},
					Fields: []contracts.PropertyStructField{
						{Name: "v", ValueType: "Float32", FieldOffset: 0},
					},
				},
				{
					Name:      "Counters",
					Kind:      "array",
					IndexBase: 1,
					MaxItems:  3,
					Address: contracts.PropertyStructAddress{
						DBNumber:    200,
						BaseOffset:  50,
						IndexStride: 2,
						Unit:        "word",
					},
					Fields: []contracts.PropertyStructField{
						{Name: "v", ValueType: "Int16", FieldOffset: 0},
					},
				},
				{
					Name:      "Alarms",
					Kind:      "array",
					IndexBase: 1,
					MaxItems:  8,
					Address: contracts.PropertyStructAddress{
						DBNumber:    200,
						BaseOffset:  150,
						IndexStride: 1,
						Unit:        "byte",
					},
					Fields: []contracts.PropertyStructField{
						{Name: "v", ValueType: "Bool", FieldOffset: 0, BitOffset: intPtr(0)},
					},
				},
			},
		},
	}
}

func intPtr(i int) *int { return &i }

func TestBuildPropertyWriteRequestsArrayKind(t *testing.T) {
	device := testArrayKindDeviceConfig()

	// Test String array write with flat scalar value
	reqs, params, err := BuildPropertyWriteRequests(device, map[string]interface{}{
		"Name[2]": "轮型2",
	})
	if err != nil {
		t.Fatalf("BuildPropertyWriteRequests() error = %v", err)
	}
	if len(reqs) != 1 || len(params) != 1 {
		t.Fatalf("expected 1 request and param, got %d and %d", len(reqs), len(params))
	}
	if params[0].Value != "轮型2" {
		t.Fatalf("expected param value '轮型2', got %v", params[0].Value)
	}

	// Test Int16 array write
	reqs, params, err = BuildPropertyWriteRequests(device, map[string]interface{}{
		"Counters[1]": int16(100),
	})
	if err != nil {
		t.Fatalf("BuildPropertyWriteRequests() error = %v", err)
	}
	if len(reqs) != 1 || len(params) != 1 {
		t.Fatalf("expected 1 request and param, got %d and %d", len(reqs), len(params))
	}

	// Test Float32 array write
	reqs, params, err = BuildPropertyWriteRequests(device, map[string]interface{}{
		"Temps[1]": float32(25.5),
	})
	if err != nil {
		t.Fatalf("BuildPropertyWriteRequests() error = %v", err)
	}
	if len(reqs) != 1 || len(params) != 1 {
		t.Fatalf("expected 1 request and param, got %d and %d", len(reqs), len(params))
	}

	// Test Bool array write
	reqs, params, err = BuildPropertyWriteRequests(device, map[string]interface{}{
		"Alarms[1]": true,
	})
	if err != nil {
		t.Fatalf("BuildPropertyWriteRequests() error = %v", err)
	}
	if len(reqs) != 1 || len(params) != 1 {
		t.Fatalf("expected 1 request and param, got %d and %d", len(reqs), len(params))
	}
}

func TestBuildPropertyReadRequestsArrayKind(t *testing.T) {
	device := testArrayKindDeviceConfig()

	reqs, bindings, err := BuildPropertyReadRequests(device, map[string]interface{}{
		"Name": map[string]interface{}{
			"2": true,
			"5": true,
		},
	})
	if err != nil {
		t.Fatalf("BuildPropertyReadRequests() error = %v", err)
	}
	if len(reqs) != 2 || len(bindings) != 2 {
		t.Fatalf("expected 2 read requests and bindings, got %d and %d", len(reqs), len(bindings))
	}
	// Verify 2-element binding path (not 3)
	for _, binding := range bindings {
		if len(binding.Path) != 2 || binding.Path[0] != "Name" {
			t.Fatalf("expected 2-element binding path [Name, indexKey], got %v", binding.Path)
		}
	}

	// Verify response structure is 2-level
	values := []*contracts.CommandValue{
		{DeviceResourceName: "Name.2.v", Type: "String", Value: "轮型2"},
		{DeviceResourceName: "Name.5.v", Type: "String", Value: "轮型5"},
	}
	response := BuildPropertyResponse(values, bindings)
	name := response["Name"].(map[string]interface{})
	if name["2"] != "轮型2" {
		t.Fatalf("expected Name[2] = '轮型2', got %v", name["2"])
	}
	if name["5"] != "轮型5" {
		t.Fatalf("expected Name[5] = '轮型5', got %v", name["5"])
	}
}

func TestBuildAutoPropertyReadRequestsArrayKind(t *testing.T) {
	device := testArrayKindDeviceConfig()
	// Enable auto-report on Name struct
	for i := range device.Property.Structs {
		if device.Property.Structs[i].Name == "Name" {
			device.Property.Structs[i].AutoReport = true
		}
	}

	reqs, bindings, err := BuildAutoPropertyReadRequests(device)
	if err != nil {
		t.Fatalf("BuildAutoPropertyReadRequests() error = %v", err)
	}
	// 1 status_text point + 7 Name indices = 8 total
	if len(reqs) != 8 || len(bindings) != 8 {
		t.Fatalf("expected 8 read requests and bindings, got %d and %d", len(reqs), len(bindings))
	}
	// Verify Name bindings have 2-element paths
	nameCount := 0
	for _, binding := range bindings {
		if binding.Path[0] == "Name" {
			nameCount++
			if len(binding.Path) != 2 {
				t.Fatalf("expected 2-element path for Name binding, got %v", binding.Path)
			}
		}
	}
	if nameCount != 7 {
		t.Fatalf("expected 7 Name bindings, got %d", nameCount)
	}
}

func TestBuildPropertyReadSelectionArrayKind(t *testing.T) {
	device := testArrayKindDeviceConfig()

	// Full selection
	selection, err := BuildPropertyReadSelectionFromNames(device, []string{"Name"})
	if err != nil {
		t.Fatalf("BuildPropertyReadSelectionFromNames() error = %v", err)
	}
	name, ok := selection["Name"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected Name to be map, got %T", selection["Name"])
	}
	if len(name) != 7 {
		t.Fatalf("expected 7 Name indices, got %d", len(name))
	}
	// Each index should be true, not empty map
	if name["1"] != true {
		t.Fatalf("expected Name[1] selection to be true, got %#v", name["1"])
	}

	// Single index selection
	selection, err = BuildPropertyReadSelectionFromNames(device, []string{"Name[3]"})
	if err != nil {
		t.Fatalf("BuildPropertyReadSelectionFromNames() error = %v", err)
	}
	name = selection["Name"].(map[string]interface{})
	if len(name) != 1 || name["3"] != true {
		t.Fatalf("expected Name[3] = true, got %#v", name)
	}

	// Range selection
	selection, err = BuildPropertyReadSelectionFromNames(device, []string{"Name[1-3]"})
	if err != nil {
		t.Fatalf("BuildPropertyReadSelectionFromNames() error = %v", err)
	}
	name = selection["Name"].(map[string]interface{})
	if len(name) != 3 {
		t.Fatalf("expected 3 Name indices, got %d", len(name))
	}
	for _, idx := range []string{"1", "2", "3"} {
		if name[idx] != true {
			t.Fatalf("expected Name[%s] = true, got %#v", idx, name[idx])
		}
	}
}

func TestBuildPropertyWriteRequestsArrayKindBackwardCompat(t *testing.T) {
	device := testArrayKindDeviceConfig()

	// Full-object write format should also work for array kind
	reqs, params, err := BuildPropertyWriteRequests(device, map[string]interface{}{
		"Name": map[string]interface{}{
			"1": "轮型1",
			"2": "轮型2",
		},
	})
	if err != nil {
		t.Fatalf("BuildPropertyWriteRequests() error = %v", err)
	}
	if len(reqs) != 2 || len(params) != 2 {
		t.Fatalf("expected 2 requests and params, got %d and %d", len(reqs), len(params))
	}
	if params[0].Value != "轮型1" {
		t.Fatalf("expected '轮型1', got %v", params[0].Value)
	}
	if params[1].Value != "轮型2" {
		t.Fatalf("expected '轮型2', got %v", params[1].Value)
	}
}
