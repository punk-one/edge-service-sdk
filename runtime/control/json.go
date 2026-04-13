package control

import (
	"encoding/json"
	"strings"
)

var jsonMarshal = json.Marshal
var jsonUnmarshal = json.Unmarshal
var stringsTrimSpace = strings.TrimSpace
var stringsContains = strings.Contains
