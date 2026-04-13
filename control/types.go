package control

// Request is the unified control request envelope shared by property and command channels.
type Request struct {
	TraceID    string                 `json:"trace_id"`
	DeviceCode string                 `json:"device_code"`
	Time       int64                  `json:"time"`
	Data       map[string]interface{} `json:"data"`
	Metadata   *Metadata              `json:"metadata,omitempty"`
}

// Metadata carries non-business execution strategy hints.
type Metadata struct {
	ExpiryTime int64  `json:"expiry_time,omitempty"`
	ExpectAck  bool   `json:"expect_ack,omitempty"`
	Strategy   string `json:"strategy,omitempty"`
}

// Result is the unified control result envelope shared by property and command channels.
type Result struct {
	TraceID string                 `json:"trace_id"`
	Code    int                    `json:"code"`
	Message string                 `json:"message"`
	Data    map[string]interface{} `json:"data,omitempty"`
	Time    int64                  `json:"time,omitempty"`
}
