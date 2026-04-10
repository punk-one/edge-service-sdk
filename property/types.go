package property

// PropertyRequest is the shared request model used by MQTT and HTTP property operations.
type PropertyRequest struct {
	DeviceCode string                 `json:"device_code"`
	Time       int64                  `json:"time"`
	TraceID    string                 `json:"trace_id,omitempty"`
	Data       map[string]interface{} `json:"data"`
}

// PropertyResponse is returned by property get operations.
type PropertyResponse struct {
	DeviceCode string                 `json:"device_code"`
	Time       int64                  `json:"time"`
	Success    bool                   `json:"success"`
	TraceID    string                 `json:"trace_id,omitempty"`
	Error      string                 `json:"error,omitempty"`
	Data       map[string]interface{} `json:"data"`
}

// PropertySetResponse is returned by property set operations.
type PropertySetResponse struct {
	DeviceCode string `json:"device_code"`
	Time       int64  `json:"time"`
	Success    bool   `json:"success"`
	TraceID    string `json:"trace_id,omitempty"`
	Error      string `json:"error,omitempty"`
}

// BootstrapInitRequest initializes the single app credential.
type BootstrapInitRequest struct {
	AppID     string `json:"appId"`
	AppSecret string `json:"appSecret"`
}

// AuthTokenRequest exchanges a signed app request for an access token.
type AuthTokenRequest struct {
	AppID     string `json:"appId"`
	Timestamp int64  `json:"timestamp"`
	Nonce     string `json:"nonce"`
	Signature string `json:"signature"`
}
