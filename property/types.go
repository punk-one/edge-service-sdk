package property

import ctl "github.com/punk-one/edge-service-sdk/control"

// PropertyRequest uses the unified control request envelope.
type PropertyRequest = ctl.Request

// PropertyResponse uses the unified control result envelope.
type PropertyResponse = ctl.Result

// PropertySetResponse uses the unified control result envelope.
type PropertySetResponse = ctl.Result

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
