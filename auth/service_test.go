package auth

import (
	"testing"
	"time"

	rtapi "github.com/punk-one/edge-service-sdk/property"
)

func TestBootstrapIssueTokenAndAuthorize(t *testing.T) {
	root := t.TempDir()
	svc, err := NewService(Config{
		SQLitePath:     root + "/runtime.db",
		KeyFile:        root + "/auth.key",
		BootstrapToken: "bootstrap-secret",
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	defer svc.Close()

	if _, err := svc.BootstrapInit(rtapi.BootstrapInitRequest{
		AppID:     "demo",
		AppSecret: "secret",
	}, "bootstrap-secret"); err != nil {
		t.Fatalf("BootstrapInit() error = %v", err)
	}

	req := rtapi.AuthTokenRequest{
		AppID:     "demo",
		Timestamp: time.Now().UnixMilli(),
		Nonce:     "nonce-1",
	}
	_, secret, err := svc.credentialSecret()
	if err != nil {
		t.Fatalf("credentialSecret() error = %v", err)
	}
	req.Signature = signTokenRequest(secret, req.AppID, req.Timestamp, req.Nonce)

	tokenResp, err := svc.IssueToken(req)
	if err != nil {
		t.Fatalf("IssueToken() error = %v", err)
	}
	if tokenResp.AccessToken == "" {
		t.Fatal("expected access token")
	}

	body := []byte(`{"product_code":"acm","device_code":"acm006"}`)
	protected := ProtectedRequest{
		Method:    "POST",
		Path:      "/api/v1/property/get",
		Body:      body,
		AppID:     "demo",
		Token:     tokenResp.AccessToken,
		Timestamp: time.Now().UnixMilli(),
		Nonce:     "nonce-2",
	}
	protected.Signature = signProtectedRequest(secret, protected.Method, protected.Path, protected.Body, protected.Token, protected.Timestamp, protected.Nonce, protected.AppID)

	if err := svc.AuthorizeProtected(protected); err != nil {
		t.Fatalf("AuthorizeProtected() error = %v", err)
	}
	if err := svc.AuthorizeProtected(protected); err == nil {
		t.Fatal("expected replayed nonce to be rejected")
	}
}
