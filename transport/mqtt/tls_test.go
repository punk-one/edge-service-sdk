package mqtt

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestBuildTLSConfigDisabled(t *testing.T) {
	cfg, err := (MQTTConfig{URL: "tcp://127.0.0.1:1883"}).buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig returned error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("buildTLSConfig returned %#v, want nil", cfg)
	}
}

func TestBuildTLSConfigIgnoresTLSSettingsForTCPBroker(t *testing.T) {
	cfg, err := (MQTTConfig{
		URL:         "tcp://127.0.0.1:1883",
		SkipTLSVer:  true,
		CAPath:      "missing-ca.crt",
		CertPath:    "missing-cert.crt",
		PrivKeyPath: "missing-key.key",
		MTLS:        true,
	}).buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig returned error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("buildTLSConfig returned %#v, want nil for non-TLS broker", cfg)
	}
}

func TestBuildTLSConfigWithInlineMaterials(t *testing.T) {
	certPEM, keyPEM := mustSelfSignedPEM(t, "mqtt.example.com")

	cfg, err := (MQTTConfig{
		URL:        "ssl://mqtt.example.com:8883",
		CACert:     string(certPEM),
		ClientCert: string(certPEM),
		ClientKey:  string(keyPEM),
		MTLS:       true,
	}).buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig returned error: %v", err)
	}
	if cfg == nil {
		t.Fatal("buildTLSConfig returned nil config")
	}
	if cfg.ServerName != "mqtt.example.com" {
		t.Fatalf("ServerName = %q, want %q", cfg.ServerName, "mqtt.example.com")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("Certificates len = %d, want 1", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs is nil, want populated cert pool")
	}
}

func TestBuildTLSConfigMissingPrivateKey(t *testing.T) {
	certPEM, _ := mustSelfSignedPEM(t, "mqtt.example.com")

	_, err := (MQTTConfig{
		URL:        "ssl://mqtt.example.com:8883",
		ClientCert: string(certPEM),
		MTLS:       true,
	}).buildTLSConfig()
	if err == nil || !strings.Contains(err.Error(), "both client certificate and private key are required") {
		t.Fatalf("buildTLSConfig error = %v, want missing key error", err)
	}
}

func TestMQTTUsesTLSTransport(t *testing.T) {
	if !mqttUsesTLSTransport("ssl://broker:8883") {
		t.Fatal("ssl:// should be treated as TLS transport")
	}
	if mqttUsesTLSTransport("tcp://broker:1883") {
		t.Fatal("tcp:// should not be treated as TLS transport")
	}
}

func mustSelfSignedPEM(t *testing.T, commonName string) ([]byte, []byte) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{commonName},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return certPEM, keyPEM
}
