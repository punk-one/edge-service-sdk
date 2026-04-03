package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
)

func (c MQTTConfig) shouldUseTLS() bool {
	return mqttUsesTLSTransport(c.URL)
}

func (c MQTTConfig) hasTLSSettings() bool {
	return strings.TrimSpace(c.CACert) != "" ||
		strings.TrimSpace(c.CAPath) != "" ||
		strings.TrimSpace(c.ClientCert) != "" ||
		strings.TrimSpace(c.ClientKey) != "" ||
		strings.TrimSpace(c.CertPath) != "" ||
		strings.TrimSpace(c.PrivKeyPath) != "" ||
		c.MTLS ||
		c.SkipTLSVer
}

func (c MQTTConfig) buildTLSConfig() (*tls.Config, error) {
	if !c.shouldUseTLS() {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.SkipTLSVer,
	}

	if serverName := mqttServerName(c.URL); serverName != "" && !c.SkipTLSVer {
		tlsConfig.ServerName = serverName
	}

	rootCAs, err := c.loadRootCAs()
	if err != nil {
		return nil, err
	}
	if rootCAs != nil {
		tlsConfig.RootCAs = rootCAs
	}

	clientCert, err := c.loadClientCertificate()
	if err != nil {
		return nil, err
	}
	if clientCert != nil {
		tlsConfig.Certificates = []tls.Certificate{*clientCert}
	}

	if c.MTLS && len(tlsConfig.Certificates) == 0 {
		return nil, fmt.Errorf("mqtt mtls enabled but no client certificate/key configured")
	}

	return tlsConfig, nil
}

func (c MQTTConfig) loadRootCAs() (*x509.CertPool, error) {
	hasCustomCA := strings.TrimSpace(c.CACert) != "" || strings.TrimSpace(c.CAPath) != ""
	if !hasCustomCA {
		return nil, nil
	}

	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}

	if pem := strings.TrimSpace(c.CACert); pem != "" {
		if !pool.AppendCertsFromPEM([]byte(pem)) {
			return nil, fmt.Errorf("failed to append mqtt ca_cert PEM")
		}
	}

	if path := strings.TrimSpace(c.CAPath); path != "" {
		pems, err := readPEMFiles(path)
		if err != nil {
			return nil, fmt.Errorf("read mqtt ca_path %s: %w", path, err)
		}
		appended := false
		for _, pem := range pems {
			if pool.AppendCertsFromPEM(pem) {
				appended = true
			}
		}
		if !appended {
			return nil, fmt.Errorf("failed to append mqtt CA certificates from %s", path)
		}
	}

	return pool, nil
}

func (c MQTTConfig) loadClientCertificate() (*tls.Certificate, error) {
	certPEM, keyPEM, err := c.clientCertificatePEM()
	if err != nil {
		return nil, err
	}
	if len(certPEM) == 0 && len(keyPEM) == 0 {
		return nil, nil
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load mqtt client certificate: %w", err)
	}
	return &cert, nil
}

func (c MQTTConfig) clientCertificatePEM() ([]byte, []byte, error) {
	certPEM, err := readOptionalPEM(strings.TrimSpace(c.ClientCert), strings.TrimSpace(c.CertPath))
	if err != nil {
		return nil, nil, fmt.Errorf("read mqtt client certificate: %w", err)
	}
	keyPEM, err := readOptionalPEM(strings.TrimSpace(c.ClientKey), strings.TrimSpace(c.PrivKeyPath))
	if err != nil {
		return nil, nil, fmt.Errorf("read mqtt client private key: %w", err)
	}

	if len(certPEM) == 0 && len(keyPEM) == 0 {
		return nil, nil, nil
	}
	if len(certPEM) == 0 || len(keyPEM) == 0 {
		return nil, nil, fmt.Errorf("both client certificate and private key are required")
	}
	return certPEM, keyPEM, nil
}

func readOptionalPEM(inlineValue string, path string) ([]byte, error) {
	if inlineValue != "" {
		return []byte(inlineValue), nil
	}
	if path == "" {
		return nil, nil
	}
	return os.ReadFile(path)
}

func readPEMFiles(path string) ([][]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		pem, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return [][]byte{pem}, nil
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var pems [][]byte
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		pem, err := os.ReadFile(filepath.Join(path, entry.Name()))
		if err != nil {
			return nil, err
		}
		pems = append(pems, pem)
	}
	return pems, nil
}

func mqttUsesTLSTransport(rawURL string) bool {
	switch mqttBrokerScheme(rawURL) {
	case "ssl", "tls", "tcps", "mqtts", "wss", "https":
		return true
	default:
		return false
	}
}

func mqttBrokerScheme(rawURL string) string {
	parsed, err := neturl.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Scheme)
}

func mqttServerName(rawURL string) string {
	parsed, err := neturl.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	host := parsed.Hostname()
	if host == "" {
		host = parsed.Host
	}
	if host == "" {
		return ""
	}
	if withoutPort, _, err := net.SplitHostPort(host); err == nil {
		return withoutPort
	}
	return host
}
