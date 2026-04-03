package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	rtapi "github.com/punk-one/edge-service-sdk/property"

	_ "modernc.org/sqlite"
)

const singleCredentialID int64 = 1

// Error carries an HTTP status for handler mapping.
type Error struct {
	status int
	msg    string
}

func (e *Error) Error() string { return e.msg }
func (e *Error) Status() int   { return e.status }

// PublicCredential is the non-secret credential view.
type PublicCredential struct {
	Initialized bool   `json:"initialized"`
	AppID       string `json:"appId,omitempty"`
	UpdatedAt   int64  `json:"updatedAt,omitempty"`
}

// TokenResponse is the token issue response.
type TokenResponse struct {
	AccessToken string `json:"accessToken"`
	TokenType   string `json:"tokenType"`
	ExpiresAt   string `json:"expiresAt"`
}

// ProtectedRequest captures the signed request metadata for protected APIs.
type ProtectedRequest struct {
	Method    string
	Path      string
	Body      []byte
	AppID     string
	Token     string
	Timestamp int64
	Nonce     string
	Signature string
}

// Config controls auth storage and validation.
type Config struct {
	SQLitePath       string
	KeyFile          string
	BootstrapToken   string
	AccessTokenTTL   time.Duration
	AllowedClockSkew time.Duration
}

type credentialRecord struct {
	AppID            string
	SecretCiphertext string
	SecretHash       string
	UpdatedAt        int64
}

// Service manages the single runtime credential and access tokens.
type Service struct {
	db  *sql.DB
	key []byte
	cfg Config
}

// NewService creates an auth service backed by the shared runtime SQLite file.
func NewService(cfg Config) (*Service, error) {
	cfg = normalizeConfig(cfg)
	key, err := ensureKeyFile(cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.SQLitePath), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", cfg.SQLitePath)
	if err != nil {
		return nil, err
	}
	svc := &Service{
		db:  db,
		key: key,
		cfg: cfg,
	}
	if err := svc.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return svc, nil
}

// Close closes the underlying database handle.
func (s *Service) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// HealthCheck validates database availability.
func (s *Service) HealthCheck() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("auth service is not initialized")
	}
	return s.db.Ping()
}

// IsInitialized reports whether the single credential exists.
func (s *Service) IsInitialized() (bool, error) {
	record, err := s.credential()
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return record.AppID != "", nil
}

// CredentialInfo returns the current public credential state.
func (s *Service) CredentialInfo() (PublicCredential, error) {
	record, err := s.credential()
	if err != nil {
		if isNotFound(err) {
			return PublicCredential{Initialized: false}, nil
		}
		return PublicCredential{}, err
	}
	return PublicCredential{
		Initialized: true,
		AppID:       record.AppID,
		UpdatedAt:   record.UpdatedAt,
	}, nil
}

// BootstrapInit writes the first and only credential if the system is not initialized yet.
func (s *Service) BootstrapInit(req rtapi.BootstrapInitRequest, bootstrapToken string) (PublicCredential, error) {
	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(bootstrapToken)), []byte(strings.TrimSpace(s.cfg.BootstrapToken))) != 1 || strings.TrimSpace(s.cfg.BootstrapToken) == "" {
		return PublicCredential{}, newStatusError(401, "invalid bootstrap token")
	}
	if err := validateCredentialInput(req.AppID, req.AppSecret); err != nil {
		return PublicCredential{}, err
	}
	initialized, err := s.IsInitialized()
	if err != nil {
		return PublicCredential{}, err
	}
	if initialized {
		return PublicCredential{}, newStatusError(409, "credential already initialized")
	}

	record, err := s.buildCredential(req.AppID, req.AppSecret)
	if err != nil {
		return PublicCredential{}, err
	}
	if err := s.replaceCredential(record); err != nil {
		return PublicCredential{}, err
	}
	return s.CredentialInfo()
}

// IssueToken validates the signed bootstrap credential and returns a short-lived token.
func (s *Service) IssueToken(req rtapi.AuthTokenRequest) (TokenResponse, error) {
	record, secret, err := s.credentialSecret()
	if err != nil {
		if isNotFound(err) {
			return TokenResponse{}, newStatusError(503, "credential is not initialized")
		}
		return TokenResponse{}, err
	}
	if req.AppID != record.AppID {
		return TokenResponse{}, newStatusError(401, "invalid appId")
	}
	if err := validateTimestamp(req.Timestamp, s.cfg.AllowedClockSkew); err != nil {
		return TokenResponse{}, err
	}
	if err := validateNonce(req.Nonce); err != nil {
		return TokenResponse{}, err
	}
	if err := validateSignature(req.Signature); err != nil {
		return TokenResponse{}, err
	}

	expected := signTokenRequest(secret, req.AppID, req.Timestamp, req.Nonce)
	if subtle.ConstantTimeCompare([]byte(strings.ToLower(req.Signature)), []byte(expected)) != 1 {
		return TokenResponse{}, newStatusError(401, "invalid signature")
	}
	if err := s.consumeNonce(req.Nonce, req.Timestamp); err != nil {
		return TokenResponse{}, err
	}

	token, tokenHash, err := newOpaqueToken()
	if err != nil {
		return TokenResponse{}, err
	}
	now := time.Now()
	expiresAt := now.Add(s.cfg.AccessTokenTTL)
	if err := s.storeToken(tokenHash, now.UnixMilli(), expiresAt.UnixMilli()); err != nil {
		return TokenResponse{}, err
	}
	return TokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresAt:   expiresAt.UTC().Format(time.RFC3339),
	}, nil
}

// AuthorizeProtected validates token, timestamp, nonce, and HMAC signature.
func (s *Service) AuthorizeProtected(req ProtectedRequest) error {
	record, secret, err := s.credentialSecret()
	if err != nil {
		if isNotFound(err) {
			return newStatusError(503, "credential is not initialized")
		}
		return err
	}
	if req.AppID != record.AppID {
		return newStatusError(401, "invalid appId")
	}
	if strings.TrimSpace(req.Token) == "" {
		return newStatusError(401, "missing access token")
	}
	if err := validateTimestamp(req.Timestamp, s.cfg.AllowedClockSkew); err != nil {
		return err
	}
	if err := validateNonce(req.Nonce); err != nil {
		return err
	}
	if err := validateSignature(req.Signature); err != nil {
		return err
	}
	if err := s.validateToken(req.Token); err != nil {
		return err
	}

	expected := signProtectedRequest(secret, req.Method, req.Path, req.Body, req.Token, req.Timestamp, req.Nonce, req.AppID)
	if subtle.ConstantTimeCompare([]byte(strings.ToLower(req.Signature)), []byte(expected)) != 1 {
		return newStatusError(401, "invalid signature")
	}
	return s.consumeNonce(req.Nonce, req.Timestamp)
}

func (s *Service) init() error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA busy_timeout = 5000;",
	}
	for _, stmt := range pragmas {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}

	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS auth_credential (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	app_id TEXT NOT NULL,
	secret_ciphertext TEXT NOT NULL,
	secret_hash TEXT NOT NULL,
	updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_tokens (
	token_hash TEXT PRIMARY KEY,
	issued_at INTEGER NOT NULL,
	expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_nonces (
	nonce_hash TEXT PRIMARY KEY,
	expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_auth_tokens_expires_at ON auth_tokens(expires_at);
CREATE INDEX IF NOT EXISTS idx_auth_nonces_expires_at ON auth_nonces(expires_at);
`)
	return err
}

func (s *Service) credential() (credentialRecord, error) {
	row := s.db.QueryRow(`SELECT app_id, secret_ciphertext, secret_hash, updated_at FROM auth_credential WHERE id = ?`, singleCredentialID)

	var record credentialRecord
	if err := row.Scan(&record.AppID, &record.SecretCiphertext, &record.SecretHash, &record.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return credentialRecord{}, newStatusError(404, "credential not found")
		}
		return credentialRecord{}, err
	}
	return record, nil
}

func (s *Service) credentialSecret() (credentialRecord, []byte, error) {
	record, err := s.credential()
	if err != nil {
		return credentialRecord{}, nil, err
	}
	secret, err := decryptSecret(s.key, record.SecretCiphertext)
	if err != nil {
		return credentialRecord{}, nil, err
	}
	return record, secret, nil
}

func (s *Service) buildCredential(appID, appSecret string) (credentialRecord, error) {
	ciphertext, err := encryptSecret(s.key, []byte(appSecret))
	if err != nil {
		return credentialRecord{}, err
	}
	return credentialRecord{
		AppID:            strings.TrimSpace(appID),
		SecretCiphertext: ciphertext,
		SecretHash:       sha256Hex(appSecret),
		UpdatedAt:        time.Now().UnixMilli(),
	}, nil
}

func (s *Service) replaceCredential(record credentialRecord) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.Exec(`DELETE FROM auth_tokens`); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM auth_nonces`); err != nil {
		return err
	}
	if _, err = tx.Exec(`
INSERT INTO auth_credential(id, app_id, secret_ciphertext, secret_hash, updated_at)
VALUES(?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	app_id = excluded.app_id,
	secret_ciphertext = excluded.secret_ciphertext,
	secret_hash = excluded.secret_hash,
	updated_at = excluded.updated_at
`, singleCredentialID, record.AppID, record.SecretCiphertext, record.SecretHash, record.UpdatedAt); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) validateToken(token string) error {
	now := time.Now().UnixMilli()
	_, _ = s.db.Exec(`DELETE FROM auth_tokens WHERE expires_at <= ?`, now)
	row := s.db.QueryRow(`SELECT COUNT(1) FROM auth_tokens WHERE token_hash = ? AND expires_at > ?`, sha256Hex(token), now)

	var count int
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return newStatusError(401, "invalid or expired access token")
	}
	return nil
}

func (s *Service) storeToken(tokenHash string, issuedAt int64, expiresAt int64) error {
	_, err := s.db.Exec(`INSERT INTO auth_tokens(token_hash, issued_at, expires_at) VALUES(?, ?, ?)`, tokenHash, issuedAt, expiresAt)
	return err
}

func (s *Service) consumeNonce(nonce string, timestamp int64) error {
	now := time.Now().UnixMilli()
	_, _ = s.db.Exec(`DELETE FROM auth_nonces WHERE expires_at <= ?`, now)
	expiresAt := timestamp + int64((s.cfg.AllowedClockSkew + time.Minute).Milliseconds())
	if expiresAt <= now {
		expiresAt = now + int64((s.cfg.AllowedClockSkew + time.Minute).Milliseconds())
	}
	_, err := s.db.Exec(`INSERT INTO auth_nonces(nonce_hash, expires_at) VALUES(?, ?)`, sha256Hex(nonce), expiresAt)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return newStatusError(401, "nonce already used")
		}
		return err
	}
	return nil
}

func normalizeConfig(cfg Config) Config {
	if strings.TrimSpace(cfg.SQLitePath) == "" {
		cfg.SQLitePath = "./data/runtime.db"
	}
	if strings.TrimSpace(cfg.KeyFile) == "" {
		cfg.KeyFile = "./data/auth.key"
	}
	if cfg.AccessTokenTTL <= 0 {
		cfg.AccessTokenTTL = 10 * time.Minute
	}
	if cfg.AllowedClockSkew <= 0 {
		cfg.AllowedClockSkew = time.Minute
	}
	return cfg
}

func validateCredentialInput(appID, appSecret string) error {
	if strings.TrimSpace(appID) == "" {
		return newStatusError(400, "appId is required")
	}
	if strings.TrimSpace(appSecret) == "" {
		return newStatusError(400, "appSecret is required")
	}
	return nil
}

func validateTimestamp(timestamp int64, skew time.Duration) error {
	if timestamp <= 0 {
		return newStatusError(400, "timestamp is required")
	}
	delta := time.Since(time.UnixMilli(timestamp))
	if delta < 0 {
		delta = -delta
	}
	if delta > skew {
		return newStatusError(401, "timestamp is outside the allowed window")
	}
	return nil
}

func validateNonce(nonce string) error {
	if strings.TrimSpace(nonce) == "" {
		return newStatusError(400, "nonce is required")
	}
	return nil
}

func validateSignature(signature string) error {
	if strings.TrimSpace(signature) == "" {
		return newStatusError(400, "signature is required")
	}
	return nil
}

func signTokenRequest(secret []byte, appID string, timestamp int64, nonce string) string {
	canonical := fmt.Sprintf("POST\n/api/v1/auth/token\n\n%d\n%s\n%s", timestamp, nonce, appID)
	return sign(secret, canonical)
}

func signProtectedRequest(secret []byte, method string, path string, body []byte, token string, timestamp int64, nonce string, appID string) string {
	bodyHash := sha256.Sum256(body)
	canonical := fmt.Sprintf("%s\n%s\n%s\n%s\n%d\n%s\n%s",
		strings.ToUpper(strings.TrimSpace(method)),
		strings.TrimSpace(path),
		hex.EncodeToString(bodyHash[:]),
		strings.TrimSpace(token),
		timestamp,
		nonce,
		appID,
	)
	return sign(secret, canonical)
}

func sign(secret []byte, canonical string) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

func sha256Hex(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func newOpaqueToken() (string, string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", err
	}
	token := base64.RawURLEncoding.EncodeToString(buf)
	return token, sha256Hex(token), nil
}

func ensureKeyFile(path string) ([]byte, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if data, err := os.ReadFile(path); err == nil {
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(strings.TrimSpace(string(data)))
		if decodeErr != nil {
			return nil, fmt.Errorf("decode auth key file: %w", decodeErr)
		}
		if len(decoded) != 32 {
			return nil, fmt.Errorf("invalid auth key length: %d", len(decoded))
		}
		return decoded, nil
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(buf)
	if err := os.WriteFile(path, []byte(encoded), 0o600); err != nil {
		return nil, err
	}
	return buf, nil
}

func encryptSecret(key []byte, plaintext []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, nil)
	payload := append(nonce, ciphertext...)
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decryptSecret(key []byte, encoded string) ([]byte, error) {
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(payload) < aead.NonceSize() {
		return nil, fmt.Errorf("invalid encrypted payload")
	}
	nonce := payload[:aead.NonceSize()]
	ciphertext := payload[aead.NonceSize():]
	return aead.Open(nil, nonce, ciphertext, nil)
}

func newStatusError(status int, msg string) error {
	return &Error{status: status, msg: msg}
}

func isNotFound(err error) bool {
	statusErr, ok := err.(*Error)
	return ok && statusErr.status == 404
}
