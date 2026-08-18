package bus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	appconfig "github.com/punk-one/edge-service-sdk/config"
	logger "github.com/punk-one/edge-service-sdk/logging"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const (
	defaultMaxAge   = 72 * time.Hour
	defaultMaxBytes = int64(1 << 30)
)

type ConsumerConfig struct {
	Durable       string
	FilterSubject string
	Workers       int
	AckWait       time.Duration
	MaxDeliver    int
	RetryDelay    time.Duration
}

// Service owns one embedded, single-node JetStream server and its in-process
// client. It is optional and must never be required by the existing MQTT path.
type Service struct {
	server *natsserver.Server
	conn   *nats.Conn
	js     nats.JetStreamContext
	logger logger.LoggingClient

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func Start(serviceName string, config appconfig.NATSBusConfig, logClient logger.LoggingClient) (*Service, error) {
	if !config.Enabled {
		return nil, nil
	}
	storeDir := strings.TrimSpace(config.StoreDir)
	if storeDir == "" {
		storeDir = filepath.Join("data", "natsbus")
	}
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, fmt.Errorf("create JetStream store directory: %w", err)
	}
	absStoreDir, err := filepath.Abs(storeDir)
	if err != nil {
		return nil, fmt.Errorf("resolve JetStream store directory: %w", err)
	}

	opts := &natsserver.Options{
		ServerName: sanitizeToken(serviceName) + "-" + strconv.Itoa(os.Getpid()),
		Host:       "127.0.0.1",
		Port:       natsserver.RANDOM_PORT,
		JetStream:  true,
		StoreDir:   absStoreDir,
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create embedded NATS server: %w", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		return nil, fmt.Errorf("embedded NATS server did not become ready")
	}

	addr := ns.Addr()
	if addr == nil {
		ns.Shutdown()
		return nil, fmt.Errorf("embedded NATS server has no client address")
	}
	nc, err := nats.Connect("nats://"+addr.String(), nats.Name(serviceName+"-sdk-bus"))
	if err != nil {
		ns.Shutdown()
		return nil, fmt.Errorf("connect embedded NATS server: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("initialize JetStream client: %w", err)
	}

	maxAge := defaultMaxAge
	if raw := strings.TrimSpace(config.MaxAge); raw != "" {
		parsed, parseErr := time.ParseDuration(raw)
		if parseErr != nil || parsed <= 0 {
			nc.Close()
			ns.Shutdown()
			return nil, fmt.Errorf("invalid bus.maxAge %q", raw)
		}
		maxAge = parsed
	}
	maxBytes := config.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}
	streamConfig := &nats.StreamConfig{
		Name:      busapi.StreamName,
		Subjects:  []string{busapi.StreamSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   nats.DiscardOld,
		MaxAge:    maxAge,
		MaxBytes:  maxBytes,
		Replicas:  1,
	}
	if _, err = js.StreamInfo(busapi.StreamName); errors.Is(err, nats.ErrStreamNotFound) {
		_, err = js.AddStream(streamConfig)
	} else if err == nil {
		_, err = js.UpdateStream(streamConfig)
	}
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("initialize JetStream stream: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	service := &Service{server: ns, conn: nc, js: js, logger: logClient, ctx: ctx, cancel: cancel}
	if logClient != nil {
		logClient.Infof("Embedded JetStream ready: address=%s store=%s", addr.String(), absStoreDir)
	}
	return service, nil
}

func (s *Service) Publish(ctx context.Context, message busapi.Message) error {
	if s == nil || s.js == nil {
		return fmt.Errorf("JetStream bus is unavailable")
	}
	subject := strings.TrimSpace(message.Subject)
	if subject == "" {
		var err error
		subject, err = busapi.SubjectFor(message.Type, message.Identifier)
		if err != nil {
			return err
		}
	}
	msg := nats.NewMsg(subject)
	msg.Data = append([]byte(nil), message.Data...)
	setHeader(msg.Header, busapi.HeaderOrigin, string(defaultOrigin(message.Origin)))
	setHeader(msg.Header, busapi.HeaderProcessName, message.ProcessName)
	setHeader(msg.Header, busapi.HeaderMessageType, string(message.Type))
	setHeader(msg.Header, busapi.HeaderDataFormat, message.DataFormat)
	setHeader(msg.Header, busapi.HeaderTraceID, message.TraceID)
	setHeader(msg.Header, busapi.HeaderCausationID, message.CausationID)
	setHeader(msg.Header, busapi.HeaderProductCode, message.ProductCode)
	setHeader(msg.Header, busapi.HeaderDeviceCode, message.DeviceCode)
	setHeader(msg.Header, busapi.HeaderCommandID, message.Identifier)
	if message.Hop > 0 {
		setHeader(msg.Header, busapi.HeaderHop, strconv.Itoa(message.Hop))
	}
	for key, value := range message.Headers {
		setHeader(msg.Header, key, value)
	}
	_, err := s.js.PublishMsg(msg, nats.Context(ctx))
	return err
}

func (s *Service) StartConsumer(config ConsumerConfig, handler func(context.Context, busapi.Message) error) error {
	if s == nil || s.js == nil {
		return fmt.Errorf("JetStream bus is unavailable")
	}
	config.Durable = sanitizeToken(config.Durable)
	if config.Durable == "" || strings.TrimSpace(config.FilterSubject) == "" {
		return fmt.Errorf("consumer durable and filter subject are required")
	}
	if config.AckWait <= 0 {
		config.AckWait = 30 * time.Second
	}
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.MaxDeliver <= 0 {
		config.MaxDeliver = 10
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 2 * time.Second
	}
	sub, err := s.js.PullSubscribe(
		config.FilterSubject,
		config.Durable,
		nats.BindStream(busapi.StreamName),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.AckWait(config.AckWait),
		nats.MaxDeliver(config.MaxDeliver),
	)
	if err != nil {
		return err
	}
	go s.consume(sub, config, handler)
	return nil
}

func (s *Service) consume(sub *nats.Subscription, config ConsumerConfig, handler func(context.Context, busapi.Message) error) {
	workers := make(chan struct{}, config.Workers)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		messages, err := sub.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			if s.logger != nil && !errors.Is(err, nats.ErrConnectionClosed) {
				s.logger.Warnf("JetStream consumer %s fetch failed: %v", config.Durable, err)
			}
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		for _, raw := range messages {
			select {
			case workers <- struct{}{}:
			case <-s.ctx.Done():
				_ = raw.Nak()
				return
			}
			go func(raw *nats.Msg) {
				defer func() { <-workers }()
				message := fromNATSMessage(raw)
				if err := callHandler(s.ctx, handler, message); err != nil {
					if s.logger != nil {
						s.logger.Warnf("JetStream consumer %s handler failed: %v", config.Durable, err)
					}
					_ = raw.NakWithDelay(config.RetryDelay)
					return
				}
				_ = raw.Ack()
			}(raw)
		}
	}
}

func (s *Service) Healthy() error {
	if s == nil || s.server == nil || s.conn == nil {
		return fmt.Errorf("JetStream bus is unavailable")
	}
	if !s.server.ReadyForConnections(0) || !s.conn.IsConnected() {
		return fmt.Errorf("JetStream bus is not connected")
	}
	return nil
}

// Address returns the runtime-selected loopback address for diagnostics and
// tests. Application processors receive an injected Publisher and do not need
// to discover this address.
func (s *Service) Address() string {
	if s == nil || s.server == nil || s.server.Addr() == nil {
		return ""
	}
	return s.server.Addr().String()
}

func (s *Service) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.conn != nil {
			_ = s.conn.Drain()
			s.conn.Close()
		}
		if s.server != nil {
			s.server.Shutdown()
			s.server.WaitForShutdown()
		}
	})
}

func fromNATSMessage(raw *nats.Msg) busapi.Message {
	messageType, identifier, _ := busapi.ParseSubject(raw.Subject)
	origin := busapi.Origin(strings.TrimSpace(raw.Header.Get(busapi.HeaderOrigin)))
	if origin == "" {
		origin = busapi.OriginNATS
	}
	hop, _ := strconv.Atoi(strings.TrimSpace(raw.Header.Get(busapi.HeaderHop)))
	headers := make(map[string]string, len(raw.Header))
	for key := range raw.Header {
		headers[key] = raw.Header.Get(key)
	}
	return busapi.Message{
		Subject:     raw.Subject,
		Type:        messageType,
		Data:        append([]byte(nil), raw.Data...),
		Headers:     headers,
		Origin:      origin,
		ProcessName: raw.Header.Get(busapi.HeaderProcessName),
		DataFormat:  raw.Header.Get(busapi.HeaderDataFormat),
		TraceID:     raw.Header.Get(busapi.HeaderTraceID),
		CausationID: raw.Header.Get(busapi.HeaderCausationID),
		ProductCode: raw.Header.Get(busapi.HeaderProductCode),
		DeviceCode:  raw.Header.Get(busapi.HeaderDeviceCode),
		Identifier:  firstNonEmpty(raw.Header.Get(busapi.HeaderCommandID), identifier),
		Hop:         hop,
	}
}

func callHandler(ctx context.Context, handler func(context.Context, busapi.Message) error, message busapi.Message) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("consumer handler panicked: %v", recovered)
		}
	}()
	return handler(ctx, message)
}

func setHeader(header nats.Header, key string, value string) {
	if strings.TrimSpace(value) != "" {
		header.Set(key, value)
	}
}

func defaultOrigin(origin busapi.Origin) busapi.Origin {
	if origin == "" {
		return busapi.OriginNATS
	}
	return origin
}

func sanitizeToken(value string) string {
	value = strings.TrimSpace(value)
	var builder strings.Builder
	for _, char := range value {
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9', char == '-', char == '_':
			builder.WriteRune(char)
		default:
			builder.WriteByte('-')
		}
	}
	return strings.Trim(builder.String(), "-")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
