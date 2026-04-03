package mqtt

import (
	"encoding/json"
	"fmt"
	"time"

	logger "github.com/punk-one/edge-service-sdk/logging"

	paho "github.com/eclipse/paho.mqtt.golang"
)

func (c MQTTConfig) keepAlive() time.Duration {
	if c.KeepAliveSec > 0 {
		return time.Duration(c.KeepAliveSec) * time.Second
	}
	return 60 * time.Second
}

func (c MQTTConfig) pingTimeout() time.Duration {
	if c.PingTimeoutSec > 0 {
		return time.Duration(c.PingTimeoutSec) * time.Second
	}
	return 5 * time.Second
}

func (c MQTTConfig) connectTimeout() time.Duration {
	if c.ConnectTimeoutSec > 0 {
		return time.Duration(c.ConnectTimeoutSec) * time.Second
	}
	return 15 * time.Second
}

func (c MQTTConfig) publishTimeout() time.Duration {
	if c.PublishTimeoutSec > 0 {
		return time.Duration(c.PublishTimeoutSec) * time.Second
	}
	return 10 * time.Second
}

func (c MQTTConfig) healthCheckInterval() time.Duration {
	if c.HealthCheckIntervalSec > 0 {
		return time.Duration(c.HealthCheckIntervalSec) * time.Second
	}
	return 30 * time.Second
}

func (c MQTTConfig) initialRetryInterval() time.Duration {
	if c.InitialRetryIntervalMs > 0 {
		return time.Duration(c.InitialRetryIntervalMs) * time.Millisecond
	}
	return time.Second
}

func (c MQTTConfig) maxReconnectInterval() time.Duration {
	if c.MaxReconnectIntervalSec > 0 {
		return time.Duration(c.MaxReconnectIntervalSec) * time.Second
	}
	return time.Minute
}

func (c MQTTConfig) disconnectQuiesce() uint {
	if c.DisconnectQuiesceMs > 0 {
		return uint(c.DisconnectQuiesceMs)
	}
	return 250
}

func newMQTTClient(config MQTTConfig, logger logger.LoggingClient) *mqttClient {
	client := &mqttClient{
		config:        config,
		logger:        logger,
		subscriptions: make(map[string]subscription),
		stopCh:        make(chan struct{}),
	}

	if err := client.connectOnce("bootstrap"); err != nil {
		client.lastConnectErr = err
		client.markUnhealthy("initial connect failed")
		logger.Warnf("Initial MQTT connection failed: %v", err)
		client.startReconnect("bootstrap", err)
	}

	go client.healthCheckLoop()
	return client
}

// NewClient creates a reusable MQTT client for generic publish/subscribe modules.
func NewClient(config MQTTConfig, logger logger.LoggingClient) Client {
	return newMQTTClient(config, logger)
}

func (c *mqttClient) registerOnConnectHook(hook func()) {
	if hook == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onConnectHooks = append(c.onConnectHooks, hook)
}

// RegisterOnConnect registers a callback that runs after each successful connect/reconnect.
func (c *mqttClient) RegisterOnConnect(hook func()) {
	c.registerOnConnectHook(hook)
}

func (c *mqttClient) buildClientOptions(clientID string) (*paho.ClientOptions, error) {
	opts := paho.NewClientOptions()
	opts.AddBroker(c.config.URL)
	opts.SetClientID(clientID)
	opts.SetAutoReconnect(false)
	opts.SetConnectRetry(false)
	opts.SetKeepAlive(c.config.keepAlive())
	opts.SetPingTimeout(c.config.pingTimeout())
	opts.SetConnectTimeout(c.config.connectTimeout())

	if c.config.Username != "" {
		opts.SetUsername(c.config.Username)
	}
	if c.config.Password != "" {
		opts.SetPassword(c.config.Password)
	}

	if !mqttUsesTLSTransport(c.config.URL) && c.config.hasTLSSettings() {
		c.logger.Warnf("MQTT TLS settings are ignored because broker URL scheme %q is not TLS; use ssl:// or tcps:// to enable TLS", mqttBrokerScheme(c.config.URL))
	}

	tlsConfig, err := c.config.buildTLSConfig()
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		opts.SetTLSConfig(tlsConfig)
	}

	opts.SetConnectionLostHandler(func(client paho.Client, err error) {
		logErr := "connection closed"
		if err != nil {
			logErr = err.Error()
		}
		c.logger.Warnf("MQTT connection lost: %s", logErr)
		c.resetClient(client)
		c.markUnhealthy("connection lost")
		c.startReconnect("connection_lost", err)
	})

	opts.SetOnConnectHandler(func(client paho.Client) {
		c.logger.Infof("Connected to MQTT broker: %s", c.config.URL)
		c.markHealthy("connected")
		c.resubscribeAll(client)
		c.runOnConnectHooks()
	})

	return opts, nil
}

func (c *mqttClient) runOnConnectHooks() {
	c.mu.Lock()
	hooks := append([]func(){}, c.onConnectHooks...)
	c.mu.Unlock()

	for _, hook := range hooks {
		go hook()
	}
}

func (c *mqttClient) connectOnce(mode string) error {
	if c.config.URL == "" {
		return fmt.Errorf("mqtt broker url is empty")
	}

	clientID := fmt.Sprintf("edge-service-s7-%d", time.Now().UnixNano())
	opts, err := c.buildClientOptions(clientID)
	if err != nil {
		return err
	}
	client := paho.NewClient(opts)
	token := client.Connect()

	timeout := c.config.connectTimeout()
	if !token.WaitTimeout(timeout) {
		client.Disconnect(0)
		return fmt.Errorf("mqtt %s connect timeout after %s", mode, timeout)
	}
	if err := token.Error(); err != nil {
		client.Disconnect(0)
		return fmt.Errorf("mqtt %s connect: %w", mode, err)
	}

	c.mu.Lock()
	oldClient := c.client
	c.client = client
	c.lastConnectErr = nil
	c.mu.Unlock()

	if oldClient != nil && oldClient != client && oldClient.IsConnectionOpen() {
		oldClient.Disconnect(c.config.disconnectQuiesce())
	}

	return nil
}

func (c *mqttClient) startReconnect(reason string, triggerErr error) {
	if c.isStopping() {
		return
	}

	c.mu.Lock()
	if c.reconnecting {
		c.mu.Unlock()
		return
	}
	c.reconnecting = true
	c.mu.Unlock()

	go func() {
		defer func() {
			c.mu.Lock()
			c.reconnecting = false
			c.mu.Unlock()
		}()

		for attempt := 0; ; attempt++ {
			if c.isStopping() {
				return
			}

			err := c.connectOnce("reconnect")
			if err == nil {
				if triggerErr != nil {
					c.logger.Infof("MQTT reconnect succeeded after %s: %v", reason, triggerErr)
				} else {
					c.logger.Infof("MQTT reconnect succeeded after %s", reason)
				}
				return
			}

			c.mu.Lock()
			c.lastConnectErr = err
			c.mu.Unlock()

			c.markUnhealthy("reconnect failed")
			c.logger.Warnf("MQTT reconnect failed, reason=%s attempt=%d err=%v", reason, attempt+1, err)

			delay := c.reconnectDelay(attempt)
			select {
			case <-c.stopCh:
				return
			case <-time.After(delay):
			}
		}
	}()
}

func (c *mqttClient) reconnectDelay(attempt int) time.Duration {
	delay := c.config.initialRetryInterval()
	maxDelay := c.config.maxReconnectInterval()
	for i := 0; i < attempt; i++ {
		if delay >= maxDelay {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func (c *mqttClient) Subscribe(topic string, qos byte, handler MessageHandler) error {
	if topic == "" {
		return nil
	}

	c.mu.Lock()
	c.subscriptions[topic] = subscription{qos: qos, handler: handler}
	client := c.client
	c.mu.Unlock()

	if !mqttClientReady(client) {
		c.startReconnect("subscribe", fmt.Errorf("mqtt client not connected"))
		return nil
	}

	if err := c.subscribeWithClient(client, topic, qos, handler); err != nil {
		if !mqttClientReady(client) {
			c.resetClient(client)
			c.markUnhealthy("subscribe failed on disconnected client")
			c.startReconnect("subscribe_failure", err)
		}
		return err
	}

	return nil
}

func (c *mqttClient) Close() error {
	c.closeOnce.Do(func() {
		close(c.stopCh)

		c.mu.Lock()
		client := c.client
		c.client = nil
		c.mu.Unlock()

		if client != nil && client.IsConnectionOpen() {
			client.Disconnect(c.config.disconnectQuiesce())
		}
	})
	return nil
}

func (c *mqttClient) resubscribeAll(client paho.Client) {
	subscriptions := c.subscriptionSnapshot()
	for topic, sub := range subscriptions {
		if err := c.subscribeWithClient(client, topic, sub.qos, sub.handler); err != nil {
			c.logger.Warnf("Failed to resubscribe topic %s: %v", topic, err)
		}
	}
}

func (c *mqttClient) subscribeWithClient(client paho.Client, topic string, qos byte, handler MessageHandler) error {
	token := client.Subscribe(topic, qos, func(_ paho.Client, msg paho.Message) {
		if handler != nil {
			handler(msg.Topic(), msg.Payload())
		}
	})
	if err := waitToken(token, c.config.publishTimeout(), "subscribe"); err != nil {
		return err
	}
	c.logger.Infof("Subscribed to MQTT topic %s", topic)
	return nil
}

func (c *mqttClient) publishMessage(message mqttMessage) error {
	if message.Topic == "" {
		return nil
	}

	client := c.currentClient()
	if !mqttClientReady(client) {
		err := fmt.Errorf("mqtt client not connected")
		c.markUnhealthy(err.Error())
		c.resetClient(client)
		c.startReconnect("publish_not_ready", err)
		return err
	}

	token := client.Publish(message.Topic, message.QoS, message.Retain, message.Payload)
	if err := waitToken(token, c.config.publishTimeout(), "publish"); err != nil {
		c.markUnhealthy("publish failed")
		c.logger.Warnf("Failed to publish MQTT topic %s: %v", message.Topic, err)
		if !mqttClientReady(client) {
			c.resetClient(client)
			c.startReconnect("publish_failure", err)
		}
		return err
	}

	c.markHealthy("publish path ready")
	return nil
}

// Publish sends raw bytes to the specified MQTT topic.
func (c *mqttClient) Publish(topic string, qos byte, retain bool, payload []byte) error {
	return c.publishMessage(mqttMessage{
		Topic:   topic,
		QoS:     qos,
		Retain:  retain,
		Payload: payload,
	})
}

// PublishJSON marshals the payload and publishes it to the specified topic.
func (c *mqttClient) PublishJSON(topic string, qos byte, retain bool, payload interface{}) error {
	if topic == "" {
		return nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return c.Publish(topic, qos, retain, body)
}

func (c *mqttClient) currentClient() paho.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client
}

func (c *mqttClient) subscriptionSnapshot() map[string]subscription {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot := make(map[string]subscription, len(c.subscriptions))
	for topic, sub := range c.subscriptions {
		snapshot[topic] = sub
	}
	return snapshot
}

func (c *mqttClient) resetClient(target paho.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if target == nil || c.client == target {
		c.client = nil
	}
}

func (c *mqttClient) isStopping() bool {
	select {
	case <-c.stopCh:
		return true
	default:
		return false
	}
}

func mqttClientReady(client paho.Client) bool {
	return client != nil && client.IsConnected() && client.IsConnectionOpen()
}

func waitToken(token paho.Token, timeout time.Duration, operation string) error {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if !token.WaitTimeout(timeout) {
		return fmt.Errorf("mqtt %s timeout after %s", operation, timeout)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt %s: %w", operation, err)
	}
	return nil
}
