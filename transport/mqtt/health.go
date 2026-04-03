package mqtt

import (
	"fmt"
	"time"
)

func (c *mqttClient) HealthCheck() error {
	if c == nil {
		return fmt.Errorf("mqtt client is nil")
	}

	if c.checkConnectionHealth() {
		return nil
	}

	c.mu.Lock()
	lastErr := c.lastConnectErr
	c.mu.Unlock()
	if lastErr != nil {
		return lastErr
	}

	return fmt.Errorf("mqtt client is unhealthy")
}

func (c *mqttClient) healthCheckLoop() {
	ticker := time.NewTicker(c.config.healthCheckInterval())
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.checkConnectionHealth()
		}
	}
}

func (c *mqttClient) checkConnectionHealth() bool {
	client := c.currentClient()
	if !mqttClientReady(client) {
		err := fmt.Errorf("mqtt health check detected disconnected client")
		c.markUnhealthy(err.Error())
		c.resetClient(client)
		c.startReconnect("health_check", err)
		return false
	}

	c.markHealthy("health check passed")
	return true
}

func (c *mqttClient) markUnhealthy(reason string) {
	c.healthMu.Lock()
	wasHealthy := c.healthy
	c.healthy = false
	c.degraded = true
	c.healthMu.Unlock()

	if wasHealthy {
		c.logger.Warnf("MQTT publisher entered degraded state: %s", reason)
	}
}

func (c *mqttClient) markHealthy(reason string) {
	c.healthMu.Lock()
	wasHealthy := c.healthy
	recovering := c.degraded
	c.healthy = true
	c.degraded = false
	c.healthMu.Unlock()

	if !wasHealthy || recovering {
		c.logger.Infof("MQTT publisher is healthy: %s", reason)
	}
}
