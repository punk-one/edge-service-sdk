package mqtt

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReconnectDelayCapped(t *testing.T) {
	client := &mqttClient{
		config: MQTTConfig{
			InitialRetryIntervalMs:  500,
			MaxReconnectIntervalSec: 3,
		},
	}

	if got := client.reconnectDelay(0); got != 500*time.Millisecond {
		t.Fatalf("attempt 0 delay = %s, want %s", got, 500*time.Millisecond)
	}
	if got := client.reconnectDelay(1); got != time.Second {
		t.Fatalf("attempt 1 delay = %s, want %s", got, time.Second)
	}
	if got := client.reconnectDelay(3); got != 3*time.Second {
		t.Fatalf("attempt 3 delay = %s, want %s", got, 3*time.Second)
	}
	if got := client.reconnectDelay(8); got != 3*time.Second {
		t.Fatalf("attempt 8 delay = %s, want %s", got, 3*time.Second)
	}
}

func TestWaitTokenTimeout(t *testing.T) {
	err := waitToken(mockToken{waitTimeoutOK: false}, 20*time.Millisecond, "publish")
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("waitToken timeout err = %v, want timeout error", err)
	}
}

func TestWaitTokenError(t *testing.T) {
	err := waitToken(mockToken{waitTimeoutOK: true, err: errors.New("boom")}, time.Second, "publish")
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("waitToken error = %v, want wrapped broker error", err)
	}
}

type mockToken struct {
	waitTimeoutOK bool
	err           error
}

func (m mockToken) Wait() bool {
	return m.waitTimeoutOK
}

func (m mockToken) WaitTimeout(time.Duration) bool {
	return m.waitTimeoutOK
}

func (m mockToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (m mockToken) Error() error {
	return m.err
}
