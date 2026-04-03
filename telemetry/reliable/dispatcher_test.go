package reliable

import (
	"testing"
	"time"
)

func TestAvailableReplayBudgetUsesReplayRate(t *testing.T) {
	dispatcher := &Dispatcher{
		cfg: Config{
			BatchSize:        100,
			ReplayRatePerSec: 20,
		},
		replayTokens:     0,
		lastReplayRefill: time.Now().Add(-1500 * time.Millisecond).UnixMilli(),
	}

	budget := dispatcher.availableReplayBudget(time.Now())
	if budget < 20 || budget > 21 {
		t.Fatalf("unexpected replay budget %d, want around 20", budget)
	}
}
