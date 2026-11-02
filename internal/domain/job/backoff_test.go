package job

import (
	"errors"
	"testing"
	"time"
)

func TestBackoffPolicy(t *testing.T) {
	policy := BackoffPolicy{
		BaseInterval: 2 * time.Second,
		MaxInterval:  30 * time.Second,
	}

	t.Run("first attempt delay is within base interval plus jitter", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			delay := policy.NextDelay(1)
			// attempt 1: 2s * 2^0 = 2s, plus jitter in [0, 2s] -> [2s, 4s]
			if delay < 2*time.Second || delay > 4*time.Second {
				t.Fatalf("expected delay between 2s and 4s, got %v", delay)
			}
		}
	})

	t.Run("exponential growth", func(t *testing.T) {
		// attempt 2: 2s * 2^1 = 4s, plus jitter in [0, 2s] -> [4s, 6s]
		delay2 := policy.NextDelay(2)
		if delay2 < 4*time.Second || delay2 > 6*time.Second {
			t.Fatalf("expected delay between 4s and 6s, got %v", delay2)
		}

		// attempt 3: 2s * 2^2 = 8s, plus jitter in [0, 2s] -> [8s, 10s]
		delay3 := policy.NextDelay(3)
		if delay3 < 8*time.Second || delay3 > 10*time.Second {
			t.Fatalf("expected delay between 8s and 10s, got %v", delay3)
		}
	})

	t.Run("delay capped at MaxInterval", func(t *testing.T) {
		// attempt 10: 2s * 2^9 = 1024s -> should be capped at 30s
		delay10 := policy.NextDelay(10)
		if delay10 != 30*time.Second {
			t.Fatalf("expected delay capped at 30s, got %v", delay10)
		}
	})
}

func TestErrorClassification(t *testing.T) {
	standardErr := errors.New("transient network timeout")
	if IsPermanent(standardErr) {
		t.Fatal("standard error should not be permanent")
	}

	permErr := MarkPermanent(standardErr)
	if !IsPermanent(permErr) {
		t.Fatal("MarkPermanent error should be permanent")
	}
	if !errors.Is(permErr, standardErr) {
		t.Fatal("unwrapping should yield original standardErr")
	}

	if !IsPermanent(ErrPermanent) {
		t.Fatal("ErrPermanent should be permanent")
	}
	if !IsPermanent(ErrPoisonMessage) {
		t.Fatal("ErrPoisonMessage should be permanent")
	}
	if !IsPoison(ErrPoisonMessage) {
		t.Fatal("ErrPoisonMessage should be recognized as poison")
	}
	if IsPoison(standardErr) {
		t.Fatal("standard error is not poison")
	}
}
