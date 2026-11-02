package redis

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/YKalisjati/forge/pkg/config"
)

func setupTestRedis(t *testing.T) (*goredis.Client, func()) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	rdb := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("skipping test: redis unreachable: %v", err)
	}

	cleanup := func() {
		_ = rdb.Close()
	}

	return rdb, cleanup
}

func TestRedisRateLimiter_NormalAndExceeded(t *testing.T) {
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	limiterCfg := LimiterConfig{
		Limit:    5,
		Window:   1 * time.Second,
		Timeout:  200 * time.Millisecond,
		FailOpen: true,
	}
	limiter := NewRateLimiter(rdb, limiterCfg)
	id := fmt.Sprintf("test-user-%d", time.Now().UnixNano())

	// 5 requests should be allowed
	for i := 0; i < 5; i++ {
		res, err := limiter.Allow(context.Background(), id)
		if err != nil {
			t.Fatalf("unexpected error on req %d: %v", i, err)
		}
		if !res.Allowed {
			t.Fatalf("expected req %d to be allowed", i)
		}
		if res.Remaining != 4-i {
			t.Errorf("expected remaining %d, got %d", 4-i, res.Remaining)
		}
	}

	// 6th request should be rejected
	res, err := limiter.Allow(context.Background(), id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Allowed {
		t.Fatalf("expected request beyond limit to be rejected")
	}
	if res.Remaining != 0 {
		t.Errorf("expected remaining 0, got %d", res.Remaining)
	}

	// Wait for window to expire
	time.Sleep(1100 * time.Millisecond)

	// Should be allowed again
	resAfter, err := limiter.Allow(context.Background(), id)
	if err != nil {
		t.Fatalf("unexpected error after window expiry: %v", err)
	}
	if !resAfter.Allowed {
		t.Fatalf("expected request allowed after window reset")
	}
}

func TestRedisRateLimiter_ConcurrentEnforcement(t *testing.T) {
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	limit := 10
	limiterCfg := LimiterConfig{
		Limit:    limit,
		Window:   2 * time.Second,
		Timeout:  500 * time.Millisecond,
		FailOpen: true,
	}
	limiter := NewRateLimiter(rdb, limiterCfg)
	id := fmt.Sprintf("test-concurrent-%d", time.Now().UnixNano())

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowedCount := 0
	rejectedCount := 0

	totalRequests := 25
	wg.Add(totalRequests)

	for i := 0; i < totalRequests; i++ {
		go func() {
			defer wg.Done()
			res, err := limiter.Allow(context.Background(), id)
			if err != nil {
				return
			}
			mu.Lock()
			if res.Allowed {
				allowedCount++
			} else {
				rejectedCount++
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if allowedCount != limit {
		t.Fatalf("expected exactly %d allowed requests under concurrency, got %d", limit, allowedCount)
	}
	if rejectedCount != totalRequests-limit {
		t.Fatalf("expected %d rejected requests, got %d", totalRequests-limit, rejectedCount)
	}
}

func TestRedisRateLimiter_FailOpenPolicy(t *testing.T) {
	// Point to unreachable Redis port
	badRdb := goredis.NewClient(&goredis.Options{
		Addr: "127.0.0.1:59998",
	})
	defer func() { _ = badRdb.Close() }()

	limiterCfg := LimiterConfig{
		Limit:    10,
		Window:   1 * time.Second,
		Timeout:  50 * time.Millisecond,
		FailOpen: true,
	}
	limiter := NewRateLimiter(badRdb, limiterCfg)

	res, err := limiter.Allow(context.Background(), "unreachable-client")
	if err == nil {
		t.Fatalf("expected error from unreachable redis")
	}
	if !res.Allowed {
		t.Fatalf("expected request to be allowed under FailOpen=true policy")
	}
}

func TestRedisRateLimiter_FailClosedPolicy(t *testing.T) {
	badRdb := goredis.NewClient(&goredis.Options{
		Addr: "127.0.0.1:59998",
	})
	defer func() { _ = badRdb.Close() }()

	limiterCfg := LimiterConfig{
		Limit:    10,
		Window:   1 * time.Second,
		Timeout:  50 * time.Millisecond,
		FailOpen: false, // Fail-closed
	}
	limiter := NewRateLimiter(badRdb, limiterCfg)

	res, err := limiter.Allow(context.Background(), "unreachable-client")
	if err == nil {
		t.Fatalf("expected error from unreachable redis")
	}
	if res.Allowed {
		t.Fatalf("expected request to be rejected under FailOpen=false policy")
	}
}
