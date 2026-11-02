package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter defines the contract for request rate evaluation.
type RateLimiter interface {
	Allow(ctx context.Context, identifier string) (Result, error)
}

// Result captures the rate limit evaluation output.
type Result struct {
	Allowed   bool
	Limit     int
	Remaining int
	ResetAt   time.Time
}

// LimiterConfig holds operational configuration for RedisRateLimiter.
type LimiterConfig struct {
	Limit    int
	Window   time.Duration
	Timeout  time.Duration
	FailOpen bool
}

// DefaultLimiterConfig provides sensible defaults (100 req/sec, 200ms timeout, fail-open).
func DefaultLimiterConfig() LimiterConfig {
	return LimiterConfig{
		Limit:    100,
		Window:   1 * time.Second,
		Timeout:  200 * time.Millisecond,
		FailOpen: true,
	}
}

// slidingWindowScript implements an atomic sliding window counter using Redis sorted sets.
//
// KEYS[1]: rate limit key
// ARGV[1]: current timestamp in milliseconds
// ARGV[2]: sliding window size in milliseconds
// ARGV[3]: request limit count
var slidingWindowScript = redis.NewScript(`
	local key = KEYS[1]
	local now = tonumber(ARGV[1])
	local window = tonumber(ARGV[2])
	local limit = tonumber(ARGV[3])
	local clearBefore = now - window

	-- 1. Remove expired timestamps outside the sliding window
	redis.call('ZREMRANGEBYSCORE', key, 0, clearBefore)

	-- 2. Count active requests within the window
	local currentRequests = redis.call('ZCARD', key)

	if currentRequests < limit then
		-- Request allowed: insert current timestamp with unique sequence member
		local seqKey = key .. ':seq'
		local seq = redis.call('INCR', seqKey)
		local member = now .. ':' .. seq
		redis.call('ZADD', key, now, member)

		local ttlSeconds = math.ceil(window / 1000) + 2
		redis.call('EXPIRE', key, ttlSeconds)
		redis.call('EXPIRE', seqKey, ttlSeconds)

		local remaining = limit - currentRequests - 1
		local resetAt = math.ceil((now + window) / 1000)
		return {1, remaining, resetAt}
	else
		-- Limit exceeded: calculate when the oldest request rolls out of the window
		local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
		local resetAt = now + window
		if #oldest >= 2 then
			resetAt = tonumber(oldest[2]) + window
		end
		return {0, 0, math.ceil(resetAt / 1000)}
	end
`)

// SlidingWindowLimiter implements atomic sliding-window rate limiting backed by Redis.
type SlidingWindowLimiter struct {
	client *redis.Client
	cfg    LimiterConfig
}

// NewRateLimiter initializes a SlidingWindowLimiter.
func NewRateLimiter(client *redis.Client, cfg LimiterConfig) *SlidingWindowLimiter {
	if cfg.Limit <= 0 {
		cfg.Limit = 100
	}
	if cfg.Window <= 0 {
		cfg.Window = 1 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 200 * time.Millisecond
	}

	return &SlidingWindowLimiter{
		client: client,
		cfg:    cfg,
	}
}

// Allow evaluates whether a request for the given identifier is permitted.
//
// Policy:
// - If Redis is responsive: evaluates atomic sliding window and returns result.
// - If Redis fails/times out: if FailOpen is true, returns Allowed=true alongside the error.
func (l *SlidingWindowLimiter) Allow(ctx context.Context, identifier string) (Result, error) {
	key := fmt.Sprintf("forge:ratelimit:%s", identifier)
	now := time.Now()
	nowMS := now.UnixNano() / int64(time.Millisecond)
	windowMS := l.cfg.Window.Milliseconds()

	evalCtx, cancel := context.WithTimeout(ctx, l.cfg.Timeout)
	defer cancel()

	res, err := slidingWindowScript.Run(
		evalCtx,
		l.client,
		[]string{key},
		nowMS,
		windowMS,
		l.cfg.Limit,
	).Slice()

	if err != nil {
		if l.cfg.FailOpen {
			// Fail-open: allow request through, reporting error for metric/log capture
			return Result{
				Allowed:   true,
				Limit:     l.cfg.Limit,
				Remaining: l.cfg.Limit,
				ResetAt:   now.Add(l.cfg.Window),
			}, fmt.Errorf("rate limiter redis error (failing open): %w", err)
		}
		return Result{
			Allowed:   false,
			Limit:     l.cfg.Limit,
			Remaining: 0,
			ResetAt:   now.Add(l.cfg.Window),
		}, fmt.Errorf("rate limiter redis error (failing closed): %w", err)
	}

	allowedVal, _ := res[0].(int64)
	remainingVal, _ := res[1].(int64)
	resetEpoch, _ := res[2].(int64)

	return Result{
		Allowed:   allowedVal == 1,
		Limit:     l.cfg.Limit,
		Remaining: int(remainingVal),
		ResetAt:   time.Unix(resetEpoch, 0),
	}, nil
}
