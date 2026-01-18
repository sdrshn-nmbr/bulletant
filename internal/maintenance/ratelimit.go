package maintenance

import (
	"context"
	"sync"
	"time"
)

type TokenBucket struct {
	rate     int64
	capacity int64
	tokens   int64
	last     time.Time
	mu       sync.Mutex
}

func NewTokenBucket(rateBytesPerSecond int64) *TokenBucket {
	if rateBytesPerSecond <= 0 {
		return &TokenBucket{}
	}
	now := time.Now()
	return &TokenBucket{
		rate:     rateBytesPerSecond,
		capacity: rateBytesPerSecond,
		tokens:   rateBytesPerSecond,
		last:     now,
	}
}

func (t *TokenBucket) WaitN(ctx context.Context, n int64) error {
	if t == nil || t.rate <= 0 || n <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		t.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(t.last)
		if elapsed > 0 {
			added := int64(float64(t.rate) * elapsed.Seconds())
			if added > 0 {
				t.tokens += added
				if t.tokens > t.capacity {
					t.tokens = t.capacity
				}
				t.last = now
			}
		}

		if t.tokens >= n {
			t.tokens -= n
			t.mu.Unlock()
			return nil
		}
		deficit := n - t.tokens
		wait := time.Duration(float64(deficit) / float64(t.rate) * float64(time.Second))
		t.mu.Unlock()

		if wait <= 0 {
			wait = time.Millisecond
		}
		if err := sleepWithContext(ctx, wait); err != nil {
			return err
		}
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
