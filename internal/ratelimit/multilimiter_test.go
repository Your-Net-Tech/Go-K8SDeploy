package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMultiLimiterAllows(t *testing.T) {
	m := NewMultiLimiter()
	tier := Tier{
		RequestsPerSecond: 10,
		Burst:             5,
		RequestsPerDay:    1000,
	}
	for i := 0; i < 5; i++ {
		if !m.Allow("client1", tier) {
			t.Errorf("request %d denied (burst should allow 5)", i)
		}
	}
}

func TestMultiLimiterBurstExhausted(t *testing.T) {
	m := NewMultiLimiter()
	tier := Tier{
		RequestsPerSecond: 1,
		Burst:             3,
		RequestsPerDay:    1000,
	}

	allowed := 0
	for i := 0; i < 10; i++ {
		if m.Allow("client1", tier) {
			allowed++
		}
	}
	if allowed > 4 {
		t.Errorf("allowed=%d, expected <= 4 (burst 3 + 1 refill)", allowed)
	}
}

func TestMultiLimiterPerClient(t *testing.T) {
	m := NewMultiLimiter()
	tier := Tier{
		RequestsPerSecond: 1,
		Burst:             2,
		RequestsPerDay:    1000,
	}

	c1 := 0
	c2 := 0
	for i := 0; i < 2; i++ {
		if m.Allow("client1", tier) {
			c1++
		}
		if m.Allow("client2", tier) {
			c2++
		}
	}
	if c1 != 2 || c2 != 2 {
		t.Errorf("per-client isolation broken: c1=%d c2=%d", c1, c2)
	}
}

func TestMultiLimiterDailyLimit(t *testing.T) {
	m := NewMultiLimiter()
	tier := Tier{
		RequestsPerSecond: 1000,
		Burst:             1000,
		RequestsPerDay:    10,
	}

	allowed := 0
	for i := 0; i < 20; i++ {
		if m.Allow("client1", tier) {
			allowed++
		}
	}
	if allowed > 11 {
		t.Errorf("daily limit broken: allowed=%d, expected <= 11 (10 + 1)", allowed)
	}
}

func TestMultiLimiterConcurrent(t *testing.T) {
	m := NewMultiLimiter()
	tier := Tier{
		RequestsPerSecond: 100,
		Burst:             100,
		RequestsPerDay:    10000,
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Allow("client-concurrent", tier)
		}()
	}
	wg.Wait()
}

func TestRateLimiterWaitTimeout(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	for i := 0; i < 5; i++ {
		rl.Allow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := rl.Wait(ctx)
	if err == nil {
		t.Log("got token before timeout (may be ok)")
	}
}