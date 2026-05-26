package dhist

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

type AdaptiveRateLimiter struct {
	mu      sync.Mutex
	current float64
	max     float64
	min     float64
	inner   *rate.Limiter
}

func NewAdaptiveRateLimiter(initial, max, min float64, burst int) *AdaptiveRateLimiter {
	return &AdaptiveRateLimiter{
		current: initial,
		max:     max,
		min:     min,
		inner:   rate.NewLimiter(rate.Limit(initial), burst),
	}
}

func (a *AdaptiveRateLimiter) Wait(ctx context.Context) error {
	return a.inner.Wait(ctx)
}

func (a *AdaptiveRateLimiter) Success() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.current = min(a.current+0.5, a.max)
	a.inner.SetLimit(rate.Limit(a.current))
}

func (a *AdaptiveRateLimiter) RateLimited() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.current = max(a.current*0.6, a.min)
	a.inner.SetLimit(rate.Limit(a.current))
}
