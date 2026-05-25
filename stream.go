package dhist

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type RateLimitError interface {
	error
	RetryAfterDuration() time.Duration
}

type StreamTelemetry struct {
	OnRequest func()
	OnBatch   func(candleCount int)
}

type StreamConfig struct {
	Telemetry *StreamTelemetry
}

type StreamOption func(*StreamConfig)

func WithTelemetry(t *StreamTelemetry) StreamOption {
	return func(c *StreamConfig) {
		c.Telemetry = t
	}
}

func StreamCandles(ctx context.Context, provider Provider, symbol string, start, end,
	granularity int64, maxReqCap int64, maxConcurrent int,
	opts ...StreamOption) (<-chan []Candlestick, <-chan error) {

	config := &StreamConfig{}
	for _, opt := range opts {
		opt(config)
	}

	blockDuration := granularity * maxReqCap
	alignedStart := (start / blockDuration) * blockDuration

	var batchStarts []int64
	for t := alignedStart; t < end; t += blockDuration {
		batchStarts = append(batchStarts, t)
	}

	outChan := make(chan []Candlestick, maxConcurrent)
	errChan := make(chan error, len(batchStarts))

	limiter := NewAdaptiveRateLimiter(8, 50, 1, maxConcurrent)

	go func() {
		defer close(outChan)
		defer close(errChan)

		var wg sync.WaitGroup
		sem := make(chan struct{}, maxConcurrent)

		for _, bStart := range batchStarts {
			if err := limiter.Wait(ctx); err != nil {
				errChan <- err
				break
			}

			sem <- struct{}{}
			wg.Add(1)

			go func(currentStart int64) {
				defer wg.Done()
				defer func() { <-sem }()

				reqEnd := min(currentStart+(granularity*(maxReqCap-1)), end)

				var batch []Candlestick
				var err error

				for attempt := range 5 {
					if config.Telemetry != nil && config.Telemetry.OnRequest != nil {
						config.Telemetry.OnRequest()
					}

					batch, err = provider.FetchCandles(ctx, symbol,
						currentStart, reqEnd, granularity)

					if err == nil {
						limiter.Success()
						if config.Telemetry != nil && config.Telemetry.OnBatch != nil {
							config.Telemetry.OnBatch(len(batch))
						}
						select {
						case outChan <- batch:
							fmt.Println("BATCH SENT")
						case <-ctx.Done():
						}
						return
					}

					// 429 — respect Retry-After exactly
					var rlErr RateLimitError
					if errors.As(err, &rlErr) {
						limiter.RateLimited()
						select {
						case <-ctx.Done():
							return
						case <-time.After(rlErr.RetryAfterDuration()):
						}
						continue
					}

					// transient error — exponential backoff with jitter
					if attempt < 4 {
						backoff := time.Duration(1<<attempt)*time.Second +
							time.Duration(rand.Intn(500))*time.Millisecond
						select {
						case <-ctx.Done():
							return
						case <-time.After(backoff):
						}
					}
				}

				errChan <- fmt.Errorf("batch at %d failed: %w", currentStart, err)
			}(bStart)
		}

		wg.Wait()
	}()

	return outChan, errChan
}
