package dhist

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/0men1/DHist/exchange"
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

type IndexedBatch struct {
	candles []exchange.Candlestick
	index   int
}

type StreamOption func(*StreamConfig)

func WithTelemetry(t *StreamTelemetry) StreamOption {
	return func(c *StreamConfig) {
		c.Telemetry = t
	}
}

func StreamCandles(ctx context.Context, provider exchange.Provider, symbol string, start, end,
	granularity int64, maxReqCap int64, maxConcurrent int,
	opts ...StreamOption) (<-chan []exchange.Candlestick, <-chan error) {

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

	outChan := make(chan []exchange.Candlestick, maxConcurrent)
	errChan := make(chan error, len(batchStarts))

	limiter := NewAdaptiveRateLimiter(8, 50, 1, maxConcurrent)

	go func() {
		defer close(outChan)
		defer close(errChan)

		var wg sync.WaitGroup
		sem := make(chan struct{}, maxConcurrent)

		resultChan := make(chan IndexedBatch, maxConcurrent)
		go func() {
			defer close(resultChan)

			for i, bStart := range batchStarts {
				if err := limiter.Wait(ctx); err != nil {
					errChan <- err
					break
				}

				sem <- struct{}{}
				wg.Add(1)

				go func(currentStart int64, index int) {
					defer wg.Done()
					defer func() { <-sem }()

					reqEnd := min(currentStart+(granularity*(maxReqCap-1)), end)

					var rawBatch []exchange.Candlestick
					var err error

					for attempt := range 5 {
						if err := limiter.Wait(ctx); err != nil {
							errChan <- err
							return
						}
						if config.Telemetry != nil && config.Telemetry.OnRequest != nil {
							config.Telemetry.OnRequest()
						}

						rawBatch, err = provider.FetchCandles(ctx, symbol,
							currentStart, reqEnd, granularity)

						if err == nil {
							limiter.Success()
							if config.Telemetry != nil && config.Telemetry.OnBatch != nil {
								config.Telemetry.OnBatch(len(rawBatch))
							}
							select {
							case resultChan <- IndexedBatch{index: index, candles: rawBatch}:
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
				}(bStart, i)
			}
			wg.Wait()
		}()

		pending := make(map[int][]exchange.Candlestick)
		next := 0

		for res := range resultChan {
			pending[res.index] = res.candles
			for {
				candles, ok := pending[next]
				if !ok {
					break
				}
				delete(pending, next)
				select {
				case outChan <- candles:
				case <-ctx.Done():
					return
				}
				next++
			}
		}
	}()

	return outChan, errChan
}
