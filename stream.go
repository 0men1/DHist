package dhist

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
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
	// OnComplete reports the reconciliation between the number of candles the
	// batch grid was expected to cover and the number actually received once
	// the stream finishes cleanly. It is not called if the stream aborts with
	// a terminal error.
	OnComplete func(expected, received int64)
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

	// expected is the number of distinct candles a complete fetch should yield.
	// Batches are contiguous, so the covered range is [alignedStart, lastReqEnd]
	// inclusive of candle-open times. alignedStart may precede the requested
	// start (the first batch is grid-aligned), so this counts those too.
	var expected int64
	if len(batchStarts) > 0 {
		lastStart := batchStarts[len(batchStarts)-1]
		lastReqEnd := min(lastStart+(granularity*(maxReqCap-1)), end)
		expected = (lastReqEnd-alignedStart)/granularity + 1
	}

	outChan := make(chan []exchange.Candlestick, maxConcurrent)
	// +1 so a terminal failure error can always be buffered even if every
	// batch has already reported an error.
	errChan := make(chan error, len(batchStarts)+1)

	limiter := NewAdaptiveRateLimiter(8, 50, 1, maxConcurrent)

	// runCtx is derived from the caller's ctx so a permanent batch failure can
	// tear down the whole run instead of silently truncating the output.
	runCtx, cancel := context.WithCancel(ctx)

	go func() {
		defer cancel()
		defer close(outChan)
		defer close(errChan)

		// aborted is set when a batch fails permanently. Once set, we must not
		// report a (misleading) clean OnComplete reconciliation.
		var aborted atomic.Bool
		var received atomic.Int64

		var wg sync.WaitGroup
		sem := make(chan struct{}, maxConcurrent)

		resultChan := make(chan IndexedBatch, maxConcurrent)
		go func() {
			defer close(resultChan)

			for i, bStart := range batchStarts {
				if err := limiter.Wait(runCtx); err != nil {
					aborted.Store(true)
					errChan <- err
					cancel()
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
						if err := limiter.Wait(runCtx); err != nil {
							return
						}
						if config.Telemetry != nil && config.Telemetry.OnRequest != nil {
							config.Telemetry.OnRequest()
						}

						rawBatch, err = provider.FetchCandles(runCtx, symbol,
							currentStart, reqEnd, granularity)

						if err == nil {
							limiter.Success()
							if config.Telemetry != nil && config.Telemetry.OnBatch != nil {
								config.Telemetry.OnBatch(len(rawBatch))
							}
							select {
							case resultChan <- IndexedBatch{index: index, candles: rawBatch}:
							case <-runCtx.Done():
							}
							return
						}

						// 429 — respect Retry-After exactly
						var rlErr RateLimitError
						if errors.As(err, &rlErr) {
							limiter.RateLimited()
							select {
							case <-runCtx.Done():
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
							case <-runCtx.Done():
								return
							case <-time.After(backoff):
							}
						}
					}

					// Permanent failure: signal a terminal error and cancel the
					// run so no succeeded-but-higher-index batch is silently
					// dropped without the caller being told the result is
					// incomplete.
					aborted.Store(true)
					errChan <- fmt.Errorf("batch at %d failed: %w", currentStart, err)
					cancel()
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
				received.Add(int64(len(candles)))
				select {
				case outChan <- candles:
				case <-runCtx.Done():
					return
				}
				next++
			}
		}

		// Reached only on clean completion (resultChan drained without abort).
		// Surface an incomplete result even if no batch errored (e.g. the API
		// returning fewer candles per request than the grid assumed).
		if !aborted.Load() && config.Telemetry != nil && config.Telemetry.OnComplete != nil {
			config.Telemetry.OnComplete(expected, received.Load())
		}
	}()

	return outChan, errChan
}
