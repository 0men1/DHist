package dhist

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/time/rate"
)

var coinbaseReadLimiter = rate.NewLimiter(rate.Limit(60), 60)

func StreamCandles(ctx context.Context, provider Provider, symbol string, start, end,
	granularity int64, maxReqCap int64, maxConcurrent int) (<-chan []Candlestick, <-chan error) {

	blockDuration := granularity * maxReqCap
	alignedStart := (start / blockDuration) * blockDuration

	var batchStarts []int64
	for t := alignedStart; t < end; t += blockDuration {
		batchStarts = append(batchStarts, t)
	}

	outChan := make(chan []Candlestick)
	errChan := make(chan error, 1)
	futures := make([]chan []Candlestick, len(batchStarts))

	for i := range futures {
		futures[i] = make(chan []Candlestick, 1)
	}

	go func() {
		sem := make(chan struct{}, maxConcurrent)

		for i, bStart := range batchStarts {
			sem <- struct{}{}

			go func(idx int, currentStart int64) {
				defer func() { <-sem }()

				reqEnd := currentStart + (granularity * (maxReqCap - 1))
				reqEnd = min(reqEnd, end)

				var batch []Candlestick
				var err error

				for attempt := range 5 {
					if err := coinbaseReadLimiter.Wait(ctx); err != nil {
						return
					}
					if attempt > 0 {
						fmt.Printf("Attempt #%d for batch %d\n", attempt, idx)
					}
					batch, err = provider.FetchCandles(ctx, symbol,
						currentStart, reqEnd, granularity)
					if err == nil {
						break
					}

					backoff := time.Duration((1<<attempt))*time.Millisecond +
						time.Duration(rand.Intn(500))*time.Millisecond

					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
				}
				if err != nil {
					select {
					case errChan <- fmt.Errorf("batch %d failed at %d: %w",
						idx, currentStart, err):
					default:
					}
					close(futures[idx])
					return
				}
				futures[idx] <- batch
				close(futures[idx])
			}(i, bStart)
		}
	}()

	go func() {
		defer close(outChan)
		defer close(errChan)

		for _, future := range futures {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			case batch, ok := <-future:
				if !ok {
					return
				}
				outChan <- batch
			}
		}
	}()

	return outChan, errChan
}
