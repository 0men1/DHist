package dhist

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/0men1/DHist/exchange"
)

// fakeProvider returns a full, contiguous set of candles for any requested
// [start, end] range (inclusive on both ends), mimicking a healthy exchange.
// If failAt is set, any request whose start equals failAt always errors,
// simulating a batch that exhausts its retries.
type fakeProvider struct {
	granularity int64
	failAt      int64
	hasFailAt   bool
	calls       atomic.Int64
}

func (p *fakeProvider) FetchCandles(_ context.Context, _ string, start, end, _ int64) ([]exchange.Candlestick, error) {
	p.calls.Add(1)
	if p.hasFailAt && start == p.failAt {
		return nil, fmt.Errorf("simulated permanent failure at %d", start)
	}
	var out []exchange.Candlestick
	for t := start; t <= end; t += p.granularity {
		out = append(out, exchange.Candlestick{Timestamp: uint64(t)})
	}
	return out, nil
}

func drain(outChan <-chan []exchange.Candlestick, errChan <-chan error) (total int64, firstErr error) {
	for batch := range outChan {
		total += int64(len(batch))
	}
	for err := range errChan {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return total, firstErr
}

func TestStreamCandles_CleanReconciliation(t *testing.T) {
	const gran = int64(60)
	const maxReqCap = int64(350)
	end := int64(1_700_100_000)
	start := end - 5000*gran

	provider := &fakeProvider{granularity: gran}

	var expected, received int64
	tel := &StreamTelemetry{
		OnComplete: func(exp, rec int64) {
			atomic.StoreInt64(&expected, exp)
			atomic.StoreInt64(&received, rec)
		},
	}

	outChan, errChan := StreamCandles(context.Background(), provider, "SOL-USD",
		start, end, gran, maxReqCap, 4, WithTelemetry(tel))

	total, err := drain(outChan, errChan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A healthy fetch must reconcile exactly: no silent gaps.
	if expected == 0 {
		t.Fatal("OnComplete was not called")
	}
	if total != expected {
		t.Fatalf("delivered %d candles, expected %d", total, expected)
	}
	if received != expected {
		t.Fatalf("reconciliation mismatch: expected=%d received=%d", expected, received)
	}
}

func TestStreamCandles_FailLoudlyNoSilentTruncation(t *testing.T) {
	const gran = int64(60)
	const maxReqCap = int64(350)
	blockDuration := gran * maxReqCap
	end := int64(1_700_100_000)
	start := end - 5000*gran
	alignedStart := (start / blockDuration) * blockDuration

	// Fail the third batch (index 2) so that, under the old merge, every
	// higher-index batch would be silently dropped without an error surfacing.
	failAt := alignedStart + 2*blockDuration

	provider := &fakeProvider{granularity: gran, failAt: failAt, hasFailAt: true}

	var completeCalled atomic.Bool
	tel := &StreamTelemetry{
		OnComplete: func(_, _ int64) { completeCalled.Store(true) },
	}

	outChan, errChan := StreamCandles(context.Background(), provider, "SOL-USD",
		start, end, gran, maxReqCap, 4, WithTelemetry(tel))

	_, err := drain(outChan, errChan)
	if err == nil {
		t.Fatal("expected a terminal error to be surfaced, got nil (silent truncation)")
	}
	if completeCalled.Load() {
		t.Fatal("OnComplete must not report a clean reconciliation on an aborted run")
	}
}
