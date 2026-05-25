package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	dhist "github.com/0men1/DHist"
	"github.com/0men1/DHist/exchange/coinbase"
)

func main() {
	provider := coinbase.NewFetcher()

	end := time.Now().Unix()
	granularity := int64(60)
	start := end - (1_000_000 * granularity)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	var batches, apiCalls, candles atomic.Uint64

	telemetry := &dhist.StreamTelemetry{
		OnRequest: func() {
			apiCalls.Add(1)
			fmt.Printf("# API CALLS: %d\n", apiCalls.Load())
		},
		OnBatch: func(count int) {
			batches.Add(1)
			candles.Add(uint64(count))
			fmt.Printf("# Batches: %d\n", batches.Load())
			fmt.Printf("Total Candles: %d\n", candles.Load())
		},
	}

	outChan, errChan := dhist.StreamCandles(
		ctx, provider, "BTC-USD", start, end, granularity, 300, 20,
		dhist.WithTelemetry(telemetry),
	)

	totalCandles := 0
	for batch := range outChan {
		if len(batch) == 0 {
			continue
		}
		fmt.Printf("Received batch of %d candles. First TS: %d\n", len(batch), batch[0].Timestamp)
		totalCandles += len(batch)
	}

	if err := <-errChan; err != nil {
		log.Fatalf("Stream failed: %v", err)
	}

	fmt.Printf("Successfully fetched %d total candles.\n", totalCandles)
}
