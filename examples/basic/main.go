package main

import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"sync/atomic"
	"time"

	dhist "github.com/0men1/DHist"
	coinbaseadvanced "github.com/0men1/DHist/exchange/coinbase_advanced"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	provider := coinbaseadvanced.NewFetcher()

	end := time.Now().Unix()
	granularity := int64(60)
	start := end - (5000 * granularity)

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
		ctx, provider, "SOL-USD", start, end, granularity, 350, 1,
		dhist.WithTelemetry(telemetry),
	)

	totalCandles := 0

StreamLoop:
	for {
		select {
		case err := <-errChan:
			if err != nil {
				log.Fatalf("Stream failed: %v", err)

			}

		case batch, ok := <-outChan:
			if !ok {
				break StreamLoop
			}
			if len(batch) == 0 {
				continue
			}
			fmt.Printf("Received batch of %d candles. First TS: %d\n", len(batch), batch[0].Timestamp)
			fmt.Println(batch)
			totalCandles += len(batch)
		}
	}

	fmt.Printf("Successfully fetched %d total candles.\n", totalCandles)
}
