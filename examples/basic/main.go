package main

import (
	"context"
	"fmt"
	"log"
	"time"

	dhist "github.com/0men1/DHist"
	"github.com/0men1/DHist/exchange/coinbase"
)

func main() {
	provider := coinbase.NewFetcher()

	// Define a 3-hour window
	end := time.Now().Unix()
	start := end - (3600 * 3)
	granularity := int64(60) // 1 minute

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	outChan, errChan := dhist.StreamCandles(ctx, provider, "BTC-USD", start, end, granularity, 300, 5)

	totalCandles := 0
	for batch := range outChan {
		fmt.Printf("Received batch of %d candles. First TS: %d\n", len(batch), batch[0].Timestamp)
		totalCandles += len(batch)
	}

	if err := <-errChan; err != nil {
		log.Fatalf("Stream failed: %v", err)
	}

	fmt.Printf("Successfully fetched %d total candles.\n", totalCandles)
}
