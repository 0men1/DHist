package coinbase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	dhist "github.com/0men1/DHist"
)

type Fetcher struct {
	client  *http.Client
	baseURL string
}

func NewFetcher() *Fetcher {
	return &Fetcher{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseURL: "https://api.exchange.coinbase.com",
	}
}

func (f *Fetcher) FetchCandles(ctx context.Context, symbol string,
	start, end, granularity int64) ([]dhist.Candlestick, error) {

	reqURL := fmt.Sprintf("%s/products/%s/candles?granularity=%d&start=%d&end=%d",
		f.baseURL, symbol, granularity, start, end)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("request creation failed: %w", err)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	fmt.Printf("%s\n", reqURL)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("exchange returned status %d", resp.StatusCode)
	}

	var rawData [][]float64
	if err := json.NewDecoder(resp.Body).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("json decoding failed: %w", err)
	}

	var candles []dhist.Candlestick
	for _, row := range rawData {
		if len(row) < 6 {
			continue
		}

		candles = append(candles, dhist.Candlestick{
			Timestamp: int64(row[0]),
			Open:      float32(row[1]),
			High:      float32(row[2]),
			Low:       float32(row[3]),
			Close:     float32(row[4]),
			Volume:    row[5],
		})
	}

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp < candles[j].Timestamp
	})

	return candles, nil
}
