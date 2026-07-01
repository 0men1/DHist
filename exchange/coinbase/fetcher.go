package coinbase

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/0men1/DHist/exchange"
	"net/http"
	"sort"
	"strconv"
	"time"
)

type CoinbaseFetcher exchange.Fetcher

func NewFetcher() *CoinbaseFetcher {
	return &CoinbaseFetcher{
		Client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		BaseURL: "https://api.exchange.coinbase.com",
	}
}

func (f *CoinbaseFetcher) FetchCandles(ctx context.Context, symbol string,
	start, end, granularity int64) ([]exchange.Candlestick, error) {

	reqURL := fmt.Sprintf("%s/products/%s/candles?granularity=%d&start=%d&end=%d",
		f.BaseURL, symbol, granularity, start, end)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("request creation failed: %w", err)
	}

	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := 2 * time.Second
		if h := resp.Header.Get("Retry-After"); h != "" {
			if secs, err := strconv.Atoi(h); err == nil {
				retryAfter = time.Duration(secs) * time.Second
			}
		}
		return nil, &exchange.RateLimitError{RetryAfter: retryAfter}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("exchange returned status %d", resp.StatusCode)
	}

	var rawData [][]float64
	if err := json.NewDecoder(resp.Body).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("json decoding failed: %w", err)
	}

	candles := make([]exchange.Candlestick, 0, len(rawData))
	for _, row := range rawData {
		if len(row) < 6 {
			continue
		}
		candles = append(candles, exchange.Candlestick{
			Timestamp: uint64(row[0]),
			Low:       float64(row[1]),
			High:      float64(row[2]),
			Open:      float64(row[3]),
			Close:     float64(row[4]),
			Volume:    row[5],
		})
	}

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp < candles[j].Timestamp
	})

	return candles, nil
}
