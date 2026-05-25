package coinbase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	dhist "github.com/0men1/DHist"
)

type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited, retry after %s", e.RetryAfter)
}

func (e *RateLimitError) RetryAfterDuration() time.Duration {
	return e.RetryAfter
}

type Fetcher struct {
	client  *http.Client
	baseURL string
}

func NewFetcher() *Fetcher {
	return &Fetcher{
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		baseURL: "https://api.coinbase.com/api/v3/brokerage/market",
	}
}

func granToText(granularity int64) string {
	switch granularity {
	case 60:
		return "ONE_MINUTE"
	case 300:
		return "FIVE_MINUTE"
	case 900:
		return "FIFTEEN_MINUTE"
	case 3600:
		return "ONE_HOUR"
	default:
		return "ONE_MINUTE"
	}
}

type CoinbaseResponse struct {
	Candles []CoinbaseCandle `json:"candles"`
}

type CoinbaseCandle struct {
	Start  string `json:"start"`
	Low    string `json:"low"`
	High   string `json:"high"`
	Open   string `json:"open"`
	Close  string `json:"close"`
	Volume string `json:"volume"`
}

func (f *Fetcher) FetchCandles(ctx context.Context, symbol string,
	start, end, granularity int64) ([]dhist.Candlestick, error) {

	// Prevent millisecond timestamp scale errors
	if start > 9999999999 || end > 9999999999 {
		return nil, fmt.Errorf("timestamps must be in seconds, not milliseconds")
	}

	reqURL := fmt.Sprintf("%s/products/%s/candles?granularity=%s&start=%d&end=%d",
		f.baseURL, symbol, granToText(granularity), start, end)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("request creation failed: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "DHist-Data-Pipeline/1.0")

	resp, err := f.client.Do(req)
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
		return nil, &RateLimitError{RetryAfter: retryAfter}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("exchange returned status %d for symbol %s", resp.StatusCode, symbol)
	}

	var rawData CoinbaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("json decoding failed: %w", err)
	}

	candles := make([]dhist.Candlestick, 0, len(rawData.Candles))
	for _, row := range rawData.Candles {
		timestamp, _ := strconv.ParseInt(row.Start, 10, 64)
		open, _ := strconv.ParseFloat(row.Open, 32)
		high, _ := strconv.ParseFloat(row.High, 32)
		low, _ := strconv.ParseFloat(row.Low, 32)
		closePrice, _ := strconv.ParseFloat(row.Close, 32)
		volume, _ := strconv.ParseFloat(row.Volume, 64)

		candles = append(candles, dhist.Candlestick{
			Timestamp: timestamp,
			Open:      float32(open),
			High:      float32(high),
			Low:       float32(low),
			Close:     float32(closePrice),
			Volume:    volume,
		})
	}

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp < candles[j].Timestamp
	})

	return candles, nil
}
