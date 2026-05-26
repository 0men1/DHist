package coinbaseadvanced

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/0men1/DHist/exchange"
)

type CoinbaseAdvancedFetcher struct {
	exchange.Fetcher
}

func NewFetcher() *CoinbaseAdvancedFetcher {
	return &CoinbaseAdvancedFetcher{
		Fetcher: exchange.Fetcher{
			Client: &http.Client{
				Timeout: 10 * time.Second,
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 20,
					IdleConnTimeout:     30 * time.Second,
				},
			},
			BaseURL: "https://api.coinbase.com/api/v3/brokerage",
		},
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

func (f *CoinbaseAdvancedFetcher) FetchCandles(ctx context.Context, symbol string,
	start, end, granularity int64) ([]exchange.Candlestick, error) {

	requestMethod := "GET"
	basePath := fmt.Sprintf("/api/v3/brokerage/products/%s/candles", symbol)
	jwt := generateJWT(requestMethod, basePath)
	queryParams := fmt.Sprintf("?granularity=%s&start=%d&end=%d", granToText(granularity), start, end)
	reqURL := fmt.Sprintf("https://api.coinbase.com%s%s", basePath, queryParams)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("request creation failed: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "DHist-Data-Pipeline/1.0")

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
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("exchange returned status %d for symbol %s. Details: %s", resp.StatusCode, symbol, string(bodyBytes))
	}

	var rawData Response
	if err := json.NewDecoder(resp.Body).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("json decoding failed: %w", err)
	}

	candles := make([]exchange.Candlestick, 0, len(rawData.Candles))
	for _, row := range rawData.Candles {
		timestamp, _ := strconv.ParseInt(row.Start, 10, 64)
		open, _ := strconv.ParseFloat(row.Open, 32)
		high, _ := strconv.ParseFloat(row.High, 32)
		low, _ := strconv.ParseFloat(row.Low, 32)
		closePrice, _ := strconv.ParseFloat(row.Close, 32)
		volume, _ := strconv.ParseFloat(row.Volume, 64)

		candles = append(candles, exchange.Candlestick{
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
