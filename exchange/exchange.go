package exchange

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited, retry after %s", e.RetryAfter)
}

type Fetcher struct {
	Client  *http.Client
	BaseURL string
}
type Provider interface {
	FetchCandles(ctx context.Context, symbol string, start, end, granularity int64) ([]Candlestick, error)
}
