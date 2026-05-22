package dhist

import "context"

type Candlestick struct {
	Timestamp int64   `json:"time"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume,omitempty"`
}

type Provider interface {
	FetchCandles(ctx context.Context, symbol string, start, end, granularity int64) ([]Candlestick, error)
}
