package dhist

import "context"

type Candlestick struct {
	Timestamp int64   `json:"time"`
	Open      float32 `json:"open"`
	High      float32 `json:"high"`
	Low       float32 `json:"low"`
	Close     float32 `json:"close"`
	Volume    float64 `json:"volume,omitempty"`
}

type Provider interface {
	FetchCandles(ctx context.Context, symbol string, start, end, granularity int64) ([]Candlestick, error)
}
