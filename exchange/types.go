package exchange

type Candlestick struct {
	Timestamp int64   `json:"time"`
	Open      float32 `json:"open"`
	High      float32 `json:"high"`
	Low       float32 `json:"low"`
	Close     float32 `json:"close"`
	Volume    float64 `json:"volume,omitempty"`
}
