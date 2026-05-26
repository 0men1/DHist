package coinbaseadvanced

type Candlestick struct {
	Start  string `json:"start"`
	Low    string `json:"low"`
	High   string `json:"high"`
	Open   string `json:"open"`
	Close  string `json:"close"`
	Volume string `json:"volume"`
}

type Response struct {
	Candles []Candlestick `json:"candles"`
}
