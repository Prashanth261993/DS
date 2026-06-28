package main

// Tumbling-window aggregation engine (event-time, watermark-driven).
// See docs/lessons/04-windowing-watermarks.md.

import (
	"sort"
	"time"
)

const (
	windowMs        = 1000 // 1-second tumbling windows
	allowedLateness = 2000 // ms; watermark = maxEvent - allowedLateness (lesson 04)
	smaShort        = 5    // bars (lesson 15)
	smaLong         = 20   // bars
)

// Bar is a 1-second OHLCV+VWAP candle for one symbol, with rolling SMAs.
type Bar struct {
	Symbol      string  `json:"symbol"`
	WindowStart int64   `json:"window_start_ms"`
	Open        float64 `json:"open"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Close       float64 `json:"close"`
	Volume      float64 `json:"volume"`
	VWAP        float64 `json:"vwap"`
	Count       int     `json:"count"`
	SMA5        float64 `json:"sma5"`
	SMA20       float64 `json:"sma20"`
}

// acc is the in-progress aggregate for one (symbol, window).
type acc struct {
	open, high, low, close float64
	volume, sumPxQty       float64
	count                  int
	minT, maxT             int64 // event times of open/close trades
}

type key struct {
	symbol string
	start  int64
}

// Windower holds open windows keyed by (symbol, windowStart) and a watermark.
type Windower struct {
	windows  map[key]*acc
	maxEvent int64              // highest event time seen
	history  map[string][]float64 // per-symbol last N closes (lesson 15)
}

func NewWindower() *Windower {
	return &Windower{windows: make(map[key]*acc), history: make(map[string][]float64)}
}

func windowStart(eventMs int64) int64 { return eventMs - eventMs%windowMs }

// Watermark: we believe all events with event-time <= this have arrived.
func (w *Windower) watermark() int64 { return w.maxEvent - allowedLateness }

// Add folds a trade into its window. Returns (late=true) if it belongs to an
// already-closed window (older than the watermark) and was dropped.
func (w *Windower) Add(symbol string, price, qty float64, eventMs int64) (late bool) {
	if eventMs > w.maxEvent {
		w.maxEvent = eventMs
	}
	start := windowStart(eventMs)
	if start+windowMs <= w.watermark() {
		return true // window already emitted; drop
	}
	k := key{symbol, start}
	a := w.windows[k]
	if a == nil {
		a = &acc{open: price, high: price, low: price, close: price, minT: eventMs, maxT: eventMs}
		w.windows[k] = a
	}
	if price > a.high {
		a.high = price
	}
	if price < a.low {
		a.low = price
	}
	if eventMs <= a.minT {
		a.minT, a.open = eventMs, price
	}
	if eventMs >= a.maxT {
		a.maxT, a.close = eventMs, price
	}
	a.volume += qty
	a.sumPxQty += price * qty
	a.count++
	return false
}

// CloseReady emits and removes every window whose end is at/below the watermark.
func (w *Windower) CloseReady() []Bar {
	wm := w.watermark()
	var bars []Bar
	for k, a := range w.windows {
		if k.start+windowMs <= wm {
			vwap := 0.0
			if a.volume > 0 {
				vwap = a.sumPxQty / a.volume
			}
			bars = append(bars, Bar{
				Symbol: k.symbol, WindowStart: k.start,
				Open: a.open, High: a.high, Low: a.low, Close: a.close,
				Volume: a.volume, VWAP: vwap, Count: a.count,
			})
			delete(w.windows, k)
		}
	}
	// Sort by (start, symbol) so per-symbol rolling SMAs stay time-ordered.
	sort.Slice(bars, func(i, j int) bool {
		if bars[i].WindowStart != bars[j].WindowStart {
			return bars[i].WindowStart < bars[j].WindowStart
		}
		return bars[i].Symbol < bars[j].Symbol
	})
	for i := range bars {
		bars[i].SMA5, bars[i].SMA20 = w.updateSMA(bars[i].Symbol, bars[i].Close)
	}
	return bars
}

// updateSMA appends a close to the symbol's rolling history and returns SMA5/SMA20.
func (w *Windower) updateSMA(symbol string, close float64) (sma5, sma20 float64) {
	h := append(w.history[symbol], close)
	if len(h) > smaLong {
		h = h[len(h)-smaLong:]
	}
	w.history[symbol] = h
	return mean(h, smaShort), mean(h, smaLong)
}

// mean returns the average of the last n elements (or fewer if not yet enough).
func mean(h []float64, n int) float64 {
	if len(h) < n {
		n = len(h)
	}
	if n == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range h[len(h)-n:] {
		sum += v
	}
	return sum / float64(n)
}

// Open returns how many windows are currently buffered (a gauge).
func (w *Windower) Open() int { return len(w.windows) }

// WatermarkLag is how far behind real time the watermark is (rough health).
func (w *Windower) WatermarkLag() time.Duration {
	if w.maxEvent == 0 {
		return 0
	}
	return time.Duration(time.Now().UnixMilli()-w.watermark()) * time.Millisecond
}
