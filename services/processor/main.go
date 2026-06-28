// FluxTape stream processor.
//
// Phase 2 / Step B: consume the `trades` topic in a consumer group, aggregate
// into event-time 1s windows (OHLCV + VWAP), and emit closed bars to `bars_1s`.
// Late events (past the watermark) are dropped and counted. Metrics on :9101.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Trade mirrors the JSON the ingestion service publishes to `trades`.
type Trade struct {
	Symbol      string  `json:"symbol"`
	Price       float64 `json:"price"`
	Quantity    float64 `json:"quantity"`
	TradeID     int64   `json:"trade_id"`
	EventTimeMs int64   `json:"event_time_ms"`
	Side        string  `json:"side"`
}

const (
	topicTrades   = "trades"
	topicBars     = "bars_1s"
	consumerGroup = "fluxtape-processor"
	metricsAddr   = ":9101"
)

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:19092")

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topicTrades),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer cl.Close()

	startMetrics(metricsAddr)
	log.Printf("metrics on %s/metrics", metricsAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w := NewWindower()
	log.Printf("stream processor started; group=%q %q -> %q from %s", consumerGroup, topicTrades, topicBars, brokers)

	var emitted int
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("fetch error (topic=%s partition=%d): %v", e.Topic, e.Partition, e.Err)
			}
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			var t Trade
			if err := json.Unmarshal(r.Value, &t); err != nil {
				log.Printf("skip bad record at offset %d: %v", r.Offset, err)
				return
			}
			tradesProcessed.Inc()
			if w.Add(t.Symbol, t.Price, t.Quantity, t.EventTimeMs) {
				lateEvents.Inc()
			}
		})

		// Emit any windows the watermark has now passed.
		for _, bar := range w.CloseReady() {
			payload, _ := json.Marshal(bar)
			cl.Produce(ctx, &kgo.Record{Topic: topicBars, Key: []byte(bar.Symbol), Value: payload}, nil)
			barsEmitted.Inc()
			emitted++
			if emitted%50 == 0 {
				log.Printf("emitted=%d last: %s start=%d ohlc=%.2f/%.2f/%.2f/%.2f vol=%.4f vwap=%.2f sma5=%.2f sma20=%.2f n=%d",
					emitted, bar.Symbol, bar.WindowStart, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume, bar.VWAP, bar.SMA5, bar.SMA20, bar.Count)
			}
		}
		openWindows.Set(float64(w.Open()))
		watermarkLag.Set(float64(w.WatermarkLag().Milliseconds()))
	}

	log.Printf("shutting down; emitted %d bars total", emitted)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
