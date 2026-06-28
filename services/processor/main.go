// FluxTape stream processor.
//
// Phase 2 / Step A: join a consumer group on the `trades` topic, deserialize
// each trade, and log throughput. Windowing (VWAP/OHLCV bars) and emitting to
// `bars_1s` are added in Step B.
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
	consumerGroup = "fluxtape-processor"
)

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:19092")

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		// Joining a consumer group gives us partition assignment + offset
		// tracking + rebalancing (lesson 14).
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topicTrades),
		// On first run (no committed offset), start from the beginning so we
		// can verify against existing data. Subsequent runs resume from commits.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer cl.Close()

	// Graceful shutdown on Ctrl+C / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("stream processor started; group=%q consuming %q from %s", consumerGroup, topicTrades, brokers)

	var consumed int
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break // context cancelled (shutdown)
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
			consumed++
			if consumed%500 == 0 {
				log.Printf("consumed=%d last: symbol=%s price=%.2f qty=%.8f part=%d offset=%d",
					consumed, t.Symbol, t.Price, t.Quantity, r.Partition, r.Offset)
			}
		})
	}

	log.Printf("shutting down; consumed %d trades total", consumed)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
