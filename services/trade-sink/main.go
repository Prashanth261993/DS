// FluxTape trade-sink: consumes trades and upserts into Postgres (source of truth).
// Idempotent on trade_id so at-least-once delivery never duplicates.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Trade struct {
	Symbol      string  `json:"symbol"`
	Price       float64 `json:"price"`
	Quantity    float64 `json:"quantity"`
	TradeID     int64   `json:"trade_id"`
	EventTimeMs int64   `json:"event_time_ms"`
	Side        string  `json:"side"`
}

const upsert = `INSERT INTO trades (trade_id,symbol,price,quantity,side,event_time)
VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (trade_id) DO NOTHING`

var (
	written = prometheus.NewCounter(prometheus.CounterOpts{Name: "tradesink_trades_written_total", Help: "Trades upserted"})
	errs    = prometheus.NewCounter(prometheus.CounterOpts{Name: "tradesink_errors_total", Help: "Write errors"})
)

func main() {
	brokers := env("KAFKA_BROKERS", "localhost:19092")
	dsn := env("DATABASE_URL", "postgres://fluxtape:fluxtape@localhost:5432/fluxtape")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup("fluxtape-trade-sink"), kgo.ConsumeTopics("trades"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer cl.Close()

	prometheus.MustRegister(written, errs)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() { _ = http.ListenAndServe(":9103", mux) }()
	log.Printf("trade-sink: trades -> trades; metrics :9103")

	for {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		f.EachRecord(func(r *kgo.Record) {
			var t Trade
			if json.Unmarshal(r.Value, &t) != nil {
				return
			}
			et := time.UnixMilli(t.EventTimeMs).UTC()
			if _, e := pool.Exec(ctx, upsert, t.TradeID, t.Symbol, t.Price, t.Quantity, t.Side, et); e != nil {
				errs.Inc()
				log.Printf("upsert error: %v", e)
				return
			}
			written.Inc()
		})
	}
	log.Println("trade-sink shutdown")
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
