// FluxTape bar-sink: consumes bars_1s and upserts into the TimescaleDB hypertable.
// Idempotent on (symbol, window_start) so at-least-once delivery is safe.
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

const upsert = `INSERT INTO bars (symbol,window_start,open,high,low,close,volume,vwap,count,sma5,sma20)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (symbol,window_start) DO UPDATE SET
 open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
 volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, count=EXCLUDED.count, sma5=EXCLUDED.sma5, sma20=EXCLUDED.sma20`

var (
	written = prometheus.NewCounter(prometheus.CounterOpts{Name: "barsink_bars_written_total", Help: "Bars upserted"})
	errs    = prometheus.NewCounter(prometheus.CounterOpts{Name: "barsink_errors_total", Help: "Write errors"})
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
		kgo.ConsumerGroup("fluxtape-bar-sink"), kgo.ConsumeTopics("bars_1s"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer cl.Close()

	prometheus.MustRegister(written, errs)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() { _ = http.ListenAndServe(":9102", mux) }()
	log.Printf("bar-sink: bars_1s -> bars; metrics :9102")

	for {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		f.EachRecord(func(r *kgo.Record) {
			var b Bar
			if json.Unmarshal(r.Value, &b) != nil {
				return
			}
			t := time.UnixMilli(b.WindowStart).UTC()
			if _, e := pool.Exec(ctx, upsert, b.Symbol, t, b.Open, b.High, b.Low, b.Close, b.Volume, b.VWAP, b.Count, b.SMA5, b.SMA20); e != nil {
				errs.Inc()
				log.Printf("upsert error: %v", e)
				return
			}
			written.Inc()
		})
	}
	log.Println("bar-sink shutdown")
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
