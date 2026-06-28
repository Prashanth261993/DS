// FluxTape API gateway (Phase 4 / Step A): REST read path over stored bars,
// with Redis cache-aside. WebSocket fanout is added in Step B.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
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

var (
	cacheHits   = prometheus.NewCounter(prometheus.CounterOpts{Name: "api_cache_hits_total", Help: "Bars cache hits"})
	cacheMiss   = prometheus.NewCounter(prometheus.CounterOpts{Name: "api_cache_misses_total", Help: "Bars cache misses"})
	cacheTTL    = 3 * time.Second
	pool        *pgxpool.Pool
	rdb         *redis.Client
)

func main() {
	dsn := env("DATABASE_URL", "postgres://fluxtape:fluxtape@localhost:5432/fluxtape")
	redisURL := env("REDIS_URL", "redis://localhost:6379")

	ctx := context.Background()
	var err error
	if pool, err = pgxpool.New(ctx, dsn); err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()
	opt, _ := redis.ParseURL(redisURL)
	rdb = redis.NewClient(opt)

	prometheus.MustRegister(cacheHits, cacheMiss)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/symbols", handleSymbols)
	mux.HandleFunc("/bars", handleBars)
	mux.Handle("/metrics", promhttp.Handler())

	log.Printf("api gateway on :8090 (REST + cache-aside)")
	log.Fatal(http.ListenAndServe(":8090", cors(mux)))
}

func handleSymbols(w http.ResponseWriter, r *http.Request) {
	rows, err := pool.Query(r.Context(), `SELECT DISTINCT symbol FROM bars ORDER BY symbol`)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		rows.Scan(&s)
		out = append(out, s)
	}
	writeJSON(w, out)
}

func handleBars(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		http.Error(w, "symbol required", 400)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	key := fmt.Sprintf("bars:%s:%d", symbol, limit)

	// Cache-aside: try Redis first.
	if cached, err := rdb.Get(r.Context(), key).Result(); err == nil {
		cacheHits.Inc()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write([]byte(cached))
		return
	}
	cacheMiss.Inc()

	rows, err := pool.Query(r.Context(),
		`SELECT symbol, extract(epoch from window_start)*1000, open,high,low,close,volume,vwap,count,sma5,sma20
		 FROM bars WHERE symbol=$1 ORDER BY window_start DESC LIMIT $2`, symbol, limit)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()
	bars := []Bar{}
	for rows.Next() {
		var b Bar
		rows.Scan(&b.Symbol, &b.WindowStart, &b.Open, &b.High, &b.Low, &b.Close, &b.Volume, &b.VWAP, &b.Count, &b.SMA5, &b.SMA20)
		bars = append(bars, b)
	}
	payload, _ := json.Marshal(bars)
	rdb.Set(r.Context(), key, payload, cacheTTL)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Write(payload)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			return
		}
		h.ServeHTTP(w, r)
	})
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
