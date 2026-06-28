package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	tradesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_trades_processed_total", Help: "Trades consumed and folded into windows",
	})
	barsEmitted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_bars_emitted_total", Help: "1s bars emitted to bars_1s",
	})
	lateEvents = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_late_events_total", Help: "Trades dropped for arriving past the watermark",
	})
	openWindows = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "processor_open_windows", Help: "Windows currently buffered",
	})
	watermarkLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "processor_watermark_lag_ms", Help: "Watermark lag behind wall clock (ms)",
	})
)

// startMetrics registers metrics and serves them on addr (/metrics).
func startMetrics(addr string) {
	prometheus.MustRegister(tradesProcessed, barsEmitted, lateEvents, openWindows, watermarkLag)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() { _ = http.ListenAndServe(addr, mux) }()
}
