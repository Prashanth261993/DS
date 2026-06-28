# FluxTape — Real-Time Market Data Platform

> A real-time crypto market-data platform that ingests live trades, computes
> streaming analytics, serves them with low latency, and replicates history via
> Change Data Capture (CDC) into an analytics/search store.
>
> Demonstrates distributed-systems patterns: streaming ETL, pub/sub, polyglot
> persistence, CQRS, CDC, edge/CDN delivery, and end-to-end observability.

---

## 1. Goals & non-goals

### Goals
- Demonstrate **production-grade distributed systems patterns**, not a CRUD app.
- Run **entirely on free tiers** AND **locally via docker-compose**.
- Produce a **compelling live demo** (ticking dashboard, anomaly alerts).
- Correct streaming semantics under out-of-order / late events.

### Non-goals
- Not a trading bot. We do not place orders or give financial advice.
- Not aiming for real HFT latency (microseconds); we aim for *correct* streaming
  semantics at sane latency (low ms) on free infrastructure.

---

## 2. The core mental model

Every design decision answers two questions:

1. **OLTP vs OLAP** — tiny fast point-reads ("latest BTC price") vs huge scans
   ("avg hourly volume over 30 days"). Opposite physics → different databases.
2. **Hot vs Cold** — recent data queried constantly vs historical data queried
   rarely → different storage and cost.

---

## 3. Architecture

```
Exchange WebSockets (Binance / Coinbase — free)
        │
   Ingestion Service  [RUST] ── reconnect, parse, backpressure
        │
   Redpanda / Kafka ── pub/sub log, partitioned by symbol
        │
   Stream Processor   [GO] ── tumbling windows, VWAP/MA, anomaly detection
        ├──► TimescaleDB  ── HOT time-series (OLTP-ish)
        ├──► Postgres     ── trade history = SOURCE OF TRUTH (CDC reads this)
        └──► Redis        ── cache + pub/sub fanout to WebSocket clients
                                  │
   CDC Connector [GO] ◄── tails Postgres WAL ──► ClickHouse / OpenSearch (COLD)
        │
   API Gateway  [GO] ── REST + gRPC + WebSocket, cache-aside on Redis
        │
   Cloudflare Worker (edge cache / CDN) ──► React Dashboard (Cloudflare Pages)

   Observability: Prometheus + Grafana + OpenTelemetry (across all services)
```

---

## 4. Components & language choices

| # | Component | Language | Why | Key concept |
|---|-----------|----------|-----|-------------|
| 1 | Ingestion (hot path) | **Rust** | No GC, predictable tail latency, HFT signal | Backpressure |
| 2 | Event backbone | **Redpanda** | Kafka API, single binary, no JVM | The log / partitioning |
| 3 | Stream processor | **Go** | Focus on streaming logic, fast iteration | Event-time windows + watermarks |
| 4 | Storage | TimescaleDB + Postgres + Redis | Right DB per access pattern | Polyglot persistence |
| 5 | CDC connector | **Go** | Great PG logical-replication libs | Change Data Capture |
| 6 | API gateway | **Go** | Best HTTP/gRPC/WS ecosystem | CQRS, API protocols |
| 7 | Edge / CDN | Cloudflare Workers + Pages | Free global CDN | Edge caching |
| 8 | Dashboard | TypeScript / React | Standard | — |
| 9 | Observability | Prometheus + Grafana + OTel | Industry standard | Consumer lag |

---

## 5. Roadmap (phased — build a vertical slice first)

> **Scope note:** v1 deliberately **DEFERS** the CDC component (#5) and the
> ClickHouse/OpenSearch sink. v1 delivers the core streaming spine end-to-end
> first; CDC is layered in Phase 5. This is a conscious scoping decision to ship
> a working vertical slice early — see ADR-0001.

| Phase | Deliverable | Status |
|-------|-------------|--------|
| 0 | Repo scaffolding, SPEC, ADRs, docker-compose skeleton | ✅ done |
| 1 | Ingestion (Rust) → Redpanda, partitioned by symbol, reconnect + backpressure | ✅ done |
| 2 | Stream processor (Go): tumbling-window VWAP/MA + out-of-order handling | ✅ done |
| 3 | Dual sink: TimescaleDB (hot) + Postgres (history); idempotent writes | ✅ done |
| 4 | API gateway (Go): REST + WebSocket + Redis cache/fanout; React dashboard | ✅ done |
| 4b | Deploy live on free tiers + Cloudflare Pages/Workers (CDN) | ⬜ |
| 5 | **CDC connector (Go)** → ClickHouse/OpenSearch (DEFERRED from v1) | ⬜ |
| 6 | Observability: Prometheus/Grafana dashboards, OTel traces, SLOs | ⬜ |
| 7 | Load test + chaos test (kill nodes), documented benchmarks | ⬜ |

Each phase = one clean PR with a writeup, so the commit history tells a
Staff-level story.

### Backlog / future enhancements (post-v1)
- **Serialization migration:** v1 publishes **JSON** to the `trades` topic for
  readability and zero setup. Migrate to **Avro or Protobuf + a Schema Registry**
  for compactness and enforced schema evolution. Worth an ADR when done.
- **Multi-exchange ingestion:** v1 ingests **Coinbase** (US-accessible). Add
  Binance/others behind a common normalized `Trade` schema.
- **Decimal money type:** replace `f64` price/quantity with a fixed-point/decimal
  type to avoid float rounding on monetary values.
- **Partition skew / hot-key handling:** v1 keys the `trades` topic by symbol
  (preserves per-symbol ordering). A dominant symbol (e.g. BTC) can create a hot
  partition under load. During load/chaos testing (Phase 7), monitor per-partition
  consumer lag; if skewed, mitigate via (a) more partitions, (b) an explicit
  balanced partitioner, or (c) sub-keying the hot symbol (`SYMBOL#n`). Sub-keying
  is safe here because the core aggregations (VWAP, MA, volume) are commutative.

---

## 6. Free-tier hosting targets (to validate later)

- **Pub/sub:** Upstash Kafka / local Redpanda
- **Redis:** Upstash Redis free tier / local
- **Postgres + Timescale:** Neon / Supabase / local
- **CDN + static + edge:** Cloudflare Pages + Workers
- **Dashboards:** Grafana Cloud free tier
- **Compute for services:** Fly.io / Railway free allowances / local docker-compose

---

## 7. Open questions (to revisit)
- Final hosting provider per service (validate free-tier limits in Phase 4b).
- Anomaly detection algorithm (start simple: z-score over rolling window).
- Whether stream processor stays Go or moves hot parts to Rust under load.
