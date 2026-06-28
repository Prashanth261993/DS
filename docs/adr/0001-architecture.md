# ADR-0001: Overall architecture & language strategy

- **Status:** Accepted
- **Date:** 2026-06-27

## Context

FluxTape is a real-time market-data platform that ingests live trades, computes
streaming analytics, and serves them at low latency. It must run both locally
(docker-compose) and on free cloud tiers, and must demonstrate correct streaming
semantics (out-of-order/late events) rather than a simple CRUD design.

## Decision

1. **Adopt a streaming, log-centric architecture** with Redpanda (Kafka API) as
   the event backbone. Everything is derived from the immutable trade log.
2. **Polyglot persistence:** TimescaleDB (hot time-series), Postgres (durable
   source of truth), Redis (cache + fanout), and later ClickHouse/OpenSearch
   (cold analytics/search via CDC).
3. **Per-component language choice:**
   - **Rust** for the ingestion hot path (no GC, predictable tail latency).
   - **Go** for the stream processor, CDC connector, and API gateway (great
     concurrency + ecosystem, fast iteration).
   - **TypeScript/React** for the dashboard.
4. **CQRS:** the write path (ingestion → processing → storage) is fully separate
   from the read path (API gateway).
5. **CDC over dual-writes:** downstream stores are kept in sync by tailing the
   Postgres WAL, not by writing to each store directly.

## Considered alternatives

- **Single database for everything** (e.g. just Postgres): simpler, but cannot
  serve OLTP point-reads and OLAP scans well simultaneously. Rejected — the
  whole point is to demonstrate the right-tool-per-workload pattern.
- **All-Rust or all-Go:** all-Rust slows learning/iteration on streaming logic;
  all-Go gives up the "no-GC hot path" signal valued in finance. Per-component
  mix captures the strengths of both and mirrors how real teams decide.
- **Managed stream processor (Flink):** powerful but heavy to run free/locally
  and adds operational weight disproportionate to v1. We implement windowing
  directly in Go for full control and a lighter footprint.
- **Dual-writes to each store:** simpler initially but suffers the dual-write
  consistency problem on partial failure. Rejected in favor of CDC.

## Scope decision: defer CDC in v1

v1 deliberately defers the CDC connector and cold-analytics sink. We first build
a working vertical slice (ingestion → log → processor → hot storage → API →
dashboard), then layer CDC in Phase 5. Rationale: shipping a working end-to-end
slice early de-risks the project and produces a demo sooner. Building everything
at once is the most common way side projects stall.

## Consequences

- **Positive:** clean separation of concerns, realistic distributed patterns,
  strong interview narrative, incremental delivery.
- **Negative / cost:** more moving parts to operate; we must invest early in
  docker-compose + observability so the system is debuggable.
- **Follow-ups:** validate free-tier limits per provider (Phase 4b); revisit
  whether the stream processor needs Rust under load (Phase 7 benchmarks).
