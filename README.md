# FluxTape

Real-time crypto market-data platform: ingest live trades → stream processing
(windowed analytics, anomaly detection) → polyglot storage → low-latency API +
live dashboard, with CDC into a cold analytics store and full observability.

**Architecture & roadmap:** see [SPEC.md](SPEC.md) and
[Architecture Decision Records](docs/adr/).

## Tech stack

| Layer | Tech |
|-------|------|
| Ingestion (hot path) | Rust |
| Event backbone | Redpanda (Kafka API) |
| Stream processor / CDC / API | Go |
| Storage | TimescaleDB · Postgres · Redis · ClickHouse (Phase 5) |
| Dashboard | TypeScript / React |
| Edge / CDN | Cloudflare Pages + Workers |
| Observability | Prometheus · Grafana · OpenTelemetry |

## Prerequisites

- **Docker Desktop** (running)
- **Go** ≥ 1.23 — for stream processor, CDC connector, API gateway
- **Rust** (stable, via rustup) — for the ingestion service

See [Local setup](#local-setup) for install steps.

## Quick start (Phase 0 — local infrastructure)

```powershell
# 1. Create your local env file (gitignored)
Copy-Item .env.example .env

# 2. Boot the whole stack
docker compose up -d

# 3. Check everything is healthy
docker compose ps
```

### Service endpoints

| Service | URL / Address | Notes |
|---------|---------------|-------|
| Redpanda (Kafka API) | `localhost:19092` | connect apps here |
| Redpanda Console | http://localhost:8080 | inspect topics/messages |
| TimescaleDB (Postgres) | `localhost:5432` | user/pass/db: `fluxtape` |
| Redis | `localhost:6379` | |
| Prometheus | http://localhost:9090 | |
| Grafana | http://localhost:3000 | anonymous access on; admin/admin |

### Stop / reset

```powershell
docker compose down        # stop containers, keep data
docker compose down -v     # stop AND wipe data volumes (fresh start)
```

## Local setup

### Install Go (Windows)

Recommended (winget):

```powershell
winget install --id GoLang.Go -e
```

Then **open a new terminal** and verify:

```powershell
go version    # expect: go version go1.23.x windows/amd64
```

Alternative: download the MSI installer from https://go.dev/dl/ and run it.
The installer adds Go to `PATH` automatically.

### Install Rust (Windows)

Rust on Windows needs the **MSVC C++ build tools** plus `rustup`.

```powershell
# 1. C++ build tools (Rust links against the MSVC toolchain)
winget install --id Microsoft.VisualStudio.2022.BuildTools -e `
  --override "--quiet --wait --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"

# 2. rustup (manages Rust toolchains)
winget install --id Rustlang.Rustup -e
```

Then **open a new terminal** and verify:

```powershell
rustc --version   # expect: rustc 1.8x.x
cargo --version   # expect: cargo 1.8x.x
```

Alternative: download `rustup-init.exe` from https://rustup.rs and run it
(choose the default stable-msvc toolchain).

> If `rustc`/`cargo` aren't found after install, restart your terminal so the
> updated `PATH` (which includes `%USERPROFILE%\.cargo\bin`) takes effect.

## Project layout

```
SPEC.md                  Architecture, components, phased roadmap
docker-compose.yml       Local infrastructure stack (Phase 0)
.env.example             Template for local secrets/config
infra/                   Prometheus + Grafana provisioning
docs/adr/                Architecture Decision Records
docs/lessons/            Personal concept notes (gitignored)
```
