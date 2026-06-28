# Rust ingestion (hot path). cmake/ssl/sasl for librdkafka against managed Kafka.
FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y cmake libssl-dev libsasl2-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY services/ingestion ./services/ingestion
WORKDIR /app/services/ingestion
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 libsasl2-2 && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/services/ingestion/target/release/ingestion /usr/local/bin/ingestion
ENV RUST_LOG=info
CMD ["ingestion"]
