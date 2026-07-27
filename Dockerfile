# Multi-stage build: compile in a full Rust image, run on a slim base.
FROM rust:1.96-slim-trixie AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p hyperion

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/hyperion /usr/local/bin/hyperion
ENTRYPOINT ["hyperion"]
