FROM rust:1.91-slim-bookworm as builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo "fn main() {}" > src/main.rs

RUN cargo build --release && rm -rf src

COPY src ./src

RUN touch src/main.rs && cargo build --release \
  && strip target/release/balances-watcher || true

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 -s /usr/sbin/nologin appuser

WORKDIR /app

COPY --from=builder /app/target/release/balances-watcher /app/balances-watcher

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8080

CMD ["./balances-watcher"]
