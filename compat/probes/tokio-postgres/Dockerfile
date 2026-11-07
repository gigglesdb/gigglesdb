FROM rust:1.90.0-slim-bookworm AS build
WORKDIR /probe
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN grep -A2 '^name = "tokio-postgres"$' Cargo.lock | grep -q '^version = "0.7.15"$' && cargo build --release --locked
FROM debian:bookworm-slim
COPY --from=build /probe/target/release/r0-tokio-probe /usr/local/bin/probe
ENTRYPOINT ["/usr/local/bin/probe"]
