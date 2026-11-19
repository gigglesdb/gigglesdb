FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev && rm -rf /var/lib/apt/lists/*
ARG FEATURES=""
WORKDIR /src
COPY . .
RUN cargo build --manifest-path crates/nunya/Cargo.toml --release --features "$FEATURES" --bin nunya
FROM debian:bookworm-slim
RUN useradd --uid 10001 --create-home nunya
COPY --from=build /src/crates/nunya/target/release/nunya /usr/local/bin/nunya
USER 10001:10001
EXPOSE 6432
ENTRYPOINT ["/usr/local/bin/nunya"]
CMD ["serve", "--config", "/etc/nunya/nunya.toml"]
