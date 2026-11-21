FROM rust:1.99.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev cmake && rm -rf /var/lib/apt/lists/*
ENV CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=4
WORKDIR /src
COPY . .
RUN GIGGLES_ICU_CACHE=/opt/icu bash scripts/icu/provision.sh > /tmp/icu-path &&     cp -a "$(cat /tmp/icu-path)" /opt/giggles-icu
RUN GIGGLES_GLIBM_CACHE=/opt/giggles-glibm bash scripts/glibm/build.sh
RUN cargo build --locked --release --manifest-path crates/skedaddle/Cargo.toml --bin skedaddle
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* &&     useradd --uid 10001 --create-home app
COPY --from=build /src/crates/skedaddle/target/release/skedaddle /usr/local/bin/skedaddle
COPY --from=build /opt/giggles-icu /opt/giggles-icu
COPY --from=build /opt/giggles-glibm /opt/giggles-glibm
ENV GIGGLES_ICU_DIR=/opt/giggles-icu GIGGLES_GLIBM_DIR=/opt/giggles-glibm
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/skedaddle"]
CMD ["--help"]
