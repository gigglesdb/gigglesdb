FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev cmake && rm -rf /var/lib/apt/lists/*
ENV CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=4
WORKDIR /src
COPY . .
RUN GIGGLES_ICU_CACHE=/opt/icu bash scripts/icu/provision.sh > /tmp/icu-path &&     cp -a "$(cat /tmp/icu-path)" /opt/giggles-icu
RUN GIGGLES_GLIBM_CACHE=/opt/giggles-glibm bash scripts/glibm/build.sh
RUN cargo build --locked --release --manifest-path crates/giggles-server/Cargo.toml --bin giggles-server --bin giggles-client
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* &&     useradd --uid 10001 --create-home app
COPY --from=build /src/crates/giggles-server/target/release/giggles-server /usr/local/bin/giggles-server
COPY --from=build /src/crates/giggles-server/target/release/giggles-client /usr/local/bin/giggles-client
COPY --from=build /opt/giggles-icu /opt/giggles-icu
COPY --from=build /opt/giggles-glibm /opt/giggles-glibm
ENV GIGGLES_ICU_DIR=/opt/giggles-icu GIGGLES_GLIBM_DIR=/opt/giggles-glibm
RUN mkdir -p /var/lib/giggles && chown 10001:10001 /var/lib/giggles
VOLUME ["/var/lib/giggles"]
EXPOSE 5432
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/giggles-server"]
CMD ["--no-stdin-control", "/etc/giggles/CONFIG.json"]
