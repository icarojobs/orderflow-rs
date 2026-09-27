# syntax=docker/dockerfile:1.7

# ---- toolchain: used for builds and for `docker compose run dev ...` ----
FROM rust:1.98-slim AS toolchain
RUN rustup component add rustfmt clippy
WORKDIR /src

# ---- build every binary in the workspace ----
FROM toolchain AS builder
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bins \
    && mkdir -p /out \
    && find target/release -maxdepth 1 -type f -executable -exec cp {} /out/ \;

# ---- runtime images: distroless with glibc, non-root ----
FROM gcr.io/distroless/cc-debian13:nonroot AS gateway
COPY --from=builder /out/gateway /usr/local/bin/gateway
EXPOSE 50051 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=5 CMD ["gateway", "healthcheck"]
ENTRYPOINT ["gateway"]

FROM gcr.io/distroless/cc-debian13:nonroot AS market-data
COPY --from=builder /out/market-data /usr/local/bin/market-data
EXPOSE 8081
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=5 CMD ["market-data", "healthcheck"]
ENTRYPOINT ["market-data"]
