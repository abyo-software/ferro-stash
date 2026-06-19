# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# Multi-stage build for the `ferro-stash` binary (Logstash-compatible
# data pipeline written in Rust).
#
# Build notes
# -----------
# * The image is built WITH the optional `ruby` filter (`--features ruby`) for
#   full Logstash drop-in compatibility. Its Artichoke (mruby) dependency is a
#   rev-pinned git dependency (abyo-software/artichoke-extended), fetched by
#   cargo — no sibling checkout or submodule needed.
# * mruby FFI (the ruby feature) needs a C compiler (clang) + cmake; the
#   `rdkafka` dependency vendors librdkafka and also needs cmake. pkg-config
#   and perl are required by some -sys crates.
# * The binary uses rustls for TLS, so no system OpenSSL is needed at
#   runtime; only `ca-certificates` for outbound TLS (S3/Datadog/ES).
#
# Build:
#   docker build -t ferro-stash:latest .
# Run:
#   docker run --rm -it \
#     -v "$PWD/config/example.conf:/etc/ferro-stash/pipeline.conf:ro" \
#     -p 9600:9600 ferro-stash:latest

# ---------------------------------------------------------------------------
# Stage 1 — builder
# ---------------------------------------------------------------------------
FROM rust:1.95-bookworm AS builder

# Build toolchain:
#   clang/llvm  -> mruby FFI (ferro-stash-ruby)
#   cmake       -> vendored librdkafka (rdkafka), other -sys crates
#   pkg-config  -> -sys crate discovery
#   perl/make   -> some build scripts
#   git         -> cargo fetches the Artichoke git dependency
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        clang \
        llvm \
        libclang-dev \
        cmake \
        pkg-config \
        make \
        perl \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . /build

# Build the release CLI with the `ruby` filter. Artichoke is fetched from its
# rev-pinned git dependency (recorded in Cargo.lock); no sibling checkout or
# nesting is needed.
RUN cargo build --release --bin ferro-stash --features ruby \
    && strip /build/target/release/ferro-stash || true

# ---------------------------------------------------------------------------
# Stage 2 — runtime
# ---------------------------------------------------------------------------
# debian:bookworm-slim keeps a shell + package manager for ops debugging while
# staying small. ca-certificates is required for outbound rustls TLS.
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    # Non-root runtime user.
    && groupadd --system --gid 65532 ferrostash \
    && useradd --system --uid 65532 --gid ferrostash \
        --home-dir /var/lib/ferro-stash --shell /usr/sbin/nologin ferrostash \
    && mkdir -p /etc/ferro-stash /var/lib/ferro-stash \
    && chown -R ferrostash:ferrostash /var/lib/ferro-stash

COPY --from=builder /build/target/release/ferro-stash \
     /usr/local/bin/ferro-stash

# Pipeline config is mounted here; the optional GeoIP database is referenced
# from the pipeline config (geoip { database => "/etc/ferro-stash/GeoLite2-City.mmdb" }).
#   docker run -v ./pipeline.conf:/etc/ferro-stash/pipeline.conf:ro ...
#   docker run -v ./GeoLite2-City.mmdb:/etc/ferro-stash/GeoLite2-City.mmdb:ro ...
VOLUME ["/var/lib/ferro-stash"]

# Monitoring / metrics API. Inside a container the default 127.0.0.1 bind would
# not be reachable, so we bind 0.0.0.0 via the ENTRYPOINT default args below.
EXPOSE 9600

USER 65532:65532
ENV RUST_LOG=info

# tini reaps zombies and forwards signals so SIGTERM triggers graceful shutdown.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/ferro-stash"]
# Default args: read the mounted pipeline, persist runtime state under the
# writable volume, expose the API on all interfaces. Override at `docker run`.
CMD ["-f", "/etc/ferro-stash/pipeline.conf", \
     "--path.data", "/var/lib/ferro-stash", \
     "--api.http.host", "0.0.0.0:9600"]
