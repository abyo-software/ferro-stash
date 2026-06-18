# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# Multi-stage build for the `ferro-stash` binary (Logstash-compatible
# data pipeline written in Rust).
#
# Build notes
# -----------
# * The `ferro-stash-ruby` crate depends on a LOCAL fork of Artichoke via a
#   relative path dependency:
#     crates/ferro-stash-ruby/Cargo.toml ->
#       ../../../../artichoke-extended/artichoke-backend
#   That is FOUR `..` hops from the crate dir
#   (.../ferro-stash-ruby -> crates -> <repo> -> <parent> -> <grandparent>),
#   so the fork resolves to the repo's GRANDPARENT dir, NOT the repo's parent.
#   This is why CI clones it to `$GITHUB_WORKSPACE/../../artichoke-extended`
#   (GITHUB_WORKSPACE = _work/ferro-stash/ferro-stash). To reproduce that here
#   we nest the repo one extra level: repo at /build/ferro-stash/ferro-stash and
#   the fork at /build/artichoke-extended. We clone the `extended` branch (this
#   mirrors .github/workflows/ci.yml). Verified: from
#   /build/ferro-stash/ferro-stash/crates/ferro-stash-ruby, four `..` =
#   /build, so .../artichoke-extended/artichoke-backend = /build/artichoke-...
# * mruby FFI in ferro-stash-ruby needs a C compiler (clang) + cmake; the
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
#   git         -> clone the Artichoke fork sibling
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

# Clone the Artichoke fork so the path dep resolves. Path math (verified):
# from /build/ferro-stash/ferro-stash/crates/ferro-stash-ruby, the four `..`
# hops in `../../../../artichoke-extended` land on /build, so the fork must
# live at /build/artichoke-extended.
RUN git clone --depth 1 --branch extended \
        https://github.com/masumi-ryugo/artichoke-extended.git \
        /build/artichoke-extended

# Copy the repo into /build/ferro-stash/ferro-stash (one extra nesting level so
# the four-`..` path dep above reaches /build — this mirrors CI's
# _work/ferro-stash/ferro-stash layout).
COPY . /build/ferro-stash/ferro-stash
WORKDIR /build/ferro-stash/ferro-stash

# Build only the release CLI binary. `--locked` would require Cargo.lock to be
# in sync with the (unpinned) path dep, which it is not by design, so we omit it.
RUN cargo build --release --bin ferro-stash \
    && strip /build/ferro-stash/ferro-stash/target/release/ferro-stash || true

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

COPY --from=builder /build/ferro-stash/ferro-stash/target/release/ferro-stash \
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
