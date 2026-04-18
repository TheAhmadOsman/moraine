# syntax=docker/dockerfile:1.7
#
# Moraine dev sandbox image — NOT published, NOT a distribution channel.
# See RFC #232 for background. Only used by scripts/dev/sandbox/moraine-sandbox.
#
# Two stages:
#   * builder  (FROM rust:1-bookworm) — optional, used by --build-in-container.
#                Produces release artifacts in /build/target/release/. The host
#                CLI can select this stage with `docker build --target builder`,
#                extract binaries, then build the runtime image.
#   * runtime  (FROM debian:bookworm-slim) — the default small image. A single
#                shared moraine-sandbox-runtime:latest image is reused across
#                every sandbox. Everything that varies per sandbox — binaries,
#                web assets, generated config, and entrypoint.sh — arrives via
#                bind mount from the active worktree (see compose.yaml). The
#                runtime stage therefore does NOT copy any of those artifacts
#                in. Select with `docker build --target runtime`.

# ---------------------------------------------------------------------------
# builder stage — optional, opt-in via --build-in-container
# ---------------------------------------------------------------------------
FROM rust:1-bookworm AS builder

WORKDIR /build

# Copy the repo as-is; the caller is responsible for .dockerignore hygiene.
COPY . /build

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --workspace --locked --release && \
    mkdir -p /out && \
    cp -a /build/target/release/. /out/

# Re-expose the artifacts at a stable path that does not collide with the
# cache mount above.
RUN mkdir -p /build/target/release && cp -a /out/. /build/target/release/

# ---------------------------------------------------------------------------
# runtime stage — the small debian-slim image the sandbox actually runs
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# sccache version — kept in sync with whatever the host has installed so disk
# cache entries are wire-compatible. Update both host (via `cargo install
# sccache`) and this value together.
ARG SCCACHE_VERSION=0.14.0
ARG TARGETARCH

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PATH=/opt/moraine/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    MORAINE_MONITOR_STATIC_DIR=/opt/moraine/web \
    RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/home/moraine/.cache/sccache \
    SCCACHE_CACHE_SIZE=20G

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tini \
        xz-utils \
        tzdata \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Install sccache. Binary targets linux-musl for static linkage so it runs
# regardless of glibc version. When the host (macOS) and the container (linux)
# both point RUSTC_WRAPPER=sccache at a shared disk cache, their entries are
# content-addressed by {compiler, flags, source} and so live side-by-side
# without collision — darwin and linux outputs share the 20G budget but never
# confuse each other.
RUN case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
        arm64)  sccache_triple=aarch64-unknown-linux-musl ;; \
        amd64)  sccache_triple=x86_64-unknown-linux-musl  ;; \
        *)      echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl -fsSL "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${sccache_triple}.tar.gz" \
        | tar -xz --strip-components=1 -C /usr/local/bin "sccache-v${SCCACHE_VERSION}-${sccache_triple}/sccache" \
    && chmod 0755 /usr/local/bin/sccache \
    && /usr/local/bin/sccache --version

# Non-root user matching the host bind-mount ownership contract.
RUN groupadd --system --gid 1000 moraine \
    && useradd --system --uid 1000 --gid 1000 \
        --home-dir /home/moraine --create-home --shell /bin/bash \
        moraine

# Runtime filesystem layout. Bind mounts will land on /opt/moraine/{bin,web}
# and /sandbox at container start; we create the mount points (and the state
# dir) ahead of time so ownership is right even before first run.
RUN mkdir -p \
        /opt/moraine/bin \
        /opt/moraine/web \
        /sandbox \
        /home/moraine/.moraine \
        /home/moraine/.cache/sccache \
    && chown -R moraine:moraine \
        /opt/moraine \
        /sandbox \
        /home/moraine

# entrypoint.sh is bind-mounted by compose at /usr/local/bin/entrypoint.sh from
# the active worktree, so it always tracks the current scripts/dev/sandbox/
# version. The placeholder here ensures the file exists (and is executable) if
# someone runs the image without compose's bind mount, and also forces a rebuild
# when the contract changes.
RUN install -m 0755 /dev/null /usr/local/bin/entrypoint.sh \
    && printf '#!/bin/sh\necho "entrypoint.sh bind-mount missing — start with moraine-sandbox, not bare docker run" >&2\nexit 1\n' \
        > /usr/local/bin/entrypoint.sh \
    && chmod 0755 /usr/local/bin/entrypoint.sh

USER moraine
WORKDIR /home/moraine

# tini reaps zombies and forwards signals to entrypoint.sh.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
CMD []
