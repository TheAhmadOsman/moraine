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
#   * runtime  (FROM debian:bookworm-slim) — the default small image. Binaries
#                arrive at runtime via bind mount at /opt/moraine/bin, so the
#                runtime stage deliberately does NOT copy artifacts from the
#                builder stage. Select with `docker build --target runtime`.

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

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PATH=/opt/moraine/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tini \
        xz-utils \
        tzdata \
        procps \
    && rm -rf /var/lib/apt/lists/*

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
    && chown -R moraine:moraine \
        /opt/moraine \
        /sandbox \
        /home/moraine

COPY --chmod=0755 entrypoint.sh /usr/local/bin/entrypoint.sh

USER moraine
WORKDIR /home/moraine

# tini reaps zombies and forwards signals to entrypoint.sh.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
CMD []
