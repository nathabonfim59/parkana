# Parkana container image.
#
# Builder and runtime are the same Debian release (bookworm, glibc 2.36) so the
# binary sees the same glibc at build and run time. CGO stays enabled from
# phase 1: DuckDB is linked through cgo later (PLAN.md §5, §9), and a fully
# static build is explicitly not a goal (PLAN.md §11).
#
# Base images are pinned by tag AND manifest-list digest. The tag documents the
# release while the digest makes the build reproducible even if the tag moves:
#
#   golang:1.27.3-bookworm
#     https://hub.docker.com/layers/library/golang/1.27.3-bookworm/images/sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9
#   debian:bookworm-slim
#     https://hub.docker.com/layers/library/debian/bookworm-slim/images/sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3
#
# Build (from the repository root):
#
#   docker build -t parkana:phase1 .
#
# The optional pushgen image-target is built from the same source for the
# phase 2 compose stack; it is not part of the Parkana runtime image:
#
#   docker build --target pushgen -t parkana-pushgen:phase2 .
#
# Run:
#
#   docker run -v parkana-data:/data -p 3100:3100 parkana:phase1
#
# The compose stack in deploy/compose/ wires this image to the Garage
# object store.

# --- Build ------------------------------------------------------------------

# Version stamped into /loki/api/v1/status/buildinfo via
# internal/version. Pass e.g. --build-arg PARKANA_VERSION=v0.1.0; without it
# the binary reports "dev".
ARG PARKANA_VERSION=dev

FROM golang:1.27.3-bookworm@sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9 AS build

ARG PARKANA_VERSION=dev

WORKDIR /src

# Cache module downloads across source-only edits.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

# CGO_ENABLED=1 is the default with gcc present (the full bookworm golang
# image ships a toolchain); it is set explicitly so the DuckDB dependency
# (cgo) builds without touching this file again.
RUN CGO_ENABLED=1 \
    go build -trimpath \
        -ldflags "-X github.com/nathabonfim59/parkana/internal/version.Version=${PARKANA_VERSION}" \
        -o /out/parkana \
        ./cmd/parkana

# pushgen is a small, CGO-free client used by the phase 2 compose stack to
# feed a finite corpus through Alloy. Keeping it in the same build stage means
# one source context and one module download for both targets.
RUN CGO_ENABLED=0 go build -trimpath -o /out/pushgen ./cmd/pushgen

# --- pushgen (optional image target) ----------------------------------------

# The generator only makes plain HTTP requests to Alloy and writes nothing.
# It needs curl for the bounded wait on loki.source.api /ready; sqlite3 is
# used by validate-phase2.sh for a read-only catalog row-count check against
# the parkana-data volume. The same bookworm-slim digest as the Parkana
# runtime keeps the OS surface identical.
FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3 AS pushgen

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl sqlite3 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10002 --home-dir /nonexistent --shell /usr/sbin/nologin pushgen

COPY --from=build /out/pushgen /usr/local/bin/pushgen
COPY deploy/compose/pushgen-wait.sh /usr/local/bin/pushgen-wait
RUN chmod 0755 /usr/local/bin/pushgen-wait

USER pushgen
ENTRYPOINT ["/usr/local/bin/pushgen-wait"]

# --- Runtime ----------------------------------------------------------------

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3 AS runtime

# ca-certificates: TLS to S3 endpoints (Garage or S3 over TLS, AWS).
# curl: the /ready healthcheck. bookworm-slim ships neither by default.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user. /data is the persistent volume (WAL, local Parquet,
# SQLite catalog). Named volumes inherit this directory's ownership on first
# mount; a host bind mount must already be owned by uid 10001.
RUN useradd --uid 10001 --home-dir /data --shell /usr/sbin/nologin parkana \
    && mkdir -p /data \
    && chown parkana:parkana /data

COPY --from=build /out/parkana /usr/local/bin/parkana

USER parkana
WORKDIR /data

VOLUME ["/data"]
EXPOSE 3100

ENTRYPOINT ["/usr/local/bin/parkana"]

# /ready answers 503 until the HTTP server is serving and every role
# readiness check passes (internal/server), then 200. The start period leaves
# room for WAL replay once the ingester lands.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=12 \
    CMD ["curl", "--fail", "--silent", "--show-error", "http://127.0.0.1:3100/ready"]
