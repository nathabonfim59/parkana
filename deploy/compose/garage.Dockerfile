# Garage helper image for Parkana's phase 1 compose stack.
#
# Both compose services that involve Garage run from this image: the garage
# server (as a non-root user, with pre-owned metadata/data directories so the
# named volumes inherit writable ownership) and the one-shot garage-init job
# (which needs a POSIX shell, which the upstream image does not carry).
#
# The upstream dxflrs/garage image is a single-layer scratch-style image with
# only the /garage binary; the binary is fully static, so copying it onto
# Debian bookworm-slim is enough. GARAGE_IMAGE selects the upstream base
# (default: the pinned stable release; override to point at a mirror or a
# different verified tag).
#
# Build context is deploy/compose/ itself:
#
#   docker build -f garage.Dockerfile \
#     --build-arg GARAGE_IMAGE=docker.io/dxflrs/garage:v2.5.0 \
#     -t parkana-garage:v2.5.0 .
#
# The Debian runtime base is pinned by tag and manifest-list digest; the
# digest is the same bookworm-slim snapshot used by the root Dockerfile.

ARG GARAGE_IMAGE=docker.io/dxflrs/garage:v2.5.0@sha256:677e5b758e4425a325b811c425c026bf35a4bf42f7ddbaa773de3f4fd779a5a5

FROM ${GARAGE_IMAGE} AS bin

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3

COPY --from=bin /garage /usr/local/bin/garage

RUN useradd --uid 10001 --home-dir /var/lib/garage --shell /usr/sbin/nologin garage \
    && mkdir -p /var/lib/garage/meta /var/lib/garage/data \
    && chown -R garage:garage /var/lib/garage

COPY garage-init.sh /usr/local/bin/garage-init.sh
RUN chmod 0755 /usr/local/bin/garage-init.sh

USER garage
