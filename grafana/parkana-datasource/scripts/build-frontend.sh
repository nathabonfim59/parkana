#!/bin/sh
# Build the Parkana SQL datasource frontend inside a pinned Node container.
#
# The build never depends on a host Node installation: everything runs in the
# image below, pinned by exact tag and manifest digest.
#
# Usage:
#   scripts/build-frontend.sh           # npm ci + production build into dist/
#   scripts/build-frontend.sh --check   # additionally run typecheck and unit tests
#
set -eu

# node:24.15.0-bookworm, linux manifest-list digest as of 2026-11-05.
NODE_IMAGE='node:24.15.0-bookworm@sha256:f22d6a1f082c02f292e86929b5b0442ac2e5eaf438a5dea9b1566601c3e05940'

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
plugin_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)

container_cmd='npm ci --ignore-scripts'
if [ "${1-}" = '--check' ]; then
  container_cmd="$container_cmd && npm run typecheck && npm run test:ci"
fi
container_cmd="$container_cmd && npm run build"

# Run as the invoking user so dist/ and node_modules/ are not root-owned on
# the host, and keep the npm cache out of the mounted plugin directory.
docker run --rm \
  --user "$(id -u):$(id -g)" \
  --env HOME=/tmp \
  --volume "$plugin_dir":/plugin \
  --workdir /plugin \
  "$NODE_IMAGE" \
  sh -c "$container_cmd"
