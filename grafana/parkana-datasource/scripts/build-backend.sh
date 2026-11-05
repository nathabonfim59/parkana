#!/usr/bin/env bash
# Build the Parkana Grafana backend plugin binary. Grafana loads the
# platform-suffixed file from dist/ (plugin.json names the base executable).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

GO="${GO:-go}"
GOOS="${GOOS:-linux}"
GOARCH="${GOARCH:-amd64}"
BINARY="gpx_parkana_sql_datasource"
OUTPUT="dist/${BINARY}_${GOOS}_${GOARCH}"

mkdir -p dist
CGO_ENABLED="${CGO_ENABLED:-0}" "$GO" build -trimpath -buildvcs=false -o "$OUTPUT" ./pkg
printf 'built %s\n' "$OUTPUT"
