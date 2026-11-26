#!/bin/sh
# Package the Parkana SQL datasource as a Grafana plugin zip.
#
# Usage:
#   scripts/dist.sh VERSION OUT_DIR
#
# Builds the frontend (scripts/build-frontend.sh, a pinned Node container,
# unless PLUGIN_FRONTEND=host), the backend for every platform in
# PLUGIN_PLATFORMS, fills plugin.json's %VERSION% and %TODAY%, and writes
# OUT_DIR/parkana-sql-datasource-VERSION.zip. The zip's top-level directory is
# the plugin id, the layout `grafana cli --pluginUrl` and the plugins
# directory expect. The zip's line in OUT_DIR/SHA256SUMS is added or replaced.
#
# The entries are sorted and carry the commit time (SOURCE_DATE_EPOCH, by
# default the time of the last commit), so two builds of one commit list the
# same files with the same times.
#
# Environment:
#   PLUGIN_PLATFORMS        GOOS/GOARCH list (default: the five release platforms)
#   PLUGIN_FRONTEND         container (default) or host (npm on the PATH)
#   PLUGIN_FRONTEND_FLAGS   passed to the frontend build, e.g. --check
#   CONTAINER               container CLI for the frontend build (default docker)
#   SOURCE_DATE_EPOCH       timestamp for plugin.json's updated date and the zip entries
set -eu

if [ $# -ne 2 ] || [ -z "$1" ]; then
  echo "usage: $0 VERSION OUT_DIR" >&2
  exit 2
fi
version=${1#v}
out_dir=$2

plugin_id=parkana-sql-datasource
binary=gpx_parkana_sql_datasource
platforms=${PLUGIN_PLATFORMS:-linux/amd64 linux/arm64 darwin/arm64 darwin/amd64 windows/amd64}

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
plugin_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
mkdir -p "$out_dir"
out_dir=$(CDPATH= cd -- "$out_dir" && pwd)

if [ -z "${SOURCE_DATE_EPOCH-}" ]; then
  SOURCE_DATE_EPOCH=$(git -C "$plugin_dir" log -1 --format=%ct 2>/dev/null || date +%s)
fi
today=$(date -u -d "@$SOURCE_DATE_EPOCH" +%Y-%m-%d)

# Frontend: webpack empties dist/ (it keeps backend binaries, which are
# removed below so the zip holds exactly the platforms asked for).
case ${PLUGIN_FRONTEND:-container} in
container)
  # shellcheck disable=SC2086 # PLUGIN_FRONTEND_FLAGS is a word list
  "$script_dir/build-frontend.sh" ${PLUGIN_FRONTEND_FLAGS-}
  ;;
host)
  (
    cd "$plugin_dir"
    npm ci --ignore-scripts
    if [ "${PLUGIN_FRONTEND_FLAGS-}" = --check ]; then
      npm run typecheck
      npm run test:ci
      npm run lint
    fi
    npm run build
  )
  ;;
*)
  echo "$0: PLUGIN_FRONTEND must be container or host" >&2
  exit 2
  ;;
esac

rm -f "$plugin_dir/dist/${binary}"_*
for p in $platforms; do
  GOOS=${p%/*} GOARCH=${p#*/} CGO_ENABLED=0 "$script_dir/build-backend.sh"
done

# Webpack fills plugin.json from package.json and the build date; the release
# version and the commit date win.
sed -e "s/%VERSION%/$version/g" -e "s/%TODAY%/$today/g" \
  "$plugin_dir/src/plugin.json" >"$plugin_dir/dist/plugin.json"

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
cp -R "$plugin_dir/dist" "$stage/$plugin_id"
find "$stage/$plugin_id" -type d -exec chmod 0755 {} +
find "$stage/$plugin_id" -type f -exec chmod 0644 {} +
chmod 0755 "$stage/$plugin_id/${binary}"_*
find "$stage/$plugin_id" -exec touch -h -d "@$SOURCE_DATE_EPOCH" {} +

zip_name=$plugin_id-$version.zip
rm -f "$out_dir/$zip_name"
(
  cd "$stage"
  find "$plugin_id" -print | LC_ALL=C sort | TZ=UTC zip -X -q -@ "$out_dir/$zip_name"
)

sums=$out_dir/SHA256SUMS
line=$(cd "$out_dir" && sha256sum "$zip_name")
if [ -f "$sums" ]; then
  awk -v f="$zip_name" '$2 != f && $2 != "*" f' "$sums" >"$sums.tmp"
else
  : >"$sums.tmp"
fi
printf '%s\n' "$line" >>"$sums.tmp"
mv "$sums.tmp" "$sums"
printf 'wrote %s\n' "$out_dir/$zip_name"
