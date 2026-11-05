#!/bin/sh
# One-shot Garage initialization for Parkana's phase 1 compose stack.
#
# Runs in its own container (the helper image serves both the garage server
# and this job) against the garage service, using the same /etc/garage.toml
# (mounted read-only). `garage` without -h reads the local node public key
# from metadata_dir/node_key.pub and resolves rpc_public_addr from the config
# to find the daemon (garage main.rs), so this container mounts the shared
# metadata volume read-only; the retry loop below covers the short window
# before the server has written the key.
#
# Steps: assign the node to a single-zone layout, apply it, import the fixed
# development S3 key, create the parkana bucket, grant the key read/write/
# owner and prove the grant. Guards key on the key ID (not the name) and on
# the ACTIVE layout (not staged changes), so a re-run on existing volumes is
# a no-op and a crash between `layout assign` and `layout apply` is retried
# correctly.
#
# DEVELOPMENT ONLY: the imported key is a fixed throwaway credential for the
# local compose network. Its values arrive via GARAGE_KEY_ID/GARAGE_KEY_SECRET
# so they are not duplicated here, and diagnostics never echo them.

set -eu

CONFIG="${GARAGE_CONFIG:-/etc/garage.toml}"
BUCKET="${GARAGE_BUCKET:-parkana}"
KEY_NAME="${GARAGE_KEY_NAME:-parkana-dev}"
: "${GARAGE_KEY_ID:?GARAGE_KEY_ID must be set}"
: "${GARAGE_KEY_SECRET:?GARAGE_KEY_SECRET must be set}"
KEY_ID="$GARAGE_KEY_ID"

garage() { /usr/local/bin/garage -c "$CONFIG" "$@"; }

# Wait for the daemon's RPC. `garage status` answers before a layout is
# applied, so this works on a fresh cluster (compose orders this job after
# garage `service_started`, not `service_healthy`: health is only green once
# a layout is applied, which is this job's job).
i=0
until garage status >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 30 ]; then
        echo "garage-init: cluster not reachable after 30s" >&2
        exit 1
    fi
    sleep 1
done

# Node ID as printed by `garage status`: the first field of the first row
# under HEALTHY NODES, a 16-hex-char prefix of the node's public key. Layout
# CLI accepts prefixes; `layout show` prints the same 16-char form.
NODE_ID=$(garage status | awk '/HEALTHY NODES/{h=1;next} h && NF>=3 && $1!="ID" {print $1; exit}')
if [ -z "$NODE_ID" ]; then
    echo "garage-init: could not read the node ID from 'garage status'" >&2
    exit 1
fi

# Assign+apply only when the node has no ACTIVE role. Matching happens in the
# section between the CURRENT header and the version line, so a staged-but-
# unapplied assignment from an interrupted earlier run still gets applied.
if garage layout show | awk -v id="$NODE_ID" '
    /^==== CURRENT CLUSTER LAYOUT/ {cur=1; next}
    /^Current cluster layout version:/ {cur=0}
    cur && $1 == id {found=1}
    END {exit !found}'; then
    echo "garage-init: node $NODE_ID already has an active layout role"
else
    garage layout assign -z garage -c 1GB "$NODE_ID"
    # `layout show` prints the exact version that `layout apply` expects; use
    # it instead of assuming current+1, so a retried bootstrap with a staged
    # change from an interrupted run applies the right version.
    SHOW=$(garage layout show)
    APPLY_VERSION=$(printf '%s\n' "$SHOW" |
        sed -n 's/^[[:space:]]*garage layout apply --version \([0-9][0-9]*\)[[:space:]]*$/\1/p' |
        head -n 1)
    case "$APPLY_VERSION" in
        "" | *[!0-9]*)
            echo "garage-init: cannot determine the layout version to apply" >&2
            printf '%s\n' "$SHOW" >&2
            exit 1
            ;;
    esac
    garage layout apply --version "$APPLY_VERSION"
fi

# Key: existence is checked by ID, so a same-named key with a different ID is
# caught instead of silently reused. The import output can echo the secret, so
# capture it and print only lines that do not contain the secret.
if garage key info "$KEY_ID" >/dev/null 2>&1; then
    echo "garage-init: key $KEY_ID already exists"
else
    if ! IMPORT_OUT=$(garage key import --yes -n "$KEY_NAME" "$GARAGE_KEY_ID" "$GARAGE_KEY_SECRET" 2>&1); then
        printf '%s\n' "$IMPORT_OUT" | grep -Fv -- "$GARAGE_KEY_SECRET" >&2 || true
        echo "garage-init: key import failed" >&2
        exit 1
    fi
    echo "garage-init: key $KEY_ID imported"
fi

if garage bucket info "$BUCKET" >/dev/null 2>&1; then
    echo "garage-init: bucket '$BUCKET' already exists"
else
    garage bucket create "$BUCKET"
fi

# Grant, then prove it: the KEYS section of `bucket info` prints the R/W/O
# flags followed by the full access key ID. No error suppression here — a
# failed grant must fail the job.
garage bucket allow --read --write --owner "$BUCKET" --key "$KEY_ID"
if ! garage bucket info "$BUCKET" | grep -Eq "^RWO[[:space:]]*$KEY_ID"; then
    echo "garage-init: key $KEY_ID does not have RWO permissions on '$BUCKET'" >&2
    exit 1
fi

garage key info "$KEY_ID" >/dev/null
echo "garage-init: bucket '$BUCKET' ready for key $KEY_ID ($KEY_NAME)"
