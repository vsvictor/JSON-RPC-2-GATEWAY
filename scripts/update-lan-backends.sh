#!/usr/bin/env bash
set -Eeuo pipefail

readonly ssh_user="${SSH_USER:-dev}"
readonly main_config="${TMS_MAIN_CONFIG_DIR:-/home/dev/TMSBackend/tms-server-main}"
readonly tracking_config="${TMS_TRACKING_CONFIG_DIR:-/home/dev/TMSBackend/tms-server-tracking}"
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
hostname -I | tr ' ' '\n' | grep -Fxq 192.168.1.131 || {
    echo "Run this script on 192.168.1.131" >&2
    exit 1
}
test -r "$main_config/.env"
test -r "$tracking_config/.env"

cargo build --locked --release -p json-rpc-gateway \
    --bin gateway-demo-backend --features tms,tracking
target_dir="${CARGO_TARGET_DIR:-$project_root/target}"
[[ "$target_dir" == /* ]] || target_dir="$project_root/$target_dir"
stage="$(mktemp -d /tmp/jrpc-backend-update.XXXXXX)"
remote_stage=""
cleanup() {
    rm -rf -- "$stage"
    if [[ -n "$remote_stage" ]]; then
        ssh -o BatchMode=yes -o ConnectTimeout=10 "$ssh_user@192.168.1.132" \
            "rm -rf -- '$remote_stage'" ||
            echo "Could not clean remote staging directory: $remote_stage" >&2
    fi
}
trap cleanup EXIT
install -m 0755 "$target_dir/release/gateway-demo-backend" "$stage/backend"
install -m 0600 "$main_config/.env" "$stage/main.env"
install -m 0600 "$tracking_config/.env" "$stage/tracking.env"
install -m 0700 scripts/update-lan-backends-node.sh "$stage/install.sh"

run_sudo() {
    if [[ -n "${SUDO_PASSWORD:-}" ]]; then
        printf '%s\n' "$SUDO_PASSWORD" | sudo -S -p '' "$@"
    else
        sudo -n "$@"
    fi
}

remote_stage="$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$ssh_user@192.168.1.132" \
    'mktemp -d /tmp/jrpc-backend-update.XXXXXX')"
if [[ ! "$remote_stage" =~ ^/tmp/jrpc-backend-update\.[[:alnum:]]{6}$ ]]; then
    remote_stage=""
    echo "Unexpected remote staging path" >&2
    exit 1
fi
scp -q "$stage/backend" "$stage/main.env" "$stage/tracking.env" "$stage/install.sh" \
    "$ssh_user@192.168.1.132:$remote_stage/"
if [[ -n "${SUDO_PASSWORD:-}" ]]; then
    printf '%s\n' "$SUDO_PASSWORD" |
        ssh -o BatchMode=yes "$ssh_user@192.168.1.132" \
            "sudo -S -p '' bash '$remote_stage/install.sh' '$remote_stage' json-rpc-backend-132-1.service json-rpc-backend-132-2.service"
else
    ssh -o BatchMode=yes "$ssh_user@192.168.1.132" \
        "sudo -n bash '$remote_stage/install.sh' '$remote_stage' json-rpc-backend-132-1.service json-rpc-backend-132-2.service"
fi
run_sudo bash "$stage/install.sh" "$stage" json-rpc-backend-131.service
printf 'All three backends updated with tms and tracking; gateway configuration unchanged.\n'
