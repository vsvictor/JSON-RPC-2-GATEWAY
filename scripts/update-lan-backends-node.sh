#!/usr/bin/env bash
set -Eeuo pipefail

[[ "$#" -ge 2 ]] || { echo "Expected staging directory and backend units" >&2; exit 1; }
stage="$1"
shift
[[ "$stage" =~ ^/tmp/jrpc-backend-update\.[[:alnum:]]{6}$ && -d "$stage" ]] || {
    echo "Invalid staging directory" >&2
    exit 1
}
trap 'rm -rf -- "$stage"' EXIT
for unit in "$@"; do
    case "$unit" in
        json-rpc-backend-131.service|json-rpc-backend-132-1.service|json-rpc-backend-132-2.service) ;;
        *) echo "Unexpected backend unit" >&2; exit 1 ;;
    esac
done

install -d -o root -g jsonrpc -m 0750 "$stage/main" "$stage/tracking"
install -o root -g jsonrpc -m 0640 "$stage/main.env" "$stage/main/.env"
install -o root -g jsonrpc -m 0640 "$stage/tracking.env" "$stage/tracking/.env"

# Check both plugins and external stores before replacing any running binary.
chown root:jsonrpc "$stage"
chmod 0750 "$stage"
if ! runuser -u jsonrpc -- timeout --preserve-status --signal=INT --kill-after=10 15 \
    env BACKEND_BIND=127.0.0.1 BACKEND_TCP_PORT=19081 \
    TMS_MAIN_CONFIG_DIR="$stage/main" \
    TMS_TRACKING_CONFIG_DIR="$stage/tracking" \
    "$stage/backend" >"$stage/preflight.log" 2>&1; then
    echo "Plugin preflight failed; installed backend binary unchanged" >&2
    exit 1
fi
grep -q 'demo backend listening on TCP and UDP' "$stage/preflight.log" || {
    echo "Plugin preflight did not reach a listening state" >&2
    exit 1
}
install -d -o root -g jsonrpc -m 0750 \
    /etc/json-rpc-ecosystem/tms-main /etc/json-rpc-ecosystem/tms-tracking
install -o root -g jsonrpc -m 0640 "$stage/main.env" /etc/json-rpc-ecosystem/tms-main/.env
install -o root -g jsonrpc -m 0640 "$stage/tracking.env" /etc/json-rpc-ecosystem/tms-tracking/.env
install -o root -g root -m 0755 "$stage/backend" /opt/json-rpc-ecosystem/bin/gateway-demo-backend.new
mv -f /opt/json-rpc-ecosystem/bin/gateway-demo-backend.new \
    /opt/json-rpc-ecosystem/bin/gateway-demo-backend
for unit in "$@"; do
    install -d -m 0755 "/etc/systemd/system/$unit.d"
    printf '%s\n' '[Service]' \
        'Environment=TMS_MAIN_CONFIG_DIR=/etc/json-rpc-ecosystem/tms-main' \
        'Environment=TMS_TRACKING_CONFIG_DIR=/etc/json-rpc-ecosystem/tms-tracking' \
        >"/etc/systemd/system/$unit.d/plugins.conf"
done
systemctl daemon-reload
for unit in "$@"; do
    systemctl restart "$unit"
    ready=false
    for ((attempt=0; attempt<60; attempt++)); do
        pid="$(systemctl show "$unit" -p MainPID --value)"
        if systemctl --quiet is-active "$unit" &&
            ss -lntp | grep -q "pid=$pid,"; then
            ready=true
            break
        fi
        sleep 1
    done
    if [[ "$ready" != true ]]; then
        echo "$unit did not become ready; inspect its systemd journal" >&2
        exit 1
    fi
    printf '%s: ready\n' "$unit"
done
