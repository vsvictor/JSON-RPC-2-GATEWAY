#!/usr/bin/env bash
set -Eeuo pipefail

stage="$1"
kind="$2"
service_user="$3"
[[ "$stage" =~ ^/tmp/jrpc-ecosystem-[0-9]+-gateway$ && "$kind" == gateway ]] || {
    echo "Expected a single-node gateway staging directory" >&2
    exit 1
}
trap 'rm -rf -- "$stage"' EXIT

command -v systemctl >/dev/null
for unit in json-rpc-backend-1.service json-rpc-backend-2.service; do
    overrides="/etc/systemd/system/$unit.d"
    if [[ -d "$overrides" ]] && find "$overrides" -maxdepth 1 -name '*.conf' -print -quit | grep -q .; then
        echo "Custom backend configuration found in $overrides; review overrides before deploying" >&2
        exit 1
    fi
done
for unit in json-rpc-backend-131.service json-rpc-backend-132-1.service json-rpc-backend-132-2.service; do
    if systemctl is-active --quiet "$unit" || systemctl is-enabled --quiet "$unit"; then
        echo "Legacy service $unit exists; explicitly stop and disable it before migrating to the two-backend topology" >&2
        exit 1
    fi
done
if [[ -f /var/lib/json-rpc-gateway/backends.json ]]; then
    python3 - "$stage/backends.json" <<'PY'
import json, sys
with open(sys.argv[1]) as file:
    seed = json.load(file)["backends"]
with open("/var/lib/json-rpc-gateway/backends.json") as file:
    saved = json.load(file)
expected = {entry["id"]: entry for entry in seed}
entries = saved["backends"]
if saved["version"] != 1 or len(entries) != len(expected) or {
    entry["config"]["id"] for entry in entries
} != set(expected):
    raise SystemExit("Persistent registry differs from the two-backend topology; back it up and migrate explicitly")
for entry in entries:
    config = entry["config"]
    if any(config[key] != expected[config["id"]][key] for key in ("host", "ports", "pool_size")):
        raise SystemExit("Persistent backend endpoints differ; migrate saved state explicitly before deployment")
PY
fi

if ! getent group "$service_user" >/dev/null 2>&1; then
    groupadd --system "$service_user"
fi
if ! id "$service_user" >/dev/null 2>&1; then
    useradd --system --no-create-home --gid "$service_user" \
        --shell /usr/sbin/nologin "$service_user"
fi

install -d -o root -g root -m 0755 /opt/json-rpc-ecosystem/bin /etc/json-rpc-ecosystem
for binary in gateway-demo-backend json-rpc-gateway; do
    install -o root -g root -m 0755 "$stage/$binary" "/opt/json-rpc-ecosystem/bin/$binary"
done
install -o root -g root -m 0644 "$stage/backends.json" /etc/json-rpc-ecosystem/backends.json
install -o root -g root -m 0640 "$stage/gateway.env" /etc/json-rpc-ecosystem/gateway.env
for id in 1 2; do
    install -o root -g root -m 0640 "$stage/backend-$id.env" "/etc/json-rpc-ecosystem/backend-$id.env"
    install -o root -g root -m 0644 "$stage/json-rpc-backend-$id.service" "/etc/systemd/system/json-rpc-backend-$id.service"
done
install -o root -g root -m 0644 "$stage/json-rpc-gateway.service" /etc/systemd/system/json-rpc-gateway.service

wait_for_tcp() {
    local env_file="$1" host_key="$2" port_key="$3"
    local host port
    host="$(sed -n "s/^$host_key=//p" "$env_file")"
    port="$(sed -n "s/^$port_key=//p" "$env_file")"
    for ((attempt=0; attempt<60; attempt++)); do
        if timeout 1 bash -c ':</dev/tcp/$1/$2' _ "$host" "$port" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "Timed out waiting for $host:$port" >&2
    return 1
}

systemctl daemon-reload
systemctl enable json-rpc-backend-1.service json-rpc-backend-2.service json-rpc-gateway.service
systemctl restart json-rpc-backend-1.service json-rpc-backend-2.service
for id in 1 2; do
    wait_for_tcp "/etc/json-rpc-ecosystem/backend-$id.env" BACKEND_BIND BACKEND_TCP_PORT
    systemctl is-active --quiet "json-rpc-backend-$id.service"
done
systemctl restart json-rpc-gateway.service
wait_for_tcp /etc/json-rpc-ecosystem/gateway.env GATEWAY_BIND GATEWAY_PORT
systemctl --no-pager --full status json-rpc-gateway.service
