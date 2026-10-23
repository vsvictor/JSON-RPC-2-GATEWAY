#!/usr/bin/env bash
set -Eeuo pipefail

readonly gateway_host="192.168.1.132"
readonly remote_backend_host="192.168.1.131"
readonly ssh_user="${SSH_USER:-$(id -un)}"
readonly service_user="${SERVICE_USER:-jsonrpc}"
readonly gateway_port="${GATEWAY_PORT:-18080}"
readonly control_port="${CONTROL_PORT:-18500}"
readonly backend_132_1_port="${BACKEND_132_1_PORT:-18081}"
readonly backend_132_2_port="${BACKEND_132_2_PORT:-18082}"
readonly backend_131_port="${BACKEND_131_PORT:-18081}"
readonly deploy_id="jrpc-ecosystem-$$"

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

validate_port() {
    local name="$1"
    local value="$2"
    [[ "$value" =~ ^[1-9][0-9]{0,4}$ ]] || fail "$name must be an integer in 1..=65535"
    (( value <= 65535 )) || fail "$name must be an integer in 1..=65535"
}

validate_port GATEWAY_PORT "$gateway_port"
validate_port CONTROL_PORT "$control_port"
validate_port BACKEND_132_1_PORT "$backend_132_1_port"
validate_port BACKEND_132_2_PORT "$backend_132_2_port"
validate_port BACKEND_131_PORT "$backend_131_port"
[[ "$backend_132_1_port" != "$backend_132_2_port" ]] ||
    fail "the two backend instances on $gateway_host need different TCP ports"
[[ "$gateway_port" != "$control_port" ]] ||
    fail "GATEWAY_PORT and CONTROL_PORT must differ"
for port in "$backend_132_1_port" "$backend_132_2_port"; do
    [[ "$port" != "$gateway_port" && "$port" != "$control_port" ]] ||
        fail "backend ports on $gateway_host must differ from gateway and control ports"
done
[[ "$service_user" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]] ||
    fail "SERVICE_USER must be a valid Unix account name"

command -v cargo >/dev/null || fail "cargo is required"
command -v ssh >/dev/null || fail "ssh is required"
command -v scp >/dev/null || fail "scp is required"

is_local_host() {
    hostname -I | tr ' ' '\n' | grep -Fxq "$1"
}

if ! is_local_host "$remote_backend_host"; then
    fail "run this script on $remote_backend_host, or update its local-node deployment logic"
fi

printf 'Checking local sudo and systemd on %s...\n' "$remote_backend_host"
if [[ -n "${SUDO_PASSWORD:-}" ]]; then
    printf '%s\n' "$SUDO_PASSWORD" | sudo -S -p '' true
else
    sudo -n true || fail "set SUDO_PASSWORD for this run or configure passwordless sudo"
fi
command -v systemctl >/dev/null || fail "systemd is required on $remote_backend_host"

printf 'Checking SSH and sudo on %s...\n' "$gateway_host"
if [[ -n "${SUDO_PASSWORD:-}" ]]; then
    printf '%s\n' "$SUDO_PASSWORD" |
        ssh -o BatchMode=yes "${ssh_user}@${gateway_host}" 'sudo -S -p "" true'
else
    ssh -o BatchMode=yes "${ssh_user}@${gateway_host}" \
        'sudo -n true && command -v systemctl >/dev/null' ||
        fail "remote sudo failed; set SUDO_PASSWORD for this run or configure passwordless sudo"
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
target_dir="${CARGO_TARGET_DIR:-$project_root/target}"
if [[ "$target_dir" != /* ]]; then
    target_dir="$project_root/$target_dir"
fi
stage_dir="$(mktemp -d)"
trap 'rm -rf -- "$stage_dir"' EXIT

mkdir -p "$stage_dir/gateway" "$stage_dir/remote-backend"
install -m 0755 "$project_root/scripts/install-lan-ecosystem-node.sh" \
    "$stage_dir/gateway/install-node.sh"
install -m 0755 "$project_root/scripts/install-lan-ecosystem-node.sh" \
    "$stage_dir/remote-backend/install-node.sh"

printf 'Building gateway and gateway-compatible demo backend...\n'
cargo build --locked --release \
    -p json-rpc-gateway \
    --bin json-rpc-gateway \
    --bin gateway-demo-backend

install -m 0755 "$target_dir/release/json-rpc-gateway" "$stage_dir/gateway/"
install -m 0755 "$target_dir/release/gateway-demo-backend" "$stage_dir/gateway/"
install -m 0755 "$target_dir/release/gateway-demo-backend" "$stage_dir/remote-backend/"

cat >"$stage_dir/gateway/backends.json" <<EOF
{
  "backends": [
    {
      "id": "backend-132-1",
      "host": "$gateway_host",
      "ports": { "tcp": $backend_132_1_port, "udp": $backend_132_1_port },
      "pool_size": 4
    },
    {
      "id": "backend-132-2",
      "host": "$gateway_host",
      "ports": { "tcp": $backend_132_2_port, "udp": $backend_132_2_port },
      "pool_size": 4
    },
    {
      "id": "backend-131",
      "host": "$remote_backend_host",
      "ports": { "tcp": $backend_131_port, "udp": $backend_131_port },
      "pool_size": 4
    }
  ]
}
EOF

cat >"$stage_dir/gateway/gateway.env" <<EOF
GATEWAY_BIND=$gateway_host
GATEWAY_PORT=$gateway_port
CONTROL_PORT=$control_port
BACKEND_CONFIG_PATH=/etc/json-rpc-ecosystem/backends.json
LOG_LEVEL=info
METRICS_BIND=127.0.0.1
METRICS_PORT=9090
EOF

cat >"$stage_dir/gateway/backend-132-1.env" <<EOF
BACKEND_ID=backend-132-1
BACKEND_BIND=$gateway_host
BACKEND_TCP_PORT=$backend_132_1_port
EOF
cat >"$stage_dir/gateway/backend-132-2.env" <<EOF
BACKEND_ID=backend-132-2
BACKEND_BIND=$gateway_host
BACKEND_TCP_PORT=$backend_132_2_port
EOF
cat >"$stage_dir/remote-backend/backend-131.env" <<EOF
BACKEND_ID=backend-131
BACKEND_BIND=$remote_backend_host
BACKEND_TCP_PORT=$backend_131_port
EOF
cp "$stage_dir/remote-backend/backend-131.env" "$stage_dir/gateway/"

write_backend_unit() {
    local id="$1"
    local env_file="$2"
    cat >"$3" <<EOF
[Unit]
Description=JSON-RPC demo backend ($id)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$service_user
Group=$service_user
EnvironmentFile=/etc/json-rpc-ecosystem/$env_file
ExecStart=/opt/json-rpc-ecosystem/bin/gateway-demo-backend
Restart=on-failure
RestartSec=2
TimeoutStopSec=30
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF
}

write_backend_unit "backend-132-1" "backend-132-1.env" "$stage_dir/gateway/json-rpc-backend-132-1.service"
write_backend_unit "backend-132-2" "backend-132-2.env" "$stage_dir/gateway/json-rpc-backend-132-2.service"
write_backend_unit "backend-131" "backend-131.env" "$stage_dir/remote-backend/json-rpc-backend-131.service"

cat >"$stage_dir/gateway/json-rpc-gateway.service" <<EOF
[Unit]
Description=JSON-RPC Gateway and Balancer
After=network-online.target json-rpc-backend-132-1.service json-rpc-backend-132-2.service
Wants=network-online.target
Requires=json-rpc-backend-132-1.service json-rpc-backend-132-2.service

[Service]
Type=simple
User=$service_user
Group=$service_user
WorkingDirectory=/opt/json-rpc-ecosystem
EnvironmentFile=/etc/json-rpc-ecosystem/gateway.env
ExecStart=/opt/json-rpc-ecosystem/bin/json-rpc-gateway
Restart=on-failure
RestartSec=5
TimeoutStopSec=30
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF

deploy_node() {
    local host="$1"
    local node_kind="$2"
    local remote="${ssh_user}@${host}"
    local remote_stage="/tmp/${deploy_id}-${node_kind}"
    local source_dir="$stage_dir/$node_kind"

    printf 'Staging files on %s...\n' "$host"
    if is_local_host "$host"; then
        mkdir -m 0700 "$remote_stage"
        cp "$source_dir"/* "$remote_stage/"
        if [[ -n "${SUDO_PASSWORD:-}" ]]; then
            printf '%s\n' "$SUDO_PASSWORD" |
                sudo -S -p '' bash "$remote_stage/install-node.sh" \
                    "$remote_stage" "$node_kind" "$service_user"
        else
            sudo -n bash "$remote_stage/install-node.sh" \
                "$remote_stage" "$node_kind" "$service_user"
        fi
    else
        ssh -o BatchMode=yes "$remote" "mkdir -m 0700 '$remote_stage'"
        scp -q "$source_dir"/* "$remote:$remote_stage/"
        if [[ -n "${SUDO_PASSWORD:-}" ]]; then
            printf '%s\n' "$SUDO_PASSWORD" |
                ssh -o BatchMode=yes "$remote" "sudo -S -p '' bash '$remote_stage/install-node.sh' '$remote_stage' '$node_kind' '$service_user'"
        else
            ssh -o BatchMode=yes "$remote" sudo -n bash \
                "$remote_stage/install-node.sh" "$remote_stage" "$node_kind" "$service_user"
        fi
    fi
}

printf 'Deploying the backend on %s first...\n' "$remote_backend_host"
deploy_node "$remote_backend_host" remote-backend
printf 'Deploying two backends and the gateway on %s...\n' "$gateway_host"
deploy_node "$gateway_host" gateway

printf '\nDeployment complete.\n'
printf 'Gateway TCP/UDP ingress: %s:%s\n' "$gateway_host" "$gateway_port"
printf 'Backends: %s:%s, %s:%s, %s:%s\n' \
    "$gateway_host" "$backend_132_1_port" \
    "$gateway_host" "$backend_132_2_port" \
    "$remote_backend_host" "$backend_131_port"
printf 'Metrics: http://127.0.0.1:9090/metrics on %s\n' "$gateway_host"
printf 'RPC test method: identify (returns the selected demo backend ID)\n'
