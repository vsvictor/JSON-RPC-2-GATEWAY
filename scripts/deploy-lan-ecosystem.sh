#!/usr/bin/env bash
set -Eeuo pipefail

readonly gateway_host="192.168.1.132"
readonly gateway_port=8443
readonly control_port=8500
readonly backend_1_port=8081
readonly backend_2_port=8082
readonly backend_1_tls_port=8083
readonly backend_2_tls_port=8084
readonly ssh_user="${SSH_USER:-dev}"
readonly service_user="${SERVICE_USER:-jsonrpc}"
readonly tms_backend_root="${TMS_BACKEND_ROOT:-/home/dev/TMSBackend}"
readonly tls_cert_file="${GATEWAY_TLS_CERT_FILE:-/home/dev/Certs/server-fullchain.pem}"
readonly tls_key_file="${GATEWAY_TLS_KEY_FILE:-/home/dev/Certs/server-key.pem}"
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
validate_port BACKEND_1_PORT "$backend_1_port"
validate_port BACKEND_2_PORT "$backend_2_port"
validate_port BACKEND_1_TLS_PORT "$backend_1_tls_port"
validate_port BACKEND_2_TLS_PORT "$backend_2_tls_port"
[[ "$backend_1_port" != "$backend_2_port" ]] ||
    fail "the two backend instances on $gateway_host need different TCP ports"
[[ "$gateway_port" != "$control_port" ]] ||
    fail "GATEWAY_PORT and CONTROL_PORT must differ"
for port in "$backend_1_port" "$backend_2_port" "$backend_1_tls_port" "$backend_2_tls_port"; do
    [[ "$port" != "$gateway_port" && "$port" != "$control_port" ]] ||
        fail "backend ports on $gateway_host must differ from gateway and control ports"
done
[[ "$backend_1_tls_port" != "$backend_2_tls_port" ]] ||
    fail "the two backend instances on $gateway_host need different TLS/QUIC ports"
for tcp_port in "$backend_1_port" "$backend_2_port"; do
    [[ "$tcp_port" != "$backend_1_tls_port" && "$tcp_port" != "$backend_2_tls_port" ]] ||
        fail "backend TCP and TLS ports must differ"
done
[[ "$service_user" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]] ||
    fail "SERVICE_USER must be a valid Unix account name"

command -v cargo >/dev/null || fail "cargo is required"
command -v ssh >/dev/null || fail "ssh is required"
command -v scp >/dev/null || fail "scp is required"
[[ -r "$tls_cert_file" ]] || fail "gateway TLS certificate is not readable: $tls_cert_file"
[[ -r "$tls_key_file" ]] || fail "gateway TLS private key is not readable: $tls_key_file"
[[ -r "$tms_backend_root/Cargo.toml" ]] ||
    fail "TMS backend Cargo.toml is not readable: $tms_backend_root/Cargo.toml"
[[ -r "$tms_backend_root/.env" ]] ||
    fail "TMS backend configuration is not readable: $tms_backend_root/.env"

is_local_host() {
    hostname -I | tr ' ' '\n' | grep -Fxq "$1"
}

printf 'Checking sudo and systemd on %s...\n' "$gateway_host"
if is_local_host "$gateway_host"; then
    command -v systemctl >/dev/null || fail "systemd is required"
else
    ssh -o BatchMode=yes "${ssh_user}@${gateway_host}" 'command -v systemctl >/dev/null' ||
        fail "systemd is required on the deployment host"
fi

printf 'Checking broker services on %s...\n' "$gateway_host"
if is_local_host "$gateway_host"; then
    systemctl is-active --quiet mosquitto ||
        fail "Mosquitto must be active before deploying all gateway profiles"
    systemctl is-active --quiet rabbitmq-server ||
        fail "RabbitMQ must be active before deploying all gateway profiles"
else
    ssh -o BatchMode=yes "${ssh_user}@${gateway_host}" \
        'systemctl is-active --quiet mosquitto && systemctl is-active --quiet rabbitmq-server' ||
        fail "Mosquitto and RabbitMQ must be active before deploying all gateway profiles"
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
gateway_max_connections="${GATEWAY_MAX_CONNECTIONS:-}"
if [[ -z "$gateway_max_connections" && -f "$project_root/.env" ]]; then
    gateway_max_connections="$(sed -n 's/^GATEWAY_MAX_CONNECTIONS=//p' "$project_root/.env" | tail -n 1)"
fi
gateway_max_connections="${gateway_max_connections:-4096}"
[[ "$gateway_max_connections" =~ ^[1-9][0-9]*$ ]] ||
    fail "GATEWAY_MAX_CONNECTIONS must be a positive integer"

target_dir="${CARGO_TARGET_DIR:-$project_root/target}"
if [[ "$target_dir" != /* ]]; then
    target_dir="$project_root/$target_dir"
fi
gateway_build_script="${GATEWAY_BUILD_SCRIPT:-$project_root/scripts/build-with-srt.sh}"
[[ -x "$gateway_build_script" ]] || fail "gateway build script is not executable: $gateway_build_script"
stage_dir="$(mktemp -d)"
trap 'rm -rf -- "$stage_dir"' EXIT

mkdir -p "$stage_dir/gateway"
install -m 0755 "$project_root/scripts/install-lan-ecosystem-node.sh" \
    "$stage_dir/gateway/install-node.sh"
mkdir -m 0700 "$stage_dir/protocols"
install -m 0755 "$project_root/scripts/enable-lan-gateway-protocols-node.sh" \
    "$stage_dir/protocols/enable-protocols.sh"
install -m 0600 "$tls_cert_file" "$stage_dir/protocols/server-fullchain.pem"
install -m 0600 "$tls_key_file" "$stage_dir/protocols/server-key.pem"
install -m 0600 "$tls_cert_file" "$stage_dir/gateway/server-fullchain.pem"
install -m 0600 "$tls_key_file" "$stage_dir/gateway/server-key.pem"
install -m 0600 "$tms_backend_root/.env" "$stage_dir/gateway/tms-backend.env"

printf 'Building gateway (including SRT) and TMS application backend...\n'
"$gateway_build_script" build --locked --release \
    -p json-rpc-gateway \
    --bin json-rpc-gateway \
    --features srt
CARGO_TARGET_DIR="$target_dir" cargo build \
    --manifest-path "$tms_backend_root/Cargo.toml" \
    --locked --release -p main-tms-server --bin main-tms-server \
    --features tms,map

install -m 0755 "$target_dir/release/json-rpc-gateway" "$stage_dir/protocols/"
install -m 0755 "$target_dir/release/json-rpc-gateway" "$stage_dir/gateway/"
install -m 0755 "$target_dir/release/main-tms-server" "$stage_dir/gateway/"

cat >"$stage_dir/gateway/backends.json" <<EOF
{
  "backends": [
    {
      "id": "backend-1",
      "host": "$gateway_host",
      "ports": { "tcp": $backend_1_port, "udp": $backend_1_port },
      "pool_size": 4
    },
    {
      "id": "backend-2",
      "host": "$gateway_host",
      "ports": { "tcp": $backend_2_port, "udp": $backend_2_port },
      "pool_size": 4
    }
  ]
}
EOF

cat >"$stage_dir/gateway/gateway.env" <<EOF
GATEWAY_BIND=$gateway_host
GATEWAY_PORT=$gateway_port
GATEWAY_MAX_CONNECTIONS=$gateway_max_connections
CONTROL_PORT=$control_port
BACKEND_CONFIG_PATH=/etc/json-rpc-ecosystem/backends.json
LOG_LEVEL=info
METRICS_BIND=127.0.0.1
METRICS_PORT=9090
EOF

write_backend_config() {
    local id="$1"
    local port="$2"
    local tls_port="$3"
    cat >"$4" <<EOF
BACKEND_ID=$id
BACKEND_BIND=$gateway_host
BACKEND_PORT=$port
BACKEND_TCP_PORT=$port
BACKEND_UDP_PORT=$port
BACKEND_TLS_PORT=$tls_port
BACKEND_QUIC_PORT=$tls_port
BACKEND_TLS_CERT_FILE=/etc/json-rpc-ecosystem/tls/server-fullchain.pem
BACKEND_TLS_KEY_FILE=/etc/json-rpc-ecosystem/tls/server-key.pem
GATEWAY_CONTROL_ADDR=$gateway_host:$control_port
JSON_RPC_HTTP_URL=http://$gateway_host:8508
RUST_LOG=info
EOF
}

write_backend_unit() {
    local id="$1"
    cat >"$2" <<EOF
[Unit]
Description=JSON-RPC TMS application backend ($id)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$service_user
Group=$service_user
WorkingDirectory=/etc/json-rpc-ecosystem/tms-backend
EnvironmentFile=/etc/json-rpc-ecosystem/$id.env
ExecStart=/opt/json-rpc-ecosystem/bin/main-tms-server
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

write_backend_config "backend-1" "$backend_1_port" "$backend_1_tls_port" \
    "$stage_dir/gateway/backend-1.env"
write_backend_config "backend-2" "$backend_2_port" "$backend_2_tls_port" \
    "$stage_dir/gateway/backend-2.env"
write_backend_unit "backend-1" "$stage_dir/gateway/json-rpc-backend-1.service"
write_backend_unit "backend-2" "$stage_dir/gateway/json-rpc-backend-2.service"

cat >"$stage_dir/gateway/json-rpc-gateway.service" <<EOF
[Unit]
Description=JSON-RPC Gateway and Balancer
After=network-online.target json-rpc-backend-1.service json-rpc-backend-2.service
Wants=network-online.target
Requires=json-rpc-backend-1.service json-rpc-backend-2.service

[Service]
Type=simple
User=$service_user
Group=$service_user
WorkingDirectory=/opt/json-rpc-ecosystem
StateDirectory=json-rpc-gateway
StateDirectoryMode=0750
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
        sudo bash "$remote_stage/install-node.sh" \
            "$remote_stage" "$node_kind" "$service_user"
    else
        ssh -o BatchMode=yes "$remote" "mkdir -m 0700 '$remote_stage'"
        scp -q "$source_dir"/* "$remote:$remote_stage/"
        ssh -tt "$remote" \
            "sudo bash '$remote_stage/install-node.sh' '$remote_stage' '$node_kind' '$service_user'"
    fi
}

printf 'Deploying two backends and the gateway on %s...\n' "$gateway_host"
deploy_node "$gateway_host" gateway

deploy_protocols() {
    local host="$1"
    local remote="${ssh_user}@${host}"
    local source_dir="$stage_dir/protocols"
    local protocol_stage

    printf 'Enabling all gateway protocol profiles on %s...\n' "$host"
    if is_local_host "$host"; then
        protocol_stage="$(mktemp -d /tmp/jrpc-protocols.XXXXXX)"
        cp "$source_dir"/* "$protocol_stage/"
        sudo bash "$protocol_stage/enable-protocols.sh" "$protocol_stage"
    else
        protocol_stage="$(ssh -o BatchMode=yes "$remote" 'mktemp -d /tmp/jrpc-protocols.XXXXXX')"
        scp -q "$source_dir"/* "$remote:$protocol_stage/"
        ssh -tt "$remote" \
            "sudo bash '$protocol_stage/enable-protocols.sh' '$protocol_stage'"
    fi
}

deploy_protocols "$gateway_host"

printf '\nDeployment complete.\n'
printf 'Gateway TCP/UDP ingress: %s:%s\n' "$gateway_host" "$gateway_port"
printf 'TMS application backends: %s:%s, %s:%s\n' \
    "$gateway_host" "$backend_1_port" \
    "$gateway_host" "$backend_2_port"
printf 'Application methods: login, getShift, tracking.ingest, map.load, map.click\n'
printf 'Metrics: http://127.0.0.1:9090/metrics on %s\n' "$gateway_host"
