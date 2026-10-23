#!/usr/bin/env bash
set -Eeuo pipefail

stage="$1"
kind="$2"
service_user="$3"
case "$stage" in
    /tmp/jrpc-ecosystem-*) ;;
    *) echo "Invalid staging path" >&2; exit 1 ;;
esac
trap 'rm -rf -- "$stage"' EXIT

command -v systemctl >/dev/null
if ! getent group "$service_user" >/dev/null 2>&1; then
    groupadd --system "$service_user"
fi
if ! id "$service_user" >/dev/null 2>&1; then
    useradd --system --no-create-home --gid "$service_user" \
        --shell /usr/sbin/nologin "$service_user"
fi

install -d -o root -g root -m 0755 /opt/json-rpc-ecosystem/bin
install -d -o root -g root -m 0755 /etc/json-rpc-ecosystem
install -o root -g root -m 0755 "$stage/gateway-demo-backend" \
    /opt/json-rpc-ecosystem/bin/gateway-demo-backend

install_backend_unit() {
    local service="$1"
    local env_file="$2"
    install -o root -g root -m 0640 "$stage/$env_file" "/etc/json-rpc-ecosystem/$env_file"
    install -o root -g root -m 0644 "$stage/$service" "/etc/systemd/system/$service"
}

wait_for_tcp() {
    local host="$1"
    local port="$2"
    local attempt
    for attempt in $(seq 1 30); do
        if timeout 1 bash -c ":</dev/tcp/$host/$port" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "Timed out waiting for $host:$port" >&2
    return 1
}

case "$kind" in
    gateway)
        install -o root -g root -m 0755 "$stage/json-rpc-gateway" \
            /opt/json-rpc-ecosystem/bin/json-rpc-gateway
        install -o root -g root -m 0644 "$stage/backends.json" \
            /etc/json-rpc-ecosystem/backends.json
        install -o root -g root -m 0640 "$stage/gateway.env" \
            /etc/json-rpc-ecosystem/gateway.env
        install -o root -g root -m 0640 "$stage/backend-131.env" \
            /etc/json-rpc-ecosystem/backend-131.env
        install_backend_unit json-rpc-backend-132-1.service backend-132-1.env
        install_backend_unit json-rpc-backend-132-2.service backend-132-2.env
        install -o root -g root -m 0644 "$stage/json-rpc-gateway.service" \
            /etc/systemd/system/json-rpc-gateway.service
        systemctl daemon-reload
        systemctl enable json-rpc-backend-132-1.service json-rpc-backend-132-2.service
        systemctl restart json-rpc-backend-132-1.service json-rpc-backend-132-2.service
        wait_for_tcp 192.168.1.132 "$(
            sed -n 's/^BACKEND_TCP_PORT=//p' /etc/json-rpc-ecosystem/backend-132-1.env
        )"
        wait_for_tcp 192.168.1.132 "$(
            sed -n 's/^BACKEND_TCP_PORT=//p' /etc/json-rpc-ecosystem/backend-132-2.env
        )"
        wait_for_tcp 192.168.1.131 "$(
            sed -n 's/^BACKEND_TCP_PORT=//p' /etc/json-rpc-ecosystem/backend-131.env
        )"
        systemctl enable json-rpc-gateway.service
        systemctl restart json-rpc-gateway.service
        wait_for_tcp 192.168.1.132 "$(
            sed -n 's/^GATEWAY_PORT=//p' /etc/json-rpc-ecosystem/gateway.env
        )"
        systemctl --no-pager --full status json-rpc-gateway.service
        ;;
    remote-backend)
        install_backend_unit json-rpc-backend-131.service backend-131.env
        systemctl daemon-reload
        systemctl enable json-rpc-backend-131.service
        systemctl restart json-rpc-backend-131.service
        wait_for_tcp 192.168.1.131 "$(
            sed -n 's/^BACKEND_TCP_PORT=//p' /etc/json-rpc-ecosystem/backend-131.env
        )"
        systemctl --no-pager --full status json-rpc-backend-131.service
        ;;
    *)
        echo "Unknown node kind: $kind" >&2
        exit 1
        ;;
esac
