#!/usr/bin/env bash
set -Eeuo pipefail

readonly gateway_host="192.168.1.132"
readonly gateway_port=8443
readonly ssh_user="${SSH_USER:-dev}"

run_node_sudo() {
    if hostname -I | tr ' ' '\n' | grep -Fxq "$gateway_host"; then
        if [[ -n "${SUDO_PASSWORD:-}" ]]; then
            printf '%s\n' "$SUDO_PASSWORD" | sudo -S -p '' "$@"
        else
            sudo -n "$@"
        fi
    elif [[ -n "${SUDO_PASSWORD:-}" ]]; then
        printf '%s\n' "$SUDO_PASSWORD" |
            ssh -o BatchMode=yes -o ConnectTimeout=10 "${ssh_user}@${gateway_host}" "sudo -S -p '' $*"
    else
        ssh -o BatchMode=yes -o ConnectTimeout=10 "${ssh_user}@${gateway_host}" "sudo -n $*"
    fi
}

run_node_sudo systemctl start json-rpc-backend-1.service json-rpc-backend-2.service
run_node_sudo systemctl start json-rpc-gateway.service
for ((attempt=0; attempt<60; attempt++)); do
    if timeout 1 bash -c ':</dev/tcp/$1/$2' _ "$gateway_host" "$gateway_port" 2>/dev/null; then
        run_node_sudo systemctl is-active --quiet json-rpc-backend-1.service json-rpc-backend-2.service json-rpc-gateway.service
        printf 'Gateway and two backends ready on %s; ingress %s\n' "$gateway_host" "$gateway_port"
        exit 0
    fi
    sleep 1
done
echo "Gateway readiness timed out on $gateway_host:$gateway_port" >&2
exit 1
