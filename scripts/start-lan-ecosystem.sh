#!/usr/bin/env bash
set -Eeuo pipefail

readonly gateway_host="192.168.1.132"
readonly local_backend_host="192.168.1.131"
readonly ssh_user="${SSH_USER:-dev}"

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

hostname -I | tr ' ' '\n' | grep -Fxq "$local_backend_host" ||
    fail "run this script on $local_backend_host"
command -v ssh >/dev/null || fail "ssh is required"

run_local_sudo() {
    if [[ -n "${SUDO_PASSWORD:-}" ]]; then
        printf '%s\n' "$SUDO_PASSWORD" | sudo -S -p '' "$@"
    else
        sudo -n "$@"
    fi
}

run_remote_sudo() {
    local command="$1"
    if [[ -n "${SUDO_PASSWORD:-}" ]]; then
        printf '%s\n' "$SUDO_PASSWORD" |
            ssh -o BatchMode=yes -o ConnectTimeout=10 "${ssh_user}@${gateway_host}" \
                "sudo -S -p '' $command"
    else
        ssh -o BatchMode=yes -o ConnectTimeout=10 "${ssh_user}@${gateway_host}" \
            "sudo -n $command"
    fi
}

printf 'Checking sudo access on both nodes...\n'
run_local_sudo true
run_remote_sudo true

printf 'Starting backend on %s...\n' "$local_backend_host"
run_local_sudo systemctl start json-rpc-backend-131.service
systemctl --quiet is-active json-rpc-backend-131.service ||
    fail "local backend failed to start; check journalctl -u json-rpc-backend-131.service"

printf 'Starting two backends and gateway on %s...\n' "$gateway_host"
run_remote_sudo \
    "systemctl start json-rpc-backend-132-1.service json-rpc-backend-132-2.service"
run_remote_sudo "systemctl start json-rpc-gateway.service"

printf 'Waiting for gateway readiness...\n'
run_remote_sudo "bash -c 'set -eu
port=\$(sed -n \"s/^GATEWAY_PORT=//p\" /etc/json-rpc-ecosystem/gateway.env)
[[ \"\$port\" =~ ^[1-9][0-9]{0,4}$ ]] && (( port <= 65535 ))
for ((attempt=0; attempt<60; attempt++)); do
    if systemctl --quiet is-active json-rpc-backend-132-1.service &&
       systemctl --quiet is-active json-rpc-backend-132-2.service &&
       systemctl --quiet is-active json-rpc-gateway.service &&
       timeout 1 bash -c \":</dev/tcp/192.168.1.132/\$port\" 2>/dev/null; then
        printf \"Gateway ready at 192.168.1.132:%s\n\" \"\$port\"
        exit 0
    fi
    sleep 1
done
echo \"Gateway readiness timed out; inspect systemctl status and journalctl on 192.168.1.132\" >&2
exit 1'"

systemctl --quiet is-active json-rpc-backend-131.service ||
    fail "local backend is no longer active; inspect journalctl"
printf 'Ecosystem started using installed binaries and configuration; no deployment performed.\n'
