#!/usr/bin/env bash
set -Eeuo pipefail

readonly gateway_host="192.168.1.132"
readonly local_backend_host="192.168.1.131"
readonly ssh_user="${SSH_USER:-dev}"

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

is_local_host() {
    hostname -I | tr ' ' '\n' | grep -Fxq "$1"
}

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
            ssh -o BatchMode=yes "${ssh_user}@${gateway_host}" \
                "sudo -S -p '' $command"
    else
        ssh -o BatchMode=yes "${ssh_user}@${gateway_host}" \
            "sudo -n $command"
    fi
}

is_local_host "$local_backend_host" ||
    fail "run this script on $local_backend_host so its local backend can be stopped"
command -v ssh >/dev/null || fail "ssh is required"

printf 'Stopping gateway and its two local backends on %s...\n' "$gateway_host"
run_remote_sudo \
    "systemctl stop json-rpc-gateway.service json-rpc-backend-132-1.service json-rpc-backend-132-2.service"
ssh -o BatchMode=yes "${ssh_user}@${gateway_host}" \
    'systemctl --no-pager --full is-active json-rpc-gateway.service json-rpc-backend-132-1.service json-rpc-backend-132-2.service || true'

printf 'Stopping backend on %s...\n' "$local_backend_host"
run_local_sudo systemctl stop json-rpc-backend-131.service
systemctl --no-pager --full is-active json-rpc-backend-131.service || true

printf 'Ecosystem stopped. Services remain enabled for the next boot.\n'
