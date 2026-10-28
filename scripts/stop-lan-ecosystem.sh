#!/usr/bin/env bash
set -Eeuo pipefail

readonly gateway_host="192.168.1.132"
readonly ssh_user="${SSH_USER:-dev}"

units="json-rpc-gateway.service json-rpc-backend-1.service json-rpc-backend-2.service"
if hostname -I | tr ' ' '\n' | grep -Fxq "$gateway_host"; then
    if [[ -n "${SUDO_PASSWORD:-}" ]]; then
        printf '%s\n' "$SUDO_PASSWORD" | sudo -S -p '' systemctl stop $units
    else
        sudo -n systemctl stop $units
    fi
elif [[ -n "${SUDO_PASSWORD:-}" ]]; then
    printf '%s\n' "$SUDO_PASSWORD" |
        ssh -o BatchMode=yes -o ConnectTimeout=10 "${ssh_user}@${gateway_host}" "sudo -S -p '' systemctl stop $units"
else
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${ssh_user}@${gateway_host}" "sudo -n systemctl stop $units"
fi
printf 'Gateway and two backends stopped on %s. Services remain enabled for the next boot.\n' "$gateway_host"
