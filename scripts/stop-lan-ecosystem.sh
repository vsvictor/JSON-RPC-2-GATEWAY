#!/usr/bin/env bash
set -Eeuo pipefail

readonly gateway_host="192.168.1.132"
readonly ssh_user="${SSH_USER:-dev}"

units=(json-rpc-gateway.service json-rpc-backend-1.service json-rpc-backend-2.service)
if hostname -I | tr ' ' '\n' | grep -Fxq "$gateway_host"; then
    sudo systemctl stop "${units[@]}"
else
    remote_command='sudo systemctl stop json-rpc-gateway.service json-rpc-backend-1.service json-rpc-backend-2.service'
    ssh -tt -o ConnectTimeout=10 "${ssh_user}@${gateway_host}" "$remote_command"
fi
printf 'Gateway and two backends stopped on %s. Services remain enabled for the next boot.\n' "$gateway_host"
