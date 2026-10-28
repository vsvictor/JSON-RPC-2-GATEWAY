#!/usr/bin/env bash
set -Eeuo pipefail

readonly gateway_host="192.168.1.132"
readonly gateway_port=8443
readonly control_port=8500

[[ $# == 1 && "$1" =~ ^/tmp/jrpc-protocols\.[[:alnum:]]{6}$ ]] || {
    echo "Expected secure gateway staging directory" >&2
    exit 1
}
stage="$1"
trap 'rm -rf -- "$stage"' EXIT
systemctl is-active --quiet mosquitto rabbitmq-server
python3 - "$gateway_port" <<'PY'
from pathlib import Path
import subprocess
import sys

path = Path("/etc/ufw/before.rules")
text = path.read_text()
rules = [
    f"-A ufw-before-input -s 192.168.1.0/24 -p {protocol} -m multiport --dports {int(sys.argv[1])} -j ACCEPT"
    for protocol in [132, 136]
]
missing = [rule for rule in rules if rule not in text]
if missing:
    table = text.index("*filter")
    insertion = text.index("-A ", table)
    updated = text[:insertion] + "\n".join(missing) + "\n" + text[insertion:]
    subprocess.run(["iptables-restore", "--test"], input=updated, text=True, check=True)
    path.write_text(updated)
    subprocess.run(["ufw", "reload"], check=True)
PY
install -d -o root -g jsonrpc -m 0750 /etc/json-rpc-ecosystem/tls
install -o root -g jsonrpc -m 0640 "$stage/server-fullchain.pem" /etc/json-rpc-ecosystem/tls/
install -o root -g jsonrpc -m 0640 "$stage/server-key.pem" /etc/json-rpc-ecosystem/tls/
cp -p /etc/json-rpc-ecosystem/gateway.env "$stage/gateway.env.before"
cp -p /opt/json-rpc-ecosystem/bin/json-rpc-gateway "$stage/gateway.before"

python3 - "$gateway_host" "$gateway_port" "$control_port" <<'PY'
from pathlib import Path
import secrets
import sys

path = Path("/etc/json-rpc-ecosystem/gateway.env")
lines = path.read_text().splitlines()
updates = {"GATEWAY_BIND": sys.argv[1], "GATEWAY_PORT": sys.argv[2], "CONTROL_PORT": sys.argv[3]}
for protocol in [
    "tcp", "tls", "udp", "rtp", "rudp", "srt", "udplite", "sctp",
    "quic", "http1", "http3", "coap", "mqtt", "amqp",
]:
    for prefix in ["", "ws-"]:
        name = (prefix + protocol).upper().replace("-", "_")
        updates[f"GATEWAY_{name}_PORT"] = sys.argv[2]
updates["TLS_CERT_PATH"] = "/etc/json-rpc-ecosystem/tls/server-fullchain.pem"
updates["TLS_KEY_PATH"] = "/etc/json-rpc-ecosystem/tls/server-key.pem"
updates["MQTT_BROKER_URL"] = "mqtt://127.0.0.1:1883"
updates["AMQP_BROKER_URL"] = "amqp://guest:guest@127.0.0.1:5672/%2f"
if not any(line.startswith("GATEWAY_SRT_PASSPHRASE=") for line in lines):
    updates["GATEWAY_SRT_PASSPHRASE"] = secrets.token_hex(20)
lines = [line for line in lines if line.split("=", 1)[0] not in updates]
lines.extend(f"{name}={value}" for name, value in updates.items())
path.write_text("\n".join(lines) + "\n")
path.chmod(0o640)
PY
install -o root -g root -m 0755 "$stage/json-rpc-gateway" /opt/json-rpc-ecosystem/bin/json-rpc-gateway.new
mv -f /opt/json-rpc-ecosystem/bin/json-rpc-gateway.new /opt/json-rpc-ecosystem/bin/json-rpc-gateway
install -d -o root -g root -m 0755 /etc/systemd/system/json-rpc-gateway.service.d
cat >/etc/systemd/system/json-rpc-gateway.service.d/transport-state.conf <<'EOF'
[Service]
StateDirectory=json-rpc-gateway
StateDirectoryMode=0750
EOF
systemctl daemon-reload
systemctl restart json-rpc-gateway.service
for ((attempt=0; attempt<60; attempt++)); do
    if python3 - "$gateway_host" "$control_port" <<'PY'
import json, socket, sys
try:
    with socket.create_connection((sys.argv[1],int(sys.argv[2])), timeout=1) as connection:
        connection.sendall(b'{"action":"list_components"}\n')
        response = json.loads(connection.makefile("rb").readline())
        listeners = response["components"]["gateway"]["listeners"]
        assert len(listeners) == 24, listeners
except (OSError, ValueError, KeyError, AssertionError):
    raise SystemExit(1)
PY
    then
        echo "Gateway ready: 28 protocols configured; DCCP unavailable in this kernel."
        exit 0
    fi
    sleep 1
done
echo "Gateway failed readiness; restoring previous binary and environment" >&2
cp -p "$stage/gateway.env.before" /etc/json-rpc-ecosystem/gateway.env
install -o root -g root -m 0755 "$stage/gateway.before" /opt/json-rpc-ecosystem/bin/json-rpc-gateway.new
mv -f /opt/json-rpc-ecosystem/bin/json-rpc-gateway.new /opt/json-rpc-ecosystem/bin/json-rpc-gateway
systemctl restart json-rpc-gateway.service
exit 1
