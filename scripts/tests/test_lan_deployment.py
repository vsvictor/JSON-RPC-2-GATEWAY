import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class LanDeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="jrpc-deploy-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.commands = self.directory / "commands"
        self.commands.mkdir()
        self.capture = self.directory / "capture"
        self.capture.mkdir()
        self.cert = self.directory / "server-fullchain.pem"
        self.key = self.directory / "server-key.pem"
        self.cert.write_text("test certificate")
        self.key.write_text("test private key")
        self.tms_backend = self.directory / "TMSBackend"
        self.tms_backend.mkdir()
        (self.tms_backend / "Cargo.toml").write_text("[workspace]\n")
        (self.tms_backend / ".env").write_text("UPSTREAM_BASE_URL=http://backend.test\n")
        self.build_script = self.commands / "build-with-srt"
        self.build_script.write_text(
            "#!/usr/bin/env bash\n"
            "set -eu\n"
            "printf 'build %s\\n' \"$*\" >>\"$CALLS\"\n"
            "mkdir -p \"$CARGO_TARGET_DIR/release\"\n"
            "touch \"$CARGO_TARGET_DIR/release/json-rpc-gateway\"\n"
        )
        self.build_script.chmod(0o755)
        self.stub("cargo", """
printf "cargo %s\\n" "$*" >>"$CALLS"
[[ "$*" == *"main-tms-server"* ]]
mkdir -p "$CARGO_TARGET_DIR/release"
touch "$CARGO_TARGET_DIR/release/main-tms-server"
""")
        self.env = os.environ.copy()
        self.env.pop("SUDO_PASSWORD", None)
        self.env.update(
            PATH=f"{self.commands}:{self.env['PATH']}",
            CAPTURE=str(self.capture),
            CALLS=str(self.directory / "calls"),
            CARGO_TARGET_DIR=str(self.directory / "target"),
            TMS_BACKEND_ROOT=str(self.tms_backend),
            GATEWAY_PORT="9999",
            GATEWAY_BUILD_SCRIPT=str(self.build_script),
            GATEWAY_TLS_CERT_FILE=str(self.cert),
            GATEWAY_TLS_KEY_FILE=str(self.key),
        )
        self.stub("hostname", 'printf "%s\\n" "192.168.1.131"')
        self.stub("ssh", """
printf "ssh %s\\n" "$*" >>"$CALLS"
if [[ "$*" == *"mktemp -d /tmp/jrpc-protocols."* ]]; then
    printf '%s\\n' /tmp/jrpc-protocols.abcdef
fi
""")
        self.stub("scp", 'printf "scp %s\\n" "$*" >>"$CALLS"; cp "${@:2:$#-2}" "$CAPTURE/"')
        self.stub("sudo", """
printf "sudo %s\\n" "$*" >>"$CALLS"
if [[ "$*" == *install-node.sh* ]]; then
    install_stage="$(dirname "$2")"
    cp "$install_stage"/* "$CAPTURE/"
    rm -f "$install_stage"/*
    rmdir "$install_stage"
elif [[ "$*" == *enable-protocols.sh* ]]; then
    for argument in "$@"; do
        if [[ "$argument" == */enable-protocols.sh ]]; then
            protocol_stage="$(dirname "$argument")"
            break
        fi
    done
    cp "$protocol_stage"/* "$CAPTURE/"
    rm -f "$protocol_stage"/*
    rmdir "$protocol_stage"
fi
""")
        self.stub("systemctl", "exit 0")
        self.stub("timeout", "exit 0")

    def stub(self, name, body):
        path = self.commands / name
        path.write_text("#!/usr/bin/env bash\nset -eu\n" + body + "\n")
        path.chmod(0o755)

    def run_script(self, name):
        subprocess.run(
            ["bash", str(ROOT / "scripts" / name)],
            env=self.env, check=True, capture_output=True, text=True,
        )

    def assert_topology(self):
        entries = json.loads((self.capture / "backends.json").read_text())["backends"]
        self.assertEqual([entry["id"] for entry in entries], ["backend-1", "backend-2"])
        self.assertEqual([entry["host"] for entry in entries], ["192.168.1.132"] * 2)
        self.assertEqual([entry["ports"] for entry in entries], [
            {"tcp": 8081, "udp": 8081}, {"tcp": 8082, "udp": 8082},
        ])
        self.assertIn("GATEWAY_PORT=8443\n", (self.capture / "gateway.env").read_text())
        self.assertIn("CONTROL_PORT=8500\n", (self.capture / "gateway.env").read_text())
        self.assertEqual(len(list(self.capture.glob("backend-*.env"))), 2)
        self.assertEqual(len(list(self.capture.glob("json-rpc-*.service"))), 3)
        for index in (1, 2):
            tls_port = 8082 + index
            self.assertIn(
                f"BACKEND_ID=backend-{index}\nBACKEND_BIND=192.168.1.132\n",
                (self.capture / f"backend-{index}.env").read_text(),
            )
            backend_env = (self.capture / f"backend-{index}.env").read_text()
            self.assertIn(f"BACKEND_TLS_PORT={tls_port}\n", backend_env)
            self.assertIn("GATEWAY_CONTROL_ADDR=192.168.1.132:8500\n", backend_env)
            self.assertIn("JSON_RPC_HTTP_URL=http://192.168.1.132:8508\n", backend_env)
            unit = (self.capture / f"json-rpc-backend-{index}.service").read_text()
            self.assertIn("ExecStart=/opt/json-rpc-ecosystem/bin/main-tms-server", unit)
            self.assertIn(
                f"EnvironmentFile=/etc/json-rpc-ecosystem/backend-{index}.env",
                unit,
            )
        unit = (self.capture / "json-rpc-gateway.service").read_text()
        self.assertIn("Requires=json-rpc-backend-1.service json-rpc-backend-2.service", unit)
        self.assertIn("StateDirectory=json-rpc-gateway", unit)
        self.assertNotIn("backend-131", unit)
        self.assertIn("test certificate", (self.capture / "server-fullchain.pem").read_text())
        self.assertIn("test private key", (self.capture / "server-key.pem").read_text())
        self.assertTrue((self.capture / "json-rpc-gateway").is_file())
        self.assertTrue((self.capture / "main-tms-server").is_file())
        self.assertFalse((self.capture / "gateway-demo-backend").exists())
        self.assertEqual(
            (self.capture / "tms-backend.env").read_text(),
            "UPSTREAM_BASE_URL=http://backend.test\n",
        )
        self.assertTrue((self.capture / "enable-protocols.sh").is_file())

    def test_remote_deployment_generates_only_two_colocated_backends(self):
        self.run_script("deploy-lan-ecosystem.sh")
        self.assert_topology()
        calls = (self.directory / "calls").read_text()
        self.assertIn("dev@192.168.1.132", calls)
        self.assertNotIn("dev@192.168.1.131", calls)
        self.assertFalse(any(line.startswith("sudo ") for line in calls.splitlines()))
        self.assertIn("--features srt", calls)
        self.assertIn("--features tms,map", calls)
        self.assertIn("enable-protocols.sh", calls)
        self.assertNotIn("gateway-demo-backend", calls)

    def test_local_deployment_does_not_use_ssh(self):
        self.stub("hostname", 'printf "%s\\n" "192.168.1.132"')
        self.run_script("deploy-lan-ecosystem.sh")
        self.assert_topology()
        calls = (self.directory / "calls").read_text()
        self.assertNotIn("ssh ", calls)
        self.assertIn("enable-protocols.sh", calls)

    def test_lifecycle_targets_only_three_services_on_gateway_node(self):
        self.run_script("start-lan-ecosystem.sh")
        self.run_script("stop-lan-ecosystem.sh")
        calls = (self.directory / "calls").read_text()
        self.assertNotIn("backend-131", calls)
        self.assertNotIn("backend-132-", calls)
        for unit in ("json-rpc-gateway", "json-rpc-backend-1", "json-rpc-backend-2"):
            self.assertIn(unit + ".service", calls)
        self.assertIn("dev@192.168.1.132", calls)
        for script in ("start-lan-ecosystem.sh", "stop-lan-ecosystem.sh"):
            self.assertNotIn("SUDO_PASSWORD", (ROOT / "scripts" / script).read_text())

    def test_protocol_readiness_checks_application_method_discovery(self):
        source = (ROOT / "scripts/enable-lan-gateway-protocols-node.sh").read_text()
        readiness = source.split(
            'if python3 - "$gateway_host" "$control_port" <<\'PY\'\n', 1
        )[1].split("\nPY", 1)[0]
        result = subprocess.run(
            ["python3", "-c", "import sys; compile(sys.stdin.read(), '<readiness>', 'exec')"],
            input=readiness, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        for method in ("login", "tracking.ingest", "map.load", "map.click"):
            self.assertIn(f'"{method}"', readiness)

    def test_persistent_registry_guard_accepts_only_matching_topology(self):
        self.run_script("deploy-lan-ecosystem.sh")
        seed = self.capture / "backends.json"
        entries = json.loads(seed.read_text())["backends"]
        saved_path = self.directory / "saved.json"
        installer = (ROOT / "scripts/install-lan-ecosystem-node.sh").read_text()
        check = installer.split('python3 - "$stage/backends.json" <<\'PY\'\n', 1)[1].split("\nPY", 1)[0]
        check = check.replace("/var/lib/json-rpc-gateway/backends.json", str(saved_path))
        saved = {"version": 1, "backends": [
            {"config": entry, "enabled": False} for entry in entries
        ]}
        for expected_status, snapshot in [
            (0, saved),
            (1, {"version": 1, "backends": saved["backends"][:1]}),
            (1, {"version": 2, "backends": saved["backends"]}),
        ]:
            saved_path.write_text(json.dumps(snapshot))
            result = subprocess.run(
                ["python3", "-c", check, str(seed)], capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, expected_status, result.stderr)
            self.assertEqual(json.loads(saved_path.read_text()), snapshot)
        saved["backends"][0]["config"]["ports"]["tcp"] = 19999
        saved_path.write_text(json.dumps(saved))
        self.assertNotEqual(subprocess.run(
            ["python3", "-c", check, str(seed)], capture_output=True,
        ).returncode, 0)


if __name__ == "__main__":
    unittest.main()
