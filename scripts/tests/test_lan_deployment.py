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
        self.env = os.environ.copy()
        self.env.pop("SUDO_PASSWORD", None)
        self.env.update(
            PATH=f"{self.commands}:{self.env['PATH']}",
            CAPTURE=str(self.capture),
            CALLS=str(self.directory / "calls"),
            CARGO_TARGET_DIR=str(self.directory / "target"),
            GATEWAY_PORT="9999",
        )
        self.stub("hostname", 'printf "%s\\n" "192.168.1.131"')
        self.stub("cargo", """
mkdir -p "$CARGO_TARGET_DIR/release"
touch "$CARGO_TARGET_DIR/release/json-rpc-gateway" "$CARGO_TARGET_DIR/release/gateway-demo-backend"
""")
        self.stub("ssh", 'printf "ssh %s\\n" "$*" >>"$CALLS"')
        self.stub("scp", 'printf "scp %s\\n" "$*" >>"$CALLS"; cp "${@:2:$#-2}" "$CAPTURE/"')
        self.stub("sudo", """
printf "sudo %s\\n" "$*" >>"$CALLS"
if [[ "$*" == *install-node.sh* ]]; then
    cp "$(dirname "$3")"/* "$CAPTURE/"
    rm -f "$(dirname "$3")"/*
    rmdir "$(dirname "$3")"
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
            self.assertIn(
                f"BACKEND_ID=backend-{index}\n",
                (self.capture / f"backend-{index}.env").read_text(),
            )
        unit = (self.capture / "json-rpc-gateway.service").read_text()
        self.assertIn("Requires=json-rpc-backend-1.service json-rpc-backend-2.service", unit)
        self.assertIn("StateDirectory=json-rpc-gateway", unit)
        self.assertNotIn("backend-131", unit)

    def test_remote_deployment_generates_only_two_colocated_backends(self):
        self.run_script("deploy-lan-ecosystem.sh")
        self.assert_topology()
        calls = (self.directory / "calls").read_text()
        self.assertIn("dev@192.168.1.132", calls)
        self.assertNotIn("dev@192.168.1.131", calls)
        self.assertFalse(any(line.startswith("sudo ") for line in calls.splitlines()))

    def test_local_deployment_does_not_use_ssh(self):
        self.stub("hostname", 'printf "%s\\n" "192.168.1.132"')
        self.run_script("deploy-lan-ecosystem.sh")
        self.assert_topology()
        self.assertNotIn("ssh ", (self.directory / "calls").read_text())

    def test_lifecycle_targets_only_three_services_on_gateway_node(self):
        self.run_script("start-lan-ecosystem.sh")
        self.run_script("stop-lan-ecosystem.sh")
        calls = (self.directory / "calls").read_text()
        self.assertNotIn("backend-131", calls)
        self.assertNotIn("backend-132-", calls)
        for unit in ("json-rpc-gateway", "json-rpc-backend-1", "json-rpc-backend-2"):
            self.assertIn(unit + ".service", calls)
        self.assertIn("dev@192.168.1.132", calls)

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
