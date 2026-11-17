"""Static checks on deploy/k8s: settings coverage, probes, resources, drain timing."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from workflow_engine.config import Settings

ROOT = Path(__file__).resolve().parents[2]
K8S = ROOT / "deploy" / "k8s"
APP_COMPONENTS = ("api", "worker", "maintenance")


def read_env(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            out[key] = value
    return out


def load(name: str) -> list[dict[str, Any]]:
    return [d for d in yaml.safe_load_all((K8S / name).read_text()) if d]


def workload(name: str) -> dict[str, Any]:
    for file in (f"{name}.yaml", "migrate-job.yaml"):
        if (K8S / file).exists():
            for doc in load(file):
                if doc["kind"] in ("Deployment", "StatefulSet", "Job") and (
                    doc["metadata"]["name"] == name
                ):
                    return doc
    raise AssertionError(f"no workload named {name}")


def pod_spec(name: str) -> dict[str, Any]:
    spec: dict[str, Any] = workload(name)["spec"]["template"]["spec"]
    return spec


def test_every_setting_is_configured_exactly_once() -> None:
    config, secrets = read_env(K8S / "config.env"), read_env(K8S / "secrets.example.env")
    assert set(secrets) == {"POSTGRES_PASSWORD", "API_KEYS"}
    assert not set(config) & set(secrets)
    # Same keys as .env.example, minus the compose-only host port for Redis.
    expected = set(read_env(ROOT / ".env.example")) - {"REDIS_PORT"}
    assert set(config) | set(secrets) == expected


def test_config_env_is_valid_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in {
        **read_env(K8S / "config.env"),
        **read_env(K8S / "secrets.example.env"),
    }.items():
        monkeypatch.setenv(key, value)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.worker_concurrency == 1  # one worker = one job at a time
    assert "@postgres:5432/workflow" in settings.sqlalchemy_url


def test_worker_grace_period_exceeds_drain_timeout() -> None:
    drain = float(read_env(K8S / "config.env")["WORKER_SHUTDOWN_GRACE_SECONDS"])
    assert pod_spec("worker")["terminationGracePeriodSeconds"] > drain + 5


@pytest.mark.parametrize("name", [*APP_COMPONENTS, "postgres", "redis", "migrate"])
def test_containers_have_requests_and_memory_limits(name: str) -> None:
    spec = pod_spec(name)
    for container in spec["containers"] + spec.get("initContainers", []):
        resources = container["resources"]
        assert resources["requests"]["memory"]
        assert resources["limits"]["memory"]


@pytest.mark.parametrize("name", [*APP_COMPONENTS, "postgres", "redis"])
def test_long_running_containers_have_probes(name: str) -> None:
    for container in pod_spec(name)["containers"]:
        assert "readinessProbe" in container
        assert "livenessProbe" in container


def test_probe_targets_match_settings() -> None:
    config = read_env(K8S / "config.env")
    api = pod_spec("api")["containers"][0]
    assert api["readinessProbe"]["httpGet"]["path"] == "/health"
    # Liveness must not depend on Postgres/Redis, or an outage restarts every API pod.
    assert "tcpSocket" in api["livenessProbe"]
    for name, port_key in (
        ("worker", "WORKER_METRICS_PORT"),
        ("maintenance", "MAINTENANCE_METRICS_PORT"),
    ):
        container = pod_spec(name)["containers"][0]
        assert container["ports"][0]["containerPort"] == int(config[port_key])
        assert container["livenessProbe"]["httpGet"] == {"path": "/metrics", "port": "metrics"}


@pytest.mark.parametrize("name", APP_COMPONENTS)
def test_app_pods_run_as_non_root_and_wait_for_migrations(name: str) -> None:
    spec = pod_spec(name)
    assert spec["securityContext"]["runAsNonRoot"] is True
    assert spec["securityContext"]["runAsUser"] == 10001  # the image's `app` user
    assert [c["name"] for c in spec["initContainers"]] == ["wait-for-migrations"]
    for container in spec["containers"]:
        assert container["securityContext"]["readOnlyRootFilesystem"] is True
        assert container["securityContext"]["allowPrivilegeEscalation"] is False


def test_kind_port_mapping_matches_api_node_port() -> None:
    (service,) = [d for d in load("api.yaml") if d["kind"] == "Service"]
    (node,) = load("kind-config.yaml")[0]["nodes"]
    (mapping,) = node["extraPortMappings"]
    assert mapping["containerPort"] == service["spec"]["ports"][0]["nodePort"]
    assert mapping["listenAddress"] == "127.0.0.1"


def test_real_secrets_file_is_ignored() -> None:
    result = subprocess.run(  # noqa: S603
        ["git", "check-ignore", "-q", str(K8S / "secrets.env")],  # noqa: S607
        cwd=ROOT,
        check=False,
    )
    assert result.returncode == 0


@pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl not installed")
def test_kustomize_renders(tmp_path: Path) -> None:
    work = tmp_path / "k8s"
    shutil.copytree(K8S, work, ignore=shutil.ignore_patterns("secrets.env"))
    shutil.copy(work / "secrets.example.env", work / "secrets.env")
    out = subprocess.run(  # noqa: S603
        ["kubectl", "kustomize", str(work)],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    docs = [d for d in yaml.safe_load_all(out) if d]
    kinds = sorted(f"{d['kind']}/{d['metadata']['name']}" for d in docs)
    for expected in (
        "Deployment/api",
        "Deployment/maintenance",
        "Deployment/redis",
        "Deployment/worker",
        "Job/migrate",
        "StatefulSet/postgres",
    ):
        assert expected in kinds
    assert all(
        d["metadata"].get("namespace") == "workflow-engine"
        for d in docs
        if d["kind"] != "Namespace"
    )
    # Generated names carry a hash suffix and every reference was rewritten to it.
    (cm,) = [d["metadata"]["name"] for d in docs if d["kind"] == "ConfigMap"]
    assert cm.startswith("workflow-config-")
    assert f"name: {cm}" in out
    assert "name: workflow-config\n" not in out


def test_worker_replicas_are_left_to_scaling() -> None:
    # A fixed count would reset `make k8s-scale` on every `make k8s-up`.
    assert "replicas" not in workload("worker")["spec"]
