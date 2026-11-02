"""Dependency manifests: Vercel installs the web list only, Docker installs both."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_FORBIDDEN_ON_VERCEL = ("confluent-kafka", "redis")


def _requirements(name: str) -> list[str]:
    out = []
    for line in (ROOT / name).read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line and not line.startswith("-"):
            out.append(line)
    return out


def _name(spec: str) -> str:
    return re.split(r"[<>=\[ ]", spec, maxsplit=1)[0].lower()


def test_pyproject_dependencies_match_requirements_txt():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert sorted(project["dependencies"]) == sorted(_requirements("requirements.txt"))


def test_vercel_manifests_exclude_pipeline_packages():
    for spec in _requirements("requirements.txt") + _requirements("dashboard/requirements.txt"):
        assert _name(spec) not in _FORBIDDEN_ON_VERCEL, spec


def test_pipeline_manifest_carries_the_kafka_and_redis_clients():
    assert {_name(s) for s in _requirements("requirements-pipeline.txt")} == set(_FORBIDDEN_ON_VERCEL)


def test_docker_image_installs_both_manifests():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "-r requirements.txt -r requirements-pipeline.txt" in dockerfile


def test_root_entrypoint_exposes_app():
    import app as entry

    assert entry.app.title == "verisync dashboard"
