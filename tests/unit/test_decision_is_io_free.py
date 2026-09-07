"""Guard rail: the decision/ package must stay dependency-free.

If a database client, a Kafka client, or an HTTP client ever gets imported in
here, the whole "unit-testable in milliseconds without infrastructure" property
is gone. Fail loudly the moment it happens.
"""

from __future__ import annotations

import ast
import pathlib

DECISION_DIR = pathlib.Path(__file__).resolve().parents[2] / "decision"

BANNED_IMPORT_ROOTS = {
    "psycopg",
    "psycopg2",
    "asyncpg",
    "sqlalchemy",
    "redis",
    "kafka",
    "confluent_kafka",
    "aiokafka",
    "requests",
    "httpx",
    "aiohttp",
    "urllib3",
    "fastapi",
    "socket",
}

STDLIB_ONLY_MODULES = {"__future__", "dataclasses", "datetime", "enum", "typing"}


def _module_files():
    return sorted(DECISION_DIR.glob("*.py"))


def test_decision_dir_exists_and_has_modules():
    files = _module_files()
    assert files, "no python modules found under decision/"


def test_no_infrastructure_client_imports_anywhere_in_decision():
    offenders: list[str] = []
    for path in _module_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            for name in names:
                root = name.split(".")[0]
                if root in BANNED_IMPORT_ROOTS:
                    offenders.append(f"{path.name}: import {name}")
    assert not offenders, "forbidden imports in decision/: " + "; ".join(offenders)


def test_decision_modules_import_only_stdlib_and_siblings():
    """Stricter than the ban-list: every third-party-looking import is rejected."""
    offenders: list[str] = []
    for path in _module_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.level > 0:  # relative sibling import, fine
                    continue
                mod = (node.module or "").split(".")[0]
                if mod not in STDLIB_ONLY_MODULES:
                    offenders.append(f"{path.name}: from {node.module} import ...")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    mod = alias.name.split(".")[0]
                    if mod not in STDLIB_ONLY_MODULES:
                        offenders.append(f"{path.name}: import {alias.name}")
    assert not offenders, "unexpected imports in decision/: " + "; ".join(offenders)
