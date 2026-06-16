"""
Versão exibida no BI + histórico de commits por release (``config/deploy_manifest.json``).
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent.parent
_VERSION_FILE = _BASE_DIR / "VERSION"
_MANIFEST_FILE = Path(__file__).resolve().parent / "deploy_manifest.json"
_CACHE: dict[str, Any] | None = None


def read_app_version() -> str:
    try:
        raw = _VERSION_FILE.read_text(encoding="utf-8").strip()
        return raw or "1.0"
    except OSError:
        return "1.0"


def _git_rev(*, short: bool) -> str:
    env_key = "RENDER_GIT_COMMIT" if short else "RENDER_GIT_COMMIT_FULL"
    env = (os.environ.get(env_key) or os.environ.get("RENDER_GIT_COMMIT") or "").strip()
    if env:
        return env[:12] if short else env
    flag = "--short=12" if short else ""
    try:
        cmd = ["git", "rev-parse"]
        if short:
            cmd.append("--short=12")
        cmd.append("HEAD")
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3,
            cwd=_BASE_DIR,
        )
        if r.returncode == 0:
            s = (r.stdout or "").strip()
            if s:
                return s[:12] if short else s
    except Exception:
        logger.debug("git rev-parse indisponível", exc_info=True)
    return ""


def _git_branch() -> str:
    env = (os.environ.get("RENDER_GIT_BRANCH") or os.environ.get("GIT_BRANCH") or "").strip()
    if env:
        return env[:80]
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=3,
            cwd=_BASE_DIR,
        )
        if r.returncode == 0:
            return (r.stdout or "").strip()[:80]
    except Exception:
        pass
    return ""


def load_deploy_manifest() -> dict[str, Any]:
    try:
        data = json.loads(_MANIFEST_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {"builds": []}


def commits_for_version(version: str, manifest: dict[str, Any] | None = None) -> list[dict[str, str]]:
    """Deploys registrados para ``version`` (ordem cronológica)."""
    m = manifest if manifest is not None else load_deploy_manifest()
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in m.get("builds") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("version") or "").strip() != version:
            continue
        commit = str(row.get("commit") or "").strip()
        if not commit or commit in seen:
            continue
        seen.add(commit)
        out.append(
            {
                "commit": commit[:12],
                "commit_full": str(row.get("commit_full") or commit)[:64],
                "recorded_at": str(row.get("recorded_at") or "")[:32],
                "branch": str(row.get("branch") or "")[:80],
            }
        )
    return out


def get_app_build_info() -> dict[str, Any]:
    global _CACHE
    if _CACHE is not None:
        return dict(_CACHE)

    version = read_app_version()
    commit_short = _git_rev(short=True)
    commit_full = _git_rev(short=False) or commit_short
    manifest = load_deploy_manifest()
    history = commits_for_version(version, manifest)

    if commit_short and not any(h.get("commit") == commit_short for h in history):
        history.append(
            {
                "commit": commit_short,
                "commit_full": commit_full,
                "recorded_at": "",
                "branch": _git_branch(),
            }
        )

    latest = (manifest.get("builds") or [])[-1] if isinstance(manifest.get("builds"), list) else {}
    built_at = ""
    if isinstance(latest, dict):
        built_at = str(latest.get("recorded_at") or "")[:32]

    info = {
        "version": version,
        "commit": commit_short,
        "commit_full": commit_full,
        "branch": _git_branch(),
        "built_at": built_at,
        "version_commits": history,
    }
    _CACHE = info
    return dict(info)


def record_deploy_build() -> dict[str, Any]:
    """Chamado no build (Render) para acrescentar commit ao manifest da versão atual."""
    version = read_app_version()
    commit_full = _git_rev(short=False) or _git_rev(short=True)
    if not commit_full:
        return {"ok": False, "erro": "commit não detectado"}

    commit_short = commit_full[:12]
    manifest = load_deploy_manifest()
    builds = list(manifest.get("builds") or [])
    for row in builds:
        if (
            isinstance(row, dict)
            and str(row.get("version") or "").strip() == version
            and str(row.get("commit") or "").strip()[:12] == commit_short
        ):
            return {"ok": True, "skipped": True, "version": version, "commit": commit_short}

    builds.append(
        {
            "version": version,
            "commit": commit_short,
            "commit_full": commit_full[:64],
            "branch": _git_branch(),
            "recorded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    )
    manifest["builds"] = builds[-200:]
    _MANIFEST_FILE.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    global _CACHE
    _CACHE = None
    return {"ok": True, "version": version, "commit": commit_short}
