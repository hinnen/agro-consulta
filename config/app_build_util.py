"""
Versão exibida no BI + carimbo de deploy (``config/build_stamp.json``).

O stamp é atualizado no build (``record_deploy``) e na subida do worker (``sync_build_stamp``),
para refletir cada deploy mesmo se o buildCommand do Render não rodar o script.
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
_STAMP_FILE = Path(__file__).resolve().parent / "build_stamp.json"
_MANIFEST_FILE = Path(__file__).resolve().parent / "deploy_manifest.json"


def read_app_version() -> str:
    try:
        raw = _VERSION_FILE.read_text(encoding="utf-8").strip()
        return raw or "1.0"
    except OSError:
        return "1.0"


def _git_rev(*, short: bool) -> str:
    env = (os.environ.get("RENDER_GIT_COMMIT") or "").strip()
    if env:
        return env[:12] if short else env
    try:
        cmd = ["git", "rev-parse"]
        if short:
            cmd.append("--short=12")
        cmd.append("HEAD")
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=3, cwd=_BASE_DIR)
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


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_build_stamp() -> dict[str, Any]:
    try:
        data = json.loads(_STAMP_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def _write_build_stamp(stamp: dict[str, Any]) -> None:
    try:
        _STAMP_FILE.write_text(
            json.dumps(stamp, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError:
        logger.warning("build_stamp: não foi possível gravar %s", _STAMP_FILE, exc_info=True)


def sync_build_stamp(*, force: bool = False) -> dict[str, Any]:
    """Grava carimbo quando a versão ou o commit mudou (startup / build)."""
    version = read_app_version()
    commit_full = _git_rev(short=False) or _git_rev(short=True)
    commit_short = commit_full[:12] if commit_full else ""
    branch = _git_branch()
    now = _utc_now()

    stamp = load_build_stamp()
    prev_version = str(stamp.get("version") or "").strip()
    prev_commit = str(stamp.get("commit") or "").strip()[:12]

    if not force and prev_version == version and prev_commit == commit_short and commit_short:
        return stamp

    deploys: list[dict[str, str]] = []
    if prev_version == version and isinstance(stamp.get("version_deploys"), list):
        for row in stamp["version_deploys"]:
            if isinstance(row, dict) and str(row.get("commit") or "").strip():
                deploys.append(
                    {
                        "commit": str(row["commit"])[:12],
                        "commit_full": str(row.get("commit_full") or row["commit"])[:64],
                        "built_at": str(row.get("built_at") or "")[:32],
                        "branch": str(row.get("branch") or "")[:80],
                    }
                )

    if prev_version != version:
        build_num = 1 if commit_short else 0
        deploys = []
    else:
        try:
            build_num = max(0, int(stamp.get("build") or 0))
        except (TypeError, ValueError):
            build_num = 0
        if commit_short and commit_short != prev_commit:
            build_num = max(1, build_num + 1)

    if commit_short and not any(d.get("commit") == commit_short for d in deploys):
        deploys.append(
            {
                "commit": commit_short,
                "commit_full": commit_full[:64] if commit_full else commit_short,
                "built_at": now,
                "branch": branch,
            }
        )
    elif force and commit_short:
        for row in deploys:
            if row.get("commit") == commit_short:
                row["built_at"] = now
                break

    new_stamp = {
        "version": version,
        "build": build_num,
        "commit": commit_short,
        "commit_full": commit_full[:64] if commit_full else commit_short,
        "branch": branch,
        "built_at": now,
        "version_deploys": deploys[-50:],
    }
    _write_build_stamp(new_stamp)
    return new_stamp


def get_app_build_info() -> dict[str, Any]:
    stamp = load_build_stamp()
    version = read_app_version()
    commit_short = _git_rev(short=True) or str(stamp.get("commit") or "")[:12]
    commit_full = _git_rev(short=False) or str(stamp.get("commit_full") or commit_short)
    branch = _git_branch() or str(stamp.get("branch") or "")

    try:
        build_num = int(stamp.get("build") or 0)
    except (TypeError, ValueError):
        build_num = 0

    version_label = version
    if build_num > 0:
        version_label = f"{version}.{build_num}"

    deploys = []
    if isinstance(stamp.get("version_deploys"), list):
        for row in stamp["version_deploys"]:
            if isinstance(row, dict) and str(row.get("commit") or "").strip():
                deploys.append(dict(row))

    if commit_short and not any(d.get("commit") == commit_short for d in deploys):
        deploys.append(
            {
                "commit": commit_short,
                "commit_full": commit_full,
                "built_at": str(stamp.get("built_at") or ""),
                "branch": branch,
            }
        )

    built_at = str(stamp.get("built_at") or "")
    if commit_short and str(stamp.get("commit") or "")[:12] != commit_short:
        built_at = ""

    return {
        "version": version,
        "version_label": version_label,
        "build": build_num,
        "commit": commit_short,
        "commit_full": commit_full,
        "branch": branch,
        "built_at": built_at,
        "version_commits": deploys[-20:],
    }


def record_deploy_build() -> dict[str, Any]:
    """Build Render: sincroniza carimbo e mantém manifest legado."""
    stamp = sync_build_stamp(force=True)
    commit_short = str(stamp.get("commit") or "")
    if not commit_short:
        return {"ok": False, "erro": "commit não detectado"}

    manifest = {"builds": []}
    try:
        manifest = json.loads(_MANIFEST_FILE.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            manifest = {"builds": []}
    except (OSError, json.JSONDecodeError):
        manifest = {"builds": []}

    builds = list(manifest.get("builds") or [])
    version = str(stamp.get("version") or read_app_version())
    if not any(
        isinstance(row, dict)
        and str(row.get("version") or "").strip() == version
        and str(row.get("commit") or "").strip()[:12] == commit_short[:12]
        for row in builds
    ):
        builds.append(
            {
                "version": version,
                "commit": commit_short[:12],
                "commit_full": str(stamp.get("commit_full") or commit_short)[:64],
                "branch": str(stamp.get("branch") or ""),
                "recorded_at": str(stamp.get("built_at") or _utc_now()),
            }
        )
        manifest["builds"] = builds[-200:]
        try:
            _MANIFEST_FILE.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        except OSError:
            pass

    return {"ok": True, "version": version, "commit": commit_short, "build": stamp.get("build")}
