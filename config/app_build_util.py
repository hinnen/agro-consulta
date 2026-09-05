"""
Versão exibida no BI.

- **Commit / branch**: sempre de ``RENDER_GIT_COMMIT`` em produção (nunca fica preso no git).
- **Número de build** (1.01.N): gravado em ``config/build_meta.py`` no build Render.
- **Histórico**: ``deploy_manifest.json`` (build) + carimbo em /tmp na subida do worker.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent.parent
_VERSION_FILE = _BASE_DIR / "VERSION"
_MANIFEST_FILE = Path(__file__).resolve().parent / "deploy_manifest.json"
_RUNTIME_STAMP_FILE = Path(
    os.environ.get("AGRO_BUILD_STAMP_PATH")
    or Path(tempfile.gettempdir()) / "agro_build_stamp.json"
)


def _read_text_file(path: Path) -> str:
    """Lê texto; tolera VERSION salvo em UTF-16 no Windows (BOM 0xFF 0xFE)."""
    try:
        raw_bytes = path.read_bytes()
    except OSError:
        return ""
    if not raw_bytes:
        return ""
    if raw_bytes.startswith(b"\xff\xfe") or raw_bytes.startswith(b"\xfe\xff"):
        try:
            return raw_bytes.decode("utf-16").strip()
        except UnicodeDecodeError:
            pass
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw_bytes.decode(encoding).strip()
        except UnicodeDecodeError:
            continue
    return ""


def read_app_version() -> str:
    raw = _read_text_file(_VERSION_FILE)
    return raw or "1.0"


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


def _load_build_meta_module() -> dict[str, Any]:
    try:
        from config import build_meta

        data = getattr(build_meta, "APP_BUILD_META", None)
        if isinstance(data, dict):
            return dict(data)
    except Exception:
        logger.debug("build_meta indisponível", exc_info=True)
    return {}


def load_deploy_manifest() -> dict[str, Any]:
    try:
        data = json.loads(_MANIFEST_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {"builds": []}


def _load_runtime_stamp() -> dict[str, Any]:
    try:
        data = json.loads(_RUNTIME_STAMP_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def _write_runtime_stamp(stamp: dict[str, Any]) -> None:
    try:
        _RUNTIME_STAMP_FILE.write_text(
            json.dumps(stamp, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError:
        logger.warning("stamp runtime: não gravou em %s", _RUNTIME_STAMP_FILE, exc_info=True)


def _deploy_rows_for_version(version: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in load_deploy_manifest().get("builds") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("version") or "").strip() != version:
            continue
        commit = str(row.get("commit") or "").strip()[:12]
        if not commit or commit in seen:
            continue
        seen.add(commit)
        out.append(
            {
                "commit": commit,
                "commit_full": str(row.get("commit_full") or commit)[:64],
                "built_at": str(row.get("recorded_at") or row.get("built_at") or "")[:32],
                "branch": str(row.get("branch") or "")[:80],
            }
        )
    return out


def _write_build_meta_py(meta: dict[str, Any]) -> None:
    path = Path(__file__).resolve().parent / "build_meta.py"
    deploys = meta.get("version_deploys") or []
    lines = [
        '"""Gerado por scripts/record_deploy.py no build Render. Não editar à mão."""',
        "",
        "APP_BUILD_META = " + json.dumps(
            {
                "version": str(meta.get("version") or read_app_version()),
                "build": int(meta.get("build") or 0),
                "commit": str(meta.get("commit") or "")[:12],
                "commit_full": str(meta.get("commit_full") or "")[:64],
                "branch": str(meta.get("branch") or "")[:80],
                "built_at": str(meta.get("built_at") or "")[:32],
                "version_deploys": deploys[-50:],
            },
            ensure_ascii=False,
            indent=4,
        ),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def sync_build_stamp(*, force: bool = False) -> dict[str, Any]:
    """Alinha carimbo em /tmp com o commit do ambiente (subida do worker)."""
    version = read_app_version()
    commit_full = _git_rev(short=False) or _git_rev(short=True)
    commit_short = commit_full[:12] if commit_full else ""
    branch = _git_branch()
    now = _utc_now()

    if not commit_short:
        return _load_build_meta_module() or _load_runtime_stamp()

    meta = _load_build_meta_module()
    stamp = _load_runtime_stamp()

    if (
        not force
        and str(stamp.get("commit") or "")[:12] == commit_short
        and str(stamp.get("version") or "").strip() == version
    ):
        return stamp

    build_num = 0
    if str(meta.get("commit") or "")[:12] == commit_short:
        try:
            build_num = max(0, int(meta.get("build") or 0))
        except (TypeError, ValueError):
            build_num = 0

    if build_num <= 0:
        try:
            build_num = max(0, int(stamp.get("build") or 0))
        except (TypeError, ValueError):
            build_num = 0

    if build_num <= 0:
        rows = _deploy_rows_for_version(version)
        build_num = max(1, len(rows)) if rows else 1
        if commit_short and not any(r.get("commit") == commit_short for r in rows):
            build_num = max(build_num, len(rows) + 1)

    deploys: list[dict[str, str]] = []
    if str(meta.get("version") or "").strip() == version and isinstance(meta.get("version_deploys"), list):
        for row in meta["version_deploys"]:
            if isinstance(row, dict) and str(row.get("commit") or "").strip():
                deploys.append(
                    {
                        "commit": str(row["commit"])[:12],
                        "commit_full": str(row.get("commit_full") or row["commit"])[:64],
                        "built_at": str(row.get("built_at") or "")[:32],
                        "branch": str(row.get("branch") or "")[:80],
                    }
                )
    if not deploys:
        deploys = _deploy_rows_for_version(version)

    if commit_short and not any(d.get("commit") == commit_short for d in deploys):
        deploys.append(
            {
                "commit": commit_short,
                "commit_full": commit_full[:64] if commit_full else commit_short,
                "built_at": now,
                "branch": branch,
            }
        )

    new_stamp = {
        "version": version,
        "build": build_num,
        "commit": commit_short,
        "commit_full": commit_full[:64] if commit_full else commit_short,
        "branch": branch,
        "built_at": now,
        "version_deploys": deploys[-50:],
    }
    _write_runtime_stamp(new_stamp)
    return new_stamp


def get_app_build_info() -> dict[str, Any]:
    """Sempre reflete o commit do ambiente; build vem do artefato do último deploy."""
    version = read_app_version()
    commit_short = _git_rev(short=True)
    commit_full = _git_rev(short=False) or commit_short
    branch = _git_branch()

    meta = _load_build_meta_module()
    stamp = sync_build_stamp()

    build_num = 0
    built_at = ""
    deploys: list[dict[str, str]] = []

    if commit_short and str(meta.get("commit") or "")[:12] == commit_short:
        try:
            build_num = int(meta.get("build") or 0)
        except (TypeError, ValueError):
            build_num = 0
        built_at = str(meta.get("built_at") or "")
        if isinstance(meta.get("version_deploys"), list):
            deploys = [dict(r) for r in meta["version_deploys"] if isinstance(r, dict)]

    if build_num <= 0 and str(stamp.get("commit") or "")[:12] == commit_short:
        try:
            build_num = int(stamp.get("build") or 0)
        except (TypeError, ValueError):
            build_num = 0
        if not built_at:
            built_at = str(stamp.get("built_at") or "")
        if not deploys and isinstance(stamp.get("version_deploys"), list):
            deploys = [dict(r) for r in stamp["version_deploys"] if isinstance(r, dict)]

    if build_num <= 0:
        rows = _deploy_rows_for_version(version)
        build_num = max(1, len(rows))
        if commit_short and not any(r.get("commit") == commit_short for r in rows):
            build_num = max(build_num, len(rows) + 1)
        deploys = deploys or rows

    if commit_short and not any(d.get("commit") == commit_short for d in deploys):
        deploys.append(
            {
                "commit": commit_short,
                "commit_full": commit_full,
                "built_at": built_at or _utc_now(),
                "branch": branch,
            }
        )

    if not built_at and commit_short:
        for row in reversed(deploys):
            if row.get("commit") == commit_short and row.get("built_at"):
                built_at = str(row["built_at"])
                break

    version_label = f"{version}.{build_num}" if build_num > 0 else version

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
    """Build Render: incrementa build, grava build_meta.py e manifest."""
    version = read_app_version()
    commit_full = _git_rev(short=False) or _git_rev(short=True)
    if not commit_full:
        return {"ok": False, "erro": "commit não detectado (RENDER_GIT_COMMIT ou git)"}

    commit_short = commit_full[:12]
    branch = _git_branch()
    now = _utc_now()

    meta = _load_build_meta_module()
    manifest = load_deploy_manifest()
    builds = list(manifest.get("builds") or [])

    prev_build = 0
    if str(meta.get("version") or "").strip() == version:
        try:
            prev_build = max(0, int(meta.get("build") or 0))
        except (TypeError, ValueError):
            prev_build = 0

    version_rows = [b for b in builds if isinstance(b, dict) and str(b.get("version") or "").strip() == version]
    if str(meta.get("commit") or "")[:12] == commit_short and prev_build > 0:
        build_num = prev_build
    elif any(str(b.get("commit") or "")[:12] == commit_short for b in version_rows):
        build_num = max(prev_build, len(version_rows)) or 1
    else:
        build_num = max(prev_build, len(version_rows)) + 1

    deploys: list[dict[str, str]] = []
    if str(meta.get("version") or "").strip() == version and isinstance(meta.get("version_deploys"), list):
        for row in meta["version_deploys"]:
            if isinstance(row, dict) and str(row.get("commit") or "").strip():
                deploys.append(
                    {
                        "commit": str(row["commit"])[:12],
                        "commit_full": str(row.get("commit_full") or row["commit"])[:64],
                        "built_at": str(row.get("built_at") or "")[:32],
                        "branch": str(row.get("branch") or "")[:80],
                    }
                )

    for row in version_rows:
        c = str(row.get("commit") or "").strip()[:12]
        if c and not any(d.get("commit") == c for d in deploys):
            deploys.append(
                {
                    "commit": c,
                    "commit_full": str(row.get("commit_full") or c)[:64],
                    "built_at": str(row.get("recorded_at") or "")[:32],
                    "branch": str(row.get("branch") or "")[:80],
                }
            )

    if not any(d.get("commit") == commit_short for d in deploys):
        deploys.append(
            {
                "commit": commit_short,
                "commit_full": commit_full[:64],
                "built_at": now,
                "branch": branch,
            }
        )

    new_meta = {
        "version": version,
        "build": build_num,
        "commit": commit_short,
        "commit_full": commit_full[:64],
        "branch": branch,
        "built_at": now,
        "version_deploys": deploys[-50:],
    }
    _write_build_meta_py(new_meta)
    _write_runtime_stamp(new_meta)

    if not any(
        isinstance(row, dict)
        and str(row.get("version") or "").strip() == version
        and str(row.get("commit") or "").strip()[:12] == commit_short
        for row in builds
    ):
        builds.append(
            {
                "version": version,
                "commit": commit_short,
                "commit_full": commit_full[:64],
                "branch": branch,
                "recorded_at": now,
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

    return {"ok": True, "version": version, "commit": commit_short, "build": build_num}
