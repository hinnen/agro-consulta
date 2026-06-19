"""Incrementa VERSION (1.01 → 1.02) — usado pelo pre-commit e pelo assistente no commit."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VERSION_FILE = ROOT / "VERSION"
BUMP_BRANCHES = frozenset({"teste", "producao"})


def parse_version(raw: str) -> tuple[int, int]:
    raw = (raw or "").strip()
    if not raw:
        return 1, 0
    parts = raw.split(".", 1)
    major = int(parts[0] or 0)
    minor = int(parts[1] or 0) if len(parts) > 1 else 0
    return major, minor


def format_version(major: int, minor: int) -> str:
    return f"{major}.{minor:02d}"


def bump_version_string(current: str) -> str:
    major, minor = parse_version(current)
    minor += 1
    if minor > 99:
        major += 1
        minor = 0
    return format_version(major, minor)


def read_version() -> str:
    try:
        return VERSION_FILE.read_text(encoding="utf-8").strip() or "1.00"
    except OSError:
        return "1.00"


def write_version(value: str) -> None:
    VERSION_FILE.write_text(f"{value.strip()}\n", encoding="utf-8")


def git_branch() -> str:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            cwd=ROOT,
            timeout=5,
        )
        if r.returncode == 0:
            return (r.stdout or "").strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return ""


def is_version_staged() -> bool:
    try:
        r = subprocess.run(
            ["git", "diff", "--cached", "--name-only", "--", "VERSION"],
            capture_output=True,
            text=True,
            cwd=ROOT,
            timeout=5,
        )
        return bool((r.stdout or "").strip())
    except (OSError, subprocess.SubprocessError):
        return False


def stage_version() -> None:
    subprocess.run(["git", "add", "VERSION"], cwd=ROOT, check=False, timeout=5)


def should_skip_hook() -> bool:
    if os.environ.get("SKIP_VERSION_BUMP") == "1":
        return True
    if is_version_staged():
        return True
    branch = git_branch()
    return branch not in BUMP_BRANCHES


def run_hook() -> int:
    if should_skip_hook():
        return 0
    current = read_version()
    new = bump_version_string(current)
    write_version(new)
    stage_version()
    print(f"VERSION: {current} -> {new}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Incrementa VERSION do SisVale.")
    parser.add_argument("--hook", action="store_true", help="Modo pre-commit (git add VERSION).")
    parser.add_argument("--print", action="store_true", help="Só imprime a próxima versão.")
    args = parser.parse_args()
    if args.hook:
        return run_hook()
    current = read_version()
    new = bump_version_string(current)
    if args.print:
        print(new)
        return 0
    write_version(new)
    print(f"VERSION: {current} -> {new}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
