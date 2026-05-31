"""Estado da sync web (arquivo) — LocMem do runserver não vê o cache do subprocess."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from django.conf import settings

_MAX_RUNNING_SEC = 600


def _path() -> Path:
    base = Path(settings.BASE_DIR) / "var"
    base.mkdir(exist_ok=True)
    return base / "clientes_sync_web.json"


def mark_running() -> None:
    _path().write_text(
        json.dumps({"status": "running", "started": time.time()}),
        encoding="utf-8",
    )


def mark_done(result: dict[str, Any]) -> None:
    _path().write_text(
        json.dumps({"status": "done", "finished": time.time(), "result": result}),
        encoding="utf-8",
    )


def mark_failed(erro: str) -> None:
    _path().write_text(
        json.dumps({"status": "failed", "finished": time.time(), "erro": erro}),
        encoding="utf-8",
    )


def read_state() -> dict[str, Any] | None:
    fp = _path()
    if not fp.is_file():
        return None
    try:
        return json.loads(fp.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def clear_state() -> None:
    try:
        _path().unlink(missing_ok=True)
    except OSError:
        pass


def sync_em_andamento() -> bool:
    st = read_state()
    if not st or st.get("status") != "running":
        return False
    started = float(st.get("started") or 0)
    if time.time() - started > _MAX_RUNNING_SEC:
        clear_state()
        return False
    return True


def consumir_mensagem_conclusao() -> tuple[str, str] | None:
    """
    Se a sync terminou, retorna ('success'|'error', texto) e limpa o arquivo.
    """
    st = read_state()
    if not st:
        return None
    status = st.get("status")
    if status == "done":
        r = st.get("result") or {}
        clear_state()
        msg = (
            f"Sincronização concluída: {r.get('criados', 0)} novos, "
            f"{r.get('atualizados', 0)} atualizados, "
            f"{r.get('ignorados_editados_local', 0)} preservados (ajustados no Agro). "
            f"Fontes: Mongo {r.get('linhas_mongo', 0)} linhas, "
            f"ERP {r.get('linhas_erp', 0)} linhas."
        )
        if r.get("erros"):
            msg += f" {r.get('erros')} linha(s) com erro."
        return ("success", msg)
    if status == "failed":
        clear_state()
        return ("error", f"Sincronização falhou: {st.get('erro', 'erro desconhecido')}")
    return None
