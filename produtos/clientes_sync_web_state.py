"""Estado da sync web (arquivo) — LocMem do runserver não vê o cache do subprocess."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from django.conf import settings
from django.core.cache import cache

_MAX_RUNNING_SEC = 600
_CLIENTES_SYNC_ERP_FINALIZADO_CACHE_KEY = "clientes_sync_erp_finalizado"


def _var_dir() -> Path:
    base = Path(settings.BASE_DIR) / "var"
    base.mkdir(exist_ok=True)
    return base


def _path() -> Path:
    return _var_dir() / "clientes_sync_web.json"


def _path_erp_finalizado() -> Path:
    return _var_dir() / "clientes_sync_erp_finalizado.json"


def clientes_sync_erp_disponivel() -> bool:
    """Sync manual ERP/Mongo na tela Clientes — só até a última puxada concluir."""
    if not getattr(settings, "AGRO_CLIENTES_SYNC_ERP_HABILITADO", True):
        return False
    if cache.get(_CLIENTES_SYNC_ERP_FINALIZADO_CACHE_KEY):
        return False
    fp = _path_erp_finalizado()
    if not fp.is_file():
        return True
    try:
        data = json.loads(fp.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return True
    if data.get("finalizado"):
        cache.set(_CLIENTES_SYNC_ERP_FINALIZADO_CACHE_KEY, True, timeout=None)
        return False
    return True


def marcar_clientes_sync_erp_finalizado() -> None:
    payload = {"finalizado": True, "finished": time.time()}
    _path_erp_finalizado().write_text(json.dumps(payload), encoding="utf-8")
    cache.set(_CLIENTES_SYNC_ERP_FINALIZADO_CACHE_KEY, True, timeout=None)


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
        if r.get("ok"):
            marcar_clientes_sync_erp_finalizado()
        msg = (
            f"Última importação do ERP concluída: {r.get('criados', 0)} novos, "
            f"{r.get('atualizados', 0)} atualizados, "
            f"{r.get('ignorados_editados_local', 0)} preservados (ajustados no Agro)."
        )
        if r.get("erros"):
            msg += f" {r.get('erros')} linha(s) com erro."
        if r.get("ok"):
            msg += " A partir de agora os clientes são só do Agro — o botão de importação foi removido."
        return ("success", msg)
    if status == "failed":
        clear_state()
        return ("error", f"Sincronização falhou: {st.get('erro', 'erro desconhecido')}")
    return None
