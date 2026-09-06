"""Rotina noturna de backup Postgres — gera ZIPs e envia se configurado (FL-048)."""
from __future__ import annotations

from typing import Any

from django.conf import settings

from produtos.pg_backup_disaster_kit import build_disaster_kit_zip
from produtos.pg_backup_registry import PG_BACKUP_ALL_SLUGS
from produtos.pg_backup_upload import upload_backup_blob, upload_status_resumo
from produtos.pg_backup_util import build_backup_zip


def nightly_backup_permitido() -> tuple[bool, str]:
    if not getattr(settings, "AGRO_PG_BACKUP_NIGHTLY_ENABLED", False):
        return False, "AGRO_PG_BACKUP_NIGHTLY_ENABLED não está true."
    if getattr(settings, "AGRO_STAGING_READONLY", False) and not getattr(
        settings, "AGRO_PG_BACKUP_NIGHTLY_ALLOW_STAGING", False
    ):
        return False, "Bloqueado no staging (use AGRO_PG_BACKUP_NIGHTLY_ALLOW_STAGING=true para testar)."
    return True, ""


def executar_pg_backup_nightly(
    *,
    username: str = "cron",
    upload: bool | None = None,
    incluir_por_categoria: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    if not force:
        ok, motivo = nightly_backup_permitido()
        if not ok:
            return {"ok": False, "erro": motivo}

    if upload is None:
        upload = bool((getattr(settings, "AGRO_PG_BACKUP_UPLOAD_MODE", "") or "").strip())

    artefatos: list[dict[str, Any]] = []
    erros: list[str] = []

    def _gerar_e_enviar(
        slugs: list[str] | None,
        *,
        include_kit: bool,
        tipo: str,
    ) -> None:
        try:
            blob, filename, manifest = build_backup_zip(
                slugs,
                username=username,
                include_kit=include_kit,
            )
            item: dict[str, Any] = {
                "tipo": tipo,
                "filename": filename,
                "bytes": len(blob),
                "categorias": manifest.get("categorias") or [],
                "total_registros": manifest.get("total_registros"),
            }
            if upload:
                item["upload"] = upload_backup_blob(blob, filename)
            else:
                item["upload"] = {"ok": False, "skipped": True, "motivo": "upload não solicitado"}
            artefatos.append(item)
        except Exception as exc:
            erros.append(f"{tipo}: {exc}")

    _gerar_e_enviar(None, include_kit=True, tipo="completo")

    try:
        kit_blob, kit_name = build_disaster_kit_zip()
        kit_item: dict[str, Any] = {
            "tipo": "kit_zero",
            "filename": kit_name,
            "bytes": len(kit_blob),
        }
        if upload:
            kit_item["upload"] = upload_backup_blob(kit_blob, kit_name)
        else:
            kit_item["upload"] = {"ok": False, "skipped": True, "motivo": "upload não solicitado"}
        artefatos.append(kit_item)
    except Exception as exc:
        erros.append(f"kit_zero: {exc}")

    if incluir_por_categoria:
        for slug in PG_BACKUP_ALL_SLUGS:
            _gerar_e_enviar([slug], include_kit=False, tipo=f"categoria:{slug}")

    return {
        "ok": not erros,
        "artefatos": artefatos,
        "erros": erros,
        "upload_config": upload_status_resumo(),
        "total_artefatos": len(artefatos),
    }
