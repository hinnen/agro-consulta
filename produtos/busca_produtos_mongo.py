"""
Busca de produtos — mesmo catálogo do PDV/Consulta.

Com ``AGRO_FONTE_CATALOGO=agro_pg`` (loja): Postgres via ``catalogo_agro``.
Legado: Mongo (``motor_busca_consulta_documentos``).
"""

from __future__ import annotations

from typing import Any


def buscar_produtos_motor_pdv(
    termo: str,
    *,
    limit: int = 80,
    include_inactive: bool = False,
    regex_stage2_cap: int | None = None,
    regex_stage3_cap: int | None = None,
    regex_stage3b_cap: int | None = None,
) -> list[dict[str, Any]]:
    """Mesmo pipeline que ``/api/buscar/?q=`` (Postgres ou Mongo). ``regex_stage*`` ignorados (compat.)."""
    from produtos.agro_fonte_config import (
        agro_catalogo_usa_postgres,
        agro_pdv_catalogo_somente_postgres,
    )

    del regex_stage2_cap, regex_stage3_cap, regex_stage3b_cap
    termo = str(termo or "").strip()
    if not termo:
        return []

    if agro_catalogo_usa_postgres() or agro_pdv_catalogo_somente_postgres():
        from produtos import catalogo_agro as cat_agro

        # ``prods_mongo_style`` já usa ativos; inativos só no legado Mongo.
        if include_inactive:
            rows = cat_agro.buscar(termo, limit=limit, inativos=True)
            return [cat_agro.row_para_doc_busca_pdv(r) for r in rows]
        return cat_agro.prods_mongo_style_busca_pdv(q=termo, limit=limit)

    from produtos.views import motor_busca_consulta_documentos, obter_conexao_mongo

    client, db = obter_conexao_mongo()
    if db is None or client is None:
        return []
    return motor_busca_consulta_documentos(
        termo,
        db,
        client,
        limit=limit,
        include_inactive=include_inactive,
        projection=None,
    )
