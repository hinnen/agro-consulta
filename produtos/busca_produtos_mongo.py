"""
Busca de produtos no espelho Mongo — delega ao mesmo motor do PDV (``index_codigos`` + texto).

Use este módulo em telas que não devem duplicar filtros (estoque, integrações futuras, etc.).
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
    from produtos.views import motor_de_busca_agro, obter_conexao_mongo

    client, db = obter_conexao_mongo()
    if db is None or client is None:
        return []
    return motor_de_busca_agro(
        termo,
        db,
        client,
        limit=limit,
        include_inactive=include_inactive,
        regex_stage2_cap=regex_stage2_cap,
        regex_stage3_cap=regex_stage3_cap,
        regex_stage3b_cap=regex_stage3b_cap,
    )
