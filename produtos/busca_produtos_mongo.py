"""
Busca de produtos no espelho Mongo — delega ao mesmo pipeline da Consulta/PDV
(``motor_de_busca_agro`` + merge de códigos do overlay), exposto em
``motor_busca_consulta_documentos`` em ``produtos.views``.
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
    """Mesmo pipeline que ``/api/buscar/?q=`` (motor + overlay). Parâmetros ``regex_stage*`` ignorados (compat.)."""
    from produtos.views import motor_busca_consulta_documentos, obter_conexao_mongo

    del regex_stage2_cap, regex_stage3_cap, regex_stage3b_cap
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
