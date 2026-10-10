"""Reatribuição segura de código 230… para um produto do catálogo Postgres."""
from __future__ import annotations

from typing import Any

from django.db import transaction

from produtos.agro_codigo_barras_loja_util import (
    alocar_proximo_codigo_barras_loja,
    bloquear_alocacao_codigo_barras_loja,
    codigos_grupo_bip_canonico,
    ean13_checksum_ok,
)
from produtos.mongo_index_codigos import (
    CAD_EXTRAS_CB_OPCIONAIS_KEYS,
    codigos_barras_opcionais_de_cadastro_extras,
)

def _digits(raw: Any) -> str:
    return "".join(ch for ch in str(raw or "") if ch.isdigit())


def _extras_sem_grupo_antigo(extras: Any, grupo_antigo: set[str], novo: str) -> dict:
    out = dict(extras) if isinstance(extras, dict) else {}
    opcionais = [
        cb
        for cb in codigos_barras_opcionais_de_cadastro_extras(out)
        if cb not in grupo_antigo and cb != novo
    ]
    for key in CAD_EXTRAS_CB_OPCIONAIS_KEYS:
        out.pop(key, None)
    if opcionais:
        out["codigos_barras_opcionais"] = opcionais
    return out


def reatribuir_cb_loja_exclusivo(
    produto,
    *,
    esperado_atual: str,
    dry_run: bool = True,
    db=None,
    col: str | None = None,
) -> dict[str, Any]:
    """Gera EAN válido/exclusivo; aplicação exige código atual esperado."""
    from produtos.models import Produto, ProdutoGestaoOverlayAgro

    esperado = _digits(esperado_atual)
    atual = _digits(getattr(produto, "codigo_barras", ""))
    if not esperado or atual != esperado:
        raise ValueError(f"Código atual divergiu: esperado {esperado or '—'}, encontrado {atual or '—'}.")

    def _alocar() -> str:
        erro, codigo = alocar_proximo_codigo_barras_loja(db, col)
        if erro is not None or not codigo or not ean13_checksum_ok(codigo):
            raise ValueError("Não foi possível alocar um EAN-13 230… válido e exclusivo.")
        return codigo

    novo = _alocar()
    resumo = {
        "produto_id": str(getattr(produto, "produto_externo_id", "") or produto.pk),
        "codigo_anterior": atual,
        "codigo_novo": novo,
        "dry_run": bool(dry_run),
    }
    if dry_run:
        return resumo

    with transaction.atomic():
        bloquear_alocacao_codigo_barras_loja()
        atual_db = Produto.objects.select_for_update().get(pk=produto.pk)
        atual_db_cb = _digits(atual_db.codigo_barras)
        if atual_db_cb != esperado:
            raise ValueError(
                f"Código mudou durante a operação: esperado {esperado}, encontrado {atual_db_cb or '—'}."
            )
        novo = _alocar()
        atual_db.codigo_barras = novo
        atual_db.save(update_fields=["codigo_barras", "atualizado_em"])

        pid = str(atual_db.produto_externo_id or "").strip()
        if pid:
            overlay, _ = ProdutoGestaoOverlayAgro.objects.select_for_update().get_or_create(
                produto_externo_id=pid,
                defaults={},
            )
            grupo_antigo = set(codigos_grupo_bip_canonico(esperado))
            overlay.codigo_barras = novo
            overlay.cadastro_extras = _extras_sem_grupo_antigo(
                overlay.cadastro_extras,
                grupo_antigo,
                novo,
            )
            overlay.save(
                update_fields=["codigo_barras", "cadastro_extras", "atualizado_em"]
            )
    resumo["codigo_novo"] = novo
    resumo["dry_run"] = False
    return resumo
