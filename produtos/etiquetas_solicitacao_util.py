"""Solicitações de etiquetas vindas do app celular GM Lojas."""

from __future__ import annotations

from typing import Any

from django.utils import timezone

from produtos.etiquetas_fila_util import _rows_por_pids
from produtos.models import EtiquetaSolicitacaoPacoteAgro

MAX_ITENS_PACOTE = 120


def _qtd_item(raw) -> int:
    try:
        q = int(raw)
    except (TypeError, ValueError):
        q = 1
    return max(1, min(q, 999))


def normalizar_itens_solicitacao_entrada(raw) -> list[dict]:
    if not isinstance(raw, list):
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for it in raw:
        if not isinstance(it, dict):
            continue
        pid = str(it.get("id") or it.get("produto_id") or "").strip()[:64]
        if not pid or pid in seen:
            continue
        seen.add(pid)
        out.append(
            {
                "id": pid,
                "qtd": _qtd_item(it.get("qtd")),
                "nome": str(it.get("nome") or "")[:300],
            }
        )
        if len(out) >= MAX_ITENS_PACOTE:
            break
    return out


def resumo_nomes_solicitacao(itens: list[dict]) -> str:
    nomes = [str(x.get("nome") or "").strip() for x in itens if str(x.get("nome") or "").strip()]
    if not nomes:
        return "—"
    if len(nomes) == 1:
        return nomes[0][:400]
    extra = len(nomes) - 1
    return f"{nomes[0][:280]} (+{extra} produto{'s' if extra != 1 else ''})"[:400]


def total_etiquetas_itens(itens: list[dict]) -> int:
    return sum(_qtd_item(x.get("qtd")) for x in itens)


def hidratar_itens_para_fila(itens_json: list) -> list[dict]:
    """Enriquece ids salvos no pacote com preço/GM atuais do catálogo."""
    base = normalizar_itens_solicitacao_entrada(itens_json)
    if not base:
        return []
    pids = [x["id"] for x in base]
    rows = _rows_por_pids(pids, inativos=True)
    out: list[dict] = []
    for it in base:
        pid = it["id"]
        row = rows.get(pid) or {}
        nome = str(row.get("nome") or it.get("nome") or "").strip()
        out.append(
            {
                "id": pid,
                "nome": nome[:300],
                "codigo_gm": str(
                    row.get("codigo_gm")
                    or row.get("codigo_nfe")
                    or row.get("codigo")
                    or ""
                )[:80],
                "codigo_barras": str(row.get("codigo_barras") or "")[:80],
                "preco_venda": float(row.get("preco_venda") or 0),
                "peso_etiqueta": str(row.get("peso_etiqueta") or "")[:40],
                "qtd": it["qtd"],
            }
        )
    return out


def serializar_pacote(
    pac: EtiquetaSolicitacaoPacoteAgro,
    *,
    incluir_itens: bool = False,
) -> dict:
    criado = pac.criado_em
    row: dict[str, Any] = {
        "id": pac.pk,
        "status": pac.status,
        "operador_nome": pac.operador_nome or "",
        "observacao": pac.observacao or "",
        "total_etiquetas": int(pac.total_etiquetas or 0),
        "qtd_linhas": len(pac.itens_json or []) if isinstance(pac.itens_json, list) else 0,
        "resumo_nomes": pac.resumo_nomes or "—",
        "criado_em": criado.isoformat() if criado else "",
        "criado_em_br": timezone.localtime(criado).strftime("%d/%m/%Y %H:%M") if criado else "",
    }
    if incluir_itens:
        row["itens"] = hidratar_itens_para_fila(pac.itens_json or [])
    return row


def criar_pacote_solicitacao(
    *,
    operador_nome: str,
    itens_raw,
    observacao: str = "",
) -> EtiquetaSolicitacaoPacoteAgro:
    itens = normalizar_itens_solicitacao_entrada(itens_raw)
    if not itens:
        raise ValueError("Nenhum produto na solicitação.")
    return EtiquetaSolicitacaoPacoteAgro.objects.create(
        status=EtiquetaSolicitacaoPacoteAgro.Status.PENDENTE,
        operador_nome=(operador_nome or "")[:120],
        observacao=(observacao or "")[:300],
        itens_json=itens,
        total_etiquetas=total_etiquetas_itens(itens),
        resumo_nomes=resumo_nomes_solicitacao(itens),
    )


def marcar_pacote_importado(pac: EtiquetaSolicitacaoPacoteAgro, *, usuario: str) -> EtiquetaSolicitacaoPacoteAgro:
    if pac.status != EtiquetaSolicitacaoPacoteAgro.Status.PENDENTE:
        raise ValueError("Pacote já foi importado ou cancelado.")
    pac.status = EtiquetaSolicitacaoPacoteAgro.Status.IMPORTADO
    pac.importado_em = timezone.now()
    pac.importado_por = (usuario or "")[:150]
    pac.save(update_fields=["status", "importado_em", "importado_por"])
    return pac
