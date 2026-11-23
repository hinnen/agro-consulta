"""Aviso de etiqueta quando o preço da prateleira muda. Vale nas duas lojas."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from django.utils import timezone

from produtos.models import (
    EtiquetaPrecoGondolaAgro,
    EtiquetaPrecoPromoBaseAgro,
    ProdutoGestaoOverlayAgro,
    PromocaoAgro,
    PromocaoProdutoAgro,
)
from produtos.pdv_deposito_util import DEPOSITO_CENTRO, DEPOSITO_VILA, normalizar_deposito

DEPOSITOS = (DEPOSITO_CENTRO, DEPOSITO_VILA)


def _dinheiro(val) -> Decimal | None:
    if val is None:
        return None
    s = str(val).strip().replace("R$", "").replace(" ", "").replace("—", "")
    if not s:
        return None
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _fmt_br(val: Decimal) -> str:
    return f"{val:.2f}".replace(".", ",")


def _dados_overlay(pid: str) -> tuple[Decimal | None, str, str]:
    ov = (
        ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid)
        .only("preco_venda", "nome", "codigo_nfe", "codigo_barras")
        .first()
    )
    if ov is None:
        return None, "", ""
    return (
        _dinheiro(ov.preco_venda),
        str(ov.nome or "")[:300],
        str(ov.codigo_nfe or ov.codigo_barras or "")[:80],
    )


def avisar_mudanca_preco_venda(
    *,
    produto_id: str,
    antes,
    depois,
    nome: str = "",
    codigo: str = "",
) -> int:
    """Cadastro, planilha ou nota: o mesmo aviso nas duas lojas."""
    pid = str(produto_id or "").strip()[:64]
    a = _dinheiro(antes)
    b = _dinheiro(depois)
    if not pid or a is None or b is None or a == b:
        return 0
    n = 0
    for dep in DEPOSITOS:
        row, _created = EtiquetaPrecoGondolaAgro.objects.get_or_create(
            produto_externo_id=pid,
            deposito=dep,
            defaults={
                "nome": (nome or "")[:300],
                "codigo": (codigo or "")[:80],
                "preco_ok": a,
                "preco_atual": b,
                "pendente": True,
            },
        )
        if row.preco_ok is None:
            row.preco_ok = a
        row.preco_atual = b
        if nome:
            row.nome = nome[:300]
        if codigo:
            row.codigo = codigo[:80]
        row.pendente = row.preco_atual != row.preco_ok
        if not row.pendente:
            row.ok_em = timezone.now()
        row.save()
        if row.pendente:
            n += 1
    return n


def _preco_prateleira_promo(promo: dict, base: Decimal) -> Decimal:
    tipo = str(promo.get("tipo") or "")
    if tipo == PromocaoAgro.Tipo.VALOR_DIRETO:
        pp = _dinheiro(promo.get("preco_produto_promo"))
        if pp and pp > 0:
            return pp
        py = _dinheiro(promo.get("preco_y"))
        if py and py > 0:
            return py
        return base
    py = _dinheiro(promo.get("preco_y"))
    if py and py > 0 and py != base:
        return py
    return base


def _promos_da_loja(deposito: str) -> dict[str, dict]:
    """Só o id do produto. O mapa do PDV também repete o código de barras."""
    from produtos.promocoes_util import buscar_promocoes_pdv_ativas

    bruto = buscar_promocoes_pdv_ativas(empresa=deposito, tela="pdv")
    if not bruto:
        return {}
    promo_ids = {int(p["id"]) for p in bruto.values() if p.get("id")}
    linhas = PromocaoProdutoAgro.objects.filter(promocao_id__in=promo_ids).only(
        "produto_externo_id", "nome_produto", "codigo", "preco_padrao", "promocao_id"
    )
    out: dict[str, dict] = {}
    for linha in linhas:
        pid = str(linha.produto_externo_id or "").strip()[:64]
        if not pid or pid in out:
            continue
        promo = bruto.get(pid)
        if not promo:
            continue
        out[pid] = {
            "promo": promo,
            "nome": str(linha.nome_produto or "")[:300],
            "codigo": str(linha.codigo or "")[:80],
            "preco_padrao": _dinheiro(linha.preco_padrao),
        }
    return out


def _aplicar_prateleira(pid: str, dep: str, shelf: Decimal, nome: str, codigo: str, *, base_se_novo: Decimal) -> None:
    row = EtiquetaPrecoGondolaAgro.objects.filter(produto_externo_id=pid, deposito=dep).first()
    if row is None:
        EtiquetaPrecoGondolaAgro.objects.create(
            produto_externo_id=pid,
            deposito=dep,
            nome=nome,
            codigo=codigo,
            preco_ok=base_se_novo,
            preco_atual=shelf,
            pendente=base_se_novo != shelf,
        )
        return
    mudou = shelf != row.preco_atual or (nome and nome != row.nome)
    if not mudou:
        return
    row.preco_atual = shelf
    if nome:
        row.nome = nome
    if codigo:
        row.codigo = codigo
    row.pendente = row.preco_atual != row.preco_ok
    row.save()


def sincronizar_promocoes(deposito: str) -> None:
    """Promoção que entra ou acaba muda a etiqueta daquela loja."""
    dep = normalizar_deposito(deposito)
    ativos = _promos_da_loja(dep)
    ja_viu = EtiquetaPrecoPromoBaseAgro.objects.filter(deposito=dep).exists()
    for pid, info in ativos.items():
        base, nome_ov, cod_ov = _dados_overlay(pid)
        if base is None:
            base = info["preco_padrao"]
        if base is None:
            continue
        shelf = _preco_prateleira_promo(info["promo"], base)
        _aplicar_prateleira(
            pid,
            dep,
            shelf,
            nome_ov or info["nome"],
            cod_ov or info["codigo"],
            base_se_novo=shelf if not ja_viu else base,
        )
    fora = EtiquetaPrecoGondolaAgro.objects.filter(deposito=dep)
    if ativos:
        fora = fora.exclude(produto_externo_id__in=list(ativos.keys()))
    for row in fora:
        base, nome_ov, _cod = _dados_overlay(row.produto_externo_id)
        if base is None or base == row.preco_atual:
            continue
        row.preco_atual = base
        if nome_ov:
            row.nome = nome_ov
        row.pendente = row.preco_atual != row.preco_ok
        row.save()
    if not ja_viu:
        EtiquetaPrecoPromoBaseAgro.objects.get_or_create(deposito=dep)


def listar_pendentes(deposito: str) -> list[dict[str, Any]]:
    dep = normalizar_deposito(deposito)
    try:
        sincronizar_promocoes(dep)
    except Exception:
        pass
    out = []
    qs = EtiquetaPrecoGondolaAgro.objects.filter(deposito=dep, pendente=True).order_by("-atualizado_em", "nome")[:80]
    for row in qs:
        out.append(
            {
                "id": row.pk,
                "produto_id": row.produto_externo_id,
                "nome": row.nome or row.produto_externo_id,
                "codigo": row.codigo,
                "preco_de": _fmt_br(row.preco_ok),
                "preco_para": _fmt_br(row.preco_atual),
                "preco_para_num": float(row.preco_atual),
            }
        )
    return out


def confirmar(pk: int, deposito: str) -> bool:
    dep = normalizar_deposito(deposito)
    row = EtiquetaPrecoGondolaAgro.objects.filter(pk=pk, deposito=dep, pendente=True).first()
    if row is None:
        return False
    row.preco_ok = row.preco_atual
    row.pendente = False
    row.ok_em = timezone.now()
    row.save(update_fields=["preco_ok", "pendente", "ok_em", "atualizado_em"])
    return True


def itens_etiqueta(deposito: str) -> list[dict]:
    dep = normalizar_deposito(deposito)
    itens = []
    for row in EtiquetaPrecoGondolaAgro.objects.filter(deposito=dep, pendente=True).order_by("nome")[:80]:
        itens.append(
            {
                "id": row.produto_externo_id,
                "nome": row.nome or row.produto_externo_id,
                "codigo_gm": row.codigo,
                "codigo_barras": row.codigo if str(row.codigo).isdigit() else "",
                "preco_venda": float(row.preco_atual),
                "qtd": 1,
            }
        )
    return itens
