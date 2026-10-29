"""Pedido / empenho / processo administrativo — grupo XML ``compra`` (NT 2011/004)."""
from __future__ import annotations

import re
from typing import Any

from produtos.nfce_sp_emissao_util import _sanitizar_texto_xml

_XPED_CAB = 60
_XNEmp = 22
_XCONT = 60
_XPED_ITEM = 15


def _txt(raw: Any, max_len: int) -> str:
    t = _sanitizar_texto_xml(str(raw or "").strip())
    t = re.sub(r"\s+", " ", t)
    return t[:max_len]


def dados_compra_nfe(doc: dict | None) -> dict[str, str]:
    """Lê pedido/empenho/processo do rascunho (fiscal ou raiz)."""
    d = doc if isinstance(doc, dict) else {}
    fiscal = d.get("fiscal") if isinstance(d.get("fiscal"), dict) else {}

    def _pick(*keys: str, max_len: int) -> str:
        for src in (fiscal, d):
            for k in keys:
                v = _txt(src.get(k), max_len)
                if v:
                    return v
        return ""

    return {
        "x_nemp": _pick("x_nemp", "xNEmp", "empenho", max_len=_XNEmp),
        "x_ped": _pick("x_ped", "xPed", "pedido", max_len=_XPED_CAB),
        "x_cont": _pick("x_cont", "xCont", "processo", "processo_administrativo", max_len=_XCONT),
    }


def tem_compra_nfe(compra: dict[str, str]) -> bool:
    return bool(compra.get("x_nemp") or compra.get("x_ped") or compra.get("x_cont"))


def partes_inf_cpl_compra(compra: dict[str, str]) -> list[str]:
    parts: list[str] = []
    if compra.get("x_ped"):
        parts.append(f"Pedido {compra['x_ped']}")
    if compra.get("x_nemp"):
        parts.append(f"Empenho {compra['x_nemp']}")
    if compra.get("x_cont"):
        parts.append(f"Processo {compra['x_cont']}")
    return parts


def inf_cpl_com_compra(doc: dict | None, extra: str = "", *, homolog: bool = False) -> str:
    d = doc if isinstance(doc, dict) else {}
    fiscal = d.get("fiscal") if isinstance(d.get("fiscal"), dict) else {}
    compra = dados_compra_nfe(d)
    partes: list[str] = []
    if homolog:
        partes.append("NF-E EMITIDA EM AMBIENTE DE HOMOLOGACAO - SEM VALOR FISCAL")
    if d.get("venda_agro_id"):
        partes.append(f"Venda Agro #{d.get('venda_agro_id')}")
    user = _txt(extra or d.get("inf_complementar") or fiscal.get("inf_compl") or "", 5000)
    if user:
        partes.append(user)
    for p in partes_inf_cpl_compra(compra):
        if p.lower() not in user.lower():
            partes.append(p)
    return _txt(" | ".join(partes), 5000)


def x_ped_item(ln: dict | None, x_ped_cab: str, n_item: int) -> tuple[str, str]:
    """prod/xPed (máx. 15) e nItemPed (1–6 dígitos)."""
    row = ln if isinstance(ln, dict) else {}
    ped = _txt(row.get("x_ped") or row.get("xPed") or x_ped_cab, _XPED_ITEM)
    nraw = str(row.get("n_item_ped") or row.get("nItemPed") or "").strip()
    n_dig = re.sub(r"\D", "", nraw)[:6]
    if not n_dig and ped:
        n_dig = str(max(1, int(n_item)))[:6]
    return ped, n_dig


def unidade_comercial_nfe(raw: Any) -> str:
    from produtos.nfe_saida_util import normalizar_unidade_nfe

    return normalizar_unidade_nfe(raw)
