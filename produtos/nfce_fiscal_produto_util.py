"""Dados fiscais por produto para montagem da NFC-e."""
from __future__ import annotations

import re
from typing import Any

from produtos.agro_produto_fiscal_defaults import merge_fiscal_padrao_cadastro_manual_sp_sn
from produtos.models import ProdutoGestaoOverlayAgro


def _so_digitos(val: Any, n: int) -> str:
    return re.sub(r"\D", "", str(val or ""))[:n]


def _fiscal_de_mongo_doc(doc: dict | None) -> dict[str, str]:
    if not isinstance(doc, dict):
        return {}
    out: dict[str, str] = {}
    for k_m, k_out in (
        ("NCM", "ncm"),
        ("CEST", "cest"),
        ("CFOP", "cfop"),
        ("CfopPadrao", "cfop"),
        ("CSOSN", "csosn"),
        ("OrigemMercadoria", "origem"),
        ("CstPisCofins", "cst_pis_cofins"),
    ):
        v = str(doc.get(k_m) or "").strip()
        if v and not out.get(k_out):
            out[k_out] = v
    return out


def fiscal_por_produto_id(
    produto_id_externo: str,
    *,
    db=None,
    col_p: str | None = None,
    aplicar_defaults_sn: bool = True,
) -> dict[str, str]:
    pid = str(produto_id_externo or "").strip()
    overlay_fis: dict[str, Any] = {}
    if pid:
        ov = (
            ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid)
            .only("cadastro_extras")
            .first()
        )
        if ov and isinstance(ov.cadastro_extras, dict):
            fis = ov.cadastro_extras.get("fiscal")
            if isinstance(fis, dict):
                overlay_fis = fis
    mongo_fis: dict[str, str] = {}
    if db is not None and col_p and pid:
        try:
            from bson import ObjectId

            filt: dict = {}
            if ObjectId.is_valid(pid):
                filt = {"_id": ObjectId(pid)}
            elif pid.isdigit():
                filt = {"Id": int(pid)}
            else:
                filt = {"Id": pid}
            doc = db[col_p].find_one(filt, {"NCM": 1, "CEST": 1, "CFOP": 1, "CfopPadrao": 1, "CSOSN": 1, "OrigemMercadoria": 1, "CstPisCofins": 1})
            mongo_fis = _fiscal_de_mongo_doc(doc)
        except Exception:
            mongo_fis = {}
    bruto = {**mongo_fis, **overlay_fis}
    merged = merge_fiscal_padrao_cadastro_manual_sp_sn(bruto) if aplicar_defaults_sn else bruto
    fonte = merged if aplicar_defaults_sn else bruto
    # Só dígitos no XML — ponto/traço no CFOP/CEST/NCM → SEFAZ 225 (schema).
    ncm = _so_digitos(fonte.get("ncm"), 8)
    if aplicar_defaults_sn and not ncm:
        ncm = "23099020"
    cfop = _so_digitos(fonte.get("cfop"), 4) or ("5102" if aplicar_defaults_sn else "")
    csosn = _so_digitos(fonte.get("csosn"), 3) or ("102" if aplicar_defaults_sn else "")
    origem = _so_digitos(fonte.get("origem"), 1) or "0"
    cest = _so_digitos(fonte.get("cest"), 7)
    cst = _so_digitos(merged.get("cst_pis_cofins"), 2) or (
        "49" if aplicar_defaults_sn else _so_digitos(bruto.get("cst_pis_cofins"), 2)
    )
    return {
        "ncm": ncm,
        "cfop": cfop,
        "csosn": csosn,
        "origem": origem,
        "cest": cest if len(cest) == 7 else "",
        "cst_pis_cofins": cst,
    }
