"""Dados fiscais por produto para montagem da NFC-e."""
from __future__ import annotations

from typing import Any

from produtos.agro_produto_fiscal_defaults import merge_fiscal_padrao_cadastro_manual_sp_sn
from produtos.models import ProdutoGestaoOverlayAgro


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


def fiscal_por_produto_id(produto_id_externo: str, *, db=None, col_p: str | None = None) -> dict[str, str]:
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
    merged = merge_fiscal_padrao_cadastro_manual_sp_sn({**mongo_fis, **overlay_fis})
    ncm = merged.get("ncm") or "23099020"
    return {
        "ncm": ncm[:8],
        "cfop": (merged.get("cfop") or "5102")[:4],
        "csosn": (merged.get("csosn") or "102")[:3],
        "origem": (merged.get("origem") or "0")[:1],
        "cest": (merged.get("cest") or "")[:7],
    }
