"""
Entrada de NF-e no Agro: parse de XML, casamento com DtoProduto e rascunhos no Mongo.
Não substitui o lançamento oficial no ERP — prepara conferência e integração futura.
"""
from __future__ import annotations

import base64
import gzip
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

NS_NFE = "http://www.portalfiscal.inf.br/nfe"
COL_ENTRADA_RASCUNHO = "AgroEntradaNotaRascunho"
COL_DFE_CURSOR = "AgroNFeDistribuicaoCursor"


def _localname(tag: str) -> str:
    if not tag:
        return ""
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return str(el.text).strip()


def _find1(root: ET.Element, path_parts: list[str]) -> ET.Element | None:
    """Busca primeiro elemento por nome local (ignora namespace)."""
    for el in root.iter():
        if _localname(el.tag) in path_parts:
            return el
    return None


def _findall_local(parent: ET.Element, name: str) -> list[ET.Element]:
    return [el for el in list(parent) if _localname(el.tag) == name]


def parse_nfe_xml_bytes(data: bytes) -> dict[str, Any]:
    """
    Extrai cabeçalho e itens de NF-e 3.x/4.x (nfeProc ou NFe).
    Retorna dict com chave, emitente, destinatário, itens (cProd, ean, descrição, qtd, v_unit, cfop, ncm).
    """
    out: dict[str, Any] = {
        "ok": False,
        "erro": None,
        "chave": "",
        "numero": "",
        "serie": "",
        "dh_emi": "",
        "emit_cnpj": "",
        "emit_nome": "",
        "dest_cnpj": "",
        "dest_cpf": "",
        "dest_nome": "",
        "valor_total": 0.0,
        "itens": [],
    }
    try:
        root = ET.fromstring(data)
    except ET.ParseError as e:
        out["erro"] = f"XML inválido: {e}"
        return out

    # nfeProc -> NFe -> infNFe OU infNFe direto
    inf = _find1(root, ["infNFe"])
    if inf is None:
        out["erro"] = "Não encontramos infNFe (arquivo não parece NF-e autorizada)."
        return out

    chave = ""
    if inf.attrib.get("Id"):
        chave = str(inf.attrib.get("Id", "")).strip().replace("NFe", "")
    out["chave"] = chave[:44]

    ide = _find1(inf, ["ide"])
    if ide is not None:
        for child in ide:
            ln = _localname(child.tag)
            if ln == "nNF":
                out["numero"] = _text(child)
            elif ln == "serie":
                out["serie"] = _text(child)
            elif ln == "dhEmi":
                out["dh_emi"] = _text(child)

    emit = _find1(inf, ["emit"])
    if emit is not None:
        for child in emit:
            ln = _localname(child.tag)
            if ln == "CNPJ":
                out["emit_cnpj"] = re.sub(r"\D", "", _text(child))[:14]
            elif ln == "CPF":
                pass
            elif ln == "xNome":
                out["emit_nome"] = _text(child)[:300]

    dest = _find1(inf, ["dest"])
    if dest is not None:
        for child in dest:
            ln = _localname(child.tag)
            if ln == "CNPJ":
                out["dest_cnpj"] = re.sub(r"\D", "", _text(child))[:14]
            elif ln == "CPF":
                out["dest_cpf"] = re.sub(r"\D", "", _text(child))[:11]
            elif ln == "xNome":
                out["dest_nome"] = _text(child)[:300]

    total_el = _find1(inf, ["total"])
    if total_el is not None:
        icms_tot = _find1(total_el, ["ICMSTot"])
        if icms_tot is not None:
            for child in icms_tot:
                if _localname(child.tag) == "vNF":
                    try:
                        out["valor_total"] = float(Decimal(_text(child) or "0"))
                    except Exception:
                        out["valor_total"] = 0.0

    itens_out: list[dict[str, Any]] = []
    for det in inf.iter():
        if _localname(det.tag) != "det":
            continue
        prod = None
        for ch in det:
            if _localname(ch.tag) == "prod":
                prod = ch
                break
        if prod is None:
            continue
        item: dict[str, Any] = {
            "n_item": str(det.attrib.get("nItem", "") or len(itens_out) + 1),
            "c_prod": "",
            "ean": "",
            "x_prod": "",
            "ncm": "",
            "cfop": "",
            "u_com": "",
            "q_com": 0.0,
            "v_un_com": 0.0,
            "v_prod": 0.0,
        }
        for child in prod:
            ln = _localname(child.tag)
            t = _text(child)
            if ln == "cProd":
                item["c_prod"] = t[:60]
            elif ln == "cEAN":
                item["ean"] = re.sub(r"\D", "", t)[:14]
            elif ln == "cEANTrib":
                if not item["ean"]:
                    item["ean"] = re.sub(r"\D", "", t)[:14]
            elif ln == "xProd":
                item["x_prod"] = t[:500]
            elif ln == "NCM":
                item["ncm"] = t[:10]
            elif ln == "CFOP":
                item["cfop"] = t[:10]
            elif ln == "uCom":
                item["u_com"] = t[:10]
            elif ln == "qCom":
                try:
                    item["q_com"] = float(Decimal(t.replace(",", ".") or "0"))
                except Exception:
                    item["q_com"] = 0.0
            elif ln == "vUnCom":
                try:
                    item["v_un_com"] = float(Decimal(t.replace(",", ".") or "0"))
                except Exception:
                    item["v_un_com"] = 0.0
            elif ln == "vProd":
                try:
                    item["v_prod"] = float(Decimal(t.replace(",", ".") or "0"))
                except Exception:
                    item["v_prod"] = 0.0
        itens_out.append(item)

    out["itens"] = itens_out
    out["ok"] = True
    return out


def decodificar_doc_zip_base64(b64: str) -> str | None:
    """docZip da Distribuição DF-e: base64 + gzip + XML."""
    try:
        raw = base64.b64decode(b64.strip())
        return gzip.decompress(raw).decode("utf-8", errors="replace")
    except Exception as exc:
        logger.warning("decodificar_doc_zip_base64: %s", exc)
        return None


def casar_produtos_mongo(db, col_p: str, itens: list[dict]) -> list[dict]:
    """Enriquece itens com produto_id / nome_catalogo quando encontra por EAN ou código."""
    if db is None or not itens:
        return itens
    col = db[col_p]
    for it in itens:
        it["produto_id"] = None
        it["nome_catalogo"] = None
        it["match_tipo"] = None
        ean = (it.get("ean") or "").strip()
        cprod = (it.get("c_prod") or "").strip()
        doc = None
        try:
            if ean and len(ean) >= 8:
                ors = [{"CodigoBarras": ean}, {"EAN_NFe": ean}]
                if ean.isdigit():
                    ors.append({"CodigoBarras": str(int(ean))})
                doc = col.find_one({"$or": ors}, {"Id": 1, "_id": 1, "Nome": 1})
                if doc:
                    it["match_tipo"] = "ean"
            if doc is None and cprod:
                doc = col.find_one(
                    {
                        "$or": [
                            {"CodigoNFe": cprod},
                            {"Codigo": cprod},
                        ]
                    },
                    {"Id": 1, "_id": 1, "Nome": 1},
                )
                if doc:
                    it["match_tipo"] = "codigo"
        except Exception as exc:
            logger.warning("casar_produtos_mongo: %s", exc)
            continue
        if doc:
            pid = str(doc.get("Id") or doc.get("_id") or "")
            it["produto_id"] = pid
            it["nome_catalogo"] = str(doc.get("Nome") or "")[:300]
    return itens


def salvar_rascunho_entrada(
    db,
    *,
    usuario: str,
    modo: str,
    cabecalho: dict,
    linhas: list[dict],
    xml_chave: str | None = None,
    extra: dict | None = None,
) -> dict[str, Any]:
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    doc = {
        "criado_em": datetime.now(timezone.utc),
        "usuario": (usuario or "")[:200],
        "modo": (modo or "manual")[:40],
        "status": "rascunho",
        "cabecalho": cabecalho,
        "linhas": linhas,
        "xml_chave": (xml_chave or "")[:44] or None,
        "extra": extra or {},
    }
    try:
        ins = db[COL_ENTRADA_RASCUNHO].insert_one(doc)
        return {"ok": True, "id": str(ins.inserted_id)}
    except Exception as exc:
        logger.exception("salvar_rascunho_entrada")
        return {"ok": False, "erro": str(exc)[:500]}


def _serialize_dt_mongo(val: Any) -> str | None:
    if isinstance(val, datetime):
        return val.replace(tzinfo=timezone.utc).isoformat()
    return None


def _serialize_rascunho_leitura(doc: dict[str, Any]) -> dict[str, Any]:
    d = dict(doc)
    if d.get("_id") is not None:
        d["_id"] = str(d["_id"])
    for k in ("criado_em", "atualizado_em"):
        if k in d:
            ser = _serialize_dt_mongo(d.get(k))
            if ser is not None:
                d[k] = ser
    return d


def listar_rascunhos_entrada(db, limit: int = 30) -> list[dict]:
    if db is None:
        return []
    try:
        cur = (
            db[COL_ENTRADA_RASCUNHO]
            .find({}, sort=[("criado_em", -1)])
            .limit(min(max(limit, 1), 100))
        )
        out = []
        for d in cur:
            out.append(_serialize_rascunho_leitura(d))
        return out
    except Exception as exc:
        logger.exception("listar_rascunhos_entrada: %s", exc)
        return []


def _object_id_rascunho(oid: str):
    from bson.errors import InvalidId
    from bson.objectid import ObjectId

    try:
        return ObjectId(str(oid).strip())
    except (InvalidId, TypeError, ValueError):
        return None


def obter_rascunho_entrada(db, oid: str) -> dict[str, Any] | None:
    if db is None:
        return None
    _id = _object_id_rascunho(oid)
    if _id is None:
        return None
    try:
        d = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id})
        if not d:
            return None
        return _serialize_rascunho_leitura(d)
    except Exception as exc:
        logger.exception("obter_rascunho_entrada: %s", exc)
        return None


def excluir_rascunho_entrada(db, oid: str) -> dict[str, Any]:
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    _id = _object_id_rascunho(oid)
    if _id is None:
        return {"ok": False, "erro": "ID inválido."}
    try:
        r = db[COL_ENTRADA_RASCUNHO].delete_one({"_id": _id})
        return {"ok": r.deleted_count == 1}
    except Exception as exc:
        logger.exception("excluir_rascunho_entrada")
        return {"ok": False, "erro": str(exc)[:500]}


def atualizar_rascunho_entrada(
    db,
    oid: str,
    *,
    usuario: str,
    modo: str,
    cabecalho: dict,
    linhas: list,
    xml_chave: str | None = None,
    extra: dict | None = None,
) -> dict[str, Any]:
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    _id = _object_id_rascunho(oid)
    if _id is None:
        return {"ok": False, "erro": "ID inválido."}
    try:
        atual = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id}, {"_id": 1})
        if not atual:
            return {"ok": False, "erro": "Rascunho não encontrado."}
        db[COL_ENTRADA_RASCUNHO].update_one(
            {"_id": _id},
            {
                "$set": {
                    "atualizado_em": datetime.now(timezone.utc),
                    "usuario_ultima_alteracao": (usuario or "")[:200],
                    "modo": (modo or "manual")[:40],
                    "cabecalho": cabecalho,
                    "linhas": linhas,
                    "xml_chave": (xml_chave or "")[:44] or None,
                    "extra": extra or {},
                }
            },
        )
        return {"ok": True, "id": str(_id)}
    except Exception as exc:
        logger.exception("atualizar_rascunho_entrada")
        return {"ok": False, "erro": str(exc)[:500]}


def obter_ult_nsu(db, cnpj: str) -> str:
    cnpj = re.sub(r"\D", "", cnpj or "")[:14]
    if not cnpj or db is None:
        return "0"
    try:
        row = db[COL_DFE_CURSOR].find_one({"cnpj": cnpj})
        if row and row.get("ult_nsu"):
            return str(row["ult_nsu"]).zfill(15)
    except Exception:
        pass
    return "0"


def buscar_fornecedores_entrada_nfe(
    db,
    col_pessoa: str,
    q: str | None,
    *,
    inicial: bool = False,
    limit: int = 50,
) -> list[dict[str, str]]:
    """
    Pessoas no Mongo (DtoPessoa) por nome ou CNPJ/CPF.
    ``inicial=True`` sem ``q`` retorna até ``limit`` registros ordenados por nome (para abrir o datalist).
    """
    out: list[dict[str, str]] = []
    if db is None or not col_pessoa:
        return out
    lim = min(max(int(limit or 50), 1), 100)
    q = (q or "").strip()
    col = db[col_pessoa]
    proj = {
        "Nome": 1,
        "RazaoSocial": 1,
        "NomeFantasia": 1,
        "CpfCnpj": 1,
        "Id": 1,
        "_id": 1,
        "CNPJ": 1,
        "Cnpj": 1,
        "CPF": 1,
        "Cpf": 1,
    }
    try:
        if not q and inicial:
            cur = col.find({}, proj).sort([("Nome", 1), ("RazaoSocial", 1)]).limit(lim)
        elif q:
            esc = re.escape(q)
            cond: dict[str, Any] = {
                "$or": [
                    {"Nome": {"$regex": esc, "$options": "i"}},
                    {"RazaoSocial": {"$regex": esc, "$options": "i"}},
                    {"NomeFantasia": {"$regex": esc, "$options": "i"}},
                ]
            }
            digits = re.sub(r"\D", "", q)
            if len(digits) >= 2:
                cond["$or"].append({"CpfCnpj": {"$regex": re.escape(digits)}})
                cond["$or"].append({"CNPJ": {"$regex": re.escape(digits)}})
                cond["$or"].append({"Cnpj": {"$regex": re.escape(digits)}})
            cur = col.find(cond, proj).sort("Nome", 1).limit(lim)
        else:
            return out
        seen: set[str] = set()
        for d in cur:
            nome = (
                str(d.get("Nome") or "").strip()
                or str(d.get("RazaoSocial") or "").strip()
                or str(d.get("NomeFantasia") or "").strip()
            )
            if not nome:
                continue
            doc_raw = (
                d.get("CpfCnpj")
                or d.get("CNPJ")
                or d.get("Cnpj")
                or d.get("CPF")
                or d.get("Cpf")
                or ""
            )
            documento = re.sub(r"\D", "", str(doc_raw))[:18]
            pid = str(d.get("Id") or d.get("_id") or "").strip()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            out.append(
                {
                    "id": pid,
                    "nome": nome[:300],
                    "documento": documento,
                    "origem": "mongo",
                }
            )
    except Exception as exc:
        logger.warning("buscar_fornecedores_entrada_nfe: %s", exc)
    return out


def gravar_ult_nsu(db, cnpj: str, ult_nsu: str) -> None:
    cnpj = re.sub(r"\D", "", cnpj or "")[:14]
    if not cnpj or db is None:
        return
    try:
        db[COL_DFE_CURSOR].update_one(
            {"cnpj": cnpj},
            {"$set": {"ult_nsu": str(ult_nsu).zfill(15), "atualizado_em": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning("gravar_ult_nsu: %s", exc)
