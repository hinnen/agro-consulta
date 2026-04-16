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

# Fluxo na tela Entrada NF-e (persistido em ``status``; exibição pode corrigir inconsistências).
ENTRADA_NFE_STATUS_COM_PENDENCIAS = "com_pendencias"
ENTRADA_NFE_STATUS_PRONTA = "pronta"
ENTRADA_NFE_STATUS_ESTOQUE_APLICADO = "estoque_aplicado"
ENTRADA_NFE_STATUS_ENCERRADA = "encerrada"
ENTRADA_NFE_STATUS_DESCARTADA = "descartada"
ENTRADA_NFE_STATUS_RASCUNHO_LEGACY = "rascunho"

ENTRADA_NFE_STATUS_CONGELADOS = frozenset(
    {ENTRADA_NFE_STATUS_ENCERRADA, ENTRADA_NFE_STATUS_DESCARTADA, ENTRADA_NFE_STATUS_ESTOQUE_APLICADO}
)

ENTRADA_NFE_STATUS_UI: dict[str, dict[str, str]] = {
    ENTRADA_NFE_STATUS_COM_PENDENCIAS: {"label": "Com pendências"},
    ENTRADA_NFE_STATUS_PRONTA: {"label": "Pronta"},
    ENTRADA_NFE_STATUS_ESTOQUE_APLICADO: {"label": "Estoque aplicado"},
    ENTRADA_NFE_STATUS_ENCERRADA: {"label": "Encerrada"},
    ENTRADA_NFE_STATUS_DESCARTADA: {"label": "Descartada"},
}


def _entrada_nfe_qtd_linha(ln: dict) -> float:
    try:
        qe = float(str(ln.get("q_estoque") or "").replace(",", ".").strip() or 0)
    except (TypeError, ValueError):
        qe = 0.0
    try:
        qc = float(str(ln.get("q_com") or "").replace(",", ".").strip() or 0)
    except (TypeError, ValueError):
        qc = 0.0
    return max(qe, qc)


def entrada_nfe_produto_id_valido(pid: Any) -> bool:
    s = str(pid or "").strip()
    if not s:
        return False
    return not s.lower().startswith("local:")


def entrada_nfe_linhas_tem_pendencias(linhas: list | None) -> bool:
    """Linha com quantidade > 0 e sem produto de catálogo válido."""
    for ln in linhas or []:
        if not isinstance(ln, dict):
            continue
        desc = str(ln.get("x_prod") or "").strip()
        qtd = _entrada_nfe_qtd_linha(ln)
        if not desc and qtd == 0:
            continue
        if qtd > 0 and not entrada_nfe_produto_id_valido(ln.get("produto_id")):
            return True
    return False


def entrada_nfe_status_derivado_linhas(linhas: list | None) -> str:
    return (
        ENTRADA_NFE_STATUS_COM_PENDENCIAS
        if entrada_nfe_linhas_tem_pendencias(linhas)
        else ENTRADA_NFE_STATUS_PRONTA
    )


def entrada_nfe_status_efetivo(doc: dict[str, Any]) -> str:
    raw = str(doc.get("status") or ENTRADA_NFE_STATUS_RASCUNHO_LEGACY).strip().lower()
    linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
    pend = entrada_nfe_linhas_tem_pendencias(linhas)
    if raw in (ENTRADA_NFE_STATUS_ENCERRADA, ENTRADA_NFE_STATUS_DESCARTADA, ENTRADA_NFE_STATUS_ESTOQUE_APLICADO):
        return raw
    if raw == ENTRADA_NFE_STATUS_RASCUNHO_LEGACY:
        return ENTRADA_NFE_STATUS_COM_PENDENCIAS if pend else ENTRADA_NFE_STATUS_PRONTA
    if raw == ENTRADA_NFE_STATUS_PRONTA:
        return ENTRADA_NFE_STATUS_COM_PENDENCIAS if pend else ENTRADA_NFE_STATUS_PRONTA
    if raw == ENTRADA_NFE_STATUS_COM_PENDENCIAS:
        return ENTRADA_NFE_STATUS_COM_PENDENCIAS if pend else ENTRADA_NFE_STATUS_PRONTA
    return ENTRADA_NFE_STATUS_COM_PENDENCIAS if pend else ENTRADA_NFE_STATUS_PRONTA


def entrada_nfe_status_ui_por_codigo(codigo: str) -> dict[str, str]:
    return ENTRADA_NFE_STATUS_UI.get(
        codigo,
        {"label": codigo or "—"},
    )


def entrada_nfe_extra_financeiro_ok(extra: Any) -> bool:
    if not isinstance(extra, dict):
        return False
    return bool(extra.get("financeiro_lancado"))


def entrada_nfe_fila_bucket_lista(d: dict[str, Any]) -> str:
    """
    Estágio exclusivo para filtros da lista (Entrada NF-e), alinhado ao fluxo:
    Nota aberta → Estoque → Financeiro → Concluída; Descartada à parte.
    """
    eff = str(d.get("entrada_status_efetivo") or "")
    fin_ok = bool(d.get("entrada_financeiro_lancado"))
    if eff == ENTRADA_NFE_STATUS_DESCARTADA:
        return "descartada"
    if eff == ENTRADA_NFE_STATUS_ENCERRADA:
        return "concluida"
    if fin_ok and eff in (ENTRADA_NFE_STATUS_ESTOQUE_APLICADO, ENTRADA_NFE_STATUS_PRONTA):
        return "concluida"
    if eff == ENTRADA_NFE_STATUS_COM_PENDENCIAS:
        return "nota_aberta"
    if eff == ENTRADA_NFE_STATUS_PRONTA:
        return "estoque"
    if eff == ENTRADA_NFE_STATUS_ESTOQUE_APLICADO and not fin_ok:
        return "financeiro"
    return "nota_aberta"


def entrada_nfe_enriquecer_doc_serializado(d: dict[str, Any]) -> dict[str, Any]:
    """Acrescenta campos de UI (lista / detalhe) sem gravar no banco."""
    eff = entrada_nfe_status_efetivo(d)
    ui = entrada_nfe_status_ui_por_codigo(eff)
    extra = d.get("extra") if isinstance(d.get("extra"), dict) else {}
    d["entrada_status_efetivo"] = eff
    d["entrada_status_label"] = ui["label"]
    d["entrada_financeiro_lancado"] = entrada_nfe_extra_financeiro_ok(extra)
    d["entrada_lista_bucket"] = entrada_nfe_fila_bucket_lista(d)
    return d


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
    st = entrada_nfe_status_derivado_linhas(linhas)
    doc = {
        "criado_em": datetime.now(timezone.utc),
        "usuario": (usuario or "")[:200],
        "modo": (modo or "manual")[:40],
        "status": st,
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
    for k in ("criado_em", "atualizado_em", "estoque_aplicado_em"):
        if k in d:
            ser = _serialize_dt_mongo(d.get(k))
            if ser is not None:
                d[k] = ser
    return entrada_nfe_enriquecer_doc_serializado(d)


def listar_rascunhos_entrada(db, limit: int = 30, *, filtro: str | None = None) -> list[dict]:
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
        f = (filtro or "todas").strip().lower()
        if f == "todas":
            return out
        # Novos filtros (fila exclusiva). Aliases antigos da URL.
        legacy = {
            "abertas": "em_andamento",
            "pendencias": "nota_aberta",
            "prontas": "estoque",
            "encerradas": "encerrada_legacy",
            "descartadas": "descartada",
            "estoque": "estoque_aplicado_legacy",
            "financeiro": "financeiro_lancado_legacy",
        }
        if f in legacy:
            f = legacy[f]
        valid_extra = frozenset(
            {
                "nota_aberta",
                "estoque",
                "financeiro",
                "concluida",
                "descartada",
                "em_andamento",
                "estoque_aplicado_legacy",
                "financeiro_lancado_legacy",
                "encerrada_legacy",
            }
        )
        if f not in valid_extra:
            return out
        filtrados: list[dict] = []
        for item in out:
            eff = str(item.get("entrada_status_efetivo") or "")
            fin_ok = bool(item.get("entrada_financeiro_lancado"))
            b = str(item.get("entrada_lista_bucket") or "")
            if f == "em_andamento":
                if b in ("nota_aberta", "estoque", "financeiro"):
                    filtrados.append(item)
            elif f == "estoque_aplicado_legacy" and eff == ENTRADA_NFE_STATUS_ESTOQUE_APLICADO:
                filtrados.append(item)
            elif f == "financeiro_lancado_legacy" and fin_ok:
                filtrados.append(item)
            elif f == "encerrada_legacy" and eff == ENTRADA_NFE_STATUS_ENCERRADA:
                filtrados.append(item)
            elif f == b:
                filtrados.append(item)
        return filtrados
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
        atual = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id})
        if not atual:
            return {"ok": False, "erro": "Rascunho não encontrado."}
        st_atual = str(atual.get("status") or "").strip().lower()
        novo_status: str | None = None
        if st_atual in (ENTRADA_NFE_STATUS_ENCERRADA, ENTRADA_NFE_STATUS_DESCARTADA, ENTRADA_NFE_STATUS_ESTOQUE_APLICADO):
            novo_status = None
        else:
            novo_status = entrada_nfe_status_derivado_linhas(linhas)
        prev_ex = atual.get("extra") if isinstance(atual.get("extra"), dict) else {}
        merged_extra = {**prev_ex, **(extra or {})}
        set_doc: dict[str, Any] = {
            "atualizado_em": datetime.now(timezone.utc),
            "usuario_ultima_alteracao": (usuario or "")[:200],
            "modo": (modo or "manual")[:40],
            "cabecalho": cabecalho,
            "linhas": linhas,
            "xml_chave": (xml_chave or "")[:44] or None,
            "extra": merged_extra,
        }
        if novo_status is not None:
            set_doc["status"] = novo_status
        db[COL_ENTRADA_RASCUNHO].update_one(
            {"_id": _id},
            {"$set": set_doc},
        )
        return {"ok": True, "id": str(_id)}
    except Exception as exc:
        logger.exception("atualizar_rascunho_entrada")
        return {"ok": False, "erro": str(exc)[:500]}


def marcar_rascunho_estoque_aplicado(
    db,
    oid: str,
    *,
    usuario: str = "",
    patch_extra: dict | None = None,
) -> dict[str, Any]:
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    _id = _object_id_rascunho(oid)
    if _id is None:
        return {"ok": False, "erro": "ID inválido."}
    agora = datetime.now(timezone.utc)
    try:
        doc = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id})
        if not doc:
            return {"ok": False, "erro": "Rascunho não encontrado."}
        ex = dict(doc.get("extra") or {})
        if patch_extra:
            ex.update(patch_extra)
        r = db[COL_ENTRADA_RASCUNHO].update_one(
            {"_id": _id},
            {
                "$set": {
                    "status": ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
                    "estoque_aplicado_em": agora,
                    "usuario_estoque_aplicado": (usuario or "")[:200],
                    "atualizado_em": agora,
                    "extra": ex,
                }
            },
        )
        if r.matched_count == 0:
            return {"ok": False, "erro": "Rascunho não encontrado."}
        return {"ok": True, "id": str(_id)}
    except Exception as exc:
        logger.exception("marcar_rascunho_estoque_aplicado")
        return {"ok": False, "erro": str(exc)[:500]}


def marcar_rascunho_financeiro_lancado(
    db,
    oid: str,
    *,
    ids: list[str],
    usuario: str = "",
) -> dict[str, Any]:
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    _id = _object_id_rascunho(oid)
    if _id is None:
        return {"ok": False, "erro": "ID inválido."}
    agora = datetime.now(timezone.utc)
    try:
        doc = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id})
        if not doc:
            return {"ok": False, "erro": "Rascunho não encontrado."}
        ex = dict(doc.get("extra") or {})
        ex["financeiro_lancado"] = True
        ex["financeiro_ids"] = [str(x) for x in (ids or [])][:80]
        ex["financeiro_lancado_em"] = agora.isoformat()
        db[COL_ENTRADA_RASCUNHO].update_one(
            {"_id": _id},
            {
                "$set": {
                    "atualizado_em": agora,
                    "usuario_ultima_alteracao": (usuario or "")[:200],
                    "extra": ex,
                }
            },
        )
        return {"ok": True, "id": str(_id)}
    except Exception as exc:
        logger.exception("marcar_rascunho_financeiro_lancado")
        return {"ok": False, "erro": str(exc)[:500]}


def pipeline_acao_rascunho_entrada(
    db,
    oid: str,
    acao: str,
    *,
    usuario: str = "",
) -> dict[str, Any]:
    """encerrar | descartar | reabrir — altera só ``status`` (e timestamps)."""
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    _id = _object_id_rascunho(oid)
    if _id is None:
        return {"ok": False, "erro": "ID inválido."}
    ac = (acao or "").strip().lower()
    agora = datetime.now(timezone.utc)
    try:
        doc = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id})
        if not doc:
            return {"ok": False, "erro": "Rascunho não encontrado."}
        st = str(doc.get("status") or "").strip().lower()
        linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
        if ac == "encerrar":
            if st == ENTRADA_NFE_STATUS_DESCARTADA:
                return {"ok": False, "erro": "Nota descartada: reabra antes de encerrar."}
            novo = ENTRADA_NFE_STATUS_ENCERRADA
        elif ac == "descartar":
            novo = ENTRADA_NFE_STATUS_DESCARTADA
        elif ac == "reabrir":
            if st not in (ENTRADA_NFE_STATUS_ENCERRADA, ENTRADA_NFE_STATUS_DESCARTADA):
                return {"ok": False, "erro": "Só é possível reabrir notas encerradas ou descartadas."}
            novo = entrada_nfe_status_derivado_linhas(linhas)
        else:
            return {"ok": False, "erro": "Ação inválida (use encerrar, descartar ou reabrir)."}
        db[COL_ENTRADA_RASCUNHO].update_one(
            {"_id": _id},
            {
                "$set": {
                    "status": novo,
                    "atualizado_em": agora,
                    "usuario_ultima_alteracao": (usuario or "")[:200],
                }
            },
        )
        return {"ok": True, "id": str(_id), "status": novo}
    except Exception as exc:
        logger.exception("pipeline_acao_rascunho_entrada")
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
