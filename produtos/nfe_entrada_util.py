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
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from .mongo_busca_similares_util import encontrar_produto_casar_entrada_nfe

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

# Lock anti-duplo POST em ``api_entrada_nota_estoque_agro`` (Mongo ``extra.estoque_agro_lock``).
ESTOQUE_AGRO_LOCK_MAX_AGE = timedelta(minutes=15)

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
    linhas_raw = doc.get("linhas")
    if not isinstance(linhas_raw, list):
        # Em payloads resumidos (lista), "linhas" pode não vir.
        # Nessa situação, preserva o status persistido sem rederivar por pendências.
        if raw in (ENTRADA_NFE_STATUS_ENCERRADA, ENTRADA_NFE_STATUS_DESCARTADA, ENTRADA_NFE_STATUS_ESTOQUE_APLICADO):
            return raw
        if raw in (ENTRADA_NFE_STATUS_COM_PENDENCIAS, ENTRADA_NFE_STATUS_PRONTA):
            return raw
        return ENTRADA_NFE_STATUS_COM_PENDENCIAS
    linhas = linhas_raw
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


def _entrada_nfe_extra_wizard_data_ok(extra: Any) -> bool:
    """
    Etapas mínimas para considerar a nota realmente pronta para seguir à etapa de estoque:
    etapa 2 (produtos) e etapa 3 (códigos) confirmadas.
    """
    if not isinstance(extra, dict):
        return False
    e2 = str(extra.get("wizard_etapa2_confirmada_em") or "").strip()
    e3 = str(extra.get("wizard_etapa3_confirmada_em") or "").strip()
    return bool(e2 and e3)


def _entrada_nfe_extra_finalizacao_ok(extra: Any) -> bool:
    """Etapa 6 confirmada (PIN/finalização gravada no Mongo)."""
    if not isinstance(extra, dict):
        return False
    return bool(str(extra.get("aprovacao_wizard_em") or "").strip())


def _entrada_nfe_extra_correcao_sistemica(extra: Any) -> bool:
    """Marca manual (lista Entrada NF-e): aguardando correção no sistema — não altera fila operacional."""
    if not isinstance(extra, dict):
        return False
    v = extra.get("correcao_sistemica")
    if v is True:
        return True
    if isinstance(v, str) and v.strip().lower() in ("1", "true", "sim", "yes"):
        return True
    return False


def entrada_nfe_fila_bucket_lista(d: dict[str, Any]) -> str:
    """
    Estágio exclusivo para filtros da lista (Entrada NF-e), alinhado ao fluxo:
    Nota aberta → Estoque → Financeiro → Finalizar (PIN etapa 6) → Concluída; Descartada à parte.

    **Concluída** só com finalização do assistente (``extra.aprovacao_wizard_em`` / PIN etapa 6),
    alinhado ao chip verde do passo 6. Estoque + financeiro sem PIN ficam em **finalizar**.
    Status ``encerrada`` (legado, antes só por botão) fica em fila própria, não em Concluída.
    """
    eff = str(d.get("entrada_status_efetivo") or "")
    fin_ok = bool(d.get("entrada_financeiro_lancado"))
    ex = d.get("extra") if isinstance(d.get("extra"), dict) else {}
    wizard_ok = _entrada_nfe_extra_wizard_data_ok(ex)
    final_ok = _entrada_nfe_extra_finalizacao_ok(ex)
    if eff == ENTRADA_NFE_STATUS_DESCARTADA:
        return "descartada"
    if eff == ENTRADA_NFE_STATUS_ENCERRADA:
        return "encerrada"
    if final_ok:
        return "concluida"
    if eff == ENTRADA_NFE_STATUS_COM_PENDENCIAS:
        return "nota_aberta"
    if eff == ENTRADA_NFE_STATUS_PRONTA:
        return "estoque" if wizard_ok else "nota_aberta"
    if eff == ENTRADA_NFE_STATUS_ESTOQUE_APLICADO:
        if not fin_ok:
            return "financeiro"
        return "finalizar"
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
    d["entrada_correcao_sistemica"] = _entrada_nfe_extra_correcao_sistemica(extra)
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
            "lote_numero": "",
            "lote_fabricacao": "",
            "lote_validade": "",
            "lote_xml": False,
        }
        for child in det:
            if _localname(child.tag) != "rastro":
                continue
            rastro: dict[str, str] = {}
            for rc in child:
                rl = _localname(rc.tag)
                rt = _text(rc)
                if rl == "nLote":
                    rastro["n_lote"] = rt[:60]
                elif rl == "dFab":
                    rastro["d_fab"] = rt[:10]
                elif rl == "dVal":
                    rastro["d_val"] = rt[:10]
            if rastro:
                item["lote_numero"] = str(rastro.get("n_lote") or "")[:60]
                item["lote_fabricacao"] = str(rastro.get("d_fab") or "")[:10]
                item["lote_validade"] = str(rastro.get("d_val") or "")[:10]
                item["lote_xml"] = bool(
                    item["lote_numero"] or item["lote_fabricacao"] or item["lote_validade"]
                )
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
    """Enriquece itens com produto_id / nome_catalogo quando encontra por EAN ou código (incl. similares ERP)."""
    if db is None or not itens:
        return itens
    for it in itens:
        it["produto_id"] = None
        it["nome_catalogo"] = None
        it["match_tipo"] = None
        ean = (it.get("ean") or "").strip()
        cprod = (it.get("c_prod") or "").strip()
        doc = None
        mtipo = None
        try:
            doc, mtipo = encontrar_produto_casar_entrada_nfe(db, col_p, ean=ean, c_prod=cprod)
        except Exception as exc:
            logger.warning("casar_produtos_mongo: %s", exc)
            continue
        if doc:
            pid = str(doc.get("Id") or doc.get("_id") or "")
            it["produto_id"] = pid
            it["nome_catalogo"] = str(doc.get("Nome") or "")[:300]
            it["match_tipo"] = mtipo
    return itens


def persistir_vinculos_c_prod_entrada_nfe_linhas(
    db,
    col_p: str,
    linhas: list[dict],
) -> int:
    """
    Grava cProd da NF no overlay Agro para o próximo parse XML casar automaticamente.
    Chamado ao salvar rascunho / concluir entrada (não depende só do JS lembrar na hora).
    """
    if not linhas:
        return 0
    from produtos.models import ProdutoGestaoOverlayAgro

    from .mongo_index_codigos import (
        aplicar_index_codigos_no_mongo,
        normalizar_c_prod_nf_entrada,
        overlay_cadastro_extras_adicionar_c_prod_nf,
    )

    pids_novos: set[str] = set()
    n = 0
    for ln in linhas:
        if not isinstance(ln, dict):
            continue
        pid = str(ln.get("produto_id") or "").strip()
        if not pid or pid.lower().startswith("local:"):
            continue
        cprod = str(ln.get("c_prod") or "").strip()
        if not normalizar_c_prod_nf_entrada(cprod):
            continue
        ov, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
            produto_externo_id=pid[:64],
            defaults={},
        )
        ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
        if overlay_cadastro_extras_adicionar_c_prod_nf(ex, cprod):
            ov.cadastro_extras = ex
            ov.save(update_fields=["cadastro_extras", "atualizado_em"])
            pids_novos.add(pid[:64])
            n += 1
    if db is not None and col_p and pids_novos:
        from bson import ObjectId

        col = db[col_p]
        for pid in pids_novos:
            try:
                ors: list[dict] = [{"Id": pid}]
                try:
                    ors.append({"Id": int(pid)})
                except (TypeError, ValueError):
                    pass
                try:
                    ors.append({"_id": ObjectId(pid)})
                except Exception:
                    pass
                doc = col.find_one({"$or": ors})
                if isinstance(doc, dict) and doc.get("_id"):
                    aplicar_index_codigos_no_mongo(db, col_p, doc, produto_externo_id=pid)
            except Exception as exc:
                logger.warning("persistir_vinculos_c_prod: index %s %s", pid, exc)
    return n


def salvar_rascunho_entrada(
    db,
    *,
    usuario: str,
    modo: str,
    cabecalho: dict,
    linhas: list[dict],
    xml_chave: str | None = None,
    extra: dict | None = None,
    col_pessoa: str | None = None,
) -> dict[str, Any]:
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    cab_norm = cabecalho
    if col_pessoa:
        cab_norm = normalizar_cabecalho_emit_fornecedor_entrada_nfe(db, col_pessoa, cabecalho)
    st = entrada_nfe_status_derivado_linhas(linhas)
    doc = {
        "criado_em": datetime.now(timezone.utc),
        "usuario": (usuario or "")[:200],
        "modo": (modo or "manual")[:40],
        "status": st,
        "cabecalho": cab_norm,
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


def rascunho_entrada_valido_para_aprovacao_wizard(doc: dict[str, Any]) -> tuple[bool, str]:
    """Conteúdo mínimo persistido no rascunho antes do carimbo do assistente (PIN). Alinhado às regras da UI."""
    cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
    if not str(cab.get("emit_nome") or "").strip():
        return False, "Fornecedor não preenchido no rascunho."
    if not str(cab.get("numero") or "").strip():
        return False, "Número da NF ausente no rascunho."
    if not str(cab.get("data_entrada") or "").strip():
        return False, "Data de entrada ausente no rascunho."
    if not str(cab.get("plano_conta") or "").strip():
        return False, "Plano de contas ausente no rascunho."
    linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
    if not linhas:
        return False, "Rascunho sem linhas de item."
    for i, ln in enumerate(linhas):
        if not isinstance(ln, dict):
            return False, f"Linha {i + 1}: formato inválido."
        pid = str(ln.get("produto_id") or "").strip()
        if not pid or pid.lower().startswith("local:"):
            return False, f"Linha {i + 1}: produto do catálogo obrigatório."
    emp = str(cab.get("empresa_faturada_id") or "").strip()
    if not emp:
        return False, "Empresa (estoque) não definida no rascunho."
    dep = str(cab.get("deposito_entrada") or "").strip()
    if dep not in ("centro", "vila"):
        return False, "Depósito inválido ou ausente no rascunho."
    return True, ""


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


def _entrada_nfe_tipo_entrada_extra(extra: dict | None) -> str:
    t = str((extra or {}).get("nfe_tipo_entrada") or "compras").strip().lower()
    return "bonificacao" if t == "bonificacao" else "compras"


def _titulos_mongo_por_ids_entrada_nfe(db, ids: list[str]) -> list[dict[str, Any]]:
    from .mongo_financeiro_util import COL_DTO_LANCAMENTO

    if db is None or not ids:
        return []
    col = db[COL_DTO_LANCAMENTO]
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in ids[:80]:
        rid = str(raw or "").strip()
        if not rid or rid in seen:
            continue
        seen.add(rid)
        proj = {"Cliente": 1, "ClienteID": 1, "Descricao": 1, "Observacao": 1, "Despesa": 1}
        doc = None
        try:
            from bson import ObjectId
            from bson.errors import InvalidId

            doc = col.find_one({"_id": ObjectId(rid)}, proj)
        except (InvalidId, Exception):
            doc = None
        if doc is None:
            try:
                doc = col.find_one({"Id": rid}, proj)
            except Exception:
                doc = None
        if doc is None:
            try:
                doc = col.find_one({"Id": int(rid)}, proj)
            except (TypeError, ValueError, Exception):
                doc = None
        if isinstance(doc, dict):
            out.append(doc)
    return out


def _titulos_mongo_por_rastro_entrada_nfe(db, cab: dict) -> list[dict[str, Any]]:
    """Busca títulos a pagar que cite a chave ou o número da NF (quando não há ``financeiro_ids``)."""
    from .mongo_financeiro_util import COL_DTO_LANCAMENTO

    if db is None or not isinstance(cab, dict):
        return []
    col = db[COL_DTO_LANCAMENTO]
    ch = str(cab.get("chave") or "").strip()
    nf = str(cab.get("numero") or "").strip()
    ors: list[dict[str, Any]] = [{"Observacao": {"$regex": re.escape("Entrada NF-e Agro"), "$options": "i"}}]
    if ch and len(ch) >= 12:
        ors.append({"Observacao": {"$regex": re.escape(ch[-24:])}})
    if nf and nf not in ("", "0", "000"):
        ors.append({"Descricao": {"$regex": re.escape(nf), "$options": "i"}})
        ors.append({"Observacao": {"$regex": re.escape(nf)}})
    try:
        cur = col.find(
            {"$and": [{"Despesa": True}, {"$or": ors}]},
            {"Cliente": 1, "ClienteID": 1, "Descricao": 1, "Observacao": 1, "Despesa": 1},
        ).limit(15)
        return [d for d in cur if isinstance(d, dict)]
    except Exception as exc:
        logger.warning("_titulos_mongo_por_rastro_entrada_nfe: %s", exc)
        return []


def auditar_financeiro_rascunho_entrada_nfe(
    db,
    doc: dict[str, Any],
    *,
    col_pessoa: str | None = None,
) -> dict[str, Any]:
    """
    Verifica se a nota tem título(s) em DtoLancamento alinhado(s) ao fornecedor da nota.
    **Concluída** (PIN etapa 6) não implica financeiro — use este relatório para conferir em lote.
    """
    cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
    extra = doc.get("extra") if isinstance(doc.get("extra"), dict) else {}
    enriched = entrada_nfe_enriquecer_doc_serializado(dict(doc))
    bucket = str(enriched.get("entrada_lista_bucket") or "")
    tipo = _entrada_nfe_tipo_entrada_extra(extra)
    fin_flag = entrada_nfe_extra_financeiro_ok(extra)
    ids_raw = extra.get("financeiro_ids")
    ids_list = [str(x).strip() for x in ids_raw if x] if isinstance(ids_raw, list) else []

    emit_nome = str(cab.get("emit_nome") or "").strip()
    emit_canon = emit_nome
    if db is not None and col_pessoa:
        cab_can = normalizar_cabecalho_emit_fornecedor_entrada_nfe(db, col_pessoa, dict(cab))
        emit_canon = str(cab_can.get("emit_nome") or emit_nome).strip()

    titulos_ids = _titulos_mongo_por_ids_entrada_nfe(db, ids_list) if fin_flag else []
    titulos_rastro: list[dict[str, Any]] = []
    if not titulos_ids:
        titulos_rastro = _titulos_mongo_por_rastro_entrada_nfe(db, cab)
    titulos = titulos_ids if titulos_ids else titulos_rastro

    clientes_titulo = list(
        dict.fromkeys(str(t.get("Cliente") or "").strip() for t in titulos if str(t.get("Cliente") or "").strip())
    )
    n_titulos = len(titulos)
    n_ids_pedidos = len(ids_list)
    n_ids_achados = len(titulos_ids)

    emit_id = str(cab.get("emit_fornecedor_id") or "").strip()
    cliente_ok = True
    if n_titulos and emit_nome:
        labels_emit = [x for x in (emit_nome, emit_canon) if str(x or "").strip()]
        cliente_ok = False
        for c in clientes_titulo:
            if any(_entrada_nfe_nomes_fornecedor_batem(lbl, c) for lbl in labels_emit):
                cliente_ok = True
                break
        if not cliente_ok and emit_id:
            for t in titulos:
                tid = str(t.get("ClienteID") or t.get("ClienteId") or "").strip()
                if tid and tid == emit_id:
                    cliente_ok = True
                    break
        if not cliente_ok and db is not None and col_pessoa:
            for c in clientes_titulo:
                doc_p = _buscar_fornecedor_dto_pessoa_entrada_nfe(
                    db, col_pessoa, pid=emit_id, nome=c, cnpj=str(cab.get("emit_cnpj") or "")
                )
                if doc_p:
                    fant = _nome_exibicao_fornecedor_dto_pessoa(doc_p)
                    razao = str(doc_p.get("RazaoSocial") or doc_p.get("razaoSocial") or "").strip()
                    for lbl in labels_emit:
                        if _entrada_nfe_nomes_fornecedor_batem(lbl, c):
                            cliente_ok = True
                            break
                        if fant and _entrada_nfe_nomes_fornecedor_batem(lbl, fant):
                            cliente_ok = True
                            break
                        if razao and _entrada_nfe_nomes_fornecedor_batem(lbl, razao):
                            cliente_ok = True
                            break
                if cliente_ok:
                    break

    if tipo == "bonificacao":
        situacao = "bonificacao"
        detalhe = "Bonificação — não gera conta a pagar."
    elif n_titulos and cliente_ok:
        situacao = "ok"
        if fin_flag and n_ids_achados >= max(1, min(n_ids_pedidos, n_titulos)):
            detalhe = f"{n_titulos} título(s) no financeiro (vinculados pelo sistema)."
        elif fin_flag:
            detalhe = f"{n_titulos} título(s) encontrado(s); conferir IDs gravados na nota."
        else:
            detalhe = f"{n_titulos} título(s) localizado(s) por NF/chave (sem flag financeiro_lancado na nota)."
    elif n_titulos and not cliente_ok:
        situacao = "cliente_divergente"
        detalhe = (
            f"Título(s) existem, mas Cliente no Mongo ({', '.join(clientes_titulo[:2])}) "
            f"não bate com «{emit_nome}». Pode não aparecer na busca de Lançamentos."
        )
    elif fin_flag and n_ids_pedidos and n_ids_achados == 0:
        situacao = "titulo_sumido"
        detalhe = "Nota marcada com financeiro, mas os IDs do título não existem mais no Mongo."
    elif fin_flag and not n_ids_pedidos:
        situacao = "flag_sem_id"
        detalhe = "Nota marcada com financeiro lançado, mas sem IDs de título salvos."
    elif bucket == "concluida" or _entrada_nfe_extra_finalizacao_ok(extra):
        situacao = "sem_titulo"
        detalhe = "Concluída (PIN) sem título a pagar encontrado — gere o financeiro ou confira se é bonificação."
    elif bucket in ("financeiro", "finalizar"):
        situacao = "pendente"
        detalhe = "Ainda na fila financeiro/finalizar — título a pagar ainda não gerado."
    else:
        situacao = "sem_titulo"
        detalhe = "Nenhum título a pagar vinculado a esta nota."

    return {
        "situacao": situacao,
        "detalhe": detalhe[:500],
        "financeiro_lancado": fin_flag,
        "financeiro_ids_qtd": n_ids_pedidos,
        "financeiro_ids_encontrados": n_ids_achados,
        "titulos_qtd": n_titulos,
        "cliente_ok": cliente_ok,
        "emit_nome": emit_nome[:300],
        "emit_nome_canonico": emit_canon[:300],
        "clientes_titulo": clientes_titulo[:5],
        "lista_bucket": bucket,
        "tipo_entrada": tipo,
    }


def auditar_entrada_nfe_financeiro_lote(
    db,
    *,
    col_pessoa: str | None = None,
    filtro_lista: str | None = "concluida",
    limit: int = 300,
) -> dict[str, Any]:
    """Auditoria em lote das notas salvas (Mongo)."""
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível", "itens": [], "resumo": {}}
    lim = min(max(int(limit or 300), 1), 500)
    f = (filtro_lista or "todas").strip().lower()
    proj_aud = {
        "_id": 1,
        "status": 1,
        "cabecalho": 1,
        "modo": 1,
        "extra": 1,
        "criado_em": 1,
        "atualizado_em": 1,
    }
    scan_cap = min(lim * 4, 800) if f != "todas" else min(lim * 2, 400)
    try:
        cur = (
            db[COL_ENTRADA_RASCUNHO]
            .find({"status": {"$ne": ENTRADA_NFE_STATUS_DESCARTADA}}, proj_aud)
            .sort("atualizado_em", -1)
            .limit(scan_cap)
        )
        docs = list(cur)
    except Exception as exc:
        logger.exception("auditar_entrada_nfe_financeiro_lote")
        return {"ok": False, "erro": str(exc)[:400], "itens": [], "resumo": {}}

    alertas: list[dict[str, Any]] = []
    resumo: dict[str, int] = {}
    ok_count = 0
    avaliados = 0
    erros_item = 0

    for raw in docs:
        if avaliados >= lim:
            break
        try:
            d = _serialize_rascunho_leitura(raw)
        except Exception as exc:
            erros_item += 1
            logger.warning("auditoria entrada nfe serialize: %s", exc)
            continue
        if f != "todas":
            legacy = {
                "abertas": "em_andamento",
                "pendencias": "nota_aberta",
                "prontas": "estoque",
                "encerradas": "encerrada_legacy",
                "descartadas": "descartada",
            }
            ff = legacy.get(f, f)
            b = str(d.get("entrada_lista_bucket") or "")
            if ff == "em_andamento":
                if b not in ("nota_aberta", "estoque", "financeiro", "finalizar"):
                    continue
            elif ff != b:
                continue
        avaliados += 1
        b = str(d.get("entrada_lista_bucket") or "")
        try:
            aud = auditar_financeiro_rascunho_entrada_nfe(db, d, col_pessoa=col_pessoa)
        except Exception as exc:
            erros_item += 1
            logger.warning("auditoria entrada nfe item %s: %s", d.get("_id"), exc)
            aud = {
                "situacao": "erro_auditoria",
                "detalhe": f"Falha ao conferir esta nota: {str(exc)[:200]}",
            }
        sit = str(aud.get("situacao") or "")
        resumo[sit] = resumo.get(sit, 0) + 1
        if sit in ("ok", "bonificacao"):
            ok_count += 1
            continue
        cab = d.get("cabecalho") if isinstance(d.get("cabecalho"), dict) else {}
        alertas.append(
            {
                "id": str(d.get("_id") or ""),
                "fornecedor": str(cab.get("emit_nome") or "—")[:200],
                "nf": str(cab.get("numero") or "—")[:40],
                "lista_bucket": b,
                "situacao": sit,
                "detalhe": str(aud.get("detalhe") or "")[:500],
                "financeiro_lancado": bool(aud.get("financeiro_lancado")),
                "titulos_qtd": int(aud.get("titulos_qtd") or 0),
                "cliente_ok": bool(aud.get("cliente_ok")),
            }
        )
        if len(alertas) >= lim:
            break

    total_avaliado = sum(resumo.values())
    nota_extra = ""
    if erros_item:
        nota_extra = f" {erros_item} nota(s) com erro interno na conferência."
    return {
        "ok": True,
        "filtro": f,
        "total_avaliado": total_avaliado,
        "erros_auditoria": erros_item,
        "total_ok_ou_bonificacao": ok_count,
        "total_alertas": len(alertas),
        "resumo": resumo,
        "alertas": alertas,
        "nota": (
            "Concluída = PIN gravado na etapa 6; não garante sozinha que «Salvar + a pagar» foi feito. "
            "Alertas listam notas sem título no financeiro ou com nome de fornecedor diferente do título."
            + nota_extra
        ),
    }


def listar_rascunhos_entrada(db, limit: int = 30, *, filtro: str | None = None) -> list[dict]:
    if db is None:
        return []
    try:
        lim = min(max(limit, 1), 100)
        cur = db[COL_ENTRADA_RASCUNHO].aggregate(
            [
                {"$sort": {"criado_em": -1}},
                {"$limit": lim},
                {
                    "$project": {
                        "_id": 1,
                        "status": 1,
                        "cabecalho": 1,
                        "modo": 1,
                        "extra": 1,
                        "criado_em": 1,
                        "atualizado_em": 1,
                        "estoque_aplicado_em": 1,
                        "linhas_count": {
                            "$cond": [{"$isArray": "$linhas"}, {"$size": "$linhas"}, 0]
                        },
                    }
                },
            ]
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
        }
        if f in legacy:
            f = legacy[f]
        valid_extra = frozenset(
            {
                "nota_aberta",
                "estoque",
                "financeiro",
                "finalizar",
                "concluida",
                "descartada",
                "encerrada",
                "em_andamento",
                "correcao_sistemica",
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
                if b in ("nota_aberta", "estoque", "financeiro", "finalizar"):
                    filtrados.append(item)
            elif f == "correcao_sistemica":
                if item.get("entrada_correcao_sistemica") and b in (
                    "nota_aberta",
                    "estoque",
                    "financeiro",
                    "finalizar",
                ):
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


def claim_rascunho_para_estoque_agro(db, oid: str) -> dict[str, Any]:
    """
    Trava o rascunho para um único POST de estoque Agro (evita somar várias vezes no duplo clique).
    """
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    _id = _object_id_rascunho(oid)
    if _id is None:
        return {"ok": False, "erro": "ID de rascunho inválido."}
    agora = datetime.now(timezone.utc)
    stale = agora - ESTOQUE_AGRO_LOCK_MAX_AGE
    try:
        doc = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id})
        if not doc:
            return {"ok": False, "erro": "Rascunho não encontrado."}
        if str(doc.get("status") or "").strip().lower() == ENTRADA_NFE_STATUS_ESTOQUE_APLICADO:
            return {
                "ok": False,
                "erro": "Estoque Agro já foi registrado para este rascunho.",
            }
        r = db[COL_ENTRADA_RASCUNHO].update_one(
            {
                "_id": _id,
                "status": {"$nin": list(ENTRADA_NFE_STATUS_CONGELADOS)},
                "$or": [
                    {"extra.estoque_agro_lock": {"$exists": False}},
                    {"extra.estoque_agro_lock": None},
                    {"extra.estoque_agro_lock": {"$lte": stale}},
                ],
            },
            {"$set": {"extra.estoque_agro_lock": agora}},
        )
        if r.modified_count == 1:
            return {"ok": True}
        doc2 = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id}) or {}
        ex2 = doc2.get("extra") if isinstance(doc2.get("extra"), dict) else {}
        if ex2.get("estoque_agro_lock"):
            return {
                "ok": False,
                "erro": "Registro de estoque em andamento ou repetido. Aguarde alguns segundos e atualize a página.",
            }
        return {"ok": False, "erro": "Não foi possível registrar o estoque (rascunho encerrado ou indisponível)."}
    except Exception as exc:
        logger.exception("claim_rascunho_para_estoque_agro")
        return {"ok": False, "erro": str(exc)[:500]}


def release_rascunho_estoque_agro_claim(db, oid: str) -> None:
    """Remove a trava de POST duplicado (falha antes de marcar estoque aplicado)."""
    if db is None:
        return
    _id = _object_id_rascunho(oid)
    if _id is None:
        return
    try:
        db[COL_ENTRADA_RASCUNHO].update_one(
            {"_id": _id},
            {"$unset": {"extra.estoque_agro_lock": ""}},
        )
    except Exception:
        logger.exception("release_rascunho_estoque_agro_claim")


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
    col_pessoa: str | None = None,
) -> dict[str, Any]:
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    _id = _object_id_rascunho(oid)
    if _id is None:
        return {"ok": False, "erro": "ID inválido."}
    cab_norm = cabecalho
    if col_pessoa:
        cab_norm = normalizar_cabecalho_emit_fornecedor_entrada_nfe(db, col_pessoa, cabecalho)
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
        # Evita corrida autosave/manual vs. ``marcar_rascunho_financeiro_lancado``: merge com snapshot
        # antigo não pode reapagar nem reintroduzir sozinho o carimbo de financeiro.
        fresh_mini = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id}, projection={"extra": 1}) or {}
        fresh_ex = fresh_mini.get("extra") if isinstance(fresh_mini.get("extra"), dict) else {}
        if entrada_nfe_extra_financeiro_ok(fresh_ex):
            merged_extra["financeiro_lancado"] = True
            if "financeiro_ids" in fresh_ex:
                ids_f = fresh_ex["financeiro_ids"]
                merged_extra["financeiro_ids"] = list(ids_f) if isinstance(ids_f, list) else ids_f
            if "financeiro_lancado_em" in fresh_ex:
                merged_extra["financeiro_lancado_em"] = fresh_ex["financeiro_lancado_em"]
        else:
            merged_extra.pop("financeiro_lancado", None)
            merged_extra.pop("financeiro_ids", None)
            merged_extra.pop("financeiro_lancado_em", None)
        set_doc: dict[str, Any] = {
            "atualizado_em": datetime.now(timezone.utc),
            "usuario_ultima_alteracao": (usuario or "")[:200],
            "modo": (modo or "manual")[:40],
            "cabecalho": cab_norm,
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
        ex.pop("estoque_agro_lock", None)
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


def reverter_integracao_entrada_nota_para_reabertura(
    db,
    oid: str,
    *,
    usuario: str = "",
) -> dict[str, Any]:
    """
    Remove carimbo ``aprovacao_wizard_*``, flags de etapa do assistente e estorna o que for rastreável:

    - Títulos Mongo em ``extra.financeiro_ids`` (só se ``financeiro_lancado``), via
      ``excluir_lancamento_mongo_agro`` (falha se quitado ou vínculo ERP).
    - Ajustes ``AjusteRapidoEstoque`` em ``extra.estoque_agro_ajuste_ids`` (só se o status
      do rascunho é ``estoque_aplicado``), origem ``entrada_nf_agro``.

    Pode ser chamado **sem** PIN final na nota quando há só travas de etapa (``wizard_etapa1/2/3_confirmada_em``)
    ou integrações já lançadas — assim «Reabrir nota» desfaz duplicidade antes de novo envio.

    Falha sem alterar o documento se alguma exclusão financeira não for permitida.
    """
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    _id = _object_id_rascunho(oid)
    if _id is None:
        return {"ok": False, "erro": "ID inválido."}
    from estoque.models import AjusteRapidoEstoque, OrigemAjusteEstoque

    from produtos.mongo_financeiro_util import excluir_lancamento_mongo_agro

    try:
        doc = db[COL_ENTRADA_RASCUNHO].find_one({"_id": _id})
        if not doc:
            return {"ok": False, "erro": "Rascunho não encontrado."}
        ex = dict(doc.get("extra") or {})
        had_pin = bool(str(ex.get("aprovacao_wizard_em") or "").strip())
        had_wiz1 = bool(str(ex.get("wizard_etapa1_confirmada_em") or "").strip())
        had_wiz2 = bool(str(ex.get("wizard_etapa2_confirmada_em") or "").strip())
        had_wiz3 = bool(str(ex.get("wizard_etapa3_confirmada_em") or "").strip())

        st_doc = str(doc.get("status") or "").strip().lower()
        had_estoque = st_doc == ENTRADA_NFE_STATUS_ESTOQUE_APLICADO
        had_fin = bool(ex.get("financeiro_lancado"))

        if not (had_pin or had_wiz1 or had_wiz2 or had_wiz3 or had_estoque or had_fin):
            return {
                "ok": False,
                "erro": "Nada para reabrir: assistente sem etapas confirmadas nem estoque/financeiro registrado.",
            }

        raw_aj = ex.get("estoque_agro_ajuste_ids") or []
        ajuste_ids: list[int] = []
        for x in raw_aj:
            try:
                ajuste_ids.append(int(x))
            except (TypeError, ValueError):
                continue

        fin_raw = ex.get("financeiro_ids") or []
        fin_ids = [str(x).strip() for x in fin_raw if str(x).strip()]

        if had_estoque and not ajuste_ids:
            return {
                "ok": False,
                "erro": (
                    "Esta entrada tem estoque aplicado sem lista de ajustes rastreada (nota antiga). "
                    "Não é possível reabrir com segurança — contate suporte."
                ),
            }
        if had_fin and not fin_ids:
            return {
                "ok": False,
                "erro": (
                    "Esta entrada tem financeiro marcado sem IDs dos títulos. "
                    "Não é possível reabrir com segurança — contate suporte."
                ),
            }

        fin_falhas: list[dict[str, str]] = []
        for fid in fin_ids:
            r = excluir_lancamento_mongo_agro(db, fid, usuario or "")
            if not r.get("ok"):
                msg = str(r.get("erro") or "exclusão não permitida")[:400]
                fin_falhas.append({"id": fid, "erro": msg})

        if fin_falhas:
            primeiro = fin_falhas[0]
            return {
                "ok": False,
                "erro": (
                    f"Não foi possível estornar o título financeiro {primeiro.get('id')}: "
                    f"{primeiro.get('erro')} "
                    "(quitado, com baixa ou vínculo ERP). Ajuste em Lançamentos ou no ERP e tente de novo."
                ),
                "financeiro_falhas": fin_falhas,
            }

        n_estoque_del = 0
        if had_estoque and ajuste_ids:
            try:
                qs = AjusteRapidoEstoque.objects.filter(
                    pk__in=ajuste_ids,
                    origem=OrigemAjusteEstoque.ENTRADA_NF_AGRO,
                )
                n_estoque_del, _ = qs.delete()
            except Exception as exc:
                logger.exception("reverter_integracao_entrada_nota_para_reabertura estoque")
                return {"ok": False, "erro": f"Falha ao estornar ajustes de estoque: {exc}"[:500]}

        linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
        novo_status = entrada_nfe_status_derivado_linhas(linhas)

        for k in (
            "aprovacao_wizard_em",
            "aprovacao_wizard_usuario",
            "wizard_etapa1_confirmada_em",
            "wizard_etapa2_confirmada_em",
            "wizard_etapa3_confirmada_em",
            "financeiro_lancado",
            "financeiro_ids",
            "financeiro_lancado_em",
            "estoque_agro_ajuste_ids",
            "estoque_agro_registrado_em",
        ):
            ex.pop(k, None)
        ex.pop("estoque_agro_lock", None)

        agora = datetime.now(timezone.utc)
        unset_doc: dict[str, str] = {}
        if had_estoque:
            unset_doc["estoque_aplicado_em"] = ""
            unset_doc["usuario_estoque_aplicado"] = ""

        upd: dict[str, Any] = {
            "$set": {
                "status": novo_status,
                "extra": ex,
                "atualizado_em": agora,
                "usuario_ultima_alteracao": (usuario or "")[:200],
            }
        }
        if unset_doc:
            upd["$unset"] = unset_doc

        db[COL_ENTRADA_RASCUNHO].update_one({"_id": _id}, upd)
        return {
            "ok": True,
            "id": str(_id),
            "status": novo_status,
            "estoque_ajustes_removidos": int(n_estoque_del),
            "financeiro_titulos_removidos": len(fin_ids),
        }
    except Exception as exc:
        logger.exception("reverter_integracao_entrada_nota_para_reabertura")
        return {"ok": False, "erro": str(exc)[:500]}


def marcar_rascunho_financeiro_lancado(
    db,
    oid: str,
    *,
    ids: list[str],
    usuario: str = "",
    lote: str | None = None,
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
        fin_ids_lim = [str(x) for x in (ids or [])][:80]
        sets: dict[str, Any] = {
            "atualizado_em": agora,
            "usuario_ultima_alteracao": (usuario or "")[:200],
            "extra.financeiro_lancado": True,
            "extra.financeiro_ids": fin_ids_lim,
            "extra.financeiro_lancado_em": agora.isoformat(),
        }
        lote_s = str(lote or "").strip().upper()
        if lote_s:
            sets["extra.financeiro_lote"] = lote_s[:32]
        db[COL_ENTRADA_RASCUNHO].update_one(
            {"_id": _id},
            {"$set": sets},
        )
        return {"ok": True, "id": str(_id)}
    except Exception as exc:
        logger.exception("marcar_rascunho_financeiro_lancado")
        return {"ok": False, "erro": str(exc)[:500]}


_LOTE_AGRO_CODIGO_RE = re.compile(r"AG[0-9A-F]{8}", re.I)
_LOTE_AGRO_NUMDOC_RE = re.compile(r"^(AG[0-9A-F]{8})(?:-\d{2}(?:-p\d+)?)?$", re.I)


def _extrair_lote_agro_lancamento(linha: dict[str, Any]) -> str:
    """Código do lote manual Agro (ex.: ``AG2C0C39E7`` em ``AG2C0C39E7-01`` ou observações)."""
    nd = str(linha.get("numero_documento") or "").strip()
    m = _LOTE_AGRO_NUMDOC_RE.match(nd)
    if m:
        return m.group(1).upper()
    texto = " ".join(
        str(linha.get(k) or "")
        for k in ("observacoes", "descricao", "numero_documento")
    )
    m2 = _LOTE_AGRO_CODIGO_RE.search(texto)
    return m2.group(0).upper() if m2 else ""


def _extrair_nf_numero_lancamento(linha: dict[str, Any]) -> str:
    nd = str(linha.get("numero_documento") or "").strip()
    if nd and nd not in ("0", "000") and not _LOTE_AGRO_NUMDOC_RE.match(nd):
        return nd
    desc = str(linha.get("descricao") or "")
    m = re.match(r"^NF\s+(\S+)", desc, re.I)
    if m:
        return m.group(1).strip("—- ").split()[0]
    return ""


def _ids_titulos_mongo_por_lote_agro(db, lote: str) -> list[str]:
    from .mongo_financeiro_util import COL_DTO_LANCAMENTO

    if db is None or not lote:
        return []
    lote = str(lote).strip().upper()
    if not _LOTE_AGRO_CODIGO_RE.fullmatch(lote):
        return []
    out: list[str] = []
    seen: set[str] = set()
    try:
        rx = re.compile(rf"^{re.escape(lote)}-", re.I)
        cur = db[COL_DTO_LANCAMENTO].find(
            {"NumeroDocumento": rx},
            {"_id": 1},
        ).limit(80)
        for doc in cur:
            sid = str(doc.get("_id", ""))
            if sid and sid not in seen:
                seen.add(sid)
                out.append(sid)
    except Exception as exc:
        logger.warning("_ids_titulos_mongo_por_lote_agro: %s", exc)
    return out


def _rascunho_entrada_por_ids_financeiro(
    db, titulo_ids: list[str]
) -> str | None:
    if db is None or not titulo_ids:
        return None
    try:
        doc = db[COL_ENTRADA_RASCUNHO].find_one(
            {
                "extra.financeiro_ids": {"$in": titulo_ids},
                "status": {"$ne": ENTRADA_NFE_STATUS_DESCARTADA},
            },
            {"_id": 1},
            sort=[("atualizado_em", -1)],
        )
        if doc:
            return str(doc.get("_id", ""))
    except Exception as exc:
        logger.warning("_rascunho_entrada_por_ids_financeiro: %s", exc)
    return None


def _rascunho_entrada_por_lote_agro(
    db, lote: str, *, cliente: str = ""
) -> str | None:
    if db is None or not lote:
        return None
    lote = str(lote).strip().upper()
    try:
        doc = db[COL_ENTRADA_RASCUNHO].find_one(
            {
                "extra.financeiro_lote": lote,
                "status": {"$ne": ENTRADA_NFE_STATUS_DESCARTADA},
            },
            {"_id": 1},
            sort=[("atualizado_em", -1)],
        )
        if doc:
            return str(doc.get("_id", ""))
    except Exception as exc:
        logger.warning("_rascunho_entrada_por_lote_agro financeiro_lote: %s", exc)

    sibling_ids = _ids_titulos_mongo_por_lote_agro(db, lote)
    rid = _rascunho_entrada_por_ids_financeiro(db, sibling_ids)
    if rid:
        return rid

    cliente = (cliente or "").strip()
    if not cliente:
        return None
    try:
        cur = db[COL_ENTRADA_RASCUNHO].find(
            {
                "status": {"$ne": ENTRADA_NFE_STATUS_DESCARTADA},
                "extra.financeiro_lancado": True,
            },
            {"cabecalho": 1, "_id": 1, "atualizado_em": 1},
        ).sort("atualizado_em", -1).limit(250)
        matches: list[str] = []
        for rz in cur:
            cab = rz.get("cabecalho") if isinstance(rz.get("cabecalho"), dict) else {}
            emit = str(cab.get("emit_nome") or "").strip()
            if emit and _entrada_nfe_nomes_fornecedor_batem(emit, cliente):
                matches.append(str(rz.get("_id", "")))
        if len(matches) == 1:
            return matches[0]
    except Exception as exc:
        logger.warning("_rascunho_entrada_por_lote_agro fornecedor: %s", exc)
    return None


def map_lancamentos_entrada_nfe_rascunho_ids(
    db, linhas: list[dict[str, Any]]
) -> dict[str, str]:
    """Mapeia ID do título Mongo → ID do rascunho Entrada NF-e (``financeiro_ids``, NF ou lote Agro)."""
    if db is None or not linhas:
        return {}
    ids = [str(x.get("id") or "").strip() for x in linhas if x.get("id")]
    ids = [x for x in ids if x]
    if not ids:
        return {}
    out: dict[str, str] = {}
    try:
        cur = db[COL_ENTRADA_RASCUNHO].find(
            {
                "extra.financeiro_ids": {"$in": ids},
                "status": {"$ne": ENTRADA_NFE_STATUS_DESCARTADA},
            },
            {"extra.financeiro_ids": 1},
        ).limit(250)
        for doc in cur:
            rid = str(doc.get("_id", ""))
            extra = doc.get("extra") if isinstance(doc.get("extra"), dict) else {}
            fin_list = extra.get("financeiro_ids") if isinstance(extra, dict) else []
            if not isinstance(fin_list, list):
                continue
            for lid in fin_list:
                sid = str(lid or "").strip()
                if sid and sid in ids and sid not in out:
                    out[sid] = rid
    except Exception as exc:
        logger.warning("map_lancamentos_entrada_nfe_rascunho_ids: %s", exc)

    missing = [ln for ln in linhas if str(ln.get("id") or "") not in out]
    nf_por_lanc: dict[str, list[str]] = {}
    for ln in missing:
        lid = str(ln.get("id") or "")
        nf = _extrair_nf_numero_lancamento(ln)
        if nf and lid:
            nf_por_lanc.setdefault(nf, []).append(lid)
    if nf_por_lanc:
        try:
            ors = [{"cabecalho.numero": nf} for nf in nf_por_lanc]
            cur2 = db[COL_ENTRADA_RASCUNHO].find(
                {
                    "$and": [
                        {"$or": ors},
                        {"status": {"$ne": ENTRADA_NFE_STATUS_DESCARTADA}},
                    ]
                },
                {"cabecalho.numero": 1},
            ).limit(120)
            for doc in cur2:
                rid = str(doc.get("_id", ""))
                cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
                nf = str(cab.get("numero") or "").strip()
                for lid in nf_por_lanc.get(nf, []):
                    if lid not in out:
                        out[lid] = rid
        except Exception as exc:
            logger.warning("map_lancamentos_entrada_nfe_rascunho_ids fallback nf: %s", exc)

    missing_lote = [ln for ln in linhas if str(ln.get("id") or "") not in out]
    lote_por_lanc: dict[str, list[str]] = {}
    lote_cliente: dict[str, str] = {}
    for ln in missing_lote:
        lid = str(ln.get("id") or "")
        lt = _extrair_lote_agro_lancamento(ln)
        if not lt or not lid:
            continue
        lote_por_lanc.setdefault(lt, []).append(lid)
        if lt not in lote_cliente:
            lote_cliente[lt] = str(ln.get("cliente") or "").strip()
    for lote, lanc_ids in lote_por_lanc.items():
        rid = _rascunho_entrada_por_lote_agro(
            db, lote, cliente=lote_cliente.get(lote) or ""
        )
        if not rid:
            continue
        for lid in lanc_ids:
            if lid not in out:
                out[lid] = rid
    return out


def enriquecer_lancamentos_entrada_nfe_rascunho(
    db, linhas: list[dict[str, Any]]
) -> None:
    """Preenche ``entrada_nfe_rascunho_id`` e ``entrada_nfe_url`` nas linhas da API."""
    if not linhas:
        return
    mp = map_lancamentos_entrada_nfe_rascunho_ids(db, linhas)
    for ln in linhas:
        lid = str(ln.get("id") or "")
        rid = mp.get(lid)
        if rid:
            ln["entrada_nfe_rascunho_id"] = rid
            ln["entrada_nfe_url"] = f"/entrada-nota/?rascunho={rid}&passo=2"


def pipeline_acao_rascunho_entrada(
    db,
    oid: str,
    acao: str,
    *,
    usuario: str = "",
) -> dict[str, Any]:
    """descartar | reabrir | correcao_sistemica_on | correcao_sistemica_off.

    ``encerrar`` está desativado: a lista trata **Concluída** só com estoque aplicado + financeiro.
    """
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
            return {
                "ok": False,
                "erro": "Encerramento manual foi desativado. Use estoque Agro e Salvar + a pagar para concluir.",
            }
        elif ac == "descartar":
            novo = ENTRADA_NFE_STATUS_DESCARTADA
        elif ac == "reabrir":
            if st not in (ENTRADA_NFE_STATUS_ENCERRADA, ENTRADA_NFE_STATUS_DESCARTADA):
                return {"ok": False, "erro": "Só é possível reabrir notas encerradas ou descartadas."}
            novo = entrada_nfe_status_derivado_linhas(linhas)
        elif ac in ("correcao_sistemica_on", "correcao_sistemica_off"):
            ex0 = doc.get("extra") if isinstance(doc.get("extra"), dict) else {}
            if str(ex0.get("aprovacao_wizard_em") or "").strip():
                return {
                    "ok": False,
                    "erro": "Nota já finalizada no assistente — não é possível alterar esta marca.",
                }
            if st == ENTRADA_NFE_STATUS_DESCARTADA:
                return {"ok": False, "erro": "Nota descartada."}
            if ac == "correcao_sistemica_on":
                db[COL_ENTRADA_RASCUNHO].update_one(
                    {"_id": _id},
                    {
                        "$set": {
                            "extra.correcao_sistemica": True,
                            "extra.correcao_sistemica_em": agora.isoformat(),
                            "atualizado_em": agora,
                            "usuario_ultima_alteracao": (usuario or "")[:200],
                        }
                    },
                )
            else:
                db[COL_ENTRADA_RASCUNHO].update_one(
                    {"_id": _id},
                    {
                        "$unset": {
                            "extra.correcao_sistemica": "",
                            "extra.correcao_sistemica_em": "",
                        },
                        "$set": {
                            "atualizado_em": agora,
                            "usuario_ultima_alteracao": (usuario or "")[:200],
                        },
                    },
                )
            return {"ok": True, "id": str(_id), "correcao_sistemica": ac == "correcao_sistemica_on"}
        else:
            return {"ok": False, "erro": "Ação inválida."}
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


def _entrada_nfe_chave_nome_fornecedor(nome) -> str:
    return " ".join(str(nome or "").strip().lower().split())


def _entrada_nfe_tokens_nome_fornecedor(nome: str) -> set[str]:
    """Palavras significativas para casar fantasia (Sn - X) com razão social."""
    s = re.sub(r"[^a-z0-9]+", " ", str(nome or "").lower())
    stop = {
        "ltda",
        "ltd",
        "sa",
        "me",
        "epp",
        "comercio",
        "comércio",
        "distribuidora",
        "distribuicao",
        "alimentos",
        "produtos",
        "industria",
        "indústria",
        "de",
        "da",
        "do",
        "das",
        "dos",
        "e",
        "sn",
        "cn",
    }
    return {t for t in s.split() if len(t) >= 4 and t not in stop}


def _entrada_nfe_nomes_fornecedor_batem(nome_a: str, nome_b: str) -> bool:
    """Fantasia vs razão social do mesmo cadastro (ex.: Sn - Europet / EUROPET DISTRIBUIDORA)."""
    ka = _entrada_nfe_chave_nome_fornecedor(nome_a)
    kb = _entrada_nfe_chave_nome_fornecedor(nome_b)
    if not ka or not kb:
        return False
    if ka == kb or ka in kb or kb in ka:
        return True
    ta = _entrada_nfe_tokens_nome_fornecedor(nome_a)
    tb = _entrada_nfe_tokens_nome_fornecedor(nome_b)
    return bool(ta and tb and (ta & tb))


def _entrada_nfe_chave_doc_fornecedor(documento: str) -> str:
    d = re.sub(r"\D", "", str(documento or ""))
    return d if len(d) >= 11 else ""


def _entrada_nfe_chave_id_fornecedor(pid: str) -> str:
    return str(pid or "").strip().lower()


def _nome_exibicao_fornecedor_dto_pessoa(doc: dict) -> str:
    """Nome que o ERP costuma exibir (fantasia antes de razão social)."""
    if not isinstance(doc, dict):
        return ""
    for chave in (
        "NomeFantasia",
        "Fantasia",
        "nomeFantasia",
        "fantasia",
        "Nome",
        "nome",
        "RazaoSocial",
        "razaoSocial",
        "Apelido",
        "apelido",
    ):
        v = doc.get(chave)
        if v is not None:
            s = str(v).strip()
            if len(s) >= 2:
                return s[:300]
    return ""


def _documento_fornecedor_dto_pessoa(doc: dict) -> str:
    if not isinstance(doc, dict):
        return ""
    for key in (
        "CpfCnpj",
        "CNPJ",
        "Cnpj",
        "CPF",
        "Cpf",
        "cpfCnpj",
        "Documento",
        "documento",
        "InscricaoFederal",
        "inscricaoFederal",
    ):
        raw = doc.get(key)
        if raw is None:
            continue
        d = re.sub(r"\D", "", str(raw))[:18]
        if len(d) >= 11:
            return d
    return ""


def _id_fornecedor_dto_pessoa(doc: dict) -> str:
    if not isinstance(doc, dict):
        return ""
    pid = str(doc.get("Id") or doc.get("id") or doc.get("ID") or "").strip()
    if pid:
        return pid
    oid = doc.get("_id")
    if oid is not None:
        return str(oid).strip()
    return ""


def _mongo_filtro_id_pessoa_externo(pid_str: str) -> dict[str, Any]:
    pid = str(pid_str or "").strip()
    if not pid:
        return {"_id": None}
    ors: list[dict[str, Any]] = [{"Id": pid}, {"id": pid}, {"ID": pid}]
    try:
        ors.append({"Id": int(pid)})
    except (TypeError, ValueError):
        pass
    try:
        from bson import ObjectId

        ors.append({"_id": ObjectId(pid)})
    except Exception:
        pass
    return {"$or": ors}


_PROJ_FORNECEDOR_ENTRADA_NFE = {
    "Nome": 1,
    "RazaoSocial": 1,
    "NomeFantasia": 1,
    "Fantasia": 1,
    "CpfCnpj": 1,
    "Id": 1,
    "_id": 1,
    "CNPJ": 1,
    "Cnpj": 1,
    "CPF": 1,
    "Cpf": 1,
}


def _linha_fornecedor_entrada_nfe_de_doc_mongo(d: dict) -> dict[str, str] | None:
    nome = _nome_exibicao_fornecedor_dto_pessoa(d)
    if not nome:
        return None
    pid = _id_fornecedor_dto_pessoa(d)
    if not pid:
        return None
    documento = _documento_fornecedor_dto_pessoa(d)
    row: dict[str, str] = {
        "id": pid,
        "nome": nome[:300],
        "documento": documento,
        "origem": "mongo",
    }
    razao = str(d.get("RazaoSocial") or d.get("razaoSocial") or "").strip()
    if (
        razao
        and _entrada_nfe_chave_nome_fornecedor(razao) != _entrada_nfe_chave_nome_fornecedor(nome)
    ):
        row["razao_social"] = razao[:300]
    return row


def _buscar_fornecedor_dto_pessoa_entrada_nfe(
    db,
    col_pessoa: str,
    *,
    pid: str = "",
    cnpj: str = "",
    nome: str = "",
) -> dict | None:
    if db is None or not col_pessoa:
        return None
    col = db[col_pessoa]
    pid = str(pid or "").strip()
    if pid and not pid.lower().startswith("local:"):
        try:
            doc = col.find_one(_mongo_filtro_id_pessoa_externo(pid), _PROJ_FORNECEDOR_ENTRADA_NFE)
            if isinstance(doc, dict):
                return doc
        except Exception:
            pass
    digits = re.sub(r"\D", "", str(cnpj or ""))
    if len(digits) >= 11:
        try:
            cond: dict[str, Any] = {
                "$or": [
                    {"CpfCnpj": {"$regex": re.escape(digits)}},
                    {"CNPJ": {"$regex": re.escape(digits)}},
                    {"Cnpj": {"$regex": re.escape(digits)}},
                ]
            }
            doc = col.find_one(cond, _PROJ_FORNECEDOR_ENTRADA_NFE)
            if isinstance(doc, dict):
                return doc
        except Exception:
            pass
    nome = str(nome or "").strip()
    if len(nome) >= 2:
        esc = re.escape(nome)
        try:
            cond = {
                "$or": [
                    {"NomeFantasia": {"$regex": f"^{esc}$", "$options": "i"}},
                    {"Nome": {"$regex": f"^{esc}$", "$options": "i"}},
                    {"RazaoSocial": {"$regex": f"^{esc}$", "$options": "i"}},
                ]
            }
            doc = col.find_one(cond, _PROJ_FORNECEDOR_ENTRADA_NFE)
            if isinstance(doc, dict):
                return doc
        except Exception:
            pass
    return None


def normalizar_cabecalho_emit_fornecedor_entrada_nfe(
    db,
    col_pessoa: str,
    cab: dict | None,
) -> dict:
    """
    Alinha emitente ao cadastro Mongo (nome fantasia + Id do ERP) para títulos a pagar
    baterem com Lançamentos / contas a pagar — evita razão social da NF sem vínculo.
    """
    if not isinstance(cab, dict):
        return cab if cab is not None else {}
    out = dict(cab)
    doc = _buscar_fornecedor_dto_pessoa_entrada_nfe(
        db,
        col_pessoa,
        pid=str(out.get("emit_fornecedor_id") or "").strip(),
        cnpj=str(out.get("emit_cnpj") or "").strip(),
        nome=str(out.get("emit_nome") or "").strip(),
    )
    if not doc:
        return out
    nome_can = _nome_exibicao_fornecedor_dto_pessoa(doc)
    pid_can = _id_fornecedor_dto_pessoa(doc)
    doc_cnpj = _documento_fornecedor_dto_pessoa(doc)
    if nome_can:
        out["emit_nome"] = nome_can[:300]
    if pid_can:
        out["emit_fornecedor_id"] = pid_can
    if doc_cnpj:
        out["emit_cnpj"] = doc_cnpj
    return out


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
    Nome exibido prioriza **fantasia** (como no ERP), não razão social.
    """
    out: list[dict[str, str]] = []
    if db is None or not col_pessoa:
        return out
    lim = min(max(int(limit or 50), 1), 100)
    q = (q or "").strip()
    col = db[col_pessoa]
    proj = _PROJ_FORNECEDOR_ENTRADA_NFE
    try:
        if not q and inicial:
            cur = col.find({}, proj).sort([("NomeFantasia", 1), ("Nome", 1)]).limit(lim)
        elif q:
            esc = re.escape(q)
            cond: dict[str, Any] = {
                "$or": [
                    {"NomeFantasia": {"$regex": esc, "$options": "i"}},
                    {"Nome": {"$regex": esc, "$options": "i"}},
                    {"RazaoSocial": {"$regex": esc, "$options": "i"}},
                ]
            }
            digits = re.sub(r"\D", "", q)
            if len(digits) >= 2:
                cond["$or"].append({"CpfCnpj": {"$regex": re.escape(digits)}})
                cond["$or"].append({"CNPJ": {"$regex": re.escape(digits)}})
                cond["$or"].append({"Cnpj": {"$regex": re.escape(digits)}})
            cur = col.find(cond, proj).sort([("NomeFantasia", 1), ("Nome", 1)]).limit(lim)
        else:
            return out
        seen: set[str] = set()
        for d in cur:
            row = _linha_fornecedor_entrada_nfe_de_doc_mongo(d)
            if not row:
                continue
            pid = row["id"]
            if pid in seen:
                continue
            seen.add(pid)
            out.append(row)
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


def propagar_precos_venda_catalogo_entrada_nota(
    db,
    client_m,
    linhas: list,
    *,
    _usuario_label: str = "",
) -> dict[str, Any]:
    """
    Copia o P. venda das linhas da NF para o espelho Mongo (``DtoProduto``) e para o overlay SQLite,
    para o PDV e a busca refletirem o preço após salvar / atualizar rascunho ou fluxos ligados.
    Ignora linhas sem ``produto_id`` de catálogo ou com preço ≤ 0.
    """
    from bson import ObjectId

    from produtos.models import ProdutoGestaoOverlayAgro

    try:
        from produtos.views import (
            _mongo_doc_produto_entrada_resolve as _doc_entrada_ln,
            _mongo_filtro_id_produto_externo as _filt_entrada_ln,
            _mongo_produto_externo_id_overlay as _ov_id_entrada,
        )
    except Exception:  # pragma: no cover - import circular em alguns testes mínimos
        _doc_entrada_ln = None
        _filt_entrada_ln = None
        _ov_id_entrada = None

    out: dict[str, Any] = {
        "ok": True,
        "atualizados_mongo": 0,
        "atualizados_overlay": 0,
        "produto_ids": [],
    }
    if db is None or client_m is None or not linhas:
        return out
    col = client_m.col_p
    ids_erp: set[str] = set()
    for ln in linhas:
        if not isinstance(ln, dict):
            continue
        pid = str(ln.get("produto_id") or "").strip()
        if not pid or pid.lower().startswith("local:"):
            continue
        try:
            pv = float(ln.get("preco_venda") or 0)
        except (TypeError, ValueError):
            continue
        if pv <= 0.004:
            continue
        pv = round(float(pv), 2)
        c_prod = str(ln.get("c_prod") or "").strip()
        ean_d = "".join(ch for ch in str(ln.get("ean") or "") if ch.isdigit())
        doc_ln = None
        if _doc_entrada_ln:
            doc_ln = _doc_entrada_ln(db, col, pid, codigo_catalogo=c_prod, ean=ean_d)
        id_u = pid
        if isinstance(doc_ln, dict):
            id_u = (
                str(doc_ln.get("Id") or "").strip() or str(doc_ln.get("_id") or "").strip() or pid
            )
        if _filt_entrada_ln:
            filt = _filt_entrada_ln(id_u)
        else:
            or_filt: list[dict[str, Any]] = [{"Id": pid}]
            try:
                or_filt.append({"Id": int(pid)})
            except (TypeError, ValueError):
                pass
            try:
                or_filt.append({"_id": ObjectId(pid)})
            except Exception:
                pass
            filt = {"$or": or_filt}
        try:
            r = db[col].update_one(filt, {"$set": {"ValorVenda": pv, "PrecoVenda": pv}})
            if r.matched_count:
                out["atualizados_mongo"] += 1
                pid_erp = ""
                if isinstance(doc_ln, dict) and str(doc_ln.get("Id") or "").strip():
                    pid_erp = str(doc_ln.get("Id")).strip()[:64]
                elif str(id_u or "").strip():
                    pid_erp = str(id_u).strip()[:64]
                if pid_erp:
                    ids_erp.add(pid_erp)
        except Exception as exc:
            logger.warning("propagar_precos mongo %s: %s", pid, exc)
        try:
            ov_key = (_ov_id_entrada(db, col, id_u) if _ov_id_entrada else None) or id_u
            dec = Decimal(str(pv))
            ProdutoGestaoOverlayAgro.objects.update_or_create(
                produto_externo_id=str(ov_key)[:64],
                defaults={"preco_venda": dec},
            )
            out["atualizados_overlay"] += 1
        except Exception as exc:
            logger.warning("propagar_precos overlay %s: %s", pid, exc)
    if ids_erp:
        out["produto_ids"] = sorted(ids_erp)
    return out
