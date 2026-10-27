"""NF-e saída (modelo 55) — rascunhos Postgres ou Mongo, prefill de VendaAgro, validação e listagem."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from decouple import config

from produtos.agro_produto_fiscal_defaults import fiscal_padrao_ui_cadastro, normalizar_ncm_somente_digitos

logger = logging.getLogger(__name__)

COL_NFE_SAIDA = "AgroNfeSaida"

NFE_SAIDA_STATUS_RASCUNHO = "rascunho"
NFE_SAIDA_STATUS_ENVIANDO = "enviando"
NFE_SAIDA_STATUS_AUTORIZADA = "autorizada"
NFE_SAIDA_STATUS_REJEITADA = "rejeitada"
NFE_SAIDA_STATUS_CANCELADA = "cancelada"
NFE_SAIDA_STATUS_DESCARTADA = "descartada"

NFE_SAIDA_STATUS_UI: dict[str, dict[str, str]] = {
    NFE_SAIDA_STATUS_RASCUNHO: {"label": "Rascunho", "chip": "amber"},
    NFE_SAIDA_STATUS_ENVIANDO: {"label": "Enviando", "chip": "sky"},
    NFE_SAIDA_STATUS_AUTORIZADA: {"label": "Autorizada", "chip": "emerald"},
    NFE_SAIDA_STATUS_REJEITADA: {"label": "Rejeitada", "chip": "red"},
    NFE_SAIDA_STATUS_CANCELADA: {"label": "Cancelada", "chip": "slate"},
    NFE_SAIDA_STATUS_DESCARTADA: {"label": "Descartada", "chip": "slate"},
}


def nfe_saida_col(db):
    if db is None:
        return None
    return db[COL_NFE_SAIDA]


def nfe_saida_usa_postgres() -> bool:
    from produtos.agro_fonte_config import agro_nfe_saida_rascunho_postgres

    return agro_nfe_saida_rascunho_postgres()


def nfe_saida_persistencia_ok(db) -> bool:
    if nfe_saida_usa_postgres():
        return True
    return nfe_saida_col(db) is not None


def _digits_doc(raw: Any, max_len: int = 14) -> str:
    return re.sub(r"\D", "", str(raw or ""))[:max_len]


def _decimal(val: Any) -> Decimal:
    try:
        return Decimal(str(val or "0").replace(",", ".")).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


def nfe_saida_serie_default() -> int:
    try:
        return max(1, min(999, int(config("NFE_EMISSAO_SERIE", default="1") or 1)))
    except (TypeError, ValueError):
        return 1


def nfe_saida_emitente_uf_default() -> str:
    return (config("NFE_DIST_DFE_UF", default="SP") or "SP").strip().upper()[:2]


def uf_emitente_do_doc(doc: dict | None) -> str:
    """UF do emitente da loja escolhida — não mistura Centro e Vila."""
    from produtos.nfe_saida_regras_validas_util import estabelecimento_do_doc

    loja = estabelecimento_do_doc(doc or {})
    if loja:
        try:
            from produtos.nfce_config_util import nfce_cfg

            uf = str(nfce_cfg(loja).get("uf") or "").strip().upper()[:2]
            if len(uf) == 2:
                return uf
        except Exception:
            pass
    return nfe_saida_emitente_uf_default()


def snapshot_emitente_loja(loja: str) -> dict[str, Any]:
    from produtos.nfe_fiscal_config_util import mascarar_cnpj, parse_estabelecimento, rotulo_estabelecimento
    from produtos.nfce_config_util import nfce_cfg

    loja_n = parse_estabelecimento(loja)
    if not loja_n:
        return {}
    cfg = nfce_cfg(loja_n)
    cnpj = re.sub(r"\D", "", str(cfg.get("cnpj") or ""))[:14]
    serie = None
    try:
        from produtos.models import NfeEstabelecimentoFiscal

        row = NfeEstabelecimentoFiscal.objects.filter(estabelecimento=loja_n).first()
        if row and row.serie_nfe_55:
            serie = int(row.serie_nfe_55)
    except Exception:
        serie = None
    if serie is None and loja_n == "centro":
        serie = nfe_saida_serie_default()
    return {
        "estabelecimento": loja_n,
        "rotulo": rotulo_estabelecimento(loja_n),
        "cnpj": cnpj,
        "cnpj_mascarado": mascarar_cnpj(cnpj) if len(cnpj) == 14 else "Pendente",
        "uf": str(cfg.get("uf") or nfe_saida_emitente_uf_default() or "SP").strip().upper()[:2],
        "serie": serie,
        "serie_exibicao": str(serie) if serie else "Pendente",
    }


def aplicar_estabelecimento_no_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """Grava loja, CNPJ e série daquela loja. Não herda série do outro estabelecimento."""
    from produtos.nfe_fiscal_config_util import parse_estabelecimento

    fiscal = doc.get("fiscal") if isinstance(doc.get("fiscal"), dict) else {}
    loja = parse_estabelecimento(doc.get("estabelecimento")) or parse_estabelecimento(fiscal.get("estabelecimento"))
    doc["estabelecimento"] = loja or ""
    fiscal = dict(fiscal)
    if not loja:
        fiscal.pop("estabelecimento", None)
        doc["fiscal"] = fiscal
        return doc
    snap = snapshot_emitente_loja(loja)
    fiscal["estabelecimento"] = loja
    if snap.get("serie"):
        fiscal["serie"] = int(snap["serie"])
        doc["serie"] = int(snap["serie"])
    elif loja == "vila":
        fiscal.pop("serie", None)
        if doc.get("serie") == nfe_saida_serie_default():
            doc["serie"] = None
    if snap.get("cnpj") and len(str(snap["cnpj"])) == 14:
        doc["emitente_cnpj"] = snap["cnpj"]
    doc["fiscal"] = fiscal
    return doc


def estabelecimento_sugerido_venda(v) -> str | None:
    """Só sugere Centro/Vila quando o depósito da venda for inequívoco."""
    from produtos.nfe_fiscal_config_util import parse_estabelecimento

    return parse_estabelecimento(getattr(v, "deposito", None))


def nfe_saida_tp_amb_default() -> int:
    try:
        return 1 if int(config("NFE_DIST_DFE_TP_AMB", default="2") or 2) == 1 else 2
    except (TypeError, ValueError):
        return 2


def documento_dest_tipo(digits: str) -> str:
    d = _digits_doc(digits)
    if len(d) == 14:
        return "cnpj"
    if len(d) == 11:
        return "cpf"
    return ""


def cfop_padrao_destino(uf_dest: str, uf_emit: str | None = None) -> str:
    ue = (uf_emit or nfe_saida_emitente_uf_default() or "SP").strip().upper()[:2]
    ud = (uf_dest or ue).strip().upper()[:2]
    if ud and ue and ud != ue:
        return "6102"
    return "5102"


def destinatario_vazio() -> dict[str, Any]:
    return {
        "cliente_agro_id": None,
        "nome": "",
        "documento": "",
        "tipo_doc": "",
        "ie": "",
        "email": "",
        "ind_final": 1,
        "situacao_icms": "",
        "finalidade_mercadoria": "",
        "ind_pres": 9,
        "cep": "",
        "uf": "",
        "cidade": "",
        "bairro": "",
        "logradouro": "",
        "numero": "",
        "complemento": "",
    }


def fiscal_nota_vazio() -> dict[str, Any]:
    return {
        "natureza": "VENDA",
        "serie": nfe_saida_serie_default(),
        "mod_frete": "9",
        "inf_compl": "",
        "x_ped": "",
        "x_nemp": "",
        "x_cont": "",
    }


def pagamento_vazio() -> list[dict[str, Any]]:
    return []


def _fiscal_produto_buscar(produto_id: str) -> dict[str, str]:
    """
    Fiscal para pré-preenchimento NF-e 55 — sem NCM/CFOP genéricos do padrão NFC-e (23099020/5102).
    Preserva NCM cadastrado (incl. 23099020 quando confirmado no produto/overlay/Mongo).
    """
    pid = str(produto_id or "").strip()
    out: dict[str, str] = {
        "ncm": "",
        "cfop": "",
        "csosn": "",
        "origem": "0",
        "cest": "",
        "cst_pis_cofins": "",
    }
    if not pid:
        return out
    try:
        from produtos.nfce_fiscal_produto_util import fiscal_por_produto_id

        cat = fiscal_por_produto_id(pid, aplicar_defaults_sn=False)
        for k in out:
            v = str(cat.get(k) or "").strip()
            if v:
                out[k] = v
    except Exception:
        pass
    # NCM do modelo legado só se overlay/Mongo não trouxer classificação corrigida.
    if not out.get("ncm"):
        try:
            from produtos.catalogo_agro import obter_produto_model

            p = obter_produto_model(pid)
            if p is not None and (p.ncm or "").strip():
                ncm = normalizar_ncm_somente_digitos(p.ncm)
                if len(ncm) == 8:
                    out["ncm"] = ncm
        except Exception:
            pass
    return out


def _cfop_item_prefill_venda(fis: dict[str, str], *, uf_dest: str, uf_emit: str | None = None) -> str:
    """Só o CFOP cadastrado no produto. Não inventa 5102/6102."""
    del uf_dest, uf_emit
    cfop_cat = re.sub(r"\D", "", str(fis.get("cfop") or ""))[:4]
    return cfop_cat if len(cfop_cat) == 4 else ""


def _mapear_tpag_forma(forma: str) -> str:
    f = (forma or "").strip().lower()
    if "pix" in f:
        return "17"
    if "dinheiro" in f or f == "cash":
        return "01"
    if "débito" in f or "debito" in f:
        return "04"
    if "crédito" in f or "credito" in f or "cartão" in f or "cartao" in f:
        return "03"
    if "boleto" in f:
        return "15"
    if "fiado" in f or "crediário" in f or "crediario" in f:
        return "05"
    if "vale" in f:
        return "99"
    return "99"


def _fiscal_nfe_extra(c) -> dict[str, Any]:
    extras = c.relacionamento_extras_json if isinstance(c.relacionamento_extras_json, dict) else {}
    fis = extras.get("fiscal_nfe")
    return dict(fis) if isinstance(fis, dict) else {}


def _campo_fiscal_ou_cadastro(fis: dict[str, Any], c, field: str, *, max_len: int = 300) -> str:
    v = str(fis.get(field) or "").strip()
    if v:
        return v[:max_len]
    return str(getattr(c, field, None) or "").strip()[:max_len]


def _documento_fiscal_cliente(c, fis: dict[str, Any]) -> str:
    doc = _digits_doc(fis.get("documento"), 14)
    if doc:
        return doc
    cnpj_extra = _digits_doc(fis.get("cnpj"), 14)
    if len(cnpj_extra) == 14:
        return cnpj_extra
    cpf_f = _digits_doc(fis.get("cpf"), 11)
    if len(cpf_f) == 11:
        return cpf_f
    return _digits_doc(c.cpf, 14)


def resolver_cliente_agro_id_dest(dest: dict[str, Any]) -> int | None:
    """Vincula destinatário ao ClienteAgro do PDV quando há id ou CPF conhecido."""
    cid = dest.get("cliente_agro_id")
    if cid is not None:
        try:
            return int(cid)
        except (TypeError, ValueError):
            pass
    doc = _digits_doc(dest.get("documento"), 14)
    if len(doc) != 11:
        return None
    try:
        from produtos.models import ClienteAgro

        c = ClienteAgro.objects.filter(cpf=doc).first()
        return int(c.pk) if c else None
    except Exception:
        return None


def cliente_agro_para_destinatario(c) -> dict[str, Any]:
    dest = destinatario_vazio()
    if c is None:
        return dest
    fis = _fiscal_nfe_extra(c)
    dest["cliente_agro_id"] = c.pk
    dest["nome"] = _campo_fiscal_ou_cadastro(fis, c, "nome", max_len=200)
    doc = _documento_fiscal_cliente(c, fis)
    dest["documento"] = doc
    dest["tipo_doc"] = documento_dest_tipo(doc)
    dest["ie"] = str(fis.get("ie") or "").strip()[:20]
    dest["email"] = str(fis.get("email") or "").strip()[:120]
    dest["cep"] = _campo_fiscal_ou_cadastro(fis, c, "cep", max_len=12)
    dest["uf"] = _campo_fiscal_ou_cadastro(fis, c, "uf", max_len=2).upper()
    dest["cidade"] = _campo_fiscal_ou_cadastro(fis, c, "cidade", max_len=120)
    dest["bairro"] = _campo_fiscal_ou_cadastro(fis, c, "bairro", max_len=120)
    dest["logradouro"] = _campo_fiscal_ou_cadastro(fis, c, "logradouro", max_len=200)
    dest["numero"] = _campo_fiscal_ou_cadastro(fis, c, "numero", max_len=40)
    dest["complemento"] = _campo_fiscal_ou_cadastro(fis, c, "complemento", max_len=120)
    dest["situacao_icms"] = str(fis.get("situacao_icms") or "").strip()
    dest["finalidade_mercadoria"] = str(fis.get("finalidade_mercadoria") or "").strip()
    return dest


def destinatario_de_venda(v) -> dict[str, Any]:
    dest = destinatario_vazio()
    dest["nome"] = str(v.cliente_nome or "").strip()
    doc = _digits_doc(v.cliente_documento, 14)
    if not doc and dest["nome"].lower() in ("consumidor final", "consumidor"):
        dest["ind_final"] = 1
    dest["documento"] = doc
    dest["tipo_doc"] = documento_dest_tipo(doc)
    if doc and dest["tipo_doc"] == "cpf":
        dest["ind_final"] = 1
    cid = str(v.cliente_id_erp or "").strip()
    if cid.isdigit():
        try:
            from produtos.models import ClienteAgro

            c = ClienteAgro.objects.filter(externo_id=cid).first()
            if c is None:
                c = ClienteAgro.objects.filter(pk=int(cid)).first()
            if c is not None:
                return cliente_agro_para_destinatario(c)
        except Exception:
            pass
    return dest


def itens_de_venda(v) -> list[dict[str, Any]]:
    dest = destinatario_de_venda(v)
    uf_dest = (dest.get("uf") or nfe_saida_emitente_uf_default())[:2]
    uf_emit = nfe_saida_emitente_uf_default()
    out: list[dict[str, Any]] = []
    for it in v.itens.all():
        pid = str(it.produto_id_externo or "").strip()
        fis = _fiscal_produto_buscar(pid)
        qtd = float(it.quantidade or 0)
        vu = float(it.valor_unitario or 0)
        vt = float(it.valor_total or 0)
        if vt <= 0 and qtd > 0:
            vt = round(qtd * vu, 2)
        ncm = normalizar_ncm_somente_digitos(fis.get("ncm"))
        out.append(
            {
                "produto_id": pid,
                "codigo": str(it.codigo or "").strip(),
                "descricao": str(it.descricao or "").strip(),
                "qtd": qtd,
                "un": str(it.unidade or "UN").strip()[:12] or "UN",
                "ncm": ncm if len(ncm) == 8 else "",
                "cfop": _cfop_item_prefill_venda(fis, uf_dest=uf_dest, uf_emit=uf_emit),
                "csosn": str(fis.get("csosn") or "").strip() or "",
                "origem": str(fis.get("origem") or "0").strip()[:1] or "0",
                "cst_pis_cofins": re.sub(r"\D", "", str(fis.get("cst_pis_cofins") or ""))[:2],
                "v_unit": vu,
                "v_total": vt,
            }
        )
    return out


def pagamentos_de_venda(v) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pj = v.pagamentos_json
    if isinstance(pj, list) and pj:
        for p in pj:
            if not isinstance(p, dict):
                continue
            forma = str(p.get("forma") or "").strip()
            val = _decimal(p.get("valor"))
            if val <= 0:
                continue
            rows.append({"forma": forma, "tPag": _mapear_tpag_forma(forma), "valor": float(val)})
        return rows
    forma = str(v.forma_pagamento or "").strip()
    tot = _decimal(v.total)
    if tot > 0:
        rows.append({"forma": forma or "Outros", "tPag": _mapear_tpag_forma(forma), "valor": float(tot)})
    return rows


def total_itens(itens: list[dict]) -> Decimal:
    t = Decimal("0")
    for ln in itens or []:
        if not isinstance(ln, dict):
            continue
        t += _decimal(ln.get("v_total"))
    return t.quantize(Decimal("0.01"))


def doc_novo_vazio(*, usuario: str = "", venda_agro_id: int | None = None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "status": NFE_SAIDA_STATUS_RASCUNHO,
        "mod": "55",
        "serie": nfe_saida_serie_default(),
        "nNF": None,
        "chave": "",
        "protocolo": "",
        "c_stat": None,
        "x_motivo": "",
        "tp_amb": nfe_saida_tp_amb_default(),
        "estabelecimento": "",
        "emitente_cnpj": "",
        "destinatario": destinatario_vazio(),
        "itens": [],
        "pagamento": pagamento_vazio(),
        "fiscal": fiscal_nota_vazio(),
        "inf_complementar": "",
        "venda_agro_id": venda_agro_id,
        "venda_total_ref": None,
        "usuario": (usuario or "")[:200],
        "created_at": now,
        "updated_at": now,
        "xml_autorizado": "",
        "pin_ok_em": None,
        "simulado": False,
    }


def prefill_de_venda_agro(v, *, usuario: str = "") -> dict[str, Any]:
    doc = doc_novo_vazio(usuario=usuario, venda_agro_id=v.pk)
    doc["destinatario"] = destinatario_de_venda(v)
    doc["itens"] = itens_de_venda(v)
    doc["pagamento"] = pagamentos_de_venda(v)
    doc["venda_total_ref"] = float(_decimal(v.total))
    sug = estabelecimento_sugerido_venda(v)
    if sug:
        doc["estabelecimento"] = sug
        doc["estabelecimento_sugerido"] = sug
        aplicar_estabelecimento_no_doc(doc)
    if v.deposito:
        doc["fiscal"]["deposito_venda"] = str(v.deposito)
    return doc


def _oid(oid: str) -> ObjectId | None:
    try:
        return ObjectId(str(oid).strip())
    except (InvalidId, TypeError):
        return None


def _serialize_doc(doc: dict) -> dict[str, Any]:
    if not doc:
        return {}
    out = dict(doc)
    _id = out.pop("_id", None)
    out["id"] = str(_id) if _id is not None else ""
    for k in ("created_at", "updated_at", "pin_ok_em"):
        v = out.get(k)
        if isinstance(v, datetime):
            out[k] = v.astimezone(timezone.utc).isoformat()
    st = str(out.get("status") or NFE_SAIDA_STATUS_RASCUNHO)
    ui = NFE_SAIDA_STATUS_UI.get(st, {"label": st, "chip": "slate"})
    out["status_label"] = ui["label"]
    out["status_chip"] = ui["chip"]
    dest = out.get("destinatario") if isinstance(out.get("destinatario"), dict) else {}
    nome = str(dest.get("nome") or "").strip() or "—"
    doc_d = _digits_doc(dest.get("documento"))
    tipo = documento_dest_tipo(doc_d)
    if tipo == "cnpj":
        dest_resumo = f"{nome} · CNPJ"
    elif tipo == "cpf":
        dest_resumo = f"{nome} · CPF"
    else:
        dest_resumo = nome
    out["lista_destinatario"] = dest_resumo[:120]
    out["lista_total"] = float(total_itens(out.get("itens") or []))
    nnf = out.get("nNF")
    serie = out.get("serie") or nfe_saida_serie_default()
    out["lista_numero_serie"] = f"{nnf or '—'} / {serie}" if nnf else "—"
    try:
        from produtos.nfe_saida_evento_util import faixa_cancelamento_sp, nota_ja_cancelada, resumo_eventos

        out["eventos_resumo"] = resumo_eventos(out)
        out["faixa_cancelamento"] = faixa_cancelamento_sp(out)
        out["pode_cancelar"] = st == NFE_SAIDA_STATUS_AUTORIZADA and not nota_ja_cancelada(out)
        out["pode_cce"] = st == NFE_SAIDA_STATUS_AUTORIZADA and not nota_ja_cancelada(out)
    except Exception:
        out["eventos_resumo"] = []
        out["pode_cancelar"] = False
        out["pode_cce"] = False
    xml_full = out.get("xml_autorizado")
    if xml_full:
        out["xml_autorizado"] = xml_full
    evs = out.get("eventos")
    if isinstance(evs, list):
        slim = []
        for e in evs:
            if not isinstance(e, dict):
                continue
            slim.append({k: v for k, v in e.items() if k != "xml"})
        out["eventos"] = slim
    from produtos.nfe_fiscal_config_util import parse_estabelecimento

    loja = parse_estabelecimento(out.get("estabelecimento"))
    if not loja:
        fis = out.get("fiscal") if isinstance(out.get("fiscal"), dict) else {}
        loja = parse_estabelecimento(fis.get("estabelecimento"))
    out["estabelecimento"] = loja or ""
    out["emitente"] = snapshot_emitente_loja(loja) if loja else {}
    return out


def salvar_documento_nfe_saida(db, doc_id: str | None, payload: dict, *, usuario: str = "") -> dict[str, Any]:
    if not nfe_saida_persistencia_ok(db):
        return {"ok": False, "erro": "Persistência NF-e saída indisponível."}
    now = datetime.now(timezone.utc)
    if nfe_saida_usa_postgres():
        from produtos.nfe_saida_rascunho_pg_util import (
            nfe_saida_doc_id_valido,
            pg_find_one,
            pg_insert,
            pg_replace,
        )

        if doc_id:
            rid = str(doc_id).strip()
            if not nfe_saida_doc_id_valido(rid):
                return {"ok": False, "erro": "ID inválido."}
            existing = pg_find_one(rid)
            if not existing:
                return {"ok": False, "erro": "Rascunho não encontrado."}
            if str(existing.get("status") or "") == NFE_SAIDA_STATUS_AUTORIZADA:
                return {"ok": False, "erro": "Nota autorizada não pode ser editada."}
        else:
            existing = None
            rid = ""

        base = dict(existing) if existing else doc_novo_vazio(usuario=usuario)
        for key in (
            "destinatario",
            "itens",
            "pagamento",
            "fiscal",
            "inf_complementar",
            "venda_agro_id",
            "venda_total_ref",
            "tp_amb",
            "serie",
            "estabelecimento",
            "emitente_cnpj",
        ):
            if key in payload:
                base[key] = payload[key]
        aplicar_estabelecimento_no_doc(base)
        if usuario:
            base["usuario"] = usuario[:200]
        base["updated_at"] = now
        base.pop("_id", None)
        if existing is None:
            base["created_at"] = now
            new_id = pg_insert(base)
            return {"ok": True, "id": new_id}
        pg_replace(rid, base)
        return {"ok": True, "id": rid}

    col = nfe_saida_col(db)
    if col is None:
        return {"ok": False, "erro": "Mongo indisponível para NF-e saída."}
    if doc_id:
        oid = _oid(doc_id)
        if oid is None:
            return {"ok": False, "erro": "ID inválido."}
        existing = col.find_one({"_id": oid})
        if not existing:
            return {"ok": False, "erro": "Rascunho não encontrado."}
        if str(existing.get("status") or "") == NFE_SAIDA_STATUS_AUTORIZADA:
            return {"ok": False, "erro": "Nota autorizada não pode ser editada."}
    else:
        existing = None
        oid = None

    base = dict(existing) if existing else doc_novo_vazio(usuario=usuario)
    for key in (
        "destinatario",
        "itens",
        "pagamento",
        "fiscal",
        "inf_complementar",
        "venda_agro_id",
        "venda_total_ref",
        "tp_amb",
        "serie",
        "estabelecimento",
        "emitente_cnpj",
    ):
        if key in payload:
            base[key] = payload[key]
    aplicar_estabelecimento_no_doc(base)
    if usuario:
        base["usuario"] = usuario[:200]
    base["updated_at"] = now
    base.pop("_id", None)
    if existing is None:
        base["created_at"] = now
        ins = col.insert_one(base)
        return {"ok": True, "id": str(ins.inserted_id)}
    col.update_one({"_id": oid}, {"$set": base})
    return {"ok": True, "id": str(oid)}


def obter_nfe_saida_bruto(db, doc_id: str) -> dict[str, Any] | None:
    if nfe_saida_usa_postgres():
        from produtos.nfe_saida_rascunho_pg_util import pg_find_one

        return pg_find_one(doc_id)
    col = nfe_saida_col(db)
    if col is None:
        return None
    oid = _oid(doc_id)
    if oid is None:
        return None
    return col.find_one({"_id": oid})


def nfe_saida_atualizar_campos(db, doc_id: str, campos: dict) -> bool:
    if not campos:
        return False
    if nfe_saida_usa_postgres():
        from produtos.nfe_saida_rascunho_pg_util import pg_update_set

        return pg_update_set(doc_id, campos)
    col = nfe_saida_col(db)
    if col is None:
        return False
    oid = _oid(doc_id)
    if oid is None:
        return False
    col.update_one({"_id": oid}, {"$set": campos})
    return True


def criar_nfe_saida_vazia(db, *, usuario: str = "") -> dict[str, Any]:
    if not nfe_saida_persistencia_ok(db):
        return {"ok": False, "erro": "Persistência NF-e saída indisponível."}
    doc = doc_novo_vazio(usuario=usuario)
    if nfe_saida_usa_postgres():
        from produtos.nfe_saida_rascunho_pg_util import pg_insert

        return {"ok": True, "id": pg_insert(doc)}
    col = nfe_saida_col(db)
    ins = col.insert_one(doc)
    return {"ok": True, "id": str(ins.inserted_id)}


def obter_documento_nfe_saida(db, doc_id: str) -> dict[str, Any] | None:
    doc = obter_nfe_saida_bruto(db, doc_id)
    if not doc:
        return None
    return _serialize_doc(doc)


def listar_nfe_saida(db, *, limit: int = 40, filtro: str | None = None, busca: str = "") -> list[dict]:
    if not nfe_saida_persistencia_ok(db):
        return []
    f = (filtro or "todas").strip().lower()
    term = (busca or "").strip().lower()
    lim = min(limit, 100)
    docs: list[dict]

    if nfe_saida_usa_postgres():
        from produtos.nfe_saida_rascunho_pg_util import pg_find_filter

        kwargs: dict[str, Any] = {"limit": lim}
        if f == "rascunho":
            kwargs["status"] = NFE_SAIDA_STATUS_RASCUNHO
        elif f == "autorizada":
            kwargs["status"] = NFE_SAIDA_STATUS_AUTORIZADA
        elif f == "rejeitada":
            kwargs["status"] = NFE_SAIDA_STATUS_REJEITADA
        elif f == "descartada":
            kwargs["status"] = NFE_SAIDA_STATUS_DESCARTADA
        elif f == "em_aberto":
            kwargs["status_nin"] = [NFE_SAIDA_STATUS_DESCARTADA, NFE_SAIDA_STATUS_CANCELADA]
        docs = pg_find_filter(**kwargs)
    else:
        col = nfe_saida_col(db)
        if col is None:
            return []
        q: dict[str, Any] = {}
        if f == "rascunho":
            q["status"] = NFE_SAIDA_STATUS_RASCUNHO
        elif f == "autorizada":
            q["status"] = NFE_SAIDA_STATUS_AUTORIZADA
        elif f == "rejeitada":
            q["status"] = NFE_SAIDA_STATUS_REJEITADA
        elif f == "descartada":
            q["status"] = NFE_SAIDA_STATUS_DESCARTADA
        elif f == "em_aberto":
            q["status"] = {"$nin": [NFE_SAIDA_STATUS_DESCARTADA, NFE_SAIDA_STATUS_CANCELADA]}
        docs = list(col.find(q).sort("updated_at", -1).limit(lim))

    out: list[dict] = []
    for doc in docs:
        ser = _serialize_doc(doc)
        if term:
            blob = " ".join(
                [
                    ser.get("lista_destinatario") or "",
                    ser.get("chave") or "",
                    str(ser.get("venda_agro_id") or ""),
                    ser.get("status_label") or "",
                ]
            ).lower()
            if term not in blob and not all(p in blob for p in term.split() if p):
                continue
        out.append(ser)
    return out


def rascunho_aberto_por_venda(db, venda_id: int) -> dict[str, Any] | None:
    if not nfe_saida_persistencia_ok(db):
        return None
    for st in (NFE_SAIDA_STATUS_RASCUNHO, NFE_SAIDA_STATUS_REJEITADA, NFE_SAIDA_STATUS_ENVIANDO):
        doc = None
        if nfe_saida_usa_postgres():
            from produtos.nfe_saida_rascunho_pg_util import pg_find_filter

            rows = pg_find_filter(venda_agro_id=int(venda_id), status=st, limit=1)
            doc = rows[0] if rows else None
        else:
            col = nfe_saida_col(db)
            if col is None:
                return None
            doc = col.find_one(
                {"venda_agro_id": int(venda_id), "status": st},
                sort=[("updated_at", -1)],
            )
        if doc:
            return _serialize_doc(doc)
    return None


def autorizada_por_venda(db, venda_id: int) -> dict[str, Any] | None:
    if not nfe_saida_persistencia_ok(db):
        return None
    doc = None
    if nfe_saida_usa_postgres():
        from produtos.nfe_saida_rascunho_pg_util import pg_find_filter

        rows = pg_find_filter(
            venda_agro_id=int(venda_id), status=NFE_SAIDA_STATUS_AUTORIZADA, limit=1
        )
        doc = rows[0] if rows else None
    else:
        col = nfe_saida_col(db)
        if col is None:
            return None
        doc = col.find_one({"venda_agro_id": int(venda_id), "status": NFE_SAIDA_STATUS_AUTORIZADA})
    if doc:
        return _serialize_doc(doc)
    return None


def criar_ou_reabrir_de_venda(db, venda_id: int, *, usuario: str = "") -> dict[str, Any]:
    from produtos.models import VendaAgro

    try:
        v = VendaAgro.objects.prefetch_related("itens").get(pk=int(venda_id))
    except VendaAgro.DoesNotExist:
        return {"ok": False, "erro": "Venda não encontrada."}
    if v.devolvida_em is not None:
        aut = autorizada_por_venda(db, v.pk)
        if aut:
            return {"ok": True, "id": aut["id"], "autorizada": True, "rascunho": aut}
        return {"ok": False, "erro": "Venda devolvida — não é possível emitir NF-e."}
    aut = autorizada_por_venda(db, v.pk)
    if aut:
        return {"ok": True, "id": aut["id"], "autorizada": True, "rascunho": aut}
    aberto = rascunho_aberto_por_venda(db, v.pk)
    if aberto:
        return {"ok": True, "id": aberto["id"], "reaberto": True, "rascunho": aberto}
    doc = prefill_de_venda_agro(v, usuario=usuario)
    if not nfe_saida_persistencia_ok(db):
        return {"ok": False, "erro": "Persistência NF-e saída indisponível."}
    if nfe_saida_usa_postgres():
        from produtos.nfe_saida_rascunho_pg_util import pg_insert

        rid = pg_insert(doc)
        return {
            "ok": True,
            "id": rid,
            "criado": True,
            "rascunho": _serialize_doc({**doc, "_id": rid}),
        }
    col = nfe_saida_col(db)
    if col is None:
        return {"ok": False, "erro": "Mongo indisponível."}
    ins = col.insert_one(doc)
    return {"ok": True, "id": str(ins.inserted_id), "criado": True, "rascunho": _serialize_doc({**doc, "_id": ins.inserted_id})}


def painel_nfe_saida_venda(venda_id: int, db=None) -> dict[str, Any]:
    """Resumo para lista/ficha de venda."""
    out = {
        "tem_autorizada": False,
        "doc_id": "",
        "tem_rascunho": False,
        "pode_emitir": True,
    }
    if not nfe_saida_usa_postgres():
        if db is None:
            try:
                from produtos.views import obter_conexao_mongo

                _c, db = obter_conexao_mongo()
            except Exception:
                db = None
        if db is None:
            return out
    elif db is None:
        db = object()  # sentinel: persistência PG não usa Mongo
    aut = autorizada_por_venda(db, venda_id)
    if aut:
        out["tem_autorizada"] = True
        out["doc_id"] = aut.get("id") or ""
        out["pode_emitir"] = False
        out["chave"] = aut.get("chave") or ""
        out["numero"] = aut.get("nNF")
        return out
    ab = rascunho_aberto_por_venda(db, venda_id)
    if ab:
        out["tem_rascunho"] = True
        out["doc_id"] = ab.get("id") or ""
    return out


def validar_destinatario(dest: dict) -> list[str]:
    from produtos.nfe_saida_tributacao_util import normalizar_destinatario_fiscal

    erros: list[str] = []
    if not str(dest.get("nome") or "").strip():
        erros.append("Nome do destinatário obrigatório.")
    doc = _digits_doc(dest.get("documento"))
    tipo = documento_dest_tipo(doc)
    if not tipo:
        erros.append("CPF (11) ou CNPJ (14) do destinatário.")
    if not str(dest.get("uf") or "").strip():
        erros.append("UF do destinatário.")
    if not str(dest.get("cidade") or "").strip():
        erros.append("Cidade do destinatário.")
    if not str(dest.get("logradouro") or "").strip():
        erros.append("Logradouro do destinatário.")
    norm = normalizar_destinatario_fiscal({**dest, "documento": doc})
    ind_ie = str(norm.get("ind_ie_dest") or "9")
    ie = str(dest.get("ie") or "").strip()
    ie_num = _digits_doc(ie, 14)
    if tipo == "cnpj":
        if ind_ie == "1" and not ie_num:
            erros.append("Inscrição estadual numérica obrigatória para CNPJ contribuinte ICMS.")
        elif ind_ie == "2" and ie.upper() != "ISENTO":
            erros.append('Para CNPJ isento de IE, informe "ISENTO" no campo IE.')
    elif tipo == "cpf" and ind_ie == "1" and not ie_num:
        erros.append("Inscrição estadual obrigatória para produtor/contribuinte ICMS (CPF).")
    return erros


def validar_itens(itens: list) -> list[str]:
    erros: list[str] = []
    if not itens:
        erros.append("Inclua ao menos um item.")
        return erros
    for i, ln in enumerate(itens):
        if not isinstance(ln, dict):
            erros.append(f"Item {i + 1}: inválido.")
            continue
        ncm = normalizar_ncm_somente_digitos(ln.get("ncm"))
        if len(ncm) != 8:
            erros.append(f"Item {i + 1}: NCM com 8 dígitos.")
        if not str(ln.get("descricao") or "").strip():
            erros.append(f"Item {i + 1}: descrição.")
        try:
            q = float(ln.get("qtd") or 0)
        except (TypeError, ValueError):
            q = 0
        if q <= 0:
            erros.append(f"Item {i + 1}: quantidade.")
    return erros


def validar_pagamentos(pagamentos: list, total: Decimal) -> list[str]:
    erros: list[str] = []
    if not pagamentos:
        erros.append("Informe forma de pagamento (etapa 4).")
        return erros
    soma = Decimal("0")
    for p in pagamentos:
        if not isinstance(p, dict):
            continue
        soma += _decimal(p.get("valor"))
    if abs(soma - total) > Decimal("0.02"):
        erros.append(f"Soma dos pagamentos (R$ {soma}) difere do total (R$ {total}).")
    return erros


def erros_cfop_coerente_uf(doc: dict) -> list[str]:
    """Incompatibilidade CFOP × UF. Não converte 5xxx↔6xxx."""
    dest = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    itens = doc.get("itens") if isinstance(doc.get("itens"), list) else []
    ue = uf_emitente_do_doc(doc)
    ud = str(dest.get("uf") or "").strip().upper()[:2]
    if not ud:
        return ["Informe a UF do destinatário."]
    inter = bool(ue and ud and ud != ue)
    erros: list[str] = []
    for ln in itens:
        if not isinstance(ln, dict):
            continue
        cf = re.sub(r"\D", "", str(ln.get("cfop") or ""))[:4]
        if len(cf) != 4:
            continue
        if inter and not cf.startswith("6"):
            erros.append(
                f"CFOP {cf} é de operação interna; origem {ue} → destino {ud} é interestadual. "
                "Não convertemos para 6xxx — cadastre e valide a regra."
            )
        elif not inter and not cf.startswith("5"):
            erros.append(f"CFOP {cf} não é interno; origem {ue} → destino {ud} é operação interna.")
    return erros


def checklist_conferencia(doc: dict, *, cert_ok: bool) -> list[dict[str, Any]]:
    dest = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    itens = doc.get("itens") if isinstance(doc.get("itens"), list) else []
    pag = doc.get("pagamento") if isinstance(doc.get("pagamento"), list) else []
    total = total_itens(itens)
    rows: list[dict[str, Any]] = []

    def row(ok: bool, label: str, det: str = "") -> None:
        rows.append({"ok": ok, "label": label, "det": det})

    er_dest = validar_destinatario(dest)
    row(len(er_dest) == 0, "Destinatário completo", "; ".join(er_dest)[:200])
    from produtos.nfe_saida_email_util import email_destinatario_valido, normalizar_email_destinatario

    em = normalizar_email_destinatario(dest)
    row(
        email_destinatario_valido(em),
        "E-mail para envio (XML + DANFE)",
        em if em else "Opcional — preencha para receber após autorizar",
    )
    er_it = validar_itens(itens)
    row(len(er_it) == 0, "Itens e NCM", "; ".join(er_it)[:200])
    from produtos.nfe_fiscal_config_util import parse_estabelecimento, rotulo_estabelecimento
    from produtos.nfe_saida_regras_validas_util import estabelecimento_do_doc

    loja = estabelecimento_do_doc(doc)
    if loja:
        row(True, "Estabelecimento emitente", rotulo_estabelecimento(loja))
    else:
        row(False, "Estabelecimento emitente", "Escolha Centro ou Vila Elias. Não assumimos Centro.")
    ue = uf_emitente_do_doc(doc)
    ud = str(dest.get("uf") or "").strip().upper()[:2]
    er_cfop = erros_cfop_coerente_uf(doc)
    if er_cfop:
        row(False, "CFOP coerente com UF", " ".join(er_cfop)[:280])
    elif ud:
        inter = bool(ue and ud and ud != ue)
        row(True, "CFOP coerente com UF", f"Origem {ue} → destino {ud} ({'interestadual' if inter else 'interna'}).")
    else:
        row(False, "CFOP coerente com UF", "Informe a UF do destinatário.")
    er_pg = validar_pagamentos(pag, total)
    row(len(er_pg) == 0, "Pagamentos = total", "; ".join(er_pg)[:120])
    row(cert_ok, "Certificado A1 configurado", "")
    amb = int(doc.get("tp_amb") or 2)
    row(True, "Ambiente", "Produção" if amb == 1 else "Homologação (teste)")
    return rows


def salvar_fiscal_cliente_agro(cliente_id: int, fiscal: dict) -> dict[str, Any]:
    """
    Grava dados fiscais NF-e em ``relacionamento_extras_json.fiscal_nfe``.
    Não altera nome/endereço de entrega do PDV — só o bloco fiscal.
    """
    from django.utils import timezone

    from produtos.models import ClienteAgro

    try:
        c = ClienteAgro.objects.get(pk=int(cliente_id))
    except ClienteAgro.DoesNotExist:
        return {"ok": False, "erro": "Cliente não encontrado."}
    extras = dict(c.relacionamento_extras_json) if isinstance(c.relacionamento_extras_json, dict) else {}
    fn = dict(_fiscal_nfe_extra(c))
    doc = _digits_doc(fiscal.get("documento") or fiscal.get("cnpj") or fiscal.get("cpf"), 14)
    if doc:
        fn["documento"] = doc
        if len(doc) == 14:
            fn["cnpj"] = doc
            fn.pop("cpf", None)
        elif len(doc) == 11:
            fn["cpf"] = doc
            fn.pop("cnpj", None)
    for k in (
        "nome",
        "ie",
        "email",
        "situacao_icms",
        "finalidade_mercadoria",
        "cep",
        "uf",
        "cidade",
        "bairro",
        "logradouro",
        "numero",
        "complemento",
    ):
        if k in fiscal:
            fn[k] = str(fiscal.get(k) or "").strip()[:300]
    fn["atualizado_em"] = timezone.now().isoformat(timespec="seconds")
    extras["fiscal_nfe"] = fn
    c.relacionamento_extras_json = extras
    if len(doc) == 11 and not str(c.cpf or "").strip():
        c.cpf = doc
    c.editado_local = True
    c.save(update_fields=["relacionamento_extras_json", "cpf", "editado_local"])
    return {"ok": True, "cliente": cliente_agro_para_destinatario(c)}


def persistir_fiscal_destinatario_no_cliente(dest: dict[str, Any]) -> None:
    if not isinstance(dest, dict):
        return
    cid = resolver_cliente_agro_id_dest(dest)
    if not cid:
        return
    dest["cliente_agro_id"] = cid
    salvar_fiscal_cliente_agro(cid, dest)
