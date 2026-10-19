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
    }


def pagamento_vazio() -> list[dict[str, Any]]:
    return []


def _fiscal_produto_buscar(produto_id: str) -> dict[str, str]:
    """NCM/CFOP/CSOSN do catálogo ou defaults SP."""
    base = fiscal_padrao_ui_cadastro()
    pid = str(produto_id or "").strip()
    if not pid:
        return base
    try:
        from produtos.catalogo_agro import obter_produto_model

        p = obter_produto_model(pid)
        if p is not None and (p.ncm or "").strip():
            base["ncm"] = normalizar_ncm_somente_digitos(p.ncm) or base["ncm"]
    except Exception:
        pass
    try:
        from produtos.models import ProdutoGestaoOverlayAgro

        ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64]).first()
        if ov and isinstance(ov.cadastro_extras, dict):
            fis = ov.cadastro_extras.get("fiscal")
            if isinstance(fis, dict):
                for k, row_k in (
                    ("ncm", "ncm"),
                    ("cfop", "cfop"),
                    ("csosn", "csosn"),
                    ("origem", "origem"),
                ):
                    v = str(fis.get(k) or "").strip()
                    if v:
                        base[row_k if row_k != "cfop" else "cfop"] = v
    except Exception:
        pass
    return base


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


def cliente_agro_para_destinatario(c) -> dict[str, Any]:
    dest = destinatario_vazio()
    if c is None:
        return dest
    dest["cliente_agro_id"] = c.pk
    dest["nome"] = str(c.nome or "").strip()
    doc = _digits_doc(c.cpf, 14)
    extras = c.relacionamento_extras_json if isinstance(c.relacionamento_extras_json, dict) else {}
    fis = extras.get("fiscal_nfe") if isinstance(extras.get("fiscal_nfe"), dict) else {}
    cnpj_extra = _digits_doc(fis.get("cnpj"), 14)
    if cnpj_extra:
        doc = cnpj_extra
    dest["documento"] = doc
    dest["tipo_doc"] = documento_dest_tipo(doc)
    dest["ie"] = str(fis.get("ie") or "").strip()
    dest["email"] = str(fis.get("email") or "").strip()
    dest["cep"] = str(c.cep or "").strip()
    dest["uf"] = str(c.uf or "").strip().upper()[:2]
    dest["cidade"] = str(c.cidade or "").strip()
    dest["bairro"] = str(c.bairro or "").strip()
    dest["logradouro"] = str(c.logradouro or "").strip()
    dest["numero"] = str(c.numero or "").strip()
    dest["complemento"] = str(c.complemento or "").strip()
    if dest["tipo_doc"] == "cnpj" and dest["ie"]:
        dest["ind_final"] = 0
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
    uf = (destinatario_de_venda(v).get("uf") or nfe_saida_emitente_uf_default())[:2]
    cfop_def = cfop_padrao_destino(uf)
    out: list[dict[str, Any]] = []
    for it in v.itens.all():
        pid = str(it.produto_id_externo or "").strip()
        fis = _fiscal_produto_buscar(pid)
        qtd = float(it.quantidade or 0)
        vu = float(it.valor_unitario or 0)
        vt = float(it.valor_total or 0)
        if vt <= 0 and qtd > 0:
            vt = round(qtd * vu, 2)
        out.append(
            {
                "produto_id": pid,
                "codigo": str(it.codigo or "").strip(),
                "descricao": str(it.descricao or "").strip(),
                "qtd": qtd,
                "un": str(it.unidade or "UN").strip()[:12] or "UN",
                "ncm": fis.get("ncm") or "",
                "cfop": fis.get("cfop") or cfop_def,
                "csosn": fis.get("csosn") or "102",
                "origem": fis.get("origem") or "0",
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
    uf = str(doc["destinatario"].get("uf") or "")[:2]
    if uf:
        cf = cfop_padrao_destino(uf)
        for ln in doc["itens"]:
            if isinstance(ln, dict) and not str(ln.get("cfop") or "").strip():
                ln["cfop"] = cf
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
        ):
            if key in payload:
                base[key] = payload[key]
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
    ):
        if key in payload:
            base[key] = payload[key]
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
    if tipo == "cnpj":
        ie = str(dest.get("ie") or "").strip()
        if not ie:
            erros.append("Inscrição estadual (ou ISENTO) para CNPJ.")
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
    ue = nfe_saida_emitente_uf_default()
    ud = str(dest.get("uf") or ue)[:2]
    inter = ud and ue and ud != ue
    cfop_ok = True
    for ln in itens:
        if not isinstance(ln, dict):
            continue
        cf = str(ln.get("cfop") or "")
        if inter and not cf.startswith("6"):
            cfop_ok = False
        if not inter and cf and not cf.startswith("5"):
            cfop_ok = False
    row(cfop_ok, "CFOP coerente com UF", "Interestadual: 6xxx · interna: 5xxx")
    er_pg = validar_pagamentos(pag, total)
    row(len(er_pg) == 0, "Pagamentos = total", "; ".join(er_pg)[:120])
    row(cert_ok, "Certificado A1 configurado", "")
    amb = int(doc.get("tp_amb") or 2)
    row(True, "Ambiente", "Produção" if amb == 1 else "Homologação (teste)")
    return rows


def salvar_fiscal_cliente_agro(cliente_id: int, fiscal: dict) -> dict[str, Any]:
    from produtos.models import ClienteAgro

    try:
        c = ClienteAgro.objects.get(pk=int(cliente_id))
    except ClienteAgro.DoesNotExist:
        return {"ok": False, "erro": "Cliente não encontrado."}
    extras = dict(c.relacionamento_extras_json) if isinstance(c.relacionamento_extras_json, dict) else {}
    fn = extras.get("fiscal_nfe") if isinstance(extras.get("fiscal_nfe"), dict) else {}
    fn = dict(fn)
    cnpj = _digits_doc(fiscal.get("cnpj") or fiscal.get("documento"), 14)
    if len(cnpj) == 14:
        fn["cnpj"] = cnpj
    elif len(cnpj) == 11:
        c.cpf = cnpj
    fn["ie"] = str(fiscal.get("ie") or "").strip()[:20]
    fn["email"] = str(fiscal.get("email") or "").strip()[:120]
    extras["fiscal_nfe"] = fn
    c.relacionamento_extras_json = extras
    for fld in ("cep", "uf", "cidade", "bairro", "logradouro", "numero", "complemento", "nome"):
        if fld in fiscal and str(fiscal.get(fld) or "").strip():
            setattr(c, fld, str(fiscal[fld]).strip()[:300])
    c.editado_local = True
    c.save()
    return {"ok": True, "cliente": cliente_agro_para_destinatario(c)}
