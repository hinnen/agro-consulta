"""
Caixa de entrada Dist DF-e (Postgres) — lista compartilhada entre operadores.

Limite: NFE_DIST_DFE_INBOX_MAX (default 80). Trim apaga concluídas/ignoradas antigas primeiro.
Cron: no máximo 1×/dia por CNPJ (além do cooldown SEFAZ 60s/1h).
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from decouple import config
from django.utils import timezone

from produtos.nfe_entrada_util import parse_nfe_xml_bytes

logger = logging.getLogger(__name__)

STATUS_PENDENTES = ("pendente", "carregada")
STATUS_CONCLUIDAS = ("processada", "ignorada")


def dfe_inbox_max() -> int:
    try:
        n = int(config("NFE_DIST_DFE_INBOX_MAX", default="80") or 80)
    except (TypeError, ValueError):
        n = 80
    return max(10, min(n, 200))


def dfe_inbox_hard_max() -> int:
    return max(dfe_inbox_max() + 20, 120)


def dfe_inbox_max_dias() -> int:
    """Notas com emissão mais antiga que isto entram como ignoradas (não poluem Pendentes)."""
    try:
        n = int(config("NFE_DIST_DFE_INBOX_MAX_DIAS", default="21") or 21)
    except (TypeError, ValueError):
        n = 21
    return max(3, min(n, 180))


def _data_emi_doc(chave: str = "", dh_emi: str = "") -> date | None:
    """Data de emissão: dhEmi ISO ou AAMM da chave (pos. 2–5)."""
    s = str(dh_emi or "").strip()
    if s:
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")[:19]).date()
        except ValueError:
            m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
            if m:
                try:
                    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                except ValueError:
                    pass
    ch = re.sub(r"\D", "", str(chave or ""))
    if len(ch) >= 6:
        try:
            aa, mm = int(ch[2:4]), int(ch[4:6])
            if 1 <= mm <= 12:
                return date(2000 + aa, mm, 1)
        except ValueError:
            pass
    return None


def _status_inicial_por_data(*, chave: str, dh_emi: str) -> str:
    from produtos.models import AgroNfeDistDfeDocumento

    dt = _data_emi_doc(chave, dh_emi)
    if dt is None:
        return AgroNfeDistDfeDocumento.Status.PENDENTE
    corte = timezone.localdate() - timedelta(days=dfe_inbox_max_dias())
    if dt < corte:
        return AgroNfeDistDfeDocumento.Status.IGNORADA
    return AgroNfeDistDfeDocumento.Status.PENDENTE


def dfe_inbox_arquivar_antigas(cnpj: str | None = None) -> int:
    """Pendentes/carregadas fora da janela → ignorada. Retorna qtd alterada."""
    from produtos.models import AgroNfeDistDfeDocumento

    cnpj_n = _cnpj14(cnpj or "")
    corte = timezone.localdate() - timedelta(days=dfe_inbox_max_dias())
    n = 0
    qs = AgroNfeDistDfeDocumento.objects.filter(status__in=STATUS_PENDENTES)
    if len(cnpj_n) == 14:
        qs = qs.filter(cnpj=cnpj_n)
    for row in qs.iterator(chunk_size=200):
        dt = _data_emi_doc(row.chave, row.dh_emi)
        if dt is not None and dt < corte:
            row.status = AgroNfeDistDfeDocumento.Status.IGNORADA
            row.save(update_fields=["status", "atualizado_em"])
            n += 1
    return n


def _cnpj14(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")[:14]


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1] if tag and "}" in tag else (tag or "")


def _dec(v: Any) -> Decimal:
    try:
        return Decimal(str(v).replace(",", ".")).quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0.00")


def _extrair_resumo_meta(xml_txt: str) -> dict[str, Any] | None:
    """resNFe / resEvento — chave e dados mínimos sem XML completo."""
    try:
        root = ET.fromstring(xml_txt.encode("utf-8") if isinstance(xml_txt, str) else xml_txt)
    except ET.ParseError:
        return None
    chave = ""
    numero = ""
    emit_nome = ""
    valor = Decimal("0.00")
    dh = ""
    tem_resumo = False
    for el in root.iter():
        ln = _local(el.tag)
        if ln in ("resNFe", "resEvento"):
            tem_resumo = True
        if ln == "chNFe" and el.text:
            chave = re.sub(r"\D", "", el.text)[:44]
        elif ln == "xNome" and el.text and not emit_nome:
            emit_nome = el.text.strip()[:300]
        elif ln == "nNF" and el.text:
            numero = el.text.strip()[:20]
        elif ln == "vNF" and el.text:
            valor = _dec(el.text)
        elif ln in ("dhEmi", "dhRecbto") and el.text and not dh:
            dh = el.text.strip()[:40]
    if not tem_resumo and not chave:
        return None
    if len(chave) != 44:
        return None
    return {
        "chave": chave,
        "numero": numero,
        "emit_nome": emit_nome,
        "valor_total": valor,
        "dh_emi": dh,
        "schema": "resumo",
    }


def classificar_xml_dfe(xml_txt: str) -> dict[str, Any]:
    """
    Retorno: schema (nfe|resumo|outro), chave, meta denormalizada, xml (só se nfe), parse_ok.
    """
    raw = (xml_txt or "").strip()
    if not raw:
        return {"schema": "outro", "chave": "", "parse_ok": False}
    parsed = parse_nfe_xml_bytes(raw.encode("utf-8"))
    if parsed.get("ok") and len(str(parsed.get("chave") or "").strip()) == 44:
        return {
            "schema": "nfe",
            "chave": str(parsed["chave"]).strip()[:44],
            "emit_nome": str(parsed.get("emit_nome") or "")[:300],
            "numero": str(parsed.get("numero") or "")[:20],
            "valor_total": _dec(parsed.get("valor_total")),
            "dh_emi": str(parsed.get("dh_emi") or "")[:40],
            "xml": raw,
            "parse_ok": True,
            "nota": parsed,
        }
    resumo = _extrair_resumo_meta(raw)
    if resumo:
        return {
            **resumo,
            "xml": raw[:8000],
            "parse_ok": False,
        }
    return {"schema": "outro", "chave": "", "parse_ok": False, "xml": ""}


def dfe_inbox_listar(cnpj: str, *, aba: str = "pendentes", limit: int = 80) -> list[dict[str, Any]]:
    from produtos.models import AgroNfeDistDfeDocumento

    cnpj = _cnpj14(cnpj)
    if len(cnpj) != 14:
        return []
    try:
        dfe_inbox_arquivar_antigas(cnpj)
    except Exception:
        logger.exception("dfe_inbox_arquivar_antigas")
    aba_n = (aba or "pendentes").strip().lower()
    if aba_n in ("concluidas", "concluídas", "processadas"):
        statuses = STATUS_CONCLUIDAS
    else:
        statuses = STATUS_PENDENTES
    lim = max(1, min(int(limit or 80), 120))
    # Pendentes: mais antigas primeiro (processar de trás pra frente).
    # Concluídas: mais recentes primeiro.
    if statuses == STATUS_PENDENTES:
        qs = (
            AgroNfeDistDfeDocumento.objects.filter(cnpj=cnpj, status__in=statuses)
            .order_by("criado_em")[:lim]
        )
    else:
        qs = (
            AgroNfeDistDfeDocumento.objects.filter(cnpj=cnpj, status__in=statuses)
            .order_by("-atualizado_em")[:lim]
        )
    out: list[dict[str, Any]] = []
    for row in qs:
        out.append(
            {
                "id": row.pk,
                "chave": row.chave,
                "nsu": row.nsu,
                "schema": row.schema,
                "emit_nome": row.emit_nome,
                "numero": row.numero,
                "valor_total": float(row.valor_total or 0),
                "dh_emi": row.dh_emi,
                "status": row.status,
                "pode_carregar": row.schema == "nfe" and bool((row.xml or "").strip()),
                "rascunho_id": row.rascunho_id or "",
                "criado_em": row.criado_em.isoformat() if row.criado_em else "",
            }
        )
    return out


def dfe_inbox_obter(doc_id: int, cnpj: str | None = None):
    from produtos.models import AgroNfeDistDfeDocumento

    qs = AgroNfeDistDfeDocumento.objects.filter(pk=int(doc_id))
    if cnpj:
        qs = qs.filter(cnpj=_cnpj14(cnpj))
    return qs.first()


def dfe_inbox_upsert_xmls(
    cnpj: str,
    notas_xml: list[str],
    *,
    nsu_ret: str = "",
) -> dict[str, Any]:
    """Grava XMLs novos. Não rebaixa status de processada/ignorada."""
    from produtos.models import AgroNfeDistDfeDocumento

    cnpj = _cnpj14(cnpj)
    novas = 0
    atualizadas = 0
    resumos = 0
    if len(cnpj) != 14:
        return {"novas": 0, "atualizadas": 0, "resumos": 0}
    nsu = re.sub(r"\D", "", str(nsu_ret or ""))[:15]
    for xml_txt in notas_xml or []:
        meta = classificar_xml_dfe(str(xml_txt or ""))
        chave = str(meta.get("chave") or "").strip()
        if len(chave) != 44:
            continue
        schema = str(meta.get("schema") or "outro")
        if schema == "resumo":
            resumos += 1
        defaults = {
            "nsu": nsu or "",
            "schema": schema if schema in ("nfe", "resumo", "outro") else "outro",
            "emit_nome": str(meta.get("emit_nome") or "")[:300],
            "numero": str(meta.get("numero") or "")[:20],
            "valor_total": meta.get("valor_total") or Decimal("0.00"),
            "dh_emi": str(meta.get("dh_emi") or "")[:40],
        }
        xml_save = str(meta.get("xml") or "") if schema == "nfe" else str(meta.get("xml") or "")[:8000]
        row = AgroNfeDistDfeDocumento.objects.filter(cnpj=cnpj, chave=chave).first()
        if row is None:
            st0 = _status_inicial_por_data(
                chave=chave, dh_emi=str(defaults.get("dh_emi") or "")
            )
            AgroNfeDistDfeDocumento.objects.create(
                cnpj=cnpj,
                chave=chave,
                xml=xml_save,
                status=st0,
                **defaults,
            )
            novas += 1
            continue
        # Já existe: atualiza meta; XML completo sobrescreve se veio nfe e status ainda pendente/carregada
        changed = False
        for k, v in defaults.items():
            if getattr(row, k) != v and v not in ("", None, Decimal("0.00")):
                setattr(row, k, v)
                changed = True
        if schema == "nfe" and xml_save and row.status in STATUS_PENDENTES:
            if (row.xml or "").strip() != xml_save:
                row.xml = xml_save
                row.schema = "nfe"
                changed = True
        if nsu and (not row.nsu or row.nsu < nsu):
            row.nsu = nsu
            changed = True
        if changed:
            row.save()
            atualizadas += 1
    trim = dfe_inbox_trim(cnpj)
    return {"novas": novas, "atualizadas": atualizadas, "resumos": resumos, "trim": trim}


def dfe_inbox_trim(cnpj: str, *, max_n: int | None = None) -> dict[str, Any]:
    from produtos.models import AgroNfeDistDfeDocumento

    cnpj = _cnpj14(cnpj)
    if len(cnpj) != 14:
        return {"apagados": 0}
    limite = int(max_n if max_n is not None else dfe_inbox_max())
    hard = dfe_inbox_hard_max()
    total = AgroNfeDistDfeDocumento.objects.filter(cnpj=cnpj).count()
    apagados = 0
    while total > limite:
        victim = (
            AgroNfeDistDfeDocumento.objects.filter(cnpj=cnpj, status__in=STATUS_CONCLUIDAS)
            .order_by("atualizado_em")
            .first()
        )
        if victim is None:
            break
        victim.delete()
        apagados += 1
        total -= 1
    while total > hard:
        victim = (
            AgroNfeDistDfeDocumento.objects.filter(cnpj=cnpj)
            .order_by("criado_em")
            .first()
        )
        if victim is None:
            break
        victim.delete()
        apagados += 1
        total -= 1
    return {"apagados": apagados, "total": total, "limite": limite}


def dfe_inbox_marcar(
    *,
    doc_id: int | None = None,
    chave: str = "",
    cnpj: str = "",
    status: str,
    rascunho_id: str = "",
) -> dict[str, Any]:
    from produtos.models import AgroNfeDistDfeDocumento

    st = str(status or "").strip().lower()
    if st not in dict(AgroNfeDistDfeDocumento.Status.choices):
        return {"ok": False, "erro": "Status inválido."}
    qs = AgroNfeDistDfeDocumento.objects.all()
    if doc_id:
        qs = qs.filter(pk=int(doc_id))
    else:
        ch = re.sub(r"\D", "", chave or "")[:44]
        cn = _cnpj14(cnpj)
        if len(ch) != 44:
            return {"ok": False, "erro": "Chave inválida."}
        qs = qs.filter(chave=ch)
        if len(cn) == 14:
            qs = qs.filter(cnpj=cn)
    row = qs.first()
    if not row:
        return {"ok": False, "erro": "Documento não encontrado."}
    row.status = st
    if rascunho_id:
        row.rascunho_id = str(rascunho_id)[:64]
    row.save(update_fields=["status", "rascunho_id", "atualizado_em"])
    return {"ok": True, "id": row.pk, "status": row.status}


def dfe_inbox_marcar_processada_por_chave(chave: str, *, cnpj: str = "", rascunho_id: str = "") -> None:
    ch = re.sub(r"\D", "", chave or "")[:44]
    if len(ch) != 44:
        return
    try:
        dfe_inbox_marcar(
            chave=ch,
            cnpj=cnpj,
            status="processada",
            rascunho_id=rascunho_id,
        )
    except Exception as exc:
        logger.warning("dfe_inbox_marcar_processada_por_chave: %s", exc)


def dfe_cron_ja_rodou_hoje(cnpj: str) -> bool:
    from django.core.cache import cache

    cnpj = _cnpj14(cnpj)
    if len(cnpj) != 14:
        return True
    return bool(cache.get(f"agro_dfe_cron_day:{cnpj}:{date.today().isoformat()}"))


def dfe_cron_marcar_hoje(cnpj: str) -> None:
    from django.core.cache import cache

    cnpj = _cnpj14(cnpj)
    if len(cnpj) != 14:
        return
    cache.set(
        f"agro_dfe_cron_day:{cnpj}:{date.today().isoformat()}",
        timezone.now().isoformat(),
        timeout=60 * 60 * 36,
    )


def dfe_executar_consulta_e_gravar(
    *,
    ult_nsu: str | None = None,
    origem: str = "manual",
) -> dict[str, Any]:
    """
    Consulta SEFAZ (respeitando cooldown do client) e grava inbox.
    origem: manual | cron
    """
    from produtos.nfe_entrada_util import gravar_ult_nsu, obter_ult_nsu
    from produtos.sefaz_dfe_client import (
        _cfg_dist_dfe,
        distribuicao_dfe_configurada,
        nfe_distribuicao_dfe_interesse,
    )

    cfg = _cfg_dist_dfe()
    cnpj = _cnpj14(str(cfg.get("cnpj") or ""))
    out: dict[str, Any] = {
        "ok": False,
        "cnpj": cnpj,
        "origem": origem,
        "inbox": {"novas": 0, "atualizadas": 0, "resumos": 0},
        "itens_pendentes": [],
    }
    if not distribuicao_dfe_configurada() or len(cnpj) != 14:
        out["erro"] = "Dist DF-e não configurada."
        return out
    if origem == "cron" and dfe_cron_ja_rodou_hoje(cnpj):
        out["ok"] = True
        out["pulado"] = True
        out["motivo"] = "Cron já rodou hoje para este CNPJ."
        out["itens_pendentes"] = dfe_inbox_listar(cnpj, aba="pendentes")
        return out

    if ult_nsu is not None and str(ult_nsu).strip() != "":
        ult = re.sub(r"\D", "", str(ult_nsu))[:15] or "0"
    else:
        ult = obter_ult_nsu(None, cnpj)

    res = nfe_distribuicao_dfe_interesse(ult)
    out.update(
        {
            "c_stat": res.get("c_stat"),
            "x_motivo": res.get("x_motivo"),
            "ult_nsu": res.get("ult_nsu"),
            "max_nsu": res.get("max_nsu"),
            "aguardar_segundos": res.get("aguardar_segundos"),
            "erro": res.get("erro"),
        }
    )
    try:
        c_stat_i = int(res.get("c_stat")) if res.get("c_stat") is not None else None
    except (TypeError, ValueError):
        c_stat_i = None
    ult_salvo = obter_ult_nsu(None, cnpj)
    # 137/138: avança com o retorno da SEFAZ.
    # 656: se a Receita devolver NSU *maior* que o da loja, adota (texto oficial:
    # «use o ultNSU nas solicitações subsequentes»). Sem isso o cursor fica preso
    # (ex. 2086) e toda Buscar volta 656. Nunca anda pra trás no 656.
    if res.get("ult_nsu") and c_stat_i in (137, 138):
        gravar_ult_nsu(None, cnpj, str(res["ult_nsu"]))
        ult_salvo = str(res["ult_nsu"]).zfill(15)[:15]
    elif c_stat_i == 656:
        sefaz_u = re.sub(r"\D", "", str(res.get("ult_nsu") or ""))[:15]
        try:
            n_salvo = int(ult_salvo or "0")
            n_sefaz = int(sefaz_u or "0") if sefaz_u else 0
        except ValueError:
            n_salvo, n_sefaz = 0, 0
        out["ult_nsu_sefaz"] = res.get("ult_nsu")
        if sefaz_u and n_sefaz > n_salvo:
            gravar_ult_nsu(None, cnpj, sefaz_u)
            ult_salvo = sefaz_u.zfill(15)[:15]
            aviso = (out.get("x_motivo") or out.get("erro") or "").strip()
            out["x_motivo"] = (
                f"{aviso} Cursor da loja atualizado para {ult_salvo.lstrip('0') or '0'} "
                "(ordem da Receita no 656). Aguarde 1h e Buscar de novo."
            ).strip()
        else:
            aviso = (out.get("x_motivo") or out.get("erro") or "").strip()
            out["x_motivo"] = (
                f"{aviso} Cursor da loja permanece {ult_salvo.lstrip('0') or '0'}."
            ).strip()
        out["ult_nsu"] = ult_salvo
    out["ult_nsu_salvo"] = ult_salvo
    if res.get("ok"):
        inbox = dfe_inbox_upsert_xmls(
            cnpj,
            list(res.get("notas_xml") or []),
            nsu_ret=str(res.get("ult_nsu") or ""),
        )
        out["inbox"] = inbox
        out["ok"] = True
        if origem == "cron":
            dfe_cron_marcar_hoje(cnpj)
    elif res.get("aguardar_segundos"):
        out["ok"] = False
    out["itens_pendentes"] = dfe_inbox_listar(cnpj, aba="pendentes")
    out["n_docs_retorno"] = len(res.get("notas_xml") or [])
    return out


def dfe_executar_download_por_chave(chave: str) -> dict[str, Any]:
    """Baixa XML pela chave (consChNFe) e grava na caixa — não mexe no ultNSU."""
    from produtos.sefaz_dfe_client import (
        _cfg_dist_dfe,
        distribuicao_dfe_configurada,
        nfe_distribuicao_dfe_por_chave,
    )

    cfg = _cfg_dist_dfe()
    cnpj = _cnpj14(str(cfg.get("cnpj") or ""))
    out: dict[str, Any] = {"ok": False, "cnpj": cnpj, "inbox": {"novas": 0}}
    if not distribuicao_dfe_configurada() or len(cnpj) != 14:
        out["erro"] = "Dist DF-e não configurada."
        return out
    res = nfe_distribuicao_dfe_por_chave(chave)
    out.update(
        {
            "c_stat": res.get("c_stat"),
            "x_motivo": res.get("x_motivo"),
            "erro": res.get("erro"),
            "aguardar_segundos": res.get("aguardar_segundos"),
        }
    )
    if res.get("ok"):
        inbox = dfe_inbox_upsert_xmls(
            cnpj,
            list(res.get("notas_xml") or []),
            nsu_ret="",
        )
        out["inbox"] = inbox
        out["ok"] = True
    out["itens_pendentes"] = dfe_inbox_listar(cnpj, aba="pendentes")
    return out
