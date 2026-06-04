"""Crédito loja (fiado): limite ERP/Mongo, saldo usado local, parcelas."""

from __future__ import annotations

import re
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from django.conf import settings

from django.db.models import Q

from produtos.caixa_util import normalizar_forma_pagamento_caixa
from produtos.models import ClienteAgro, VendaAgro


def forma_pagamento_erp_fiado_label() -> str:
    """Nome da forma no ERP para vendas fiado do PDV (padrão: Crédito Loja)."""
    raw = getattr(settings, "VENDA_ERP_FORMA_PAGAMENTO_FIADO", None)
    if raw is None:
        try:
            from decouple import config as dec_config

            raw = dec_config("VENDA_ERP_FORMA_PAGAMENTO_FIADO", default="Crédito Loja")
        except Exception:
            raw = "Crédito Loja"
    txt = str(raw or "").strip()
    return txt or "Crédito Loja"


def forma_pagamento_texto_envio_erp(fn: str) -> str:
    """No Agro a forma é «Fiado»; no Pedidos/Salvar do ERP deve ir como Crédito Loja."""
    fn = str(fn or "").strip()
    if not fn:
        return ""
    if normalizar_forma_pagamento_caixa(fn) == "Fiado":
        return forma_pagamento_erp_fiado_label()
    return fn


def forma_pagamento_resumo_envio_erp(resumo: str) -> str:
    """Aplica ``forma_pagamento_texto_envio_erp`` em resumos «Forma A + Forma B»."""
    base = str(resumo or "").strip()
    if not base:
        return ""
    parts = [p.strip() for p in re.split(r"\s+\+\s+", base) if p.strip()]
    if len(parts) <= 1:
        return forma_pagamento_texto_envio_erp(base)
    out: list[str] = []
    for p in parts:
        t = forma_pagamento_texto_envio_erp(p)
        if t and t not in out:
            out.append(t)
    return " + ".join(out)[:200]

_LIMITE_DOC_KEYS = (
    "LimiteCredito",
    "LimiteFiado",
    "LimiteDeCredito",
    "ValorLimiteCredito",
    "CreditoLimite",
    "LimiteCreditoLoja",
    "limiteCredito",
    "limiteFiado",
    "Limite",
)


def cliente_agro_pk_de_ref(cliente_id: str = "", cliente_agro_pk=None) -> int | None:
    if cliente_agro_pk is not None:
        try:
            return int(cliente_agro_pk)
        except (TypeError, ValueError):
            pass
    raw = str(cliente_id or "").strip()
    for prefix in ("local:", "agro:"):
        if raw.lower().startswith(prefix):
            try:
                return int(raw.split(":", 1)[1])
            except (TypeError, ValueError):
                return None
    return None


def resolver_cliente_fiado(
    cliente_id: str = "",
    *,
    cliente_agro_pk=None,
) -> tuple[str, int | None, ClienteAgro | None]:
    """
    Referência do cliente para fiado: id ERP (se houver), pk Agro e registro local.
    """
    cid = str(cliente_id or "").strip()
    pk = cliente_agro_pk_de_ref(cid, cliente_agro_pk)
    cli = ClienteAgro.objects.filter(pk=pk).first() if pk else None
    erp_id = ""
    if cid and not cid.lower().startswith(("local:", "agro:", "erp-doc:")):
        erp_id = cid
    elif cli and (cli.externo_id or "").strip():
        erp_id = str(cli.externo_id).strip()
    agro_pk = pk or (cli.pk if cli else None)
    ref_local = f"agro:{agro_pk}" if agro_pk else ""
    return erp_id, agro_pk, cli


def cliente_ref_valida_para_fiado(
    cliente_id: str = "",
    *,
    cliente_agro_pk=None,
    cliente_nome: str = "",
) -> bool:
    if re.search(r"consumidor\s+n[aã]o\s+identificado", str(cliente_nome or ""), re.I):
        return False
    erp_id, agro_pk, _cli = resolver_cliente_fiado(cliente_id, cliente_agro_pk=cliente_agro_pk)
    return bool(erp_id or agro_pk)


def fiado_limite_padrao() -> Decimal:
    raw = getattr(settings, "AGRO_FIADO_LIMITE_PADRAO", 5000)
    try:
        return Decimal(str(raw)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("5000.00")


def _dec(val) -> Decimal:
    try:
        if val is None:
            return Decimal("0")
        return Decimal(str(val).replace(",", ".")).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


def _extrair_limite_de_doc(doc: dict | None) -> Decimal | None:
    if not doc or not isinstance(doc, dict):
        return None
    for key in _LIMITE_DOC_KEYS:
        if key not in doc:
            continue
        v = _dec(doc.get(key))
        if v > 0:
            return v
    for nested_key in ("DadosCadastrais", "dadosCadastrais", "Credito", "credito", "Financeiro", "financeiro"):
        sub = doc.get(nested_key)
        if isinstance(sub, dict):
            found = _extrair_limite_de_doc(sub)
            if found is not None:
                return found
    return None


def buscar_limite_credito_mongo(cliente_id_erp: str, *, db=None, client_m=None) -> tuple[Decimal, bool]:
    """
    Retorna (limite, usou_padrao).
  """
    cid = str(cliente_id_erp or "").strip()
    if not cid:
        return fiado_limite_padrao(), True
    if db is None or client_m is None:
        from produtos.views import obter_conexao_mongo

        client_m, db = obter_conexao_mongo()
    if db is None:
        return fiado_limite_padrao(), True
    from bson import ObjectId

    from produtos.views import _colecoes_pessoa_disponiveis

    filtros = [{"Id": cid}, {"id": cid}]
    try:
        if len(cid) == 24:
            filtros.append({"_id": ObjectId(cid)})
    except Exception:
        pass
    query = {"$or": filtros}
    proj = {k: 1 for k in _LIMITE_DOC_KEYS}
    proj.update({"DadosCadastrais": 1, "Credito": 1, "Financeiro": 1})
    for coll in _colecoes_pessoa_disponiveis(db, client_m):
        try:
            doc = db[coll].find_one(query, proj)
        except Exception:
            continue
        if not doc:
            continue
        lim = _extrair_limite_de_doc(doc)
        if lim is not None:
            return lim, False
    return fiado_limite_padrao(), True


def venda_payload_tem_fiado(data: dict | None) -> bool:
    if not data or not isinstance(data, dict):
        return False
    raw = data.get("pagamentos")
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            fn = normalizar_forma_pagamento_caixa(
                str(
                    row.get("formaPagamento")
                    or row.get("forma_pagamento")
                    or row.get("forma")
                    or ""
                )
            )
            vp = _dec(row.get("valorPagamento", row.get("valor_pagamento", row.get("valor"))))
            if fn == "Fiado" and vp > 0:
                return True
    forma = str(data.get("forma_pagamento") or data.get("formaPagamento") or "")
    return "fiado" in forma.lower()


def valor_fiado_no_payload(data: dict | None) -> Decimal:
    if not data or not isinstance(data, dict):
        return Decimal("0")
    total = Decimal("0")
    raw = data.get("pagamentos")
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            fn = normalizar_forma_pagamento_caixa(
                str(
                    row.get("formaPagamento")
                    or row.get("forma_pagamento")
                    or row.get("forma")
                    or ""
                )
            )
            if fn != "Fiado":
                continue
            total += _dec(row.get("valorPagamento", row.get("valor_pagamento", row.get("valor"))))
    if total > 0:
        return total
    if venda_payload_tem_fiado(data):
        return _dec(data.get("total") or data.get("valor_total"))
    return Decimal("0")


def valor_fiado_venda_local(venda: VendaAgro) -> Decimal:
    total = Decimal("0")
    pj = venda.pagamentos_json
    if isinstance(pj, list) and pj:
        for row in pj:
            if not isinstance(row, dict):
                continue
            if normalizar_forma_pagamento_caixa(str(row.get("forma") or "")) == "Fiado":
                total += _dec(row.get("valor"))
        if total > 0:
            return total
    if "fiado" in str(venda.forma_pagamento or "").lower():
        return _dec(venda.total)
    return Decimal("0")


def venda_local_tem_fiado(venda: VendaAgro) -> bool:
    return valor_fiado_venda_local(venda) > 0


def valor_fiado_usado_cliente_vendas(
    cliente_id_erp: str = "",
    *,
    cliente_agro_pk: int | None = None,
    excluir_venda_id: int | None = None,
) -> Decimal:
    """Saldo fiado pela soma de vendas (legado, antes do ledger de títulos)."""
    erp_id, agro_pk, _cli = resolver_cliente_fiado(cliente_id_erp, cliente_agro_pk=cliente_agro_pk)
    filtros = Q()
    if erp_id:
        filtros |= Q(cliente_id_erp=erp_id)
    if agro_pk:
        filtros |= Q(cliente_id_erp=f"agro:{agro_pk}")
        filtros |= Q(cliente_id_erp=f"local:{agro_pk}")
    if not filtros:
        return Decimal("0")
    qs = VendaAgro.objects.filter(filtros, devolvida_em__isnull=True).only(
        "pk", "pagamentos_json", "forma_pagamento", "total", "cliente_id_erp"
    )
    total = Decimal("0")
    for v in qs:
        if excluir_venda_id and v.pk == excluir_venda_id:
            continue
        total += valor_fiado_venda_local(v)
    return total.quantize(Decimal("0.01"))


def valor_fiado_usado_cliente(
    cliente_id_erp: str = "",
    *,
    cliente_agro_pk: int | None = None,
    cliente_nome: str = "",
    excluir_venda_id: int | None = None,
) -> Decimal:
    try:
        from produtos.fiado_gestao_util import valor_fiado_usado_cliente as _usado_titulos

        return _usado_titulos(
            cliente_id_erp,
            cliente_agro_pk=cliente_agro_pk,
            cliente_nome=cliente_nome,
            excluir_venda_id=excluir_venda_id,
        )
    except Exception:
        return valor_fiado_usado_cliente_vendas(
            cliente_id_erp,
            cliente_agro_pk=cliente_agro_pk,
            excluir_venda_id=excluir_venda_id,
        )


def montar_cronograma_fiado(
    valor_total: Decimal | float,
    num_parcelas: int,
    dias_primeira: int,
    *,
    base_date: date | None = None,
) -> list[dict[str, Any]]:
    n = max(1, min(int(num_parcelas or 1), 6))
    dias_base = max(1, int(dias_primeira or 30))
    total = _dec(valor_total)
    if total <= 0:
        return []
    base = (total / n).quantize(Decimal("0.01"))
    ref = base_date or date.today()
    out: list[dict[str, Any]] = []
    acum = Decimal("0")
    for i in range(n):
        if i == n - 1:
            parcela_val = (total - acum).quantize(Decimal("0.01"))
        else:
            parcela_val = base
            acum += parcela_val
        dias = dias_base * (i + 1)
        venc = ref + timedelta(days=dias)
        out.append(
            {
                "parcela": i + 1,
                "dias": dias,
                "vencimento": venc.isoformat(),
                "valor": float(parcela_val),
            }
        )
    return out


def pagamentos_json_com_metadados_de_payload(data: dict | None) -> list[dict]:
    """Preserva metadados de fiado em pagamentos_json."""
    from produtos.caixa_util import pagamentos_json_de_payload

    base = pagamentos_json_de_payload(data)
    if not data or not isinstance(data, dict):
        return base
    raw = data.get("pagamentos")
    if not isinstance(raw, list):
        return base
    by_forma_valor: dict[tuple[str, float], dict] = {}
    for row in raw:
        if not isinstance(row, dict):
            continue
        fn = normalizar_forma_pagamento_caixa(
            str(
                row.get("formaPagamento")
                or row.get("forma_pagamento")
                or row.get("forma")
                or ""
            )
        )
        vp = float(_dec(row.get("valorPagamento", row.get("valor_pagamento", row.get("valor")))))
        by_forma_valor[(fn, vp)] = row
    enriched: list[dict] = []
    for item in base:
        fn = item.get("forma")
        vp = float(item.get("valor") or 0)
        src = by_forma_valor.get((fn, vp)) or by_forma_valor.get((fn, round(vp, 2)))
        row = dict(item)
        if fn == "Fiado" and src:
            np = src.get("fiadoParcelas") or src.get("fiado_parcelas")
            nd = src.get("fiadoDiasVencimento") or src.get("fiado_dias_vencimento")
            cron = src.get("fiadoCronograma") or src.get("fiado_cronograma")
            if np is not None:
                row["fiado_parcelas"] = int(np)
            if nd is not None:
                row["fiado_dias_primeiro"] = int(nd)
            if isinstance(cron, list) and cron:
                row["fiado_cronograma"] = cron
            elif np and nd:
                row["fiado_cronograma"] = montar_cronograma_fiado(vp, int(np), int(nd))
        enriched.append(row)
    return enriched


def resumo_credito_fiado_cliente(
    cliente_id_erp: str = "",
    *,
    cliente_agro_pk: int | None = None,
    cliente_nome: str = "",
    valor_nova_venda_fiado: Decimal | None = None,
    excluir_venda_id: int | None = None,
    db=None,
    client_m=None,
) -> dict[str, Any]:
    erp_id, agro_pk, cli = resolver_cliente_fiado(cliente_id_erp, cliente_agro_pk=cliente_agro_pk)
    limite, padrao = (
        buscar_limite_credito_mongo(erp_id, db=db, client_m=client_m)
        if erp_id
        else (fiado_limite_padrao(), True)
    )
    if cli is None and agro_pk:
        cli = ClienteAgro.objects.filter(pk=agro_pk).first()
    limite_local = _dec(cli.limite_fiado_local) if cli else Decimal("0")
    if limite_local > 0:
        limite = limite_local
        padrao = False
    nome_cli = (cliente_nome or "").strip() or ((cli.nome or "").strip() if cli else "")
    usado = valor_fiado_usado_cliente(
        cliente_id_erp,
        cliente_agro_pk=agro_pk,
        cliente_nome=nome_cli,
        excluir_venda_id=excluir_venda_id,
    )
    disponivel = (limite - usado).quantize(Decimal("0.01"))
    novo = _dec(valor_nova_venda_fiado) if valor_nova_venda_fiado is not None else Decimal("0")
    apos = (disponivel - novo).quantize(Decimal("0.01"))
    tem_pendencia = usado > Decimal("0.009")

    from produtos.fiado_gestao_util import vencidos_fiado_cliente

    cod_cli = erp_id or (
        str(cliente_id_erp).strip() if str(cliente_id_erp or "").strip().isdigit() else ""
    )
    titulos_venc, total_venc = vencidos_fiado_cliente(
        cliente_agro_pk=agro_pk if not nome_cli else None,
        cliente_nome=nome_cli,
        cliente_codigo=cod_cli if not nome_cli else "",
        limit=40,
    )
    tem_vencido = bool(titulos_venc) and total_venc > Decimal("0.009")
    bloqueado_nova_venda = tem_vencido
    bloqueado_motivo = ""
    if tem_vencido:
        bloqueado_motivo = (
            f"Cliente com fiado vencido ({f'R$ {total_venc:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')}). "
            "Quite os títulos vencidos antes de nova venda fiado."
        )
    return {
        "ok": True,
        "cliente_id": erp_id or (f"agro:{agro_pk}" if agro_pk else ""),
        "cliente_agro_pk": agro_pk,
        "limite": float(limite),
        "limite_texto": f"R$ {limite:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        "usado": float(usado),
        "usado_texto": f"R$ {usado:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        "disponivel": float(disponivel),
        "disponivel_texto": f"R$ {disponivel:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        "limite_padrao": padrao,
        "permite_fiado": bool(erp_id or agro_pk),
        "tem_pendencia": tem_pendencia,
        "tem_vencido": tem_vencido,
        "titulos_vencidos": titulos_venc,
        "total_vencido": float(total_venc),
        "total_vencido_texto": f"R$ {total_venc:,.2f}".replace(",", "X")
        .replace(".", ",")
        .replace("X", "."),
        "bloqueado_nova_venda": bloqueado_nova_venda,
        "bloqueado_motivo": bloqueado_motivo,
        "apos_venda": float(apos),
        "excede": bool(novo > 0 and apos < -Decimal("0.009")),
    }
