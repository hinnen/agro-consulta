"""Kardex / histórico de movimentação por produto (cadastro ERP modal)."""
from __future__ import annotations

import re
from datetime import timedelta
from decimal import Decimal
from typing import Any

from django.utils import timezone

from estoque.models import AjusteRapidoEstoque, OrigemAjusteEstoque

_ORIGEM_LABEL = {
    OrigemAjusteEstoque.BAIXA_VENDA_PDV: "Venda",
    OrigemAjusteEstoque.DEVOLUCAO_VENDA_PDV: "Devolução",
    OrigemAjusteEstoque.ENTRADA_NF_AGRO: "Entrada NF",
    OrigemAjusteEstoque.TRANSFERENCIA_UI: "Transferência",
    OrigemAjusteEstoque.AJUSTE_PIN: "Ajuste PIN",
    OrigemAjusteEstoque.OUTRO: "Ajuste gestão",
    OrigemAjusteEstoque.VENCIMENTO_EM_LOJA: "Vencimento",
    OrigemAjusteEstoque.PLANILHA: "Planilha",
}

_RE_VENDA = re.compile(r"venda\s*#\s*(\d+)", re.I)
_RE_OPERADOR_PAREN = re.compile(r"\(([^)]{2,80})\)\s*$")
_RE_PIN_SOLO = re.compile(r"^\d{3,6}$")
_RE_NF_REF = re.compile(r"Entrada NF-e Agro\s*\(([^)]*)\)", re.I)


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.001"))
    except Exception:
        return Decimal("0.000")


def _nome_usuario(user) -> str:
    if user is None:
        return ""
    try:
        nome = (user.get_full_name() or "").strip()
        if nome:
            return nome[:120]
        return (getattr(user, "username", None) or "")[:120]
    except Exception:
        return ""


def _candidato_operador_ok(cand: str) -> bool:
    c = (cand or "").strip()
    if not c or len(c) > 80:
        return False
    if _RE_PIN_SOLO.match(c):
        return False
    if "PIN" in c.upper():
        return False
    return True


def _operador_sem_pin(row: AjusteRapidoEstoque) -> str:
    """
    Nome do operador da operação — nunca o PIN.
    Prefere o rótulo gravado na hora (PIN do PDV / label da NF) ao usuário
    Django da sessão Chrome (que costuma ficar «preso» num login).
    """
    texto = str(row.nome_produto or "")
    origem = str(row.origem or "")

    if origem in (
        OrigemAjusteEstoque.BAIXA_VENDA_PDV,
        OrigemAjusteEstoque.DEVOLUCAO_VENDA_PDV,
    ):
        m = _RE_OPERADOR_PAREN.search(texto)
        if m and _candidato_operador_ok(m.group(1)):
            return (m.group(1) or "").strip()[:120]

    if origem == OrigemAjusteEstoque.ENTRADA_NF_AGRO:
        # "nome · Entrada NF-e Agro (NF 123) · Operador"
        parts = [p.strip() for p in texto.split("·") if p.strip()]
        if parts:
            cand = parts[-1]
            if (
                _candidato_operador_ok(cand)
                and not cand.upper().startswith("ENTRADA")
                and not re.match(r"^NF\b", cand, re.I)
            ):
                return cand[:120]

    nome = _nome_usuario(getattr(row, "usuario", None))
    if nome and _candidato_operador_ok(nome):
        return nome

    m = _RE_OPERADOR_PAREN.search(texto)
    if m and _candidato_operador_ok(m.group(1)):
        return (m.group(1) or "").strip()[:120]

    obs = str(row.observacao or "").strip()
    if obs and _candidato_operador_ok(obs) and "pin" not in obs.lower() and len(obs) <= 80:
        return obs[:120]
    return "—"


def _numero_nf_digits(raw: str) -> str:
    return re.sub(r"\D+", "", str(raw or ""))


def _documento_e_venda(row: AjusteRapidoEstoque) -> tuple[str, int | None, str]:
    """Retorna (rótulo documento, venda_id, numero_nf_digits)."""
    texto = f"{row.nome_produto or ''} {row.observacao or ''}"
    m = _RE_VENDA.search(texto)
    venda_id = int(m.group(1)) if m else None
    origem = str(row.origem or "")
    if venda_id:
        if origem == OrigemAjusteEstoque.DEVOLUCAO_VENDA_PDV:
            return f"Devolução venda #{venda_id}", venda_id, ""
        return f"Venda #{venda_id}", venda_id, ""
    if origem == OrigemAjusteEstoque.ENTRADA_NF_AGRO:
        ref = ""
        mref = _RE_NF_REF.search(texto)
        if mref:
            ref = (mref.group(1) or "").strip()
        # Evita «NF NF 398454» quando a ref já vem com prefixo NF
        ref_clean = re.sub(r"^NF\s*", "", ref, flags=re.I).strip()
        digits = _numero_nf_digits(ref_clean)
        return (f"NF {ref_clean}" if ref_clean else "Entrada NF"), None, digits
    if origem == OrigemAjusteEstoque.TRANSFERENCIA_UI:
        return "Transferência", None, ""
    if origem == OrigemAjusteEstoque.AJUSTE_PIN:
        return "Ajuste (PIN)", None, ""
    if origem == OrigemAjusteEstoque.OUTRO:
        obs = str(row.observacao or "").strip()
        return (obs[:80] if obs else "Ajuste gestão"), None, ""
    if origem == OrigemAjusteEstoque.VENCIMENTO_EM_LOJA:
        return "Vencimento em loja", None, ""
    return (str(row.observacao or "").strip()[:80] or "—"), None, ""


def _match_compra(
    quando,
    compras: list[dict],
    *,
    numero_nf_digits: str = "",
) -> tuple[str, float | None]:
    """Casa entrada NF com compra: prioridade número da NF; senão data do dia (sem fallback cego)."""
    if not compras:
        return "", None

    num = _numero_nf_digits(numero_nf_digits)
    if num:
        for c in compras:
            cnum = _numero_nf_digits(
                str(c.get("numero_doc") or c.get("numero") or c.get("numero_nf") or "")
            )
            if not cnum:
                continue
            if num == cnum or num in cnum or cnum in num:
                forn = str(c.get("fornecedor") or "").strip()
                try:
                    preco = float(c.get("preco_pago") or 0)
                except (TypeError, ValueError):
                    preco = None
                return forn[:200], preco

    if quando is None:
        return "", None
    try:
        dia = quando.date() if hasattr(quando, "date") else None
    except Exception:
        dia = None
    if dia is None:
        return "", None

    matches: list[dict] = []
    for c in compras:
        raw = str(c.get("data") or "")[:10]
        if not raw:
            continue
        try:
            y, m, d = [int(x) for x in raw.replace("/", "-").split("-")[:3]]
            from datetime import date

            if date(y, m, d) == dia:
                matches.append(c)
        except Exception:
            continue
    # Só usa data se houver exatamente 1 compra naquele dia (evita «RBS» errado)
    if len(matches) == 1:
        c = matches[0]
        forn = str(c.get("fornecedor") or "").strip()
        try:
            preco = float(c.get("preco_pago") or 0)
        except (TypeError, ValueError):
            preco = None
        return forn[:200], preco
    return "", None


def montar_movimentos_produto(
    *,
    produto_ids: list[str],
    deposito: str = "",
    origem: str = "",
    dias: int = 90,
    limit: int = 50,
    offset: int = 0,
    compras_linhas: list[dict] | None = None,
) -> dict[str, Any]:
    """
    Lista kardex do produto a partir de ``AjusteRapidoEstoque``.

    ``dias``: 0 = sem corte de data (ainda limitado ao teto interno).
    ``limit``: máx 50 por página; ``offset`` para carregar mais (teto 200).
    """
    pids = [str(x).strip()[:100] for x in (produto_ids or []) if str(x).strip()]
    pids = list(dict.fromkeys(pids))[:120]
    if not pids:
        return {"ok": True, "linhas": [], "tem_mais": False, "total_estimado": 0}

    limit = max(1, min(int(limit or 50), 50))
    offset = max(0, min(int(offset or 0), 150))
    teto = 200
    if offset + limit > teto:
        limit = max(0, teto - offset)
    if limit <= 0:
        return {"ok": True, "linhas": [], "tem_mais": False, "total_estimado": teto}

    qs = AjusteRapidoEstoque.objects.filter(produto_externo_id__in=pids).select_related(
        "usuario"
    )
    dep = (deposito or "").strip().lower()
    if dep in ("centro", "vila"):
        qs = qs.filter(deposito=dep)
    origem_f = (origem or "").strip()
    if origem_f:
        qs = qs.filter(origem=origem_f)
    dias_n = int(dias if dias is not None else 90)
    if dias_n > 0:
        qs = qs.filter(criado_em__gte=timezone.now() - timedelta(days=dias_n))

    # Janela cronológica (ASC) para calcular deltas; teto de leitura.
    crono = list(qs.order_by("criado_em", "id")[:2500])
    # Chave por produto+depósito — misturar IDs variantes no mesmo saldo gerava
    # «Entrada NF» com saída inventada (ex. −172).
    prev_saldo: dict[tuple[str, str], Decimal] = {}
    enriched: list[dict] = []
    compras = list(compras_linhas or [])
    venda_ids: list[int] = []

    for row in crono:
        dep_k = str(row.deposito or "centro").lower()
        pid_k = str(row.produto_externo_id or "").strip()
        saldo = _dec(row.saldo_informado)
        chave = (pid_k, dep_k)
        antes = prev_saldo.get(chave)
        if antes is None:
            delta = None
        else:
            delta = saldo - antes
        prev_saldo[chave] = saldo

        qtd_ent = 0.0
        qtd_sai = 0.0
        if delta is not None:
            if delta > 0:
                qtd_ent = float(delta)
            elif delta < 0:
                qtd_sai = float(-delta)

        doc, venda_id, nf_digits = _documento_e_venda(row)
        if venda_id:
            venda_ids.append(venda_id)
        forn = ""
        preco = None
        if row.origem == OrigemAjusteEstoque.ENTRADA_NF_AGRO and compras:
            forn, preco = _match_compra(
                row.criado_em, compras, numero_nf_digits=nf_digits
            )

        quando = row.criado_em
        if timezone.is_aware(quando):
            quando_local = timezone.localtime(quando)
        else:
            quando_local = quando

        enriched.append(
            {
                "id": row.pk,
                "quando": quando_local.strftime("%d/%m/%Y %H:%M") if quando_local else "",
                "quando_iso": quando.isoformat() if quando else "",
                "tipo_label": _ORIGEM_LABEL.get(row.origem, "Outro"),
                "origem": str(row.origem or ""),
                "documento": doc,
                "venda_id": venda_id,
                "fornecedor": forn or "",
                "preco_pago": preco,
                "deposito": dep_k,
                "qtd_entrada": qtd_ent,
                "qtd_saida": qtd_sai,
                "saldo_depois": float(saldo),
                "operador": _operador_sem_pin(row),
            }
        )

    # Quem da venda = operador do PDV gravado na VendaAgro (fonte da verdade)
    if venda_ids:
        try:
            from produtos.models import VendaAgro

            uniq = list(dict.fromkeys(venda_ids))[:500]
            mapa_op = {
                int(pk): str(op or "").strip()
                for pk, op in VendaAgro.objects.filter(pk__in=uniq).values_list(
                    "pk", "usuario_registro"
                )
            }
            for e in enriched:
                vid = e.get("venda_id")
                if not vid:
                    continue
                op_v = mapa_op.get(int(vid)) or ""
                if op_v and _candidato_operador_ok(op_v):
                    e["operador"] = op_v[:120]
        except Exception:
            pass

    # Mais recente primeiro
    enriched.reverse()
    total = len(enriched)
    page = enriched[offset : offset + limit]
    tem_mais = (offset + limit) < min(total, teto)

    return {
        "ok": True,
        "linhas": page,
        "tem_mais": tem_mais,
        "total_estimado": min(total, teto),
        "offset": offset,
        "limit": limit,
    }
