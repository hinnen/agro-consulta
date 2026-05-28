"""
Resumo de caixa por forma de pagamento, movimentos (reforço/retirada) e conferência no fechamento.
"""
from __future__ import annotations

import re
from collections import defaultdict
from decimal import Decimal
from typing import Any

FORMAS_PAGAMENTO_CAIXA: tuple[str, ...] = (
    "Dinheiro",
    "PIX",
    "Cartão de débito",
    "Cartão de crédito",
    "Crédito parcelado",
    "Fiado",
    "Vale crédito",
    "Cashback",
    "Outro",
)

_FORMA_ALIASES = {
    "dinheiro": "Dinheiro",
    "pix": "PIX",
    "cartao de debito": "Cartão de débito",
    "cartão de débito": "Cartão de débito",
    "cartao de credito": "Cartão de crédito",
    "cartão de crédito": "Cartão de crédito",
    "credito parcelado": "Crédito parcelado",
    "crédito parcelado": "Crédito parcelado",
    "fiado": "Fiado",
    "vale credito": "Vale crédito",
    "vale crédito": "Vale crédito",
    "cashback": "Cashback",
    "outro": "Outro",
}


def _dec(val, default: Decimal = Decimal("0")) -> Decimal:
    if isinstance(val, Decimal):
        return val.quantize(Decimal("0.01"))
    try:
        if val is None:
            return default
        s = str(val).strip().replace(".", "").replace(",", ".") if isinstance(val, str) and "," in str(val) else str(val).strip().replace(",", ".")
        return Decimal(s).quantize(Decimal("0.01"))
    except Exception:
        return default


def normalizar_forma_pagamento_caixa(raw: str) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return "Outro"
    base = re.sub(r"\s+\d+x\s*$", "", txt, flags=re.IGNORECASE).strip()
    base = re.sub(r"\s*Mercado Pago.*$", "", base, flags=re.IGNORECASE).strip()
    base = re.sub(r"\s*Sicredi.*$", "", base, flags=re.IGNORECASE).strip()
    base = re.sub(r"\s*Sicoob.*$", "", base, flags=re.IGNORECASE).strip()
    key = base.lower()
    if key in _FORMA_ALIASES:
        return _FORMA_ALIASES[key]
    for canon in FORMAS_PAGAMENTO_CAIXA:
        if canon.lower() == key or key.startswith(canon.lower()):
            return canon
    return base[:80] if base else "Outro"


def pagamentos_json_de_payload(data: dict | None) -> list[dict]:
    """Extrai lista {forma, valor} a partir do payload do PDV / pedido ERP."""
    if not data or not isinstance(data, dict):
        return []
    raw = data.get("pagamentos")
    if not isinstance(raw, list) or not raw:
        return []
    out: list[dict] = []
    for row in raw[:30]:
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
        if vp <= 0:
            continue
        out.append({"forma": fn, "valor": float(vp)})
    return out


def pagamentos_por_forma_venda(venda) -> dict[str, Decimal]:
    """Totais por forma para uma venda (usa pagamentos_json quando existir)."""
    totais: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    pj = getattr(venda, "pagamentos_json", None)
    if isinstance(pj, list) and pj:
        for row in pj:
            if not isinstance(row, dict):
                continue
            fn = normalizar_forma_pagamento_caixa(str(row.get("forma") or ""))
            totais[fn] += _dec(row.get("valor"))
        if totais:
            return dict(totais)
    forma_txt = str(getattr(venda, "forma_pagamento", "") or "").strip()
    total = _dec(getattr(venda, "total", 0))
    if not total:
        return {}
    if " + " in forma_txt:
        partes = [p.strip() for p in forma_txt.split(" + ") if p.strip()]
        fn = normalizar_forma_pagamento_caixa(partes[0] if partes else "Outro")
        totais[fn] += total
    else:
        fn = normalizar_forma_pagamento_caixa(forma_txt or "Outro")
        totais[fn] += total
    return dict(totais)


def resumo_esperado_por_forma(sessao) -> dict[str, Decimal]:
    """Esperado no turno: abertura (Dinheiro) + vendas + reforços − retiradas."""
    totais: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    totais["Dinheiro"] += _dec(getattr(sessao, "valor_abertura", 0))

    vendas = getattr(sessao, "vendas", None)
    if vendas is not None:
        for v in vendas.all():
            if getattr(v, "devolvida_em", None):
                continue
            for fn, val in pagamentos_por_forma_venda(v).items():
                totais[fn] += val

    movimentos = getattr(sessao, "movimentos", None)
    if movimentos is not None:
        for m in movimentos.all():
            fn = normalizar_forma_pagamento_caixa(m.forma_pagamento)
            val = _dec(m.valor)
            if m.tipo == "reforco":
                totais[fn] += val
            elif m.tipo == "retirada":
                totais[fn] -= val

    return {k: v.quantize(Decimal("0.01")) for k, v in totais.items() if v != 0 or k in FORMAS_PAGAMENTO_CAIXA}


def linhas_resumo_caixa(sessao) -> list[dict[str, Any]]:
    """Lista ordenada para tela: forma, esperado, vendas, reforços, retiradas."""
    esperado = resumo_esperado_por_forma(sessao)
    vendas_por: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    reforco_por: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    retirada_por: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

    for v in sessao.vendas.all():
        if getattr(v, "devolvida_em", None):
            continue
        for fn, val in pagamentos_por_forma_venda(v).items():
            vendas_por[fn] += val

    for m in sessao.movimentos.all():
        fn = normalizar_forma_pagamento_caixa(m.forma_pagamento)
        if m.tipo == "reforco":
            reforco_por[fn] += _dec(m.valor)
        else:
            retirada_por[fn] += _dec(m.valor)

    formas = set(FORMAS_PAGAMENTO_CAIXA) | set(esperado.keys()) | set(vendas_por.keys())
    linhas: list[dict[str, Any]] = []
    abertura = _dec(sessao.valor_abertura)
    for fn in FORMAS_PAGAMENTO_CAIXA:
        if fn not in formas and fn != "Dinheiro":
            continue
        esp = esperado.get(fn, Decimal("0"))
        if fn == "Dinheiro" and abertura and not vendas_por.get(fn) and not reforco_por.get(fn):
            if esp == abertura:
                pass
        if esp == 0 and not vendas_por.get(fn) and not reforco_por.get(fn) and not retirada_por.get(fn) and fn != "Dinheiro":
            if abertura == 0 or fn != "Dinheiro":
                if fn != "Dinheiro":
                    continue
        linhas.append(
            {
                "forma": fn,
                "esperado": esp,
                "vendas": vendas_por.get(fn, Decimal("0")),
                "reforcos": reforco_por.get(fn, Decimal("0")),
                "retiradas": retirada_por.get(fn, Decimal("0")),
                "abertura_dinheiro": abertura if fn == "Dinheiro" else Decimal("0"),
            }
        )
    extras = sorted(formas - set(FORMAS_PAGAMENTO_CAIXA))
    for fn in extras:
        linhas.append(
            {
                "forma": fn,
                "esperado": esperado.get(fn, Decimal("0")),
                "vendas": vendas_por.get(fn, Decimal("0")),
                "reforcos": reforco_por.get(fn, Decimal("0")),
                "retiradas": retirada_por.get(fn, Decimal("0")),
                "abertura_dinheiro": Decimal("0"),
            }
        )
    return linhas


def linhas_conferencia_fechar(sessao) -> list[dict[str, Any]]:
    """Formas com movimento no turno (para tela de fechamento)."""
    out: list[dict[str, Any]] = []
    abertura = _dec(getattr(sessao, "valor_abertura", 0))
    for L in linhas_resumo_caixa(sessao):
        tem_mov = (
            L["esperado"] != 0
            or L["vendas"] != 0
            or L["reforcos"] != 0
            or L["retiradas"] != 0
        )
        if not tem_mov and not (L["forma"] == "Dinheiro" and abertura > 0):
            continue
        out.append(L)
    return out


def linhas_conferencia_agregada(sessoes, *, todas_formas: bool = False) -> list[dict[str, Any]]:
    """Soma esperado/vendas/reforços/retiradas por forma em várias sessões abertas."""
    merged: dict[str, dict[str, Any]] = {}
    for sessao in sessoes:
        for L in linhas_conferencia_fechar(sessao):
            fn = L["forma"]
            if fn not in merged:
                merged[fn] = {
                    "forma": fn,
                    "esperado": Decimal("0"),
                    "vendas": Decimal("0"),
                    "reforcos": Decimal("0"),
                    "retiradas": Decimal("0"),
                    "abertura_dinheiro": Decimal("0"),
                }
            row = merged[fn]
            row["esperado"] += _dec(L["esperado"])
            row["vendas"] += _dec(L["vendas"])
            row["reforcos"] += _dec(L["reforcos"])
            row["retiradas"] += _dec(L["retiradas"])
            row["abertura_dinheiro"] += _dec(L["abertura_dinheiro"])
    if not merged and not todas_formas:
        return []

    def _row(fn: str, src: dict | None) -> dict[str, Any]:
        if src:
            return {
                "forma": fn,
                "esperado": _dec(src["esperado"]).quantize(Decimal("0.01")),
                "vendas": _dec(src["vendas"]).quantize(Decimal("0.01")),
                "reforcos": _dec(src["reforcos"]).quantize(Decimal("0.01")),
                "retiradas": _dec(src["retiradas"]).quantize(Decimal("0.01")),
                "abertura_dinheiro": _dec(src["abertura_dinheiro"]).quantize(Decimal("0.01")),
            }
        return {
            "forma": fn,
            "esperado": Decimal("0"),
            "vendas": Decimal("0"),
            "reforcos": Decimal("0"),
            "retiradas": Decimal("0"),
            "abertura_dinheiro": Decimal("0"),
        }

    out: list[dict[str, Any]] = []
    if todas_formas:
        for fn in FORMAS_PAGAMENTO_CAIXA:
            out.append(_row(fn, merged.get(fn)))
        for fn in sorted(set(merged.keys()) - set(FORMAS_PAGAMENTO_CAIXA)):
            out.append(_row(fn, merged[fn]))
        return out

    ordem = [fn for fn in FORMAS_PAGAMENTO_CAIXA if fn in merged]
    ordem.extend(sorted(set(merged.keys()) - set(FORMAS_PAGAMENTO_CAIXA)))
    for fn in ordem:
        out.append(_row(fn, merged[fn]))
    return out


def obter_sessao_caixa_aberta_request(request):
    """Sessão de caixa gravada no cookie de sessão do navegador."""
    from produtos.models import SessaoCaixa

    sid = request.session.get("pdv_sessao_caixa_id")
    if not sid:
        return None
    try:
        return SessaoCaixa.objects.get(pk=int(sid), fechado_em__isnull=True)
    except Exception:
        request.session.pop("pdv_sessao_caixa_id", None)
        return None


def adotar_sessao_caixa_unica_aberta(request):
    """
    Quando há um único caixa aberto (ou um do usuário logado), associa ao navegador.
    Evita vendas «sem caixa» quando o turno está aberto mas o cookie de sessão não foi setado.
    """
    from produtos.models import SessaoCaixa

    atual = obter_sessao_caixa_aberta_request(request)
    if atual:
        return atual
    qs = SessaoCaixa.objects.filter(fechado_em__isnull=True).order_by("-aberto_em")
    usuario = getattr(request, "user", None)
    if usuario is not None and getattr(usuario, "is_authenticated", False):
        su = qs.filter(usuario=usuario).first()
        if su:
            request.session["pdv_sessao_caixa_id"] = su.pk
            request.session.modified = True
            return su
    if qs.count() == 1:
        s = qs.first()
        if s:
            request.session["pdv_sessao_caixa_id"] = s.pk
            request.session.modified = True
            return s
    return None


MSG_CAIXA_FECHADO_VENDA = "Abra o caixa antes de registrar vendas."


class SessaoCaixaObrigatoriaError(Exception):
    """Nenhuma SessaoCaixa aberta para vincular à venda."""

    def __init__(self, mensagem: str | None = None):
        super().__init__(mensagem or MSG_CAIXA_FECHADO_VENDA)


def exigir_sessao_caixa_para_venda(request, data: dict | None = None):
    """Exige turno de caixa aberto; levanta SessaoCaixaObrigatoriaError se não houver."""
    sessao = resolver_sessao_caixa_para_venda(request, data)
    if not sessao:
        raise SessaoCaixaObrigatoriaError()
    return sessao


def resolver_sessao_caixa_para_venda(request, data: dict | None = None):
    """
    Vincula venda ao caixa: sessão do navegador → id enviado pelo PDV → único caixa aberto.
    """
    from produtos.models import SessaoCaixa

    sessao = obter_sessao_caixa_aberta_request(request)
    if sessao:
        return sessao
    raw_id = None
    if isinstance(data, dict):
        raw_id = data.get("sessao_caixa_id") or data.get("sessaoCaixaId")
    if raw_id is not None and str(raw_id).strip() != "":
        try:
            sid = int(raw_id)
        except (TypeError, ValueError):
            sid = 0
        if sid > 0:
            sessao = SessaoCaixa.objects.filter(pk=sid, fechado_em__isnull=True).first()
            if sessao:
                request.session["pdv_sessao_caixa_id"] = sessao.pk
                request.session.modified = True
                return sessao
    return adotar_sessao_caixa_unica_aberta(request)


def registrar_retirada_turno_caixa(request, *, valor, forma_nome: str, observacao: str = ""):
    """Após saída financeira (plano de conta), registra retirada na sessão aberta."""
    from produtos.models import MovimentoCaixa, SessaoCaixa

    sid = request.session.get("pdv_sessao_caixa_id")
    if not sid:
        return None
    try:
        sessao = SessaoCaixa.objects.get(pk=int(sid), fechado_em__isnull=True)
    except Exception:
        return None
    v = _dec(valor)
    if v <= 0:
        return None
    fn = normalizar_forma_pagamento_caixa(forma_nome or "Dinheiro")
    return MovimentoCaixa.objects.create(
        sessao_caixa=sessao,
        tipo=MovimentoCaixa.Tipo.RETIRADA,
        forma_pagamento=fn,
        valor=v,
        observacao=str(observacao or "")[:500],
        usuario=request.user if getattr(request, "user", None) and request.user.is_authenticated else None,
    )


def format_moeda_br(val) -> str:
    """Valor monetário para tela: 1.234,56 (sem prefixo R$)."""
    if val is None:
        return "0,00"
    try:
        q = _dec(val).quantize(Decimal("0.01"))
    except Exception:
        return "0,00"
    neg = q < 0
    q = abs(q)
    inteiro, _, frac = f"{q:.2f}".partition(".")
    partes: list[str] = []
    while inteiro:
        partes.append(inteiro[-3:])
        inteiro = inteiro[:-3]
    corpo = ".".join(reversed(partes)) if partes else "0"
    s = f"{corpo},{frac}"
    return f"-{s}" if neg else s


def format_quantidade_br(val) -> str:
    """Quantidade: inteiro sem casas quando couber; senão até 4 casas, vírgula decimal."""
    if val is None:
        return "0"
    try:
        d = _dec(val)
    except Exception:
        return "0"
    if d == d.to_integral_value():
        return str(int(d))
    q = d.quantize(Decimal("0.0001"))
    if q == q.to_integral_value():
        return str(int(q))
    s = format(q, "f").rstrip("0").rstrip(".")
    return s.replace(".", ",")


def parse_valor_moeda_br(raw) -> Decimal | None:
    txt = str(raw or "").strip()
    if not txt:
        return None
    txt = re.sub(r"^R\$\s*", "", txt, flags=re.IGNORECASE).replace(" ", "")
    if "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    else:
        txt = txt.replace(",", ".")
    try:
        return Decimal(txt).quantize(Decimal("0.01"))
    except Exception:
        return None


def pagamentos_lista_de_venda(venda) -> list[dict[str, Any]]:
    """Parcelas da venda como lista [{forma, valor}] (default para devolução)."""
    out: list[dict[str, Any]] = []
    for fn, val in pagamentos_por_forma_venda(venda).items():
        v = _dec(val)
        if v > 0:
            out.append({"forma": fn, "valor": float(v.quantize(Decimal("0.01")))})
    if not out:
        tot = _dec(getattr(venda, "total", 0))
        if tot > 0:
            fn = normalizar_forma_pagamento_caixa(
                str(getattr(venda, "forma_pagamento", "") or "Outro")
            )
            out.append({"forma": fn, "valor": float(tot)})
    return out


def normalizar_pagamentos_devolucao(
    raw_list,
    *,
    total_venda: Decimal,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """Valida pagamentos informados na devolução; soma deve bater com o total da venda."""
    if not isinstance(raw_list, list) or not raw_list:
        return None, "Informe ao menos uma forma de pagamento para devolver."
    merged: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in raw_list[:30]:
        if not isinstance(row, dict):
            continue
        fn = normalizar_forma_pagamento_caixa(str(row.get("forma") or row.get("forma_pagamento") or ""))
        val = parse_valor_moeda_br(row.get("valor"))
        if val is None or val <= 0:
            continue
        merged[fn] += val
    if not merged:
        return None, "Nenhum valor válido nas formas de pagamento."
    soma = sum(merged.values(), Decimal("0")).quantize(Decimal("0.01"))
    tot = _dec(total_venda).quantize(Decimal("0.01"))
    if abs(soma - tot) > Decimal("0.02"):
        return (
            None,
            f"A soma devolvida (R$ {soma}) deve ser igual ao total da venda (R$ {tot}).",
        )
    out = [
        {"forma": fn, "valor": float(v.quantize(Decimal("0.01")))}
        for fn, v in merged.items()
    ]
    return out, None
