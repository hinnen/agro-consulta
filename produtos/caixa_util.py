"""
Resumo de caixa por forma de pagamento, movimentos (reforço/retirada) e conferência no fechamento.
"""
from __future__ import annotations

import re
from collections import defaultdict
from decimal import Decimal
from typing import Any

from django.utils import timezone

FORMAS_PAGAMENTO_CAIXA: tuple[str, ...] = (
    "Dinheiro",
    "PIX",
    "Cartão de débito",
    "Cartão de crédito",
    "Cartão de crédito parcelado",
    "Fiado",
    "Vale crédito",
    "Cashback",
    "Outro",
)

CEDULAS_DENOMINACOES_CAIXA: tuple[dict[str, str], ...] = (
    {"valor": "200", "label": "R$ 200", "img": "produtos/img/cedulas/nota_200.png", "tipo": "nota"},
    {"valor": "100", "label": "R$ 100", "img": "produtos/img/cedulas/nota_100.png", "tipo": "nota"},
    {"valor": "50", "label": "R$ 50", "img": "produtos/img/cedulas/nota_50.png", "tipo": "nota"},
    {"valor": "20", "label": "R$ 20", "img": "produtos/img/cedulas/nota_20.png", "tipo": "nota"},
    {"valor": "10", "label": "R$ 10", "img": "produtos/img/cedulas/nota_10.png", "tipo": "nota"},
    {"valor": "5", "label": "R$ 5", "img": "produtos/img/cedulas/nota_5.png", "tipo": "nota"},
    {"valor": "2", "label": "R$ 2", "img": "produtos/img/cedulas/nota_2.png", "tipo": "nota"},
    {"valor": "1", "label": "R$ 1", "img": "produtos/img/cedulas/moeda_1.png", "tipo": "moeda"},
    {"valor": "0.50", "label": "R$ 0,50", "img": "produtos/img/cedulas/moeda_050.png", "tipo": "moeda"},
    {"valor": "0.25", "label": "R$ 0,25", "img": "produtos/img/cedulas/moeda_025.png", "tipo": "moeda"},
    {"valor": "0.10", "label": "R$ 0,10", "img": "produtos/img/cedulas/moeda_010.png", "tipo": "moeda"},
    {"valor": "0.05", "label": "R$ 0,05", "img": "produtos/img/cedulas/moeda_005.png", "tipo": "moeda"},
)

_FORMA_ALIASES = {
    "dinheiro": "Dinheiro",
    "pix": "PIX",
    "cartao de debito": "Cartão de débito",
    "cartão de débito": "Cartão de débito",
    "cartão débito": "Cartão de débito",
    "cartao de credito": "Cartão de crédito",
    "cartão de crédito": "Cartão de crédito",
    "cartão crédito": "Cartão de crédito",
    "cartão credíto": "Cartão de crédito",
    "cartao credito": "Cartão de crédito",
    "cartao crédito": "Cartão de crédito",
    "cartao credíto": "Cartão de crédito",
    "credito parcelado": "Cartão de crédito parcelado",
    "crédito parcelado": "Cartão de crédito parcelado",
    "cartão de crédito parcelado": "Cartão de crédito parcelado",
    "cartao de credito parcelado": "Cartão de crédito parcelado",
    "fiado": "Fiado",
    "crédito loja": "Fiado",
    "credito loja": "Fiado",
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
    low_full = txt.lower()
    if "pix" in low_full or (re.search(r"mercado\s+pago", low_full) and "qr" in low_full):
        return "PIX"
    base = re.sub(r"\s+\d+x\s*$", "", txt, flags=re.IGNORECASE).strip()
    base = re.sub(r"\s*Mercado Pago.*$", "", base, flags=re.IGNORECASE).strip()
    base = re.sub(r"\s*Sicredi.*$", "", base, flags=re.IGNORECASE).strip()
    base = re.sub(r"\s*Sicoob.*$", "", base, flags=re.IGNORECASE).strip()
    key = base.lower()
    if key in _FORMA_ALIASES:
        return _FORMA_ALIASES[key]
    try:
        from produtos.fiado_credito_util import forma_pagamento_erp_fiado_label

        if key == forma_pagamento_erp_fiado_label().lower():
            return "Fiado"
    except Exception:
        pass
    for canon in FORMAS_PAGAMENTO_CAIXA:
        if canon.lower() == key or key.startswith(canon.lower()):
            return canon
    return base[:80] if base else "Outro"


def _forma_e_valor_pagamento_row(row: dict) -> tuple[str, Decimal] | None:
    """Lê forma e valor de uma linha de pagamentos_json (PDV ou venda salva)."""
    if not isinstance(row, dict):
        return None
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
        return None
    return fn, vp


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
            parsed = _forma_e_valor_pagamento_row(row)
            if not parsed:
                continue
            fn, vp = parsed
            totais[fn] += vp
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


def eh_movimento_retirada_devolucao(obs: str) -> bool:
    """Retirada gerada por devolução de venda (obs «Devolução venda #…»)."""
    o = (obs or "").strip().lower()
    return o.startswith("devolução venda") or o.startswith("devolucao venda")


def _pk_venda_devolucao_obs(obs: str) -> int | None:
    m = re.search(r"#(\d+)", str(obs or ""))
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def _movimentos_retirada_devolucao_duplicados_turno(sessao, movimentos) -> set[int]:
    """
    IDs de retirada de devolução que não devem subtrair do esperado: a venda devolvida
    já saiu da soma de vendas do mesmo turno (evita FL-017 — desconto em dobro).
    Devolução de venda de outro turno continua contando só pela retirada.
    """
    sid = getattr(sessao, "pk", None)
    if sid is None:
        return set()
    candidatos: list[tuple[int, int]] = []
    for m in movimentos:
        if getattr(m, "tipo", None) != "retirada":
            continue
        if not eh_movimento_retirada_devolucao(getattr(m, "observacao", None)):
            continue
        vp = _pk_venda_devolucao_obs(getattr(m, "observacao", None))
        mpk = getattr(m, "pk", None)
        if vp is not None and mpk is not None:
            candidatos.append((int(mpk), vp))
    if not candidatos:
        return set()
    from produtos.models import VendaAgro

    venda_pks = {vp for _, vp in candidatos}
    mesma_sessao = set(
        VendaAgro.objects.filter(pk__in=venda_pks, sessao_caixa_id=sid).values_list("pk", flat=True)
    )
    return {mpk for mpk, vp in candidatos if vp in mesma_sessao}


def _agregar_resumo_turno_sessao(sessao) -> tuple[dict[str, Decimal], dict[str, Decimal], dict[str, Decimal], dict[str, Decimal]]:
    """Uma passagem no turno: esperado, vendas, reforços e retiradas por forma."""
    esperado: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    vendas_por: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    reforco_por: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    retirada_por: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    esperado["Dinheiro"] += _dec(getattr(sessao, "valor_abertura", 0))

    vendas_rel = getattr(sessao, "vendas", None)
    if vendas_rel is not None:
        for v in vendas_rel.all():
            if getattr(v, "devolvida_em", None):
                continue
            for fn, val in pagamentos_por_forma_venda(v).items():
                vendas_por[fn] += val
                esperado[fn] += val

    movimentos = getattr(sessao, "movimentos", None)
    if movimentos is not None:
        mov_list = list(movimentos.all())
        retiradas_dev_dup = _movimentos_retirada_devolucao_duplicados_turno(sessao, mov_list)
        for m in mov_list:
            fn = normalizar_forma_pagamento_caixa(m.forma_pagamento)
            val = _dec(m.valor)
            if m.tipo == "reforco":
                reforco_por[fn] += val
                esperado[fn] += val
            elif m.tipo == "retirada":
                retirada_por[fn] += val
                if getattr(m, "pk", None) not in retiradas_dev_dup:
                    esperado[fn] -= val

    q = lambda d: {k: v.quantize(Decimal("0.01")) for k, v in d.items()}
    esperado_out = {
        k: v.quantize(Decimal("0.01"))
        for k, v in esperado.items()
        if v != 0 or k in FORMAS_PAGAMENTO_CAIXA
    }
    return esperado_out, q(vendas_por), q(reforco_por), q(retirada_por)


def resumo_esperado_por_forma(sessao) -> dict[str, Decimal]:
    """Esperado no turno: abertura (Dinheiro) + vendas + reforços − retiradas."""
    esperado, _, _, _ = _agregar_resumo_turno_sessao(sessao)
    return esperado


def linhas_resumo_caixa(sessao) -> list[dict[str, Any]]:
    """Lista ordenada para tela: forma, esperado, vendas, reforços, retiradas."""
    esperado, vendas_por, reforco_por, retirada_por = _agregar_resumo_turno_sessao(sessao)
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


def usuario_label_sessao_caixa(sessao) -> str:
    if not getattr(sessao, "usuario_id", None):
        return "—"
    u = sessao.usuario
    return (u.get_full_name() or "").strip() or u.get_username() or f"#{u.pk}"


def fmt_linhas_caixa_template(linhas) -> list[dict[str, str]]:
    return [
        {
            "forma": L["forma"],
            "esperado": str(L["esperado"]),
            "vendas": str(L["vendas"]),
            "reforcos": str(L["reforcos"]),
            "retiradas": str(L["retiradas"]),
            "abertura_dinheiro": str(L["abertura_dinheiro"]),
        }
        for L in linhas
    ]


def linha_conferencia_tem_movimento(linha: dict) -> bool:
    for k in ("esperado", "vendas", "reforcos", "retiradas", "abertura_dinheiro"):
        try:
            if _dec(linha[k]) != 0:
                return True
        except Exception:
            pass
    return False


def serializar_estado_conferencia_fechar(sessoes) -> dict[str, Any]:
    """JSON para tela Fechar caixa (valores esperados após reforço/retirada)."""
    linhas_todos_raw = linhas_conferencia_agregada(sessoes, todas_formas=True)
    tot_esperado_din = Decimal("0")
    for L in linhas_todos_raw:
        if L["forma"] == "Dinheiro":
            tot_esperado_din = _dec(L["esperado"])
            break
    all_linhas = fmt_linhas_caixa_template(linhas_todos_raw)
    linhas: list[dict[str, Any]] = []
    for i, L in enumerate(all_linhas):
        row = dict(L)
        row["idx"] = i
        row["com_movimento"] = linha_conferencia_tem_movimento(L)
        linhas.append(row)
    cards: list[dict[str, Any]] = []
    for c in montar_cards_caixas_abertos(sessoes):
        cards.append(
            {
                "sessao_id": c["sessao"].pk,
                "usuario": c["usuario"],
                "qtd_vendas": c["qtd_vendas"],
                "total_vendas": c["total_vendas"],
                "esperado_dinheiro": c["esperado_dinheiro"],
                "linhas": c["linhas"],
            }
        )
    return {
        "qtd_caixas": len(sessoes),
        "tot_esperado_dinheiro": str(tot_esperado_din.quantize(Decimal("0.01"))),
        "linhas": linhas,
        "cards": cards,
    }


def montar_cards_caixas_abertos(sessoes) -> list[dict[str, Any]]:
    """Resumo por sessão aberta (painel «todos» e fechamento individual)."""
    cards: list[dict[str, Any]] = []
    for s in sessoes:
        vendas = s.vendas.all()
        qtd = len(vendas)
        tot = sum((_dec(v.total) for v in vendas), Decimal("0")).quantize(Decimal("0.01"))
        linhas_sess = linhas_conferencia_fechar(s)
        esp_din = Decimal("0")
        for L in linhas_sess:
            if L["forma"] == "Dinheiro":
                esp_din = _dec(L["esperado"])
                break
        cards.append(
            {
                "sessao": s,
                "ponto_caixa": getattr(s, "ponto_caixa", PONTO_CAIXA_GAVETA),
                "ponto_label": rotulo_ponto_caixa(getattr(s, "ponto_caixa", PONTO_CAIXA_GAVETA)),
                "usuario": usuario_label_sessao_caixa(s),
                "qtd_vendas": qtd,
                "total_vendas": str(tot),
                "esperado_dinheiro": str(esp_din.quantize(Decimal("0.01"))),
                "linhas": fmt_linhas_caixa_template(linhas_sess),
            }
        )
    return cards


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
    Nunca mistura Caixa Teste com Gaveta/Notebook: adoção respeita o ponto do navegador ou só
    um turno aberto no sistema.
    """
    from produtos.models import SessaoCaixa

    atual = obter_sessao_caixa_aberta_request(request)
    if atual:
        return atual
    ponto_nav = ponto_operacao_browser(request)
    qs = SessaoCaixa.objects.filter(fechado_em__isnull=True).order_by("-aberto_em")
    if ponto_nav == PONTO_CAIXA_TESTE:
        qs = qs.filter(ponto_caixa=PONTO_CAIXA_TESTE)
    elif ponto_nav in (PONTO_CAIXA_GAVETA, PONTO_CAIXA_NOTEBOOK):
        qs = qs.filter(ponto_caixa=PONTO_CAIXA_GAVETA)
    else:
        qs_op = qs.filter(ponto_caixa=PONTO_CAIXA_GAVETA)
        qs_te = qs.filter(ponto_caixa=PONTO_CAIXA_TESTE)
        if qs_op.count() == 1 and qs_te.count() == 0:
            qs = qs_op
        elif qs_te.count() == 1 and qs_op.count() == 0:
            qs = qs_te
        elif qs.count() != 1:
            return None
    usuario = getattr(request, "user", None)
    if usuario is not None and getattr(usuario, "is_authenticated", False):
        su = qs.filter(usuario=usuario).first()
        if su:
            ponto = getattr(su, "ponto_caixa", PONTO_CAIXA_GAVETA)
            if ponto_nav == PONTO_CAIXA_NOTEBOOK and sessao_caixa_e_operacional(su):
                definir_ponto_operacao_browser(request, PONTO_CAIXA_NOTEBOOK, su.pk)
            else:
                definir_ponto_operacao_browser(request, ponto, su.pk)
            return su
    if qs.count() == 1:
        s = qs.first()
        if s:
            ponto = getattr(s, "ponto_caixa", PONTO_CAIXA_GAVETA)
            if ponto_nav == PONTO_CAIXA_NOTEBOOK and sessao_caixa_e_operacional(s):
                definir_ponto_operacao_browser(request, PONTO_CAIXA_NOTEBOOK, s.pk)
            else:
                definir_ponto_operacao_browser(request, ponto, s.pk)
            return s
    return None


MSG_CAIXA_FECHADO_VENDA = "Abra o caixa antes de registrar vendas."
MSG_CAIXA_PIN_ALHEIO = "Informe seu PIN para gerenciar outro caixa."


def validar_pin_operador(pin: str) -> tuple[bool, str]:
    """PIN de operador (``PerfilUsuario.senha_rapida``), mesmo critério do estoque / empréstimo."""
    from base.models import PerfilUsuario

    pin = (pin or "").strip()
    if not pin:
        return False, "Informe o PIN."
    if pin == "1234":
        return False, "Senha padrão (1234) bloqueada. Troque seu PIN."
    if not PerfilUsuario.objects.filter(senha_rapida=pin).exists():
        return False, "PIN incorreto."
    return True, ""


def rotulo_operador_pin(pin: str) -> str:
    from base.models import PerfilUsuario

    pin = (pin or "").strip()
    if not pin or pin == "1234":
        return ""
    perfil = (
        PerfilUsuario.objects.filter(senha_rapida=pin)
        .select_related("user")
        .first()
    )
    if not perfil:
        return ""
    u = perfil.user
    return (u.get_full_name() or u.first_name or u.username or perfil.codigo_vendedor or "").strip()


def usuario_django_de_pin(pin: str):
    from base.models import PerfilUsuario

    pin = (pin or "").strip()
    if not pin:
        return None
    perfil = PerfilUsuario.objects.filter(senha_rapida=pin).select_related("user").first()
    return perfil.user if perfil else None


def id_sessao_caixa_browser(request) -> int:
    try:
        return int(request.session.get("pdv_sessao_caixa_id") or 0)
    except (TypeError, ValueError):
        return 0


def sessao_caixa_e_do_browser(request, sessao) -> bool:
    if not sessao:
        return False
    return int(sessao.pk) == id_sessao_caixa_browser(request)


def obter_sessao_caixa_aberta_por_id(sessao_id) -> Any | None:
    from produtos.models import SessaoCaixa

    try:
        sid = int(sessao_id)
    except (TypeError, ValueError):
        return None
    if sid <= 0:
        return None
    return SessaoCaixa.objects.filter(pk=sid, fechado_em__isnull=True).first()


def vincular_sessao_caixa_browser(request, sessao, *, ponto: str | None = None) -> None:
    p = normalizar_ponto_caixa(ponto or getattr(sessao, "ponto_caixa", PONTO_CAIXA_GAVETA))
    definir_ponto_operacao_browser(request, p, int(sessao.pk))


def qtd_caixas_abertos() -> int:
    from produtos.models import SessaoCaixa

    return SessaoCaixa.objects.filter(fechado_em__isnull=True).count()


PONTO_CAIXA_GAVETA = "gaveta"
PONTO_CAIXA_NOTEBOOK = "notebook"
PONTO_CAIXA_TESTE = "teste"

PONTOS_CAIXA_ABERTURA: tuple[tuple[str, str], ...] = (
    (PONTO_CAIXA_GAVETA, "Caixa Gaveta"),
    (PONTO_CAIXA_NOTEBOOK, "Caixa Notebook"),
    (PONTO_CAIXA_TESTE, "Caixa Teste"),
)

SESSION_PONTO_OPERACAO_KEY = "pdv_ponto_operacao"


def normalizar_ponto_caixa(valor: str | None) -> str:
    v = (valor or "").strip().lower()
    if v in (PONTO_CAIXA_GAVETA, PONTO_CAIXA_NOTEBOOK, PONTO_CAIXA_TESTE):
        return v
    return PONTO_CAIXA_GAVETA


def rotulo_ponto_caixa(ponto: str | None) -> str:
    for cod, rot in PONTOS_CAIXA_ABERTURA:
        if cod == (ponto or "").strip().lower():
            return rot
    return "Caixa"


def rotulo_sessao_caixa(sessao, *, com_turno: bool = True) -> str:
    """
    Rótulo legível da sessão: ponto fixo (Gaveta / Teste) + id do turno.
    O número (#11, #12…) é a abertura/fechamento daquele dia, não outro caixa físico.
    """
    if isinstance(sessao, dict):
        ponto = sessao.get("ponto_caixa")
        pk = sessao.get("pk")
    else:
        ponto = getattr(sessao, "ponto_caixa", PONTO_CAIXA_GAVETA)
        pk = getattr(sessao, "pk", None)
    rot = rotulo_ponto_caixa(ponto)
    if com_turno and pk is not None:
        return f"{rot} · turno #{pk}"
    return rot


def quando_sessao_caixa_local(dt) -> str:
    if not dt:
        return ""
    try:
        return timezone.localtime(dt).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""


def formatar_opcao_sessao_caixa(row: dict) -> str:
    """Texto do filtro «Caixa» em relatórios (ponto + data + turno)."""
    rot = rotulo_ponto_caixa(row.get("ponto_caixa"))
    pk = row.get("pk")
    when = row.get("fechado_em") or row.get("aberto_em")
    txt = quando_sessao_caixa_local(when)
    partes = [rot]
    if txt:
        partes.append(txt)
    if pk is not None:
        partes.append(f"#{pk}")
    return " · ".join(partes)


def obter_caixa_gaveta_aberto():
    """Turno principal da loja (gaveta), obrigatório antes do notebook."""
    from produtos.models import SessaoCaixa

    return (
        SessaoCaixa.objects.filter(
            fechado_em__isnull=True,
            ponto_caixa=PONTO_CAIXA_GAVETA,
        )
        .select_related("usuario")
        .order_by("aberto_em")
        .first()
    )


def obter_caixa_teste_aberto():
    from produtos.models import SessaoCaixa

    return (
        SessaoCaixa.objects.filter(
            fechado_em__isnull=True,
            ponto_caixa=PONTO_CAIXA_TESTE,
        )
        .select_related("usuario")
        .order_by("aberto_em")
        .first()
    )


def sessao_caixa_e_operacional(sessao) -> bool:
    """Turno da loja (gaveta; notebook usa o PK da gaveta no navegador)."""
    return normalizar_ponto_caixa(getattr(sessao, "ponto_caixa", None)) != PONTO_CAIXA_TESTE


def sessao_caixa_e_teste(sessao) -> bool:
    return normalizar_ponto_caixa(getattr(sessao, "ponto_caixa", None)) == PONTO_CAIXA_TESTE


def filtrar_sessoes_operacional(sessoes) -> list:
    return [s for s in sessoes if sessao_caixa_e_operacional(s)]


def filtrar_sessoes_teste(sessoes) -> list:
    return [s for s in sessoes if sessao_caixa_e_teste(s)]


def qtd_caixas_operacional_abertos() -> int:
    from produtos.models import SessaoCaixa

    return SessaoCaixa.objects.filter(
        fechado_em__isnull=True, ponto_caixa=PONTO_CAIXA_GAVETA
    ).count()


def qtd_caixas_teste_abertos() -> int:
    from produtos.models import SessaoCaixa

    return SessaoCaixa.objects.filter(
        fechado_em__isnull=True, ponto_caixa=PONTO_CAIXA_TESTE
    ).count()


def obter_caixa_pai_aberto():
    """Caixa principal operacional (Caixa Gaveta aberto)."""
    return obter_caixa_gaveta_aberto()


def ponto_operacao_browser(request) -> str:
    try:
        return normalizar_ponto_caixa(request.session.get(SESSION_PONTO_OPERACAO_KEY))
    except Exception:
        return PONTO_CAIXA_GAVETA


def definir_ponto_operacao_browser(request, ponto: str, sessao_id: int | None = None) -> None:
    request.session[SESSION_PONTO_OPERACAO_KEY] = normalizar_ponto_caixa(ponto)
    if sessao_id:
        request.session["pdv_sessao_caixa_id"] = int(sessao_id)
    request.session.modified = True


def limpar_ponto_operacao_browser(request) -> None:
    if SESSION_PONTO_OPERACAO_KEY in request.session:
        del request.session[SESSION_PONTO_OPERACAO_KEY]
        request.session.modified = True


def rotulo_caixa_browser(request, sessao=None) -> str:
    from produtos.models import SessaoCaixa

    if sessao is None:
        sessao = obter_sessao_caixa_aberta_request(request)
    if not sessao:
        return "Caixa fechado"
    ponto_nav = ponto_operacao_browser(request)
    if ponto_nav == PONTO_CAIXA_NOTEBOOK:
        gaveta = sessao
        if isinstance(sessao, SessaoCaixa) and sessao.ponto_caixa != PONTO_CAIXA_GAVETA:
            gaveta = obter_caixa_gaveta_aberto() or sessao
        pk = getattr(gaveta, "pk", sessao.pk)
        return f"Caixa Notebook · Gaveta #{pk}"
    rotulo = rotulo_ponto_caixa(getattr(sessao, "ponto_caixa", None) or ponto_nav)
    return f"{rotulo} #{sessao.pk}"


def resolver_sessao_caixa_operacao(
    request, data: dict | None = None, *, permitir_adotar_unico: bool = True
) -> tuple[Any | None, str | None, int]:
    """
  Sessão para movimentos no caixa: turno deste navegador ou outro turno aberto com PIN.
  Retorna (sessao, mensagem_erro, status_http).
    """
    data = data if isinstance(data, dict) else {}
    pin = str(data.get("pin") or "").strip()
    raw_sid = data.get("sessao_caixa_id") or data.get("sessaoCaixaId")

    local = obter_sessao_caixa_aberta_request(request)
    sid = 0
    if raw_sid is not None and str(raw_sid).strip() != "":
        try:
            sid = int(raw_sid)
        except (TypeError, ValueError):
            sid = 0

    if sid > 0:
        alvo = obter_sessao_caixa_aberta_por_id(sid)
        if not alvo:
            return None, "Caixa não encontrado ou já fechado.", 400
        if local and int(local.pk) == sid:
            return local, None, 200
        ok, err = validar_pin_operador(pin)
        if not ok:
            return None, err or MSG_CAIXA_PIN_ALHEIO, 403
        return alvo, None, 200

    if local:
        return local, None, 200
    if permitir_adotar_unico:
        adotado = adotar_sessao_caixa_unica_aberta(request)
        if adotado:
            return adotado, None, 200
    return None, "Nenhum caixa aberto neste navegador.", 400


def exigir_pin_gerir_caixa(request, sessao, pin: str) -> tuple[bool, str]:
    """Exige PIN quando a sessão não é a vinculada a este navegador."""
    if sessao_caixa_e_do_browser(request, sessao):
        return True, ""
    return validar_pin_operador(pin)


def rotulo_usuario_registro_venda(request, data: dict | None = None) -> str:
    """
    Rótulo do vendedor/operador na venda Agro: operador do PDV (descanso/PIN),
    não o login Django (ex.: admin).
    """
    data = data if isinstance(data, dict) else {}
    for key in ("operador_pdv", "operador", "operador_nome", "vendedor"):
        val = str(data.get(key) or "").strip()
        if val:
            return val[:150]
    pin = str(data.get("pin") or data.get("pin_operador") or "").strip()
    if pin:
        rot = rotulo_operador_pin(pin)
        if rot:
            return rot[:150]
    try:
        sess_op = str(request.session.get("pdv_operador_nome") or "").strip()
    except Exception:
        sess_op = ""
    if sess_op:
        return sess_op[:150]
    u = getattr(request, "user", None)
    if u is not None and getattr(u, "is_authenticated", False):
        nome = (u.get_full_name() or u.first_name or "").strip()
        if nome:
            return nome[:150]
        un = (u.get_username() if hasattr(u, "get_username") else str(u.pk)).strip()
        if un and un.lower() not in ("admin", "administrator", "root"):
            return un[:150]
    return ""


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


def extrair_linhas_conferencia_sessao(sessao) -> list[dict[str, Any]]:
    """Esperado / contado / diferença por forma a partir do JSON de fechamento."""
    conf = getattr(sessao, "conferencia_fechamento", None)
    if not isinstance(conf, dict):
        conf = {}
    lote = conf.get("_lote")
    src: dict = lote if isinstance(lote, dict) else conf
    linhas: list[dict[str, Any]] = []
    for fn, row in src.items():
        if not isinstance(row, dict) or str(fn).startswith("_"):
            continue
        forma = str(fn).strip() or "Outro"
        esp = _dec(row.get("esperado"))
        raw_cont = str(row.get("contado") or "").strip()
        cont = _dec(raw_cont) if raw_cont else None
        raw_diff = str(row.get("diferenca") or "").strip()
        if raw_diff:
            dif = _dec(raw_diff)
        elif cont is not None:
            dif = (cont - esp).quantize(Decimal("0.01"))
        else:
            dif = None
        if esp == 0 and cont is None and dif is None:
            continue
        linhas.append(
            {
                "forma": forma,
                "esperado": esp,
                "contado": cont,
                "diferenca": dif,
                "esperado_str": str(esp.quantize(Decimal("0.01"))),
                "contado_str": str(cont.quantize(Decimal("0.01"))) if cont is not None else "",
                "diferenca_str": str(dif.quantize(Decimal("0.01"))) if dif is not None else "",
            }
        )
    if not linhas and getattr(sessao, "valor_fechamento", None) is not None:
        vf = _dec(sessao.valor_fechamento)
        linhas.append(
            {
                "forma": "Dinheiro",
                "esperado": Decimal("0"),
                "contado": vf,
                "diferenca": None,
                "esperado_str": "",
                "contado_str": str(vf.quantize(Decimal("0.01"))),
                "diferenca_str": "",
            }
        )
    return linhas


def ultimo_fechamento_sugestao_abertura(
    *, ponto: str | None = PONTO_CAIXA_GAVETA
) -> dict[str, Any] | None:
    """Último caixa fechado do ponto: dinheiro contado (sugestão de fundo na próxima abertura)."""
    from produtos.models import SessaoCaixa

    p = normalizar_ponto_caixa(ponto)
    s = (
        SessaoCaixa.objects.filter(fechado_em__isnull=False, ponto_caixa=p)
        .select_related("usuario")
        .order_by("-fechado_em")
        .first()
    )
    if not s:
        return None
    dinheiro: Decimal | None = None
    if s.valor_fechamento is not None:
        dinheiro = _dec(s.valor_fechamento)
    if dinheiro is None:
        for L in extrair_linhas_conferencia_sessao(s):
            if L["forma"] == "Dinheiro" and L.get("contado") is not None:
                dinheiro = L["contado"]
                break
    if dinheiro is None:
        return None
    dinheiro = dinheiro.quantize(Decimal("0.01"))
    return {
        "sessao_pk": s.pk,
        "fechado_em": s.fechado_em,
        "usuario": usuario_label_sessao_caixa(s),
        "dinheiro_contado": str(dinheiro),
        "dinheiro_contado_br": format_moeda_br(dinheiro),
    }


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


def listar_fiado_vendas_conferencia_caixa(sessoes) -> list[dict[str, Any]]:
    """Vendas fiado do turno (uma linha por pedido) para conferência no fechamento."""
    from produtos.fiado_credito_util import valor_fiado_venda_local, venda_local_tem_fiado
    from produtos.fiado_gestao_util import criar_titulos_de_venda
    from produtos.models import FiadoTituloAgro, VendaAgro

    ids = [int(s.pk) for s in sessoes if getattr(s, "pk", None)]
    if not ids:
        return []
    vendas_qs = (
        VendaAgro.objects.filter(
            sessao_caixa_id__in=ids,
            devolvida_em__isnull=True,
        )
        .select_related("sessao_caixa")
        .order_by("criado_em", "pk")
    )
    out: list[dict[str, Any]] = []
    vistos: set[int] = set()
    for venda in vendas_qs:
        if not venda_local_tem_fiado(venda):
            continue
        valor = valor_fiado_venda_local(venda)
        if valor <= 0 or venda.pk in vistos:
            continue
        try:
            criar_titulos_de_venda(venda)
        except Exception:
            pass
        vistos.add(venda.pk)
        titulo = (
            FiadoTituloAgro.objects.filter(
                venda_agro=venda,
                origem=FiadoTituloAgro.Origem.PDV,
            )
            .order_by("pk")
            .first()
        )
        nome = (
            (titulo.cliente_nome if titulo else "")
            or getattr(venda, "cliente_nome", "")
            or ""
        ).strip() or "Cliente"
        sessao = getattr(venda, "sessao_caixa", None)
        out.append(
            {
                "id": venda.pk,
                "cliente_nome": nome,
                "valor": str(valor.quantize(Decimal("0.01"))),
                "sessao_label": rotulo_sessao_caixa(sessao) if sessao else "—",
                "operacional": sessao_caixa_e_operacional(sessao) if sessao else True,
            }
        )
    titulos = (
        FiadoTituloAgro.objects.filter(
            origem=FiadoTituloAgro.Origem.PDV,
            venda_agro__sessao_caixa_id__in=ids,
            venda_agro__devolvida_em__isnull=True,
        )
        .select_related("venda_agro", "venda_agro__sessao_caixa")
        .order_by("criado_em", "pk")
    )
    for titulo in titulos:
        venda = titulo.venda_agro
        if not venda or venda.pk in vistos:
            continue
        valor = valor_fiado_venda_local(venda)
        if valor <= 0:
            valor = _dec(titulo.valor_bruto)
        if valor <= 0:
            continue
        vistos.add(venda.pk)
        sessao = getattr(venda, "sessao_caixa", None)
        nome = (titulo.cliente_nome or venda.cliente_nome or "Cliente").strip() or "Cliente"
        out.append(
            {
                "id": venda.pk,
                "cliente_nome": nome,
                "valor": str(valor.quantize(Decimal("0.01"))),
                "sessao_label": rotulo_sessao_caixa(sessao) if sessao else "—",
                "operacional": sessao_caixa_e_operacional(sessao) if sessao else True,
            }
        )
    return out


def listar_fiado_baixas_conferencia_caixa(sessoes) -> list[dict[str, Any]]:
    """Baixas de fiado recebidas no turno — conferir retirada da nota da caixa de fiados."""
    from django.db.models import Q

    from produtos.models import FiadoBaixaAgro

    ids = [int(s.pk) for s in sessoes if getattr(s, "pk", None)]
    if not ids:
        return []
    baixas = (
        FiadoBaixaAgro.objects.filter(
            Q(sessao_caixa_id__in=ids) | Q(movimento_caixa__sessao_caixa_id__in=ids)
        )
        .select_related("titulo", "sessao_caixa", "movimento_caixa__sessao_caixa")
        .order_by("criado_em", "pk")
    )
    out: list[dict[str, Any]] = []
    for baixa in baixas:
        nome = ""
        if baixa.titulo_id and baixa.titulo:
            nome = (baixa.titulo.cliente_nome or "").strip()
        sessao = baixa.sessao_caixa
        if not sessao and baixa.movimento_caixa_id and baixa.movimento_caixa:
            sessao = baixa.movimento_caixa.sessao_caixa
        out.append(
            {
                "id": baixa.pk,
                "cliente_nome": nome or "Cliente",
                "valor": str(_dec(baixa.valor)),
                "sessao_label": rotulo_sessao_caixa(sessao) if sessao else "—",
                "operacional": sessao_caixa_e_operacional(sessao) if sessao else True,
            }
        )
    return out


def fiado_conferencia_operacional(
    vendas: list[dict[str, Any]],
    baixas: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Itens obrigatórios no fechamento da loja (exclui Caixa Teste)."""
    v = [row for row in vendas if row.get("operacional", True)]
    b = [row for row in baixas if row.get("operacional", True)]
    return v, b


def validar_conferencia_fiado_caixa(
    post,
    vendas_fiado: list[dict[str, Any]],
    baixas_fiado: list[dict[str, Any]],
) -> str | None:
    pulou_vendas = (post.get("fiado_vendas_pulado") or "").strip() == "1"
    pulou_baixas = (post.get("fiado_baixas_pulado") or "").strip() == "1"

    if vendas_fiado and not pulou_vendas:
        for row in vendas_fiado:
            key = f"fiado_assinado_{row['id']}"
            if not (post.get(key) or "").strip():
                return (
                    f"Marque que a nota fiado de {row['cliente_nome']} "
                    f"(R$ {row['valor']}) está assinada e guardada."
                )
    elif vendas_fiado and pulou_vendas:
        pin = (post.get("fiado_vendas_pulo_pin") or post.get("pin") or "").strip()
        ok, err = validar_pin_operador(pin)
        if not ok:
            return err or "Informe PIN válido para pular a conferência de vendas fiado."

    if baixas_fiado and not pulou_baixas:
        for row in baixas_fiado:
            key = f"fiado_retirado_{row['id']}"
            if not (post.get(key) or "").strip():
                return (
                    f"Marque que a nota paga de {row['cliente_nome']} "
                    f"(R$ {row['valor']}) foi retirada da caixa de fiados."
                )
    elif baixas_fiado and pulou_baixas:
        pin = (post.get("fiado_baixas_pulo_pin") or post.get("pin") or "").strip()
        ok, err = validar_pin_operador(pin)
        if not ok:
            return err or "Informe PIN válido para pular a conferência de pagamentos fiado."
    return None
