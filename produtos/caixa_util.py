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


def vincular_sessao_caixa_browser(request, sessao) -> None:
    request.session["pdv_sessao_caixa_id"] = int(sessao.pk)
    request.session.modified = True


def qtd_caixas_abertos() -> int:
    from produtos.models import SessaoCaixa

    return SessaoCaixa.objects.filter(fechado_em__isnull=True).count()


def obter_caixa_pai_aberto():
    """
    Caixa pai operacional da loja: primeiro turno aberto do dia/fluxo atual.
    """
    from produtos.models import SessaoCaixa

    return (
        SessaoCaixa.objects.filter(fechado_em__isnull=True)
        .select_related("usuario")
        .order_by("aberto_em")
        .first()
    )


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


def ultimo_fechamento_sugestao_abertura() -> dict[str, Any] | None:
    """Último caixa fechado: dinheiro contado na gaveta (sugestão de fundo na próxima abertura)."""
    from produtos.models import SessaoCaixa

    s = (
        SessaoCaixa.objects.filter(fechado_em__isnull=False)
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
