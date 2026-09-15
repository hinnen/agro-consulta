# -*- coding: utf-8 -*-
"""Prova detalhada — PDV-ENT-LOJA-LANC (estoque/caixa no lançamento da entrega).

  set AGRO_PIN_TESTE=9973
  python scripts/verify_pdv_ent_loja_lanc_path.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _resolve_dep_venda_simulada(data: dict, sessao_ponto: str | None, browser_dep: str) -> str:
    """Espelha a regra de views.py (dep_explicito não sobrescrito pelo caixa)."""
    from produtos.caixa_util import deposito_de_ponto_caixa
    from produtos.pdv_deposito_util import deposito_de_loja_id, normalizar_deposito

    dep_payload = (
        data.get("deposito")
        or data.get("pdv_deposito")
        or data.get("loja_entrega")
        or data.get("loja_id")
    )
    dep_explicito = False
    if dep_payload is not None and str(dep_payload).strip() != "":
        raw_dep = str(dep_payload).strip().lower()
        if raw_dep in ("1", "2"):
            dep_v = deposito_de_loja_id(raw_dep)
        else:
            dep_v = normalizar_deposito(raw_dep)
        dep_explicito = dep_v in ("centro", "vila")
    else:
        dep_v = normalizar_deposito(browser_dep)
    if not dep_explicito and sessao_ponto is not None:
        dep_sess = deposito_de_ponto_caixa(sessao_ponto)
        if dep_sess in ("centro", "vila"):
            dep_v = dep_sess
    if dep_v not in ("centro", "vila"):
        dep_v = "centro"
    return dep_v


def test_estatico() -> None:
    print("== Estático ==")
    js = _read("produtos/static/produtos/js/pdv_wizard.js")
    views = _read("produtos/views.py")
    caixa = _read("produtos/caixa_util.py")
    util = _read("produtos/entrega_pdv_pendente_util.py")

    check("fix_dep_explicito", "dep_explicito" in views)
    check("fix_nao_sobrescreve_sempre", "if not dep_explicito and sessao is not None:" in views)
    check("fix_loja_entrega_no_dep", 'data.get("loja_entrega")' in views)
    check("fix_sessao_loja_pag", 'body.get("loja_pagamento")' in caixa)
    check("fix_sessao_pai_aberto", "obter_caixa_pai_aberto(loja_pag)" in caixa)
    check("js_injetar_ambos", "payload.loja_pagamento = lojaP" in js and "payload.loja_entrega = lojaE" in js)
    check("js_prosseguir_so_entrega_pdv", "wizardIrParaPagamentoComImpressao()" in js)
    check(
        "js_prosseguir_pag_outra_painel",
        "lojaPagamentoEntregaAtual(state) !== depositoPdvAtivo()" in js
        and "wizardEnviarEntregaPainel()" in js,
    )
    # Regressão: não voltar a mandar SEMPRE pro painel só porque saída ≠ PDV
    idx = js.find("function tryProsseguirEntregaStep")
    body = js[idx : idx + 900] if idx >= 0 else ""
    check(
        "js_nao_painel_so_por_saida",
        "entregaVaiParaOutraLoja(state) || lp === 'entrega'" not in body,
        "só-entrega outra loja segue no PDV",
    )
    check("js_painel_aceita_loja_pag_outra", "pagOutraLoja" in js and "lp === 'loja' && pagOutraLoja" in js)
    check("api_registrar_loja", 'campos["loja_entrega"]' in views and 'campos["loja_pagamento"]' in views)
    check("util_mudar_loja", "def mudar_loja_entrega_pdv" in util)


def test_runtime_deposito() -> None:
    print("== Depósito na venda (regra) ==")
    # Bug antigo: caixa Centro sobrescrevia depósito Vila
    dep = _resolve_dep_venda_simulada(
        {"deposito": "vila", "loja_entrega": "vila", "loja_pagamento": "centro"},
        sessao_ponto="gaveta",
        browser_dep="centro",
    )
    check("so_entrega_estoque_vila", dep == "vila", dep)

    dep2 = _resolve_dep_venda_simulada(
        {"deposito": "centro", "loja_entrega": "centro", "loja_pagamento": "vila"},
        sessao_ponto="vila",
        browser_dep="centro",
    )
    check("so_pag_estoque_centro", dep2 == "centro", dep2)

    dep3 = _resolve_dep_venda_simulada(
        {"deposito": "vila", "loja_entrega": "vila", "loja_pagamento": "vila"},
        sessao_ponto="vila",
        browser_dep="centro",
    )
    check("ambas_estoque_vila", dep3 == "vila", dep3)

    # Sem depósito explícito → cai no caixa
    dep4 = _resolve_dep_venda_simulada({}, sessao_ponto="vila", browser_dep="centro")
    check("sem_explicito_usa_caixa", dep4 == "vila", dep4)

    dep5 = _resolve_dep_venda_simulada({"loja_id": "2"}, sessao_ponto="gaveta", browser_dep="centro")
    check("loja_id_2_vila", dep5 == "vila", dep5)


def test_runtime_sessao_e_api() -> None:
    print("== Sessão venda + API + PIN ==")
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.caixa_util import (
        PONTO_CAIXA_GAVETA,
        PONTO_CAIXA_VILA,
        resolver_sessao_caixa_para_venda,
        rotulo_operador_pin,
        validar_pin_operador,
    )
    from produtos.entrega_pdv_pendente_util import (
        loja_pagamento_efetiva,
        resolver_sessao_caixa_entrega_pdv,
    )
    from produtos.models import PedidoEntrega, SessaoCaixa

    ok_pin, err = validar_pin_operador(PIN)
    check("pin_9973", ok_pin, (err or "")[:80])
    quem = rotulo_operador_pin(PIN) if ok_pin else "Verify"
    check("pin_rotulo", bool(quem), str(quem)[:40])

    user = get_user_model().objects.filter(is_active=True).order_by("id").first()
    check("user_ativo", user is not None)
    if not user:
        return

    gav = SessaoCaixa.objects.create(usuario=user, ponto_caixa=PONTO_CAIXA_GAVETA, valor_abertura=0)
    vil = SessaoCaixa.objects.create(usuario=user, ponto_caixa=PONTO_CAIXA_VILA, valor_abertura=0)
    criados: list[int] = []

    class ReqCentro:
        session = {"pdv_deposito": "centro", "pdv_sessao_caixa_id": gav.pk}

    ReqCentro.user = user  # type: ignore[attr-defined]

    try:
        # Só entrega Vila + pagar Centro → caixa fica no aparelho
        s = resolver_sessao_caixa_para_venda(
            ReqCentro(),
            {"loja_pagamento": "centro", "loja_entrega": "vila", "deposito": "vila"},
        )
        check(
            "venda_so_ent_caixa_centro",
            s is not None and s.pk == gav.pk,
            str(getattr(s, "ponto_caixa", None)),
        )

        # Pagamento Vila (as duas / só pag) → caixa Vila
        s2 = resolver_sessao_caixa_para_venda(
            ReqCentro(),
            {"loja_pagamento": "vila", "loja_entrega": "vila", "deposito": "vila"},
        )
        check(
            "venda_pag_vila_caixa_vila",
            s2 is not None and getattr(s2, "ponto_caixa", "") == PONTO_CAIXA_VILA,
        )

        # Só pagamento Vila + saída Centro → caixa Vila, estoque Centro (regra dep acima)
        s3 = resolver_sessao_caixa_para_venda(
            ReqCentro(),
            {"loja_pagamento": "vila", "loja_entrega": "centro", "deposito": "centro"},
        )
        check(
            "venda_so_pag_caixa_vila",
            s3 is not None and getattr(s3, "ponto_caixa", "") == PONTO_CAIXA_VILA,
        )

        # Sem caixa destino → None (não inventa). Fecha todos Vila abertos só neste teste.
        from django.utils import timezone as tz

        vila_abertas = list(
            SessaoCaixa.objects.filter(ponto_caixa=PONTO_CAIXA_VILA, fechado_em__isnull=True)
        )
        agora = tz.now()
        for s_v in vila_abertas:
            s_v.fechado_em = agora
            s_v.save(update_fields=["fechado_em"])
        try:
            s4 = resolver_sessao_caixa_para_venda(ReqCentro(), {"loja_pagamento": "vila"})
            check("venda_pag_vila_sem_caixa", s4 is None)
        finally:
            for s_v in vila_abertas:
                s_v.fechado_em = None
                s_v.save(update_fields=["fechado_em"])

        # Entrega registrar: loja_entrega + loja_pagamento gravados
        s_ent = resolver_sessao_caixa_entrega_pdv(
            ReqCentro(), {"loja_pagamento": "vila", "loja_entrega": "centro"}
        )
        check(
            "entrega_resolver_pag_vila",
            s_ent is not None and getattr(s_ent, "ponto_caixa", "") == PONTO_CAIXA_VILA,
        )

        client = Client(HTTP_HOST="127.0.0.1")
        client.force_login(user)
        url = reverse("api_entrega_registrar")
        body = {
            "cliente_nome": "Verify Loja Lanc",
            "telefone": "13999990001",
            "endereco_linha": "Rua Path 100",
            "itens": [{"nome": "Item", "qtd": 1, "preco": 12.5}],
            "total_texto": "R$ 12,50",
            "forma_pagamento": "Dinheiro",
            "troco_precisa": False,
            "aguarda_pagamento_pdv": True,
            "loja_entrega": "vila",
            "loja_pagamento": "centro",
            "origem": "pdv",
            "pin": PIN,
        }
        resp = client.post(url, data=json.dumps(body), content_type="application/json")
        try:
            data = resp.json()
        except Exception:
            data = {"_raw": (resp.content or b"")[:200].decode("utf-8", "replace")}
        check(
            "http_registrar_ok",
            resp.status_code == 200 and data.get("ok") is True,
            f"status={resp.status_code} {str(data)[:140]}",
        )
        ent_id = data.get("id")
        if ent_id:
            criados.append(int(ent_id))
            ent = PedidoEntrega.objects.filter(pk=ent_id).first()
            check(
                "http_loja_entrega_vila",
                ent is not None and (ent.loja_entrega or "") == "vila",
            )
            check(
                "http_loja_pag_centro",
                ent is not None and loja_pagamento_efetiva(ent) == "centro",
            )
            # Depois: Mudar Loja (como Renan fazia) ainda funciona
            url_m = reverse("api_pdv_entrega_pendente_mudar_loja", args=[ent_id])
            resp_m = client.post(
                url_m,
                data=json.dumps({"loja": "centro", "escopo": "entrega", "pin": PIN}),
                content_type="application/json",
            )
            try:
                dm = resp_m.json()
            except Exception:
                dm = {}
            check(
                "http_mudar_loja_ok",
                resp_m.status_code == 200 and dm.get("ok") is True,
                f"status={resp_m.status_code} {str(dm)[:100]}",
            )
            ent.refresh_from_db()
            check("http_apos_mudar_ent_centro", (ent.loja_entrega or "") == "centro")
            check("http_apos_mudar_pag_centro", loja_pagamento_efetiva(ent) == "centro")
        else:
            check("http_registrar_id", False, str(data)[:120])
    finally:
        if criados:
            PedidoEntrega.objects.filter(pk__in=criados).delete()
        SessaoCaixa.objects.filter(pk__in=[gav.pk, vil.pk]).delete()


def main() -> int:
    print("=== PDV-ENT-LOJA-LANC ===")
    print(f"PIN teste = {PIN!r}")
    test_estatico()
    try:
        test_runtime_deposito()
    except Exception as e:
        check("dep_crash", False, str(e)[:220])
    try:
        test_runtime_sessao_e_api()
    except Exception as e:
        check("runtime_crash", False, str(e)[:220])
    print("")
    print(f"VERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas: " + ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
