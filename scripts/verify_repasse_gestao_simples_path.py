#!/usr/bin/env python
"""Prova detalhada — REPASSE-GESTAO-SIMPLES (gestão alinhada ao PDV + saída nos 2 cofres).

  python scripts/verify_repasse_gestao_simples_path.py

Contratos: fonte · PIN 9973 · Django (retirada salário + Vila Elias + estorno) · HTTP se up.
VERIFY_REPASSE_GESTAO_SIMPLES_PATH_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ["DEBUG"] = "False"

PIN = "9973"
BASE = os.environ.get("AGRO_VERIFY_BASE", "http://127.0.0.1:8000").rstrip("/")
PREFIX = "verify-gestao-simples"
USER_BOT = "verify_gestao_simples_bot"

fails: list[str] = []
oks = 0


def check(cond, msg: str) -> None:
    global oks
    if cond:
        oks += 1
        print("OK", msg)
    else:
        fails.append(msg)
        print("FAIL", msg)


def needle(path: str, *needles: str, forbid: bool = False) -> None:
    text = (ROOT / path).read_text(encoding="utf-8", errors="replace")
    for n in needles:
        found = n in text
        if forbid:
            check(not found, f"{path} sem {n!r}")
        else:
            check(found, f"{path} tem {n!r}")


def prova_fonte() -> None:
    print("=== fonte ===")
    GESTAO = "produtos/templates/produtos/repasse_vila.html"
    VIEW = "produtos/views_repasse_vila.py"
    UTIL = "produtos/repasse_vila_util.py"
    OVERLAY = "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"

    # Dois cofres + saída em destaque
    needle(
        GESTAO,
        "rv-cofrinho-card",
        "rv-cofre-ve-card",
        'data-cofre="salario"',
        'data-cofre="vila_elias"',
        "Lançar saída do cofre",
        "rv-saida",
        "rv-saida--ve",
        'id="rv-cofre-tipo"',
        'id="rv-cofre-ve-tipo"',
        'id="rv-cofre-plano"',
        'id="rv-cofre-ve-plano"',
        "Plano de conta",
        "rv-planos-cofre-boot",
        'value="retirada"',
        "Retirada / uso",
        'id="rv-cofre-movimentar"',
        'id="rv-cofre-ve-movimentar"',
        "Registrar",
        "Detalhes do dia",
        "rv-fundo-troco",
        "Fundo troco gaveta",
        "Cofrinho Salário funcionário",
        "Cofre Vila Elias",
        "Saldo inicial",
        'value="saldo_inicial"',
        "Ainda separar",
        "rv-log-lista",
        "penúltimo",
        "Dias 1 a 15",
    )
    # Layout: cofres no topo (primeiro id no arquivo = salário card)
    html = (ROOT / GESTAO).read_text(encoding="utf-8", errors="replace")
    i_sal = html.find('id="rv-cofrinho-card"')
    i_ve = html.find('id="rv-cofre-ve-card"')
    i_cfg = html.find("Configurar envio")
    check(0 <= i_sal < i_ve < i_cfg, "ordem: Salario -> Vila Elias -> Configurar")
    # Botão laranja Registrar nos dois blocos de saída
    check(html.count("bg-orange-500") >= 3, "botões laranja (Transferir + 2× Registrar)")
    # Sem duplicar cards antigos
    check(html.count('id="rv-cofrinho-card"') == 1, "um só card salário")
    check(html.count('id="rv-cofre-ve-card"') == 1, "um só card Vila Elias")
    check(html.count('id="rv-cofre-saldo"') == 1, "um só id saldo salário")
    check(html.count('id="rv-cofre-ve-saldo"') == 1, "um só id saldo VE")

    needle(VIEW, "api_repasse_vila_cofrinho_movimento", '"retirada"', "registrar_uso_ou_ajuste_cofrinho")
    needle(UTIL, "registrar_uso_ou_ajuste_cofrinho", "COFRE_VILA_ELIAS", "COFRE_SALARIO")
    # Overlay PDV continua com link Gestão
    needle(OVERLAY, "repasse_vila", "Gestão", "pdv-rp-input-cofre-sal", "pdv-rp-input-cofre-ve")


def prova_pin() -> None:
    print("=== PIN ===")
    import django

    django.setup()
    from produtos import caixa_util as cu

    ok_pin, label, err = cu.operador_label_de_pin(PIN)
    check(ok_pin and bool(label) and not err, f"PIN {PIN} reconhece operador ({label})")


def cleanup_movs() -> None:
    from produtos.models import RepasseVilaReservaMovimentoAgro, TituloFinanceiroAgro

    qs = RepasseVilaReservaMovimentoAgro.objects.filter(
        observacao__contains=PREFIX
    ) | RepasseVilaReservaMovimentoAgro.objects.filter(
        idempotencia_chave__startswith=PREFIX
    )
    ids = list(qs.values_list("id", flat=True))
    if ids:
        # Estornos primeiro (FK protegida)
        RepasseVilaReservaMovimentoAgro.objects.filter(estornado_de_id__in=ids).delete()
        RepasseVilaReservaMovimentoAgro.objects.filter(id__in=ids).delete()
    TituloFinanceiroAgro.objects.filter(descricao__icontains=PREFIX).delete()
    TituloFinanceiroAgro.objects.filter(observacoes__icontains=PREFIX).delete()

def prova_django_dois_cofres() -> None:
    print("=== Django retirada 2 cofres ===")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.utils import timezone

    from produtos.models import RepasseVilaConfigAgro, RepasseVilaReservaMovimentoAgro
    from produtos.repasse_vila_util import (
        COFRE_SALARIO,
        COFRE_VILA_ELIAS,
        estornar_movimento_cofrinho,
        obter_config,
        registrar_uso_ou_ajuste_cofrinho,
        saldo_cofrinho_vila,
    )

    User = get_user_model()
    cfg = obter_config()
    sal_antes = saldo_cofrinho_vila(cfg, cofre=COFRE_SALARIO)
    ve_antes = saldo_cofrinho_vila(cfg, cofre=COFRE_VILA_ELIAS)
    cleanup_movs()

    user, _ = User.objects.get_or_create(
        username=USER_BOT, defaults={"is_staff": True, "is_superuser": True}
    )
    dia = timezone.localdate()
    op = f"Bot {PREFIX}"

    # Garante saldo mínimo sem sujar saldo real: usa ajuste_mais isolado + retirada + estorno
    chave_in_sal = f"{PREFIX}-in-sal"
    chave_out_sal = f"{PREFIX}-out-sal"
    chave_in_ve = f"{PREFIX}-in-ve"
    chave_out_ve = f"{PREFIX}-out-ve"
    valor = Decimal("12.34")

    from produtos.saida_caixa_planos import listar_planos_cofre_vila

    planos_cf = listar_planos_cofre_vila()
    check(bool(planos_cf), "planos cofre listados")
    plano_id = str((planos_cf[0] or {}).get("id") or "")

    mov_in_s, ok_s, err_s = registrar_uso_ou_ajuste_cofrinho(
        tipo="ajuste",
        valor=valor,
        operador=op,
        observacao=f"{PREFIX} entrada salário teste",
        data_ref=dia,
        idempotencia_chave=chave_in_sal,
        cofre=COFRE_SALARIO,
    )
    check(ok_s and mov_in_s and not err_s, "ajuste + salário ok")

    mov_out_s, ok_os, err_os = registrar_uso_ou_ajuste_cofrinho(
        tipo="retirada",
        valor=valor,
        operador=op,
        observacao=f"{PREFIX} retirada salário teste",
        data_ref=dia,
        idempotencia_chave=chave_out_sal,
        cofre=COFRE_SALARIO,
        plano_id=plano_id,
    )
    check(ok_os and mov_out_s and not err_os, f"retirada salário ok ({err_os})")
    check(
        abs(saldo_cofrinho_vila(cofre=COFRE_SALARIO) - sal_antes) < Decimal("0.01"),
        "salário volta ao saldo anterior após entrada+retirada",
    )

    mov_in_v, ok_v, err_v = registrar_uso_ou_ajuste_cofrinho(
        tipo="ajuste",
        valor=valor,
        operador=op,
        observacao=f"{PREFIX} entrada VE teste",
        data_ref=dia,
        idempotencia_chave=chave_in_ve,
        cofre=COFRE_VILA_ELIAS,
    )
    check(ok_v and mov_in_v and not err_v, "ajuste + Vila Elias ok")

    mov_out_v, ok_ov, err_ov = registrar_uso_ou_ajuste_cofrinho(
        tipo="retirada",
        valor=valor,
        operador=op,
        observacao=f"{PREFIX} retirada VE teste",
        data_ref=dia,
        idempotencia_chave=chave_out_ve,
        cofre=COFRE_VILA_ELIAS,
        plano_id=plano_id,
    )
    check(ok_ov and mov_out_v and not err_ov, f"retirada Vila Elias ok ({err_ov})")
    check(
        abs(saldo_cofrinho_vila(cofre=COFRE_VILA_ELIAS) - ve_antes) < Decimal("0.01"),
        "Vila Elias volta ao saldo anterior após entrada+retirada",
    )

    # Negativo bloqueado
    mov_neg, ok_neg, err_neg = registrar_uso_ou_ajuste_cofrinho(
        tipo="retirada",
        valor=saldo_cofrinho_vila(cofre=COFRE_VILA_ELIAS) + Decimal("99999"),
        operador=op,
        observacao=f"{PREFIX} negativa VE",
        data_ref=dia,
        idempotencia_chave=f"{PREFIX}-neg-ve",
        cofre=COFRE_VILA_ELIAS,
        plano_id=plano_id,
    )
    check(not ok_neg and mov_neg is None and err_neg, "bloqueia retirada maior que saldo VE")

    # Idempotência
    mov_dup, criado_dup, err_dup = registrar_uso_ou_ajuste_cofrinho(
        tipo="retirada",
        valor=valor,
        operador=op,
        observacao=f"{PREFIX} retirada VE teste",
        data_ref=dia,
        idempotencia_chave=chave_out_ve,
        cofre=COFRE_VILA_ELIAS,
        plano_id=plano_id,
    )
    check(
        mov_dup is not None and mov_dup.pk == mov_out_v.pk and criado_dup is False and not err_dup,
        "idempotencia VE reusa movimento",
    )

    # API HTTP Django Client
    client = Client(HTTP_HOST="127.0.0.1")
    client.force_login(user)
    page = client.get("/repasse-vila/")
    body = page.content.decode("utf-8", errors="replace")
    check(page.status_code == 200, "GET /repasse-vila/ 200")
    check("Lançar saída do cofre" in body and "rv-cofre-ve-movimentar" in body, "UI saída VE na página")
    check(body.find("rv-cofrinho-card") < body.find("rv-cofre-ve-card") < body.find("Configurar envio"), "ordem HTML na resposta")

    r_sal = client.get("/api/repasse-vila/cofrinho/?cofre=salario")
    r_ve = client.get("/api/repasse-vila/cofrinho/?cofre=vila_elias")
    check(r_sal.status_code == 200 and r_sal.json().get("ok", True) is not False, "API cofrinho salário")
    js_ve = r_ve.json()
    check(r_ve.status_code == 200 and js_ve.get("cofre") == "vila_elias", "API cofrinho=vila_elias")

    # API movimento VE (ajuste mínimo + estorno)
    chave_api = f"{PREFIX}-api-ve"
    # Usa Client POST JSON (com PIN 9973 como na loja)
    resp = client.post(
        "/api/repasse-vila/cofrinho/movimento/",
        data=json.dumps(
            {
                "tipo": "ajuste",
                "valor": 1.11,
                "observacao": f"{PREFIX} api ajuste VE",
                "data_ref": dia.isoformat(),
                "idempotencia_chave": chave_api,
                "cofre": "vila_elias",
                "pin": PIN,
            }
        ),
        content_type="application/json",
    )
    j = resp.json() if resp.status_code < 500 else {}
    check(resp.status_code == 200 and j.get("ok"), f"API movimento ajuste VE ({resp.status_code} {j.get('erro')})")
    if j.get("ok") and j.get("movimento_id"):
        est, ok_e, err_e = estornar_movimento_cofrinho(
            movimento_id=j["movimento_id"],
            operador=op,
            observacao=f"{PREFIX} estorno api VE",
        )
        check(ok_e and est and not err_e, "estorno movimento API VE")

    # Restaura saldos se ainda divergirem (limpa movimentos do prefix)
    cleanup_movs()
    # Recria config saldos se cleanup não reverte saldo — movimentos estornados/apagados
    # Apagar movimentos muda o ledger: saldo_cofrinho é campo na config, não derivado só do ledger.
    # Garantir restore explícito:
    cfg2 = RepasseVilaConfigAgro.objects.get(pk=cfg.pk)
    cfg2.saldo_reserva_vila = sal_antes
    cfg2.saldo_cofre_vila_elias = ve_antes
    cfg2.save(update_fields=["saldo_reserva_vila", "saldo_cofre_vila_elias"])
    check(
        saldo_cofrinho_vila(cofre=COFRE_SALARIO) == sal_antes
        and saldo_cofrinho_vila(cofre=COFRE_VILA_ELIAS) == ve_antes,
        "saldos restaurados após cleanup",
    )


def prova_http_opcional() -> None:
    print("=== HTTP local (opcional) ===")
    try:
        with urlopen(Request(BASE + "/healthz", method="GET"), timeout=3) as r:
            check(r.status == 200, f"healthz {BASE}")
    except (URLError, HTTPError, TimeoutError, OSError) as e:
        check(True, f"HTTP skip (servidor off: {type(e).__name__})")


def main() -> int:
    prova_fonte()
    prova_pin()
    prova_django_dois_cofres()
    prova_http_opcional()
    print("---")
    total = oks + len(fails)
    print(f"oks={oks} fails={len(fails)} total={total}")
    for item in fails:
        print("FAIL_ITEM", item)
    if fails:
        print("VERIFY_FAIL")
        return 1
    print(f"VERIFY_REPASSE_GESTAO_SIMPLES_PATH_OK {oks}/{oks}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
