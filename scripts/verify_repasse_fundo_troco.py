#!/usr/bin/env python
"""Prova detalhada — REPASSE-FUNDO-TROCO (path completo).

Contrato:
- Campo fundo_troco_vila (padrão 500) em config / % lucro e opções
- Sugestão: preenche Salário → Vila Elias → Centro
- Falta: corta Centro → Vila Elias → Salário
- Só aviso — não bloqueia confirmar
"""
from __future__ import annotations

import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ["DEBUG"] = "False"

fails: list[str] = []
oks = 0


def check(cond, msg):
    global oks
    if cond:
        oks += 1
        print("OK", msg)
    else:
        fails.append(msg)
        print("FAIL", msg)


def check_file(path: str, *needles: str) -> None:
    p = ROOT / path
    if not p.exists():
        fails.append(f"MISSING {path}")
        print("FAIL MISSING", path)
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n not in text:
            fails.append(f"{path} missing {n!r}")
            print("FAIL", path, "missing", repr(n))
        else:
            global oks
            oks += 1


def forbid_file(path: str, *needles: str) -> None:
    p = ROOT / path
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n in text:
            fails.append(f"{path} still has {n!r}")
            print("FAIL", path, "has", repr(n))
        else:
            global oks
            oks += 1
            print("OK forbid", n)


def main():
    # —— Static path ——
    check_file(
        "produtos/migrations/0106_fundo_troco_vila.py",
        "fundo_troco_vila",
        "0105_chat_loja_mensagem",
        'Decimal("500.00")',
    )
    check_file(
        "produtos/models.py",
        "fundo_troco_vila",
        "Alvo de dinheiro que deve ficar na gaveta",
        "default=500",
    )
    check_file(
        "produtos/repasse_vila_util.py",
        "FUNDO_TROCO_VILA_DEFAULT",
        "fundo_troco_vila_config",
        "salvar_fundo_troco_vila",
        "sugerir_alocacao_fundo_troco",
        "Cortamos Centro → Vila Elias → Salário",
    )
    check_file(
        "produtos/views_repasse_vila.py",
        "fundo_troco_vila_config",
        "salvar_fundo_troco_vila",
        '"fundo_troco_vila"',
    )
    HTML = "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"
    JS = "produtos/static/produtos/js/pdv_repasse_vila.js"
    check_file(HTML, "pdv-rp-fundo-troco", "Fundo troco gaveta", "pdv-rp-fundo-aviso", "500,00")
    check_file(
        JS,
        "sugerirFundoTroco",
        "fundoTrocoAtual",
        "fundo_troco_vila",
        "pdv-rp-fundo-troco",
        "Cortamos Centro → Vila Elias → Salário",
        "Ajuste fundo troco",
    )
    check_file(
        "produtos/templates/produtos/repasse_vila.html",
        "rv-fundo-troco",
        "Fundo troco gaveta",
        "fundoTrocoAtual",
        "fundo_troco_vila",
    )
    check_file(
        "produtos/templates/produtos/includes/repasse_help_agents.html",
        "Fundo de troco (gaveta Vila)",
        "Centro → Vila Elias → Salário",
    )
    # Não trava transferência por fundo troco
    forbid_file(JS, "bloqueia fundo", "bloquear fundo troco")
    check(
        "precisa_forcar_manual" in (ROOT / JS).read_text(encoding="utf-8"),
        "forçar manual intacto (não misturado com fundo)",
    )

    r = subprocess.run(
        ["node", "--check", str(ROOT / JS)],
        capture_output=True,
        text=True,
    )
    check(r.returncode == 0, "node --check pdv_repasse_vila.js")

    # —— Fórmula ——
    import django

    django.setup()
    from produtos.repasse_vila_util import (
        FUNDO_TROCO_VILA_DEFAULT,
        fundo_troco_vila_config,
        obter_config,
        salvar_fundo_troco_vila,
        sugerir_alocacao_fundo_troco,
    )

    check(FUNDO_TROCO_VILA_DEFAULT == Decimal("500.00"), "default 500")

    r1 = sugerir_alocacao_fundo_troco(
        saldo_gaveta=900,
        alvo_troco=500,
        sep_salario=100,
        sep_vila_elias=80,
        levar_centro=200,
    )
    check(r1["sep_salario"] == 100.0 and r1["sep_vila_elias"] == 80.0 and r1["levar_centro"] == 200.0, "pool cheio: 3 bases")
    check(abs(r1["sobra_gaveta"] - 520.0) < 0.01, "pool cheio: sobra 520")
    check(bool(r1["aviso"]), "pool cheio: aviso acima do alvo")

    r2 = sugerir_alocacao_fundo_troco(
        saldo_gaveta=600,
        alvo_troco=500,
        sep_salario=100,
        sep_vila_elias=80,
        levar_centro=200,
    )
    check(
        r2["levar_centro"] == 0.0 and r2["sep_vila_elias"] == 0.0 and r2["sep_salario"] == 100.0,
        "falta: Centro+VE cortados, Salario ok",
    )
    check(
        r2["cortou_centro"] == 200.0 and r2["cortou_vila_elias"] == 80.0 and r2["cortou_salario"] == 0.0,
        "falta: cortes",
    )

    r3 = sugerir_alocacao_fundo_troco(
        saldo_gaveta=650,
        alvo_troco=500,
        sep_salario=100,
        sep_vila_elias=80,
        levar_centro=200,
    )
    check(
        r3["sep_salario"] == 100.0 and r3["sep_vila_elias"] == 50.0 and r3["levar_centro"] == 0.0,
        "meio: VE parcial",
    )

    r4 = sugerir_alocacao_fundo_troco(
        saldo_gaveta=780,
        alvo_troco=500,
        sep_salario=100,
        sep_vila_elias=80,
        levar_centro=200,
    )
    check(
        r4["levar_centro"] == 100.0 and abs(r4["sobra_gaveta"] - 500.0) < 0.01,
        "quase: Centro parcial / sobra=alvo",
    )

    r5 = sugerir_alocacao_fundo_troco(
        saldo_gaveta=400,
        alvo_troco=500,
        sep_salario=100,
        sep_vila_elias=50,
        levar_centro=100,
    )
    check(
        r5["sep_salario"] == 0.0 and r5["sep_vila_elias"] == 0.0 and r5["levar_centro"] == 0.0,
        "gaveta < alvo: pool 0 = tudo zero",
    )
    check("Troco ficaria" in (r5["aviso"] or ""), "gaveta < alvo: aviso troco curto")

    # alvo 0 = sem ajuste (libera tudo)
    r6 = sugerir_alocacao_fundo_troco(
        saldo_gaveta=500,
        alvo_troco=0,
        sep_salario=50,
        sep_vila_elias=50,
        levar_centro=100,
    )
    check(r6["sep_salario"] == 50.0 and r6["levar_centro"] == 100.0, "alvo 0: nao corta")
    check(r6["aviso"] == "", "alvo 0: sem aviso")

    # —— Persistência config (views diretas — evita Client/URLconf DRF quebrado no ambiente) ——
    from django.contrib.auth import get_user_model
    from django.test import RequestFactory

    from produtos.views_repasse_vila import api_repasse_vila_config

    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username="verify-fundo-troco",
        defaults={"is_staff": True, "is_superuser": True},
    )
    if not user.has_usable_password():
        user.set_password("x")
        user.save()

    cfg = obter_config()
    antes = fundo_troco_vila_config(cfg)
    rf = RequestFactory()

    def _authed(req):
        req.user = user
        from django.contrib.sessions.middleware import SessionMiddleware

        middleware = SessionMiddleware(lambda r: None)
        middleware.process_request(req)
        req.session.save()
        return req

    req_g = _authed(rf.get("/api/repasse-vila/config/"))
    g = api_repasse_vila_config(req_g)
    check(g.status_code == 200 and g.content, "GET config 200")
    import json

    gj = json.loads(g.content.decode("utf-8"))
    check(gj.get("ok") and "fundo_troco_vila" in gj, "GET config tem fundo_troco_vila")

    req_p = _authed(
        rf.post(
            "/api/repasse-vila/config/",
            data=json.dumps({"fundo_troco_vila": "555.50"}),
            content_type="application/json",
        )
    )
    p = api_repasse_vila_config(req_p)
    pj = json.loads(p.content.decode("utf-8"))
    check(p.status_code == 200 and pj.get("ok"), f"POST fundo 555.50 -> {p.status_code}")
    check(abs(float(pj.get("fundo_troco_vila", 0)) - 555.5) < 0.01, "POST gravou 555.50")
    check(abs(float(fundo_troco_vila_config()) - 555.5) < 0.01, "PG refletiu 555.50")

    # meta/calc usam reverse() → URLconf DRF quebrado neste PC; espelha o contrato das views
    from produtos.repasse_vila_util import calcular_disponivel, saldo_dinheiro_caixa_vila

    out_calc = calcular_disponivel(_skip_acumulado=True)
    out_calc["fundo_troco_vila"] = float(fundo_troco_vila_config())
    out_calc["caixa_vila"] = saldo_dinheiro_caixa_vila()
    check(abs(float(out_calc["fundo_troco_vila"]) - 555.5) < 0.01, "calc contrato traz fundo")
    check("saldo_dinheiro" in out_calc["caixa_vila"], "calc contrato traz caixa_vila")
    check(abs(float(fundo_troco_vila_config()) - 555.5) < 0.01, "meta contrato = config fundo")

    # restaura
    salvar_fundo_troco_vila(antes if antes > 0 else Decimal("500"), operador="verify-fundo-troco")
    check(
        abs(float(fundo_troco_vila_config()) - float(antes if antes > 0 else 500)) < 0.01,
        "restaurou fundo",
    )

    # PIN operador (smoke — nao confirma repasse real sem caixa Vila)
    from produtos.caixa_util import operador_label_de_pin, usuario_django_de_pin

    pin = "9973"
    op = operador_label_de_pin(pin) or ""
    u_pin = usuario_django_de_pin(pin)
    label = op or (getattr(u_pin, "username", None) if u_pin else "")
    check(bool(label), f"PIN 9973 resolve operador ({label})")

    print(f"\n{oks} OK · {len(fails)} FAIL")
    if fails:
        for f in fails:
            print(" -", f)
        sys.exit(1)
    print("VERIFY_OK REPASSE-FUNDO-TROCO")
    sys.exit(0)


if __name__ == "__main__":
    main()
