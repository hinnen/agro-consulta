# -*- coding: utf-8 -*-
"""
GESTAO-PIN-FOLGA — prova detalhada.

Gestão (lista + cadastro): descanso 5 min. PIN uma vez vale até o descanso.
PDV: descanso 3 min e cada ação pede de novo (~45s).

Runtime: PIN 9973 no Postgres local.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

PIN_TESTE = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
VALOR_PROVA = "zz prova pin folga nao usar"

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def check_static() -> None:
    print("--- static ---")
    sspin = read("produtos/templates/produtos/_screensaver_pin.html")
    gestao = read("produtos/templates/produtos/produtos_gestao.html")
    cad = read("produtos/templates/produtos/produtos_cadastro_erp.html")
    wiz = read("produtos/templates/produtos/pdv_wizard.html")
    consulta = read("produtos/templates/produtos/consulta_produtos.html")
    checkout = read("produtos/templates/produtos/pdv_checkout.html")
    views = read("produtos/views.py")
    pick = read("produtos/static/produtos/js/agro_picklist.js")
    modal = read("produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html")
    transf = read("produtos/pdv_transf_loja_util.py")

    check('{% else %}data-idle-min="3"{% endif %}' in sspin, "padrão continua 3 min")
    check("data-sspin-gestao" in sspin, "sspin só marca gestão quando a tela pede")
    check("gmSspinGestaoLiberado" in sspin, "gestão lembra o PIN da sessão")
    check("gestaoOkLocal()" in sspin, "ação na gestão não reabre o PIN")
    idx_fn = sspin.find("window.gmSspinGarantirOperador = function")
    fn = sspin[idx_fn : idx_fn + 2200] if idx_fn >= 0 else ""
    check("gestaoOkLocal()" in fn and fn.find("gestaoOkLocal()") < fn.find("pedirPin"), "atalho vem antes de pedir PIN")
    check("limparGestaoOk()" in sspin and "function openLock" in sspin, "descanso apaga o PIN da gestão")
    lock = sspin[sspin.find("function openLock") : sspin.find("function openLock") + 1600]
    check("limparGestaoOk()" in lock, "abrir descanso limpa a sessão da gestão")
    check("origem" in sspin and "gestao" in sspin, "PIN da gestão avisa o servidor")

    check("sspin_idle_min=5" in gestao and "sspin_gestao=1" in gestao, "gestão lista descanso 5 min")
    check("sspin_idle_min=5" in cad and "sspin_gestao=1" in cad, "cadastro gestão descanso 5 min")
    check("sspin_gestao" not in wiz, "wizard PDV sem folga")
    check("sspin_gestao" not in consulta and "sspin_gestao" not in checkout, "consulta e checkout sem folga")
    check("sspin_idle_min" not in wiz and "sspin_idle_min" not in consulta, "PDV sem tempo próprio")
    check("PDV_OPERADOR_FRESCO_TTL_S = 45" in transf, "PDV continua 45s por ação")
    check("PDV_OPERADOR_FRESCO_VENDA_TTL_S = 45" in transf, "fechar venda continua 45s")

    check("gestao_pin_liberado" in views, "servidor aceita o PIN já dado na gestão")
    check("gestao_limpar" in views, "servidor apaga no descanso")
    check('origem == "gestao"' in views, "login da gestão grava a sessão")
    check("usar_sessao" in pick, "lista + não pede PIN de novo se já identificado")
    check("if (!p && !liberado)" in pick, "sem sessão a lista + ainda exige PIN")
    check("usar_sessao" in modal, "cadastro + também reaproveita o PIN")
    check("if (!pin && !liberado)" in modal, "cadastro + sem sessão ainda exige PIN")

    extras = []
    for p in (ROOT / "produtos/templates").rglob("*.html"):
        if p.name in ("produtos_gestao.html", "produtos_cadastro_erp.html", "_screensaver_pin.html"):
            continue
        txt = p.read_text(encoding="utf-8", errors="replace")
        if "sspin_gestao" in txt or "sspin_idle_min" in txt:
            extras.append(str(p.relative_to(ROOT)))
    check(not extras, "nenhuma outra tela afrouxa o PIN" + (f" ({extras})" if extras else ""))


def _limpar_log_prova() -> None:
    from produtos.models import ProdutoCadastroAlteracaoAgro

    ProdutoCadastroAlteracaoAgro.objects.filter(
        produto_externo_id="__faceta__",
        valor_depois__startswith=VALOR_PROVA,
    ).delete()


def check_runtime() -> None:
    print("--- runtime PIN 9973 ---")
    import django

    django.setup()

    from django.conf import settings
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.urls import reverse

    from produtos.pdv_transf_loja_util import PDV_OPERADOR_FRESCO_KEY

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.filter(is_active=True).first()
    check(user is not None, "usuario Django para o teste")
    if not user:
        return

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or [])
    for h in ("testserver", "localhost", "127.0.0.1"):
        if h not in hosts:
            hosts.append(h)

    url_login = reverse("api_login_mobile")
    url_op = reverse("api_pdv_registrar_operador")
    url_fac = reverse("api_produtos_cadastro_faceta_nova")

    with override_settings(ALLOWED_HOSTS=hosts):
        c = Client()
        c.force_login(user)

        sess = c.session
        sess.pop("gestao_pin_liberado", None)
        sess.pop("gestao_pin_nonce", None)
        sess.pop("pdv_operador_nome", None)
        sess.pop(PDV_OPERADOR_FRESCO_KEY, None)
        sess.save()

        r_bad = c.post(url_login, data={"pin": "0000", "origem": "gestao", "gestao_nonce": "ruim"})
        check(r_bad.status_code == 403, f"PIN errado na gestão = 403 ({r_bad.status_code})")
        check(not c.session.get("gestao_pin_liberado"), "PIN errado não libera a gestão")

        r_pdv = c.post(url_login, data={"pin": PIN_TESTE})
        j_pdv = r_pdv.json() if r_pdv.status_code == 200 else {}
        check(r_pdv.status_code == 200 and j_pdv.get("ok"), f"PIN {PIN_TESTE} no PDV ok ({j_pdv.get('operador')!r})")
        if not j_pdv.get("ok"):
            fail("PIN 9973 nao encontrado — resto do runtime parado")
            return
        check(not c.session.get("gestao_pin_liberado"), "PIN do PDV não libera a gestão")

        sess = c.session
        sess[PDV_OPERADOR_FRESCO_KEY] = time.time() - 120
        sess.save()
        r_velho = c.get(url_op)
        j_velho = r_velho.json() if r_velho.status_code == 200 else {}
        check(j_velho.get("fresco") is False, "PDV depois de 2 min pede PIN de novo")

        _limpar_log_prova()
        r_sem = c.post(
            url_fac,
            data=json.dumps({"tipo": "marca", "valor": VALOR_PROVA, "usar_sessao": True, "tela_gestao": True}),
            content_type="application/json",
        )
        check(r_sem.status_code == 403, f"ação na gestão sem PIN da gestão = 403 ({r_sem.status_code})")

        nonce = "9973-gestao-prova"
        r_g = c.post(
            url_login,
            data={"pin": PIN_TESTE, "origem": "gestao", "gestao_nonce": nonce},
        )
        j_g = r_g.json() if r_g.status_code == 200 else {}
        check(r_g.status_code == 200 and j_g.get("ok"), "PIN 9973 na gestão ok")
        check(j_g.get("gestao_nonce") == nonce, "servidor devolve o mesmo nonce")
        check(c.session.get("gestao_pin_liberado") is True, "sessão da gestão liberada")
        check((c.session.get("pdv_operador_nome") or "").strip() != "", "nome do operador gravado")

        sess = c.session
        sess[PDV_OPERADOR_FRESCO_KEY] = time.time() - 120
        sess.save()
        r_pdv2 = c.get(url_op)
        j_pdv2 = r_pdv2.json() if r_pdv2.status_code == 200 else {}
        check(j_pdv2.get("fresco") is False, "PDV segue pedindo PIN mesmo com a gestão liberada")

        r_ok = c.post(
            url_fac,
            data=json.dumps(
                {"tipo": "marca", "valor": VALOR_PROVA, "usar_sessao": True, "pin": ""}
            ),
            content_type="application/json",
        )
        j_ok = r_ok.json() if r_ok.headers.get("Content-Type", "").startswith("application/json") else {}
        check(r_ok.status_code == 200 and j_ok.get("ok") is True, f"segunda ação na gestão sem PIN ({r_ok.status_code})")
        check((j_ok.get("operador") or "") == (c.session.get("pdv_operador_nome") or ""), "segunda ação fica no mesmo operador")

        r_keep = c.post(
            url_op,
            data=json.dumps({"gestao_limpar": True, "gestao_nonce": "outro"}),
            content_type="application/json",
        )
        check(r_keep.status_code == 200, "nonce errado no descanso responde ok")
        check(c.session.get("gestao_pin_liberado") is True, "nonce errado não apaga a gestão")

        r_clear = c.post(
            url_op,
            data=json.dumps({"gestao_limpar": True, "gestao_nonce": nonce}),
            content_type="application/json",
        )
        check(r_clear.status_code == 200, "descanso apaga a sessão")
        check(not c.session.get("gestao_pin_liberado"), "depois do descanso a gestão pede PIN")
        nome_depois = (c.session.get("pdv_operador_nome") or "").strip()
        check(nome_depois != "", "nome do PDV continua na sessão")

        r_de_novo = c.post(
            url_fac,
            data=json.dumps({"tipo": "marca", "valor": VALOR_PROVA + " 2", "usar_sessao": True}),
            content_type="application/json",
        )
        check(r_de_novo.status_code == 403, "depois do descanso a ação pede PIN")

    _limpar_log_prova()


def main() -> int:
    print("=== GESTAO-PIN-FOLGA ===")
    check_static()
    check_runtime()
    print(f"OKS={OKS} FAILS={len(FAILS)}")
    for f in FAILS:
        print(" -", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
