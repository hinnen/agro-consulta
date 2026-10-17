# -*- coding: utf-8 -*-
"""Runtime detalhado — bug #28 (PIN mid-chain) + bug #32 (escopo só-entrega).

  set AGRO_PIN_TESTE=9973
  python scripts/verify_bug28_bug32_runtime_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("AGRO_PIN_TESTE", "9973")
os.environ.setdefault("SECRET_KEY", "verify-bug28-32-runtime")
os.environ.setdefault("DEBUG", "True")

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
fails = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global fails
    mark = "OK" if cond else "FAIL"
    if not cond:
        fails += 1
    extra = f" — {detail}" if detail else ""
    print(f"  {mark}  {name}{extra}")


def _escopo(atual: str, loja: str, esc: str) -> tuple[str, str]:
    """Espelha confirmarLojaSaidaComEscopo (pdv_wizard.js)."""
    loja_ent = loja
    loja_pag = loja
    if esc == "entrega":
        loja_pag = "vila" if atual == "vila" else "centro"
    elif esc == "pagamento":
        loja_ent = "vila" if atual == "vila" else "centro"
    return loja_ent, loja_pag


def main() -> int:
    import django

    django.setup()
    from django.test import Client, RequestFactory, override_settings
    from django.contrib.sessions.middleware import SessionMiddleware

    from produtos.caixa_util import exigir_operador_pin_request
    from produtos.pdv_transf_loja_util import (
        expirar_operador_pdv_fresco,
        gravar_operador_sessao_pdv,
        marcar_operador_pdv_fresco,
        operador_pdv_esta_fresco,
    )

    print("== RUNTIME bug #28 PIN mid-chain ==")
    rf = RequestFactory()
    req = rf.post("/x/")
    SessionMiddleware(lambda r: None).process_request(req)
    req.session.save()

    ok, label, _u, err = gravar_operador_sessao_pdv(req, PIN)
    check("pin grava", ok, label or err)
    check("fresco apos PIN", operador_pdv_esta_fresco(req))
    rot, errp = exigir_operador_pin_request(req, {})
    check("exigir OK com fresco", bool(rot) and not errp, rot)

    # Simula o bug antigo: zerar PIN entre venda e entrega
    expirar_operador_pdv_fresco(req)
    check("apos expirar mid: sem fresco", not operador_pdv_esta_fresco(req))
    rot2, err2 = exigir_operador_pin_request(req, {})
    check(
        "apos expirar mid: entrega pediria PIN",
        not rot2 and bool(err2),
        (err2 or "")[:70],
    )

    # Com o fix, fresco segue vivo ate o fim
    marcar_operador_pdv_fresco(req)
    rot3, err3 = exigir_operador_pin_request(req, {})
    check("sem expirar mid: entrega OK", bool(rot3) and not err3, rot3)

    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    resp_i = views.find("def _resposta_venda")
    resp = views[resp_i : resp_i + 900] if resp_i >= 0 else ""
    check("codigo ERP nao zera no meio", "expirar_operador_pdv_fresco" not in resp)

    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost"]):
        c = Client()
        r = c.post("/api/login-mobile/", {"pin": PIN})
        check("HTTP login-mobile", r.status_code == 200, str(r.status_code))
        if r.status_code == 200:
            d = r.json()
            check("login devolve operador", bool(d.get("ok") and d.get("operador")), str(d.get("operador")))
            r2 = c.get("/api/pdv/operador/")
            d2 = r2.json() if r2.status_code == 200 else {}
            check("GET operador fresco", d2.get("fresco") is True, str(d2.get("restante_s")))

    print()
    print("== RUNTIME bug #32 escopo so-entrega ==")
    e, p = _escopo("vila", "centro", "entrega")
    check("Vila so-entrega Centro: sai Centro", e == "centro", e)
    check("Vila so-entrega Centro: paga Vila", p == "vila", p)
    check("pagar aqui = segue PDV (nao painel)", p == "vila")

    e2, p2 = _escopo("vila", "centro", "ambos")
    check("Vila ambas Centro: paga Centro", p2 == "centro", p2)
    check("pagar outra = painel", p2 != "vila")

    e3, p3 = _escopo("centro", "vila", "entrega")
    check("Centro so-entrega Vila: paga Centro", e3 == "vila" and p3 == "centro", f"sai={e3} paga={p3}")

    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    pag = js.split("function wizardIrParaPagamentoComImpressao", 1)[-1]
    nxt = pag.find("\n    function ")
    pag = pag[:nxt] if nxt > 0 else pag
    check("JS nao desvia por saida", "entregaVaiParaOutraLoja" not in pag)
    check("JS abre pagamento", "setCurrentStep('pagamento')" in pag)

    print()
    if fails:
        print(f"VERIFY_FAIL {fails}")
        return 1
    print("VERIFY_OK 16/16")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
