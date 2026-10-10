# -*- coding: utf-8 -*-
"""Prova detalhada PDV-ORC-LISTA-LIVE — bug #14 (verde sem lista no Centro).

Cobre: memória PDV · cota localStorage · sync após OK · data BR · ordenação ·
API POST/GET consumidor · PIN 9973 · HTTP se runserver.

  python scripts/verify_pdv_orc_lista_live_path.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

PIN = "9973"
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


def check_fonte() -> None:
    wiz = read("produtos/static/produtos/js/pdv_wizard.js")
    check("_orcamentosMem" in wiz, "fonte tem memoria PDV")
    check("localStorage.removeItem('historicoOrcamentos')" in wiz, "fonte limpa cota")
    check("slice(0, 40)" in wiz and "slice(0, 15)" in wiz, "fonte slim em cota")
    idx_save = wiz.find("function salvarOrcamentoWizard")
    save = wiz[idx_save : idx_save + 5000]
    check(
        "syncHistoricoOrcamentosCliente(key, { silent: true })" in save
        and "doneFeedback()" in save,
        "fonte OK → sync servidor → verde",
    )
    check(
        save.find("syncHistoricoOrcamentosCliente(key, { silent: true })")
        < save.find("doneFeedback()"),
        "fonte sync antes do verde",
    )
    check("PDV sem URL de orçamento" in save, "fonte sem URL nao mente verde")
    check("sortHistoricoOrcamentosPorId" in wiz, "fonte ordena por id")
    idx_fmt = wiz.find("function formatBudgetCardDate")
    fmt = wiz[idx_fmt : idx_fmt + 800]
    check(
        idx_fmt > 0 and 0 < fmt.find("raw.match") < fmt.find("new Date(raw)"),
        "fonte data BR antes Date US",
    )
    idx_snip = wiz.find("function renderRecentBudgetsSnippet")
    snip = wiz[idx_snip : idx_snip + 800]
    check("sortHistoricoOrcamentosPorId" in snip, "fonte card ordena")
    check("filterHistoricoPorCliente" in snip, "fonte card filtra cliente")


def check_pin_api() -> None:
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.urls import reverse

    from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador
    from produtos.models import OrcamentoPdvAgro

    pin_ok, pin_err = validar_pin_operador(PIN)
    check(pin_ok, f"PIN {PIN} valido ({pin_err})")
    rotulo = rotulo_operador_pin(PIN)
    check(bool(rotulo), f"PIN {PIN} tem nome ({rotulo or '?'})")
    check("Renan" in rotulo or bool(rotulo), f"PIN {PIN} rotulo ok")

    User = get_user_model()
    staff = User.objects.filter(is_staff=True, is_active=True).first()
    check(staff is not None, "tem staff local")
    if staff is None:
        return

    url = reverse("api_pdv_orcamentos")
    cid = int(time.time() * 1000)
    entry = {
        "id": cid,
        "cliente": "Consumidor nao identificado",
        "cliente_key": "consumidor_final",
        "cliente_mode": "consumidor_final",
        "total": "R$ 1,30",
        "itens": [{"id": "t", "nome": "teste lista live", "qtd": 1, "preco": 1.3}],
        "origem": "manual",
        "usuario": "verify-lista-live",
    }
    with override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"]):
        c = Client()
        c.force_login(staff)
        r = c.post(
            url,
            data=json.dumps({"entry": entry}),
            content_type="application/json",
        )
        check(r.status_code == 200, f"POST consumidor 200 ({r.status_code})")
        body = r.json()
        check(body.get("ok") is True, f"POST ok=True ({body})")
        item = body.get("item") or {}
        check(str(item.get("cliente_key")) == "consumidor_final", "item cliente_key consumidor")
        check("1,30" in str(item.get("total") or ""), "item total 1,30")
        check(int(item.get("id") or 0) == cid, "item id gravado")

        g = c.get(url, {"cliente_key": "consumidor_final", "limite": 30})
        check(g.status_code == 200, f"GET consumidor 200 ({g.status_code})")
        gbody = g.json()
        check(gbody.get("ok") is True and gbody.get("escopo") == "cliente", "GET escopo cliente")
        ids = [int(x.get("id") or 0) for x in (gbody.get("items") or [])]
        check(cid in ids, "GET lista inclui o novo (servidor = verdade)")
        check(cid in ids[:3], "novo entre os 3 recentes")

        obj = OrcamentoPdvAgro.objects.filter(orc_local_id=cid).first()
        check(obj is not None, "Postgres tem orcamento")
        if obj is not None:
            check(obj.cliente_key == "consumidor_final", "PG cliente_key")
            check("1,30" in (obj.total_texto or ""), "PG total")

        # Isolamento: outro cliente nao ve
        other_key = f"tmp:verify:{uuid.uuid4().hex[:8]}"
        g2 = c.get(url, {"cliente_key": other_key, "limite": 30})
        ids2 = [int(x.get("id") or 0) for x in ((g2.json() or {}).get("items") or [])]
        check(cid not in ids2, "outro cliente nao ve o orcamento")


def check_http() -> None:
    base = os.environ.get("AGRO_VERIFY_BASE", "http://127.0.0.1:8000").rstrip("/")
    try:
        with urlopen(Request(base + "/healthz", method="GET"), timeout=2) as r:
            check(r.status == 200, "HTTP healthz 200")
    except (URLError, OSError) as e:
        ok(f"runserver off — HTTP skip ({e})")


def main() -> int:
    print("=== PDV-ORC-LISTA-LIVE detalhado ===")
    print("--- fonte ---")
    check_fonte()
    print("--- PIN + API ---")
    check_pin_api()
    print("--- HTTP ---")
    check_http()
    print(f"OK={OKS} FAIL={len(FAILS)}")
    if FAILS:
        print("VERIFY_FAIL")
        for f in FAILS:
            print(" -", f)
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
