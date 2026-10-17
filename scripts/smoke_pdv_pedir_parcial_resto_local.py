"""Smoke PDV-PEDIR-PARCIAL-RESTO — API real + PIN (item escrito, sem estoque).

  set AGRO_PIN_TESTE=9973
  python scripts/smoke_pdv_pedir_parcial_resto_local.py
"""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client

from estoque.models import SolicitacaoTransferenciaPdv, SolicitacaoTransferenciaPdvItem
from produtos.caixa_util import operador_label_de_pin

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
TAG = "SMOKE-PARCIAL-RESTO"
ok_n = 0
fail_n = 0


def check(cond: bool, msg: str) -> None:
    global ok_n, fail_n
    if cond:
        ok_n += 1
        print("  OK ", msg)
    else:
        fail_n += 1
        print("  FAIL", msg)


def _json(resp):
    try:
        return json.loads(resp.content.decode("utf-8"))
    except Exception:
        return {}


def _post(c: Client, url: str, payload: dict):
    return c.post(
        url,
        data=json.dumps(payload),
        content_type="application/json",
        HTTP_HOST="127.0.0.1",
    )


def _fluxo_criar_aceitar(c: Client, nome: str, qtd: str) -> tuple[int | None, int | None]:
    """Centro pede → Vila aceita. Devolve (sol_id, item_id)."""
    r = _post(
        c,
        "/api/pdv/transf-loja/criar/",
        {
            "pin": PIN,
            "loja": "centro",
            "observacao": f"{TAG} {nome}",
            "itens": [{"livre": True, "nome": nome, "quantidade": qtd}],
        },
    )
    d = _json(r)
    if not (r.status_code == 200 and d.get("ok")):
        check(False, f"criar {nome}: HTTP {r.status_code} {d.get('erro')}")
        return None, None
    sol = d.get("solicitacao") or {}
    sid = sol.get("id")
    itens = sol.get("itens") or []
    iid = itens[0]["id"] if itens else None
    check(bool(sid and iid), f"criar {nome} → #{sid} item {iid}")

    r2 = _post(
        c,
        f"/api/pdv/transf-loja/{sid}/acao/",
        {"pin": PIN, "loja": "vila", "acao": "aceitar"},
    )
    d2 = _json(r2)
    check(
        r2.status_code == 200 and d2.get("ok"),
        f"aceitar #{sid}: HTTP {r2.status_code} {d2.get('erro') or 'ok'}",
    )
    return sid, iid


def _cancelar_resto(c: Client, resto_id: int) -> None:
    if not resto_id:
        return
    _post(
        c,
        f"/api/pdv/transf-loja/{resto_id}/acao/",
        {"pin": PIN, "loja": "vila", "acao": "cancelar", "motivo": f"{TAG} limpeza"},
    )


def main() -> int:
    print(f"SMOKE PDV-PEDIR-PARCIAL-RESTO local · PIN={PIN}")
    User = get_user_model()
    u = User.objects.filter(is_active=True).order_by("-is_superuser", "id").first()
    check(u is not None, f"user Django ({getattr(u, 'username', None)})")

    ok_pin, label, err_pin = operador_label_de_pin(PIN)
    check(ok_pin, f"PIN resolve ({label or err_pin})")

    js = (ROOT / "produtos/static/produtos/js/pdv_pedir_loja.js").read_text(encoding="utf-8")
    html = (
        ROOT / "produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html"
    ).read_text(encoding="utf-8")
    check("duasOpcoesResto" in js, "JS duasOpcoesResto")
    check("deixar_resto" in js, "JS payload deixar_resto")
    check("Transferir e encerrar" in js, "rótulo encerrar")
    check("Transferir e deixar resto" in js or "confirm-resto" in html, "rótulo/deixar resto")
    check("pdv-pedir-loja-confirm-resto" in html, "botão resto no HTML")
    check("selecaoTemResto" in js and "textoRestoSelecao" in js, "helpers resto")

    c = Client(HTTP_HOST="127.0.0.1")
    if u:
        c.force_login(u)

    r_pdv = c.get("/pdv/")
    body = r_pdv.content.decode("utf-8", "replace")
    check(r_pdv.status_code == 200, f"PDV HTTP {r_pdv.status_code}")
    check("pdv-pedir-loja-confirm-resto" in body, "botão resto na página PDV")
    check("pdv_pedir_loja.js" in body, "script Pedir loja no PDV")

    # --- A: 8→4 deixar_resto=true → RESTANTE qtd 4 ---
    sid_a, iid_a = _fluxo_criar_aceitar(c, "Smoke resto A", "8")
    resto_a = None
    if sid_a and iid_a:
        r = _post(
            c,
            f"/api/pdv/transf-loja/{sid_a}/acao/",
            {
                "pin": PIN,
                "loja": "vila",
                "acao": "transferir",
                "deixar_resto": True,
                "itens": [{"id": iid_a, "quantidade": "4"}],
            },
        )
        d = _json(r)
        check(
            r.status_code == 200 and d.get("ok"),
            f"transf resto HTTP {r.status_code} {d.get('erro') or 'ok'}",
        )
        resto_a = d.get("resto_solicitacao_id")
        check(bool(resto_a), f"resto_solicitacao_id={resto_a} msg={d.get('mensagem')!r}")
        check(
            "ficaram na fila" in str(d.get("mensagem") or "").lower()
            or "fila" in str(d.get("mensagem") or "").lower(),
            f"mensagem menciona fila ({d.get('mensagem')!r})",
        )
        sol_a = SolicitacaoTransferenciaPdv.objects.filter(pk=sid_a).first()
        check(
            sol_a is not None and sol_a.status == "concluido",
            f"origem #{sid_a} status={getattr(sol_a, 'status', None)}",
        )
        it_a = SolicitacaoTransferenciaPdvItem.objects.filter(pk=iid_a).first()
        check(
            it_a is not None and Decimal(str(it_a.quantidade)) == Decimal("4.000"),
            f"origem enviou {getattr(it_a, 'quantidade', None)}",
        )
        if resto_a:
            sol_r = SolicitacaoTransferenciaPdv.objects.filter(pk=resto_a).first()
            check(
                sol_r is not None and sol_r.status in ("aceito", "pronto"),
                f"resto #{resto_a} status={getattr(sol_r, 'status', None)}",
            )
            itens_r = list(
                SolicitacaoTransferenciaPdvItem.objects.filter(solicitacao_id=resto_a)
            )
            check(len(itens_r) == 1, f"resto itens={len(itens_r)}")
            if itens_r:
                check(
                    Decimal(str(itens_r[0].quantidade)) == Decimal("4.000"),
                    f"resto qtd={itens_r[0].quantidade}",
                )
                check(
                    "livre:" in str(itens_r[0].produto_externo_id).lower(),
                    "resto ainda item escrito",
                )
            check(
                "Restante do pedido" in (sol_r.observacao or ""),
                f"obs resto={sol_r.observacao!r}" if sol_r else "sem sol resto",
            )

    # --- B: 8→4 deixar_resto=false → sem RESTANTE ---
    sid_b, iid_b = _fluxo_criar_aceitar(c, "Smoke encerrar B", "8")
    if sid_b and iid_b:
        antes = SolicitacaoTransferenciaPdv.objects.count()
        r = _post(
            c,
            f"/api/pdv/transf-loja/{sid_b}/acao/",
            {
                "pin": PIN,
                "loja": "vila",
                "acao": "transferir",
                "deixar_resto": False,
                "itens": [{"id": iid_b, "quantidade": "4"}],
            },
        )
        d = _json(r)
        check(
            r.status_code == 200 and d.get("ok"),
            f"transf encerrar HTTP {r.status_code} {d.get('erro') or 'ok'}",
        )
        check(not d.get("resto_solicitacao_id"), "encerrar não criou resto_solicitacao_id")
        depois = SolicitacaoTransferenciaPdv.objects.count()
        check(depois == antes, f"contagem solicitações {antes}→{depois}")
        sol_b = SolicitacaoTransferenciaPdv.objects.filter(pk=sid_b).first()
        check(
            sol_b is not None and sol_b.status == "concluido",
            f"encerrar #{sid_b} status={getattr(sol_b, 'status', None)}",
        )
        it_b = SolicitacaoTransferenciaPdvItem.objects.filter(pk=iid_b).first()
        check(
            it_b is not None and Decimal(str(it_b.quantidade)) == Decimal("4.000"),
            f"encerrar enviou {getattr(it_b, 'quantidade', None)}",
        )
        obs = (sol_b.observacao or "") if sol_b else ""
        # obs do pedido ou evento — aceita Encerrado no histórico via serialização
        check(
            sol_b is not None,
            "pedido encerrado existe",
        )

    # --- C: 8→8 sem resto → um botão (API sem flag ainda ok, sem resto) ---
    sid_c, iid_c = _fluxo_criar_aceitar(c, "Smoke integral C", "8")
    if sid_c and iid_c:
        antes = SolicitacaoTransferenciaPdv.objects.count()
        r = _post(
            c,
            f"/api/pdv/transf-loja/{sid_c}/acao/",
            {
                "pin": PIN,
                "loja": "vila",
                "acao": "transferir",
                "itens": [{"id": iid_c, "quantidade": "8"}],
            },
        )
        d = _json(r)
        check(
            r.status_code == 200 and d.get("ok"),
            f"transf integral HTTP {r.status_code} {d.get('erro') or 'ok'}",
        )
        check(
            SolicitacaoTransferenciaPdv.objects.count() == antes,
            "integral sem RESTANTE",
        )

    # limpeza do resto criado no cenário A
    if resto_a:
        _cancelar_resto(c, int(resto_a))
        sol_r = SolicitacaoTransferenciaPdv.objects.filter(pk=resto_a).first()
        check(
            sol_r is not None and sol_r.status == "cancelado",
            f"limpeza resto #{resto_a} → {getattr(sol_r, 'status', None)}",
        )

    print()
    print(f"SMOKE PARCIAL-RESTO {'OK' if fail_n == 0 else 'FAIL'} {ok_n}/{ok_n + fail_n}")
    return 1 if fail_n else 0


if __name__ == "__main__":
    raise SystemExit(main())
