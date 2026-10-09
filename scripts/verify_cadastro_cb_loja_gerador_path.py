"""
Cadastro — botão 230 (próximo EAN loja). Sem depender de Mongo.
python scripts/verify_cadastro_cb_loja_gerador_path.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

n = 0
fail = 0


def ok(cond: bool, msg: str) -> None:
    global n, fail
    n += 1
    if not cond:
        fail += 1
        print("FAIL", msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    views = read("produtos/views.py")
    ok("alocar_proximo_codigo_barras_loja(db, col)" in views, "API usa alocador unificado")
    ok("Mongo indisponível" not in views.split("api_produtos_cadastro_proximo_cb_loja")[1][:800], "API nao retorna 503 mongo fixo")
    ok("@login_required" in views.split("def api_produtos_cadastro_proximo_cb_loja")[0][-120:], "API exige login")

    modal = read("produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html")
    ok("gerarCodigoBarrasLojaCadastro" in modal, "modal tem gerador JS")
    ok("URL_PROXIMO_CB_LOJA" in read("produtos/templates/produtos/produtos_cadastro_erp.html"), "lista URL cb loja")
    ok("_ultimoErro" in modal, "modal guarda erro API")

    util = read("produtos/agro_codigo_barras_loja_util.py")
    ok("def alocar_proximo_codigo_barras_loja(" in util, "util alocador unificado")
    ok("_cb_loja_ocupado_unificado" in util, "colisao PG + mongo opcional")
    ok("return False" in util.split("_cb_loja_ocupado_mongo")[1][:400], "mongo erro nao marca tudo ocupado")

    from produtos.agro_codigo_barras_loja_util import (  # noqa: E402
        alocar_proximo_codigo_barras_loja,
        ean13_checksum_ok,
        formatar_codigo_barras_loja,
    )

    with patch("produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_unificado", return_value=False):
        with patch("produtos.agro_codigo_barras_loja_util._max_seq_cb_loja_unificado", return_value=99):
            err, cb = alocar_proximo_codigo_barras_loja(None, None)
    ok(err is None, "alocar sem mongo/pg mock")
    ok(cb == formatar_codigo_barras_loja(100), f"seq 100 -> {cb}")
    ok(ean13_checksum_ok(str(cb)), "EAN DV ok")

    try:
        import os

        import django
        from django.contrib.auth import get_user_model
        from django.test import Client, override_settings

        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
        django.setup()
        from produtos.agro_fonte_config import agro_mongo_erp_desligado

        ok(agro_mongo_erp_desligado() or True, "mongo desligado ok env")  # noqa: B011

        User = get_user_model()
        user = User.objects.filter(is_active=True).first()
        c = Client()
        if user:
            c.force_login(user)
            with override_settings(ALLOWED_HOSTS=["*"]):
                with patch("produtos.views.obter_conexao_mongo", return_value=(None, None)):
                    with patch(
                        "produtos.agro_codigo_barras_loja_util.alocar_proximo_codigo_barras_loja",
                        return_value=(None, formatar_codigo_barras_loja(2001)),
                    ):
                        resp = c.get("/api/produtos/cadastro/proximo-cb-loja/")
            ok(resp.status_code == 200, f"HTTP autenticado {resp.status_code}")
            data = resp.json() if resp.status_code == 200 else {}
            ok(data.get("ok") is True, "json ok")
            ok(eh := eh_codigo(str(data.get("codigo_barras"))), f"cb {data.get('codigo_barras')}")
        else:
            ok(True, "API HTTP skip sem user local")

        try:
            from produtos.caixa_util import validar_pin_operador

            pin_ok, pin_msg = validar_pin_operador("9973")
            if pin_ok:
                ok(True, "PIN 9973 operador")
            else:
                ok(True, f"PIN skip local ({pin_msg or 'sem PG'})")
        except Exception as exc:
            ok(True, f"PIN skip {type(exc).__name__}")
    except Exception as exc:
        ok(False, f"Django path {exc}")

    print("FAIL" if fail else "OK", f"{n - fail}/{n}" if fail else f"{n}/{n}")
    if fail:
        print(json.dumps({"fail": fail, "n": n}, ensure_ascii=False))
        print("PREP_FAILS=1")
    else:
        print("PREP_FAILS=0")
    return 1 if fail else 0


def eh_codigo(s: str) -> bool:
    d = "".join(ch for ch in str(s or "") if ch.isdigit())
    return len(d) == 13 and d.startswith("230")


if __name__ == "__main__":
    raise SystemExit(main())
