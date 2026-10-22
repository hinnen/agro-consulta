#!/usr/bin/env python
"""Prova path PDV Rápidos — tela cheia, Alt+6, Esc, F6 de orçamento intacto."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

ok = 0
fail = 0


def check(name: str, cond: bool, detail: str = ""):
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fail += 1
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def main():
    print("=== PDV-RAPIDOS ===")
    html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/pdv_rapidos.js").read_text(encoding="utf-8")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    check("html_botao", 'id="pdv-topbar-rapidos-btn"' in html and "Alt+6" in html)
    check("html_overlay", 'id="pdv-rapidos-overlay"' in html and 'id="pdv-rapidos-fechar"' in html)
    check("html_esc_dica", "Esc sai" in html)
    check("html_js", "pdv_rapidos.js" in html)
    check("js_alt6", "ev.code === 'Digit6'" in js and "ev.altKey" in js)
    check("js_esc", "ev.key === 'Escape'" in js and "fechar()" in js)
    check("js_num", "Numpad9" in js and "vender(pos)" in js)
    check("js_nao_f6", "F6" not in js)
    check("wiz_f6_orcamento", "if (event.code === 'F6')" in wiz and "openBudgetHistory" in wiz)
    check("wiz_bridge", "AgroPdvRapidosBridge" in wiz)
    check("url", "api_pdv_rapidos" in urls and "api_pdv_rapidos_foto" in urls)
    check("mig", (ROOT / "produtos/migrations/0150_pdv_rapido_slot.py").exists())

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.pdv_topbar_layout_util import QUENTE_DEFAULT, normalizar_layout

    check("quente_default", "rapidos" in QUENTE_DEFAULT)
    n = normalizar_layout(quente=["fiado", "nova_venda"], frio=["pin"])
    check(
        "layout_insere_antes_nova",
        "rapidos" in n["quente"] and n["quente"].index("rapidos") < n["quente"].index("nova_venda"),
    )

    from django.contrib.auth import get_user_model
    from django.db import connection, transaction

    from produtos.models import PdvRapidoSlotAgro
    from produtos.pdv_rapidos_util import gravar_foto, listar_slots, preparar_foto, resposta_foto, salvar_slots
    from produtos.tests_pdv_rapidos import _jpeg

    tabelas = set(connection.introspection.table_names())
    nome_tab = PdvRapidoSlotAgro._meta.db_table
    criou = nome_tab not in tabelas
    if criou:
        with connection.schema_editor() as editor:
            editor.create_model(PdvRapidoSlotAgro)
    try:
        with transaction.atomic():
            user, _ = get_user_model().objects.get_or_create(username="verify_pdv_rapidos")
            slots = listar_slots()
            check("db_nove", len(slots) == 9 and all(not s["produto_id"] for s in slots))
            ok_save, err = salvar_slots(
                [{"posicao": 1, "produto_id": "p1", "codigo": "GM1", "nome": "Milho grande"}],
                usuario=user,
            )
            check("db_salvar", ok_save, err)
            ok_foto, err_foto = gravar_foto(1, _jpeg(), content_type="image/jpeg", usuario=user)
            check("db_foto", ok_foto, err_foto)
            slot = listar_slots()[0]
            check("db_sem_preco_gravado", slot["nome"] == "Milho grande" and "preco" not in slot and slot["tem_foto"])
            resp = resposta_foto(1)
            check("db_jpeg", resp.status_code == 200 and resp.content[:2] == b"\xff\xd8")
            ruim, msg = preparar_foto(b"xx", "image/jpeg")
            check("db_foto_ruim", ruim is None and bool(msg))
            ok_limpa, err_limpa = salvar_slots([{"posicao": 1, "limpar": True}])
            check("db_limpar", ok_limpa and resposta_foto(1).status_code == 404, err_limpa)
            transaction.set_rollback(True)
    finally:
        if criou:
            with connection.schema_editor() as editor:
                editor.delete_model(PdvRapidoSlotAgro)
    print(f"\n{ok} ok, {fail} fail")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
