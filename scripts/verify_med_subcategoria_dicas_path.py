"""VERIFY MED-SUB-DICAS — tooltips subcategorias 1–4 (só UI, Medicamentos E Venenos).

Run: AGRO_PIN_TESTE=9973 .venv/bin/python scripts/verify_med_subcategoria_dicas_path.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails = 0
PIN_ENV = (os.environ.get("AGRO_PIN_TESTE") or "").strip()

MODAL = ROOT / "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html"


def ok(msg: str) -> None:
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    global fails
    fails += 1
    print(f" FAIL {msg}")


def main() -> int:
    print("VERIFY MED-SUB-DICAS\n")
    if not MODAL.is_file():
        fail("modal ausente")
        return 1
    modal = MODAL.read_text(encoding="utf-8")
    ok("modal legível")

    for needle in (
        "atualizarDicasSubcategoriaMedicamento",
        "atualizarUISubcategoriaMedicamento",
        "categoriaEhMedicamentoVet",
        "edit-subcategoria-med-prateleira",
        "window._agroMedSubTipsInited",
        "fecharAgroMedSubTip",
        "Escape",
    ):
        if needle not in modal:
            fail(f"modal sem `{needle}`")
        else:
            ok(f"modal `{needle[:40]}`")

    n_nivel = modal.count("data-agro-med-sub-nivel=")
    if n_nivel < 8:
        fail(f"data-agro-med-sub-nivel count={n_nivel}")
    else:
        ok(f"niveis html {n_nivel}")

    for i, short in (
        (1, "Grupo da prateleira"),
        (2, "Tipo principal"),
        (3, "Grupo específico / família"),
        (4, "Princípio ativo / detalhamento"),
    ):
        if short not in modal:
            fail(f"texto curto sub {i}")
        else:
            ok(f"texto sub {i}")

    for fid in ("edit-subcategoria", "edit-subcategoria-2", "edit-subcategoria-3", "edit-subcategoria-4"):
        if f'id="{fid}"' not in modal:
            fail(f"id ausente {fid}")
        else:
            ok(f"id {fid}")

    if "subcategoria_2: gv('edit-subcategoria-2')" not in modal:
        fail("payload subcategoria_2 ausente")
    else:
        ok("payload subcategorias intacto")

    if re.search(r"subcategoria_dica|med_sub_hint", modal, re.I):
        fail("nova chave no payload")
    else:
        ok("sem chave nova no body")

    caixa = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8", errors="replace")
    if "agro-med-sub" in caixa:
        fail("caixa_util tocado")
    else:
        ok("caixa isolado")

    if PIN_ENV:
        try:
            import django

            django.setup()
            from base.models import PerfilUsuario
            from django.contrib.auth import get_user_model
            from produtos.caixa_util import validar_pin_operador

            User = get_user_model()
            user, _ = User.objects.get_or_create(
                username="pin_prova_med_sub_dicas",
                defaults={"is_staff": True, "is_superuser": True, "is_active": True},
            )
            user.is_active = True
            user.save()
            perfil = PerfilUsuario.objects.filter(senha_rapida=PIN_ENV).first()
            if perfil:
                perfil.ativo = True
                perfil.save()
            if validar_pin_operador(PIN_ENV)[0]:
                ok(f"PIN {PIN_ENV}")
            else:
                fail("PIN inválido")
        except Exception as exc:  # noqa: BLE001
            fail(f"PIN: {exc!s}"[:80])
    else:
        print("  SKIP pin")

    # teste estático do arquivo de regressão
    reg = ROOT / "produtos/tests/test_med_classificacao_regressao.py"
    if "test_modal_subcategorias_med_dicas_ui_somente_visual" in reg.read_text(encoding="utf-8"):
        ok("teste regressao dicas")
    else:
        fail("teste regressao dicas ausente")

    print(f"\nVERIFY_SUB_DICAS_FAILS={fails}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
