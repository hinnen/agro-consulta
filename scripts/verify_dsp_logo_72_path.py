# -*- coding: utf-8 -*-
"""
Prova — Dispenser A6 aceita 72 logos (`DSP-LOGO-72`).

Tela e sistema no mesmo teto. Pet e ingrediente continuam em 24.
O teste grava logos de prova e desfaz no mesmo passo.

  python scripts/verify_dsp_logo_72_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []

TINY = (
    "data:image/png;base64,"
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)
PREFIX = "verify-dsp-logo-72-"


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_arquivos() -> None:
    print("== Tela e teto no sistema ==")
    html = (ROOT / "produtos/templates/produtos/dispenser_a6_studio.html").read_text(encoding="utf-8")
    util = (ROOT / "produtos/dispenser_a6_util.py").read_text(encoding="utf-8")
    view = (ROOT / "produtos/views_dispenser_a6.py").read_text(encoding="utf-8")
    cloud = (ROOT / "produtos/static/produtos/dispenser-a6/js/dispenser_cloud.js").read_text(encoding="utf-8")

    check("tela_logo_72", "var LOGO_MAX_CUSTOM = 72;" in html)
    check("tela_ainda_nao_24", "var LOGO_MAX_CUSTOM = 24;" not in html)
    check("pet_continua_24", "var PET_MAX_CUSTOM = 24;" in html)
    check("ing_continua_24", "var ING_MAX_CUSTOM = 24;" in html)
    bloco = html[html.find('getElementById("dspLogoFile")') : html.find('getElementById("dspLogoFile")') + 900]
    check("tela_barra_no_teto", "customs.length >= LOGO_MAX_CUSTOM" in bloco)
    check("aviso_usa_o_teto", "Limite de \" + LOGO_MAX_CUSTOM + \" logos neste computador." in bloco)
    check("sistema_logo_72", "DispenserMidiaAgro.TIPO_LOGO: 72," in util)
    check("sistema_pet_24", "DispenserMidiaAgro.TIPO_PET: 24," in util)
    check("sistema_ing_24", "DispenserMidiaAgro.TIPO_ING: 24," in util)
    check("sistema_icone_80", "DispenserMidiaAgro.TIPO_FLAVOR_ICO: 80," in util)
    check("api_usa_o_teto", "upsert_midia(" in view and "util.upsert_midia" in view)
    check("nuvem_manda_o_logo", 'cloudPushMidia("logo"' in html and "upsertMidia" in cloud)


def _host_local() -> tuple[bool, str]:
    from django.conf import settings

    url = str(settings.DATABASES["default"].get("NAME") or "")
    engine = str(settings.DATABASES["default"].get("ENGINE") or "")
    host = str(settings.DATABASES["default"].get("HOST") or "")
    if "sqlite" in engine:
        return True, "sqlite local"
    remoto = ("render.com", "amazonaws", "neon.tech")
    h = (host or "").lower()
    if any(x in h for x in remoto):
        return False, h or "?"
    parsed = urlparse(url) if "://" in url else None
    if parsed and any(x in (parsed.hostname or "") for x in remoto):
        return False, parsed.hostname or "?"
    return True, host or "local"


def test_banco() -> None:
    print("== Grava e desfaz ==")
    import django

    django.setup()
    from django.db import transaction

    from produtos.dispenser_a6_util import (
        MAX_POR_TIPO,
        delete_midia,
        listar_biblioteca,
        upsert_midia,
    )
    from produtos.models import DispenserMidiaAgro

    local, onde = _host_local()
    check("banco_local", local, onde)
    if not local:
        check("nao_gravou_fora", True, "pulado — banco de fora")
        return

    antes = DispenserMidiaAgro.objects.filter(tipo=DispenserMidiaAgro.TIPO_LOGO).count()
    pets_antes = DispenserMidiaAgro.objects.filter(tipo=DispenserMidiaAgro.TIPO_PET).count()

    with transaction.atomic():
        limites = listar_biblioteca().get("limites", {}).get("midia", {})
        check("lista_diz_72", limites.get("logo") == 72, str(limites.get("logo")))
        check("lista_pet_24", limites.get("pet") == 24)
        check("constante_72", MAX_POR_TIPO[DispenserMidiaAgro.TIPO_LOGO] == 72)

        n = DispenserMidiaAgro.objects.filter(tipo="logo").count()
        falta = 72 - n
        criados = 0
        erro_meio = ""
        for i in range(max(0, falta)):
            row, err = upsert_midia(
                tipo="logo",
                item_id=f"{PREFIX}{i}",
                label=f"prova {i}",
                data_url=TINY,
            )
            if not row:
                erro_meio = err
                break
            criados += 1
        check("enche_ate_72", not erro_meio and criados == max(0, falta), erro_meio or f"+{criados}")

        cheio = DispenserMidiaAgro.objects.filter(tipo="logo").count()
        check("contagem_72", cheio == 72, str(cheio))

        extra, err_extra = upsert_midia(
            tipo="logo",
            item_id=f"{PREFIX}extra",
            label="demais",
            data_url=TINY,
        )
        check("o_73_nao_entra", extra is None and "72" in (err_extra or ""), err_extra)

        primeiro = DispenserMidiaAgro.objects.filter(tipo="logo").order_by("item_id").first()
        upd, err_upd = upsert_midia(
            tipo="logo",
            item_id=primeiro.item_id if primeiro else "",
            label="prova atualiza",
            data_url=TINY,
        )
        ainda = DispenserMidiaAgro.objects.filter(tipo="logo").count()
        check("atualizar_nao_estoura", upd is not None and ainda == 72, err_upd or str(ainda))

        vazio, err_vazio = upsert_midia(tipo="logo", item_id=f"{PREFIX}vazio", label="x", data_url="")
        check("logo_vazio_recusa", vazio is None and "vazia" in (err_vazio or "").lower(), err_vazio)

        gigante = "data:image/png;base64," + ("A" * 1_700_001)
        grande, err_grande = upsert_midia(
            tipo="logo",
            item_id=f"{PREFIX}grande",
            label="grande",
            data_url=gigante,
        )
        check("logo_grande_recusa", grande is None and "grande" in (err_grande or "").lower(), err_grande[:80])

        if criados:
            ok_del, err_del = delete_midia(tipo="logo", item_id=f"{PREFIX}0")
            depois_del = DispenserMidiaAgro.objects.filter(tipo="logo").count()
            volta, err_volta = upsert_midia(
                tipo="logo",
                item_id=f"{PREFIX}volta",
                label="volta",
                data_url=TINY,
            )
            check("apagar_abre_vaga", ok_del and volta is not None and depois_del == 71, err_del or err_volta)

        pet_n = DispenserMidiaAgro.objects.filter(tipo="pet").count()
        pet_falhou = False
        pet_msg = ""
        i = 0
        while pet_n < 24 and i < 30:
            row, err = upsert_midia(tipo="pet", item_id=f"{PREFIX}pet-{i}", label="pet", data_url=TINY)
            if not row:
                pet_falhou = True
                pet_msg = err
                break
            pet_n += 1
            i += 1
        pet_extra, pet_err = upsert_midia(
            tipo="pet",
            item_id=f"{PREFIX}pet-extra",
            label="pet extra",
            data_url=TINY,
        )
        check(
            "pet_continua_24",
            (not pet_falhou) and pet_extra is None and "24" in (pet_err or ""),
            pet_msg or pet_err,
        )

        transaction.set_rollback(True)

    ficou = DispenserMidiaAgro.objects.filter(tipo=DispenserMidiaAgro.TIPO_LOGO).count()
    pets_ficou = DispenserMidiaAgro.objects.filter(tipo=DispenserMidiaAgro.TIPO_PET).count()
    resto = DispenserMidiaAgro.objects.filter(item_id__startswith=PREFIX).count()
    check("logos_voltaram", ficou == antes, f"antes {antes} · agora {ficou}")
    check("pets_voltaram", pets_ficou == pets_antes, f"antes {pets_antes} · agora {pets_ficou}")
    check("nada_de_prova_ficou", resto == 0, str(resto))


def main() -> int:
    test_arquivos()
    test_banco()
    print(f"\n{len(oks)} ok · {len(fails)} falha")
    if fails:
        for nome in fails:
            print(f"  - {nome}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
