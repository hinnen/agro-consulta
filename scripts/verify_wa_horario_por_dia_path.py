# -*- coding: utf-8 -*-
"""
WA-HORARIO-DIA — prova: horário de atendimento por dia da semana.

  python scripts/verify_wa_horario_por_dia_path.py
  PIN 9973 (Renan) · AGRO_PIN_TESTE opcional
"""
from __future__ import annotations

import copy
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()

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
    return (ROOT / rel).read_text(encoding="utf-8")


def prova_estatico() -> None:
    print("=== estatico ===")
    cfg = read("produtos/atendimento_whatsapp_bot_config.py")
    check('"horario_por_dia"' in cfg, "config default horario_por_dia")
    check("def normalizar_horario_por_dia" in cfg, "config normalizar_horario_por_dia")
    check("def _espelhar_horario_legado" in cfg, "config espelhar legado")
    check("hpd = normalizar_horario_por_dia(cfg)" in cfg, "fora_do_horario usa por dia")

    html = read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    check('id="wa-bot-horario-dias"' in html, "HTML container 7 dias")
    check('name="horario_ini"' not in html, "HTML sem Abre unico")
    check('name="horario_fim"' not in html, "HTML sem Fecha unico")
    check("Aviso fora do horario" in html.replace("horário", "horario") or "Aviso fora do horário" in html, "HTML aviso unico")

    js = read("produtos/static/produtos/js/atendimento_whatsapp_bot.js")
    check("function montarHorarioPorDia" in js, "JS montarHorarioPorDia")
    check("function lerHorarioPorDia" in js, "JS lerHorarioPorDia")
    check("o.horario_por_dia = lerHorarioPorDia()" in js, "JS coletar horario_por_dia")
    check("montarHorarioPorDia(bot.horario_por_dia" in js, "JS preencher horario_por_dia")

    skin = read("produtos/templates/produtos/_wa_skin.html")
    check(".wa-horario-dias" in skin, "CSS wa-horario-dias")


def prova_logica() -> None:
    print("=== logica fora_do_horario ===")
    import django

    django.setup()
    from django.utils import timezone

    from produtos.atendimento_whatsapp_bot_config import (
        BOT_DEFAULT,
        _merge,
        fora_do_horario,
        normalizar_horario_por_dia,
    )
    from produtos.caixa_util import rotulo_operador_pin

    rot = (rotulo_operador_pin(PIN) or "").strip()
    check(bool(rot), f"PIN {PIN} rotulo={rot!r}")

    domingo = timezone.make_aware(datetime(2026, 9, 6, 10, 0, 0))
    segunda = timezone.make_aware(datetime(2026, 9, 7, 12, 0, 0))
    sabado_12 = timezone.make_aware(datetime(2026, 9, 5, 12, 0, 0))
    sabado_13 = timezone.make_aware(datetime(2026, 9, 5, 13, 0, 0))

    check(fora_do_horario(BOT_DEFAULT, domingo) is True, "default domingo fora")
    check(fora_do_horario(BOT_DEFAULT, segunda) is False, "default segunda 12h dentro")

    cfg = copy.deepcopy(BOT_DEFAULT)
    cfg["horario_por_dia"]["6"] = {"ativo": True, "ini": "08:00", "fim": "12:00"}
    check(fora_do_horario(cfg, sabado_12) is False, "sabado 12h ainda dentro (fim=12)")
    check(fora_do_horario(cfg, sabado_13) is True, "sabado 13h fora")
    check(fora_do_horario(cfg, segunda) is False, "segunda 12h dentro (18h)")

    legado = _merge(
        BOT_DEFAULT,
        {"horario_ini": "08:00", "horario_fim": "18:00", "horario_dias": [1, 2, 3, 4, 5, 6]},
    )
    hpd = normalizar_horario_por_dia(legado)
    check(hpd["0"]["ativo"] is False, "legado domingo off")
    check(hpd["6"]["ativo"] is True, "legado sabado on")
    check(legado["horario_dias"] == [1, 2, 3, 4, 5, 6], "espelha dias 1-6")


def main() -> int:
    prova_estatico()
    prova_logica()
    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
