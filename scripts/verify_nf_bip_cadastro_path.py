#!/usr/bin/env python
"""Prova NF-BIP-CAD — etapa 3 grava EAN no cadastro (regra B). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAIL: list[str] = []
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" — {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8").replace("\r\n", "\n")


def _fn_body(src: str, name: str) -> str:
    m = re.search(rf"^def {re.escape(name)}\b", src, re.M)
    if not m:
        return ""
    # Assinatura pode ser multilinha até o primeiro «):» / «:\n» do corpo.
    rest = src[m.start() :]
    body_start = re.search(r"\)\s*(?:->[^:]+)?:\n", rest)
    if not body_start:
        return ""
    after = rest[body_start.end() :]
    nxt = re.search(r"^def \w+", after, re.M)
    end = body_start.end() + (nxt.start() if nxt else len(after))
    return rest[:end]


def main() -> None:
    import django

    django.setup()

    print("== Markers backend ==")
    mic = _read("produtos/mongo_index_codigos.py")
    views = _read("produtos/views.py")
    cmd = _read("produtos/management/commands/contar_bip_entrada_nf_cadastro.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")

    troca = _fn_body(mic, "aplicar_bip_entrada_nf_troca_inteligente")
    check("fn_troca_inteligente", bool(troca), f"chars={len(troca)}")
    check("troca_promove_230", "eh_codigo_barras_loja" in troca and "promove" in troca)
    check("troca_opcional", '"opcional"' in troca)
    check("mesclar_fn", "def mesclar_codigos_barras_opcionais_adicionar" in mic)

    check("views_payload_bip", "codigo_barras_bip_entrada_nf" in views)
    check("views_chama_troca", "aplicar_bip_entrada_nf_troca_inteligente" in views)
    check("views_conflito_outro", "conflito" in views and "promover = False" in views)
    check("views_merge_add", "codigos_barras_opcionais_adicionar" in views)

    check("cmd_fonte_ean", "--fonte" in cmd and "ean" in cmd)
    check("cmd_somente_real", "somente-ean-real" in cmd or "somente_ean_real" in cmd)
    check("cmd_parece_ean", "def parece_ean_fabrica_br" in cmd)
    check("cmd_aplicar", "--aplicar" in cmd)

    print("== Markers UI etapa 3 ==")
    check("ui_campo_principal", 'id="nfe-bip-cod"' in html)
    check("ui_sem_campo_aux_html", 'id="nfe-bip-cod-alt"' not in html)
    check("ui_sem_details_auxiliar", "Código auxiliar (marca paralela)" not in html)
    check(
        "ui_grava_bip_api",
        "codigo_barras_bip_entrada_nf" in html
        and "entradaNfeLembrarBarrasOpcionaisAposBip" in html,
    )
    check(
        "ui_chama_ok_e_similar",
        html.count("entradaNfeLembrarBarrasOpcionaisAposBip") >= 2,
    )
    check("ui_origem_nf", "origem_entrada_nf: true" in html)

    print("== Unit rules ==")
    from produtos.agro_codigo_barras_loja_util import eh_codigo_barras_loja
    from produtos.management.commands.contar_bip_entrada_nf_cadastro import (
        parece_ean_fabrica_br,
    )
    from produtos.mongo_index_codigos import aplicar_bip_entrada_nf_troca_inteligente

    check("eh_230", eh_codigo_barras_loja("2300000001490"))
    check("nao_eh_789_loja", not eh_codigo_barras_loja("7898752405197"))

    r1 = aplicar_bip_entrada_nf_troca_inteligente(
        codigo_barras_atual="2300000001490",
        cadastro_extras={},
        bip="7898752405197",
    )
    check("rule_promove", r1.get("acao") == "promove", str(r1.get("acao")))
    check("rule_promove_principal", r1.get("codigo_barras") == "7898752405197")
    check("rule_promove_230_opc", "2300000001490" in (r1.get("codigos_barras_opcionais") or []))

    r2 = aplicar_bip_entrada_nf_troca_inteligente(
        codigo_barras_atual="7891111111111",
        cadastro_extras={"codigos_barras_opcionais": ["7892222222222"]},
        bip="7898752405197",
    )
    check("rule_opc", r2.get("acao") == "opcional")
    check(
        "rule_opc_merge",
        set(r2.get("codigos_barras_opcionais") or [])
        >= {"7892222222222", "7898752405197"},
    )

    r3 = aplicar_bip_entrada_nf_troca_inteligente(
        codigo_barras_atual="2300000001490",
        cadastro_extras={},
        bip="7898752405197",
        promover_se_loja=False,
    )
    check("rule_conflito_so_opc", r3.get("acao") == "opcional")

    check("filtro_789_ok", parece_ean_fabrica_br("7898242031950"))
    check("filtro_111_lixo", not parece_ean_fabrica_br("1111111111111"))
    check("filtro_300_lixo", not parece_ean_fabrica_br("3000000052600"))

    print("== Django tests ==")
    proc = subprocess.run(
        [
            sys.executable,
            "manage.py",
            "test",
            "produtos.tests_codigos_barras_opcionais",
            "-v0",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    check("django_tests_exit0", proc.returncode == 0, out[-400:].replace("\n", " "))
    m = re.search(r"Ran (\d+) test", out)
    n = int(m.group(1)) if m else 0
    check("django_tests_count", n >= 13, f"ran={n}")

    print("")
    total = OK + len(FAIL)
    if FAIL:
        print(f"VERIFY_FAIL {OK}/{total}")
        for f in FAIL:
            print(f"  - {f}")
        sys.exit(1)
    print(f"VERIFY_OK {OK}/{total}")
    sys.exit(0)


if __name__ == "__main__":
    main()
