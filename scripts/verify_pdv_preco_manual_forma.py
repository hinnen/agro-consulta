#!/usr/bin/env python3
"""Prova detalhada do path PDV-PRECO-MANUAL-FORMA.

Cobre: fonte JS (teste), regressao tipica da loja, path lista->digita->forma,
carrinho misto, troca de forma, qtd>1, campanha, precos_forma, consulta legado,
envio (precoEnvioItem). Nao sobe servidor.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JS_PROMO = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_promocoes.js"
JS_STATE = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_state.js"
JS_CAMP = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_campanha.js"
JS_FORMA = ROOT / "produtos" / "static" / "produtos" / "js" / "precos_forma_pagamento.js"
JS_CONSULTA = ROOT / "produtos" / "static" / "produtos" / "js" / "consulta_produtos.js"

PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    safe = msg.encode("ascii", "replace").decode("ascii")
    if ok:
        PASS += 1
        print(f"  OK  {safe}")
    else:
        FAIL += 1
        print(f" FAIL {safe}")


def to_num(v, fallback=0.0) -> float:
    try:
        n = float(v)
        return n if n == n else float(fallback)
    except (TypeError, ValueError):
        return float(fallback)


def approx(a, b, eps=0.001) -> bool:
    return abs(to_num(a) - to_num(b)) < eps


# --- Espelhos do fix (teste) ---


def aplicar_no_item_fix(item: dict, forma: str = "") -> None:
    if item.get("preco_manual"):
        item["preco_pos_promo"] = to_num(item.get("preco"), 0)
        item["preco_base_forma"] = to_num(item.get("preco"), 0)
        return
    if item.get("preco_pos_promo") is not None:
        item["preco"] = to_num(item["preco_pos_promo"], item.get("preco"))
    item["preco_base_forma"] = to_num(item.get("preco"), 0)
    _ = forma


def aplicar_no_item_loja_bug(item: dict) -> None:
    """Comportamento ainda em origin/producao (restaura cache)."""
    if item.get("preco_manual"):
        if item.get("preco_pos_promo") is not None:
            item["preco"] = to_num(item["preco_pos_promo"], item.get("preco"))
        item["preco_base_forma"] = to_num(item.get("preco"), 0)
        return
    item["preco_base_forma"] = to_num(item.get("preco"), 0)


def update_item_price_fix(item: dict, preco: float) -> None:
    item["preco"] = float(preco)
    item["preco_manual"] = True
    item["preco_pos_promo"] = float(preco)
    item["preco_base_forma"] = float(preco)
    item["preco_grupo_preview"] = ""


def update_item_price_loja_bug(item: dict, preco: float) -> None:
    """updateItemPrice da loja: so marca manual, nao alinha cache."""
    item["preco"] = float(preco)
    item["preco_manual"] = True
    item["preco_grupo_preview"] = ""


def aplicar_campanha_fix(itens: list, ativa: bool = False, fator: float = 0.95) -> None:
    for item in itens:
        if not item:
            continue
        if item.get("preco_manual"):
            item["preco_pos_promo"] = to_num(item.get("preco"), 0)
            item.pop("campanha_id", None)
            item.pop("campanha_pct", None)
            item.pop("campanha_usou", None)
            continue
        apos = to_num(item.get("preco"), 0)
        item["preco_pos_promo"] = apos
        if ativa:
            base = to_num(item.get("preco_base_forma"), apos)
            com = round(base * fator * 10000) / 10000
            final = apos
            usou = False
            if com > 0 and (final <= 0 or com < final):
                final = com
                usou = True
            item["preco"] = round(final * 100) / 100
            item["campanha_id"] = "camp-test"
            item["campanha_usou"] = "campanha" if usou else "promo_ou_lista"


def recalc_com_forma_fix(itens: list, forma: str, campanha: bool = False) -> None:
    for item in itens:
        if item:
            aplicar_no_item_fix(item, forma)
    for item in itens:
        if not item:
            continue
        item.pop("campanha_id", None)
        item.pop("campanha_pct", None)
        item.pop("campanha_usou", None)
        item.pop("preco_pos_promo", None)
    aplicar_campanha_fix(itens, ativa=campanha)


def preco_envio_fix(item: dict, campanha_ativa: bool = False) -> float:
    if not item:
        return 0.0
    if item.get("preco_manual"):
        return to_num(item.get("preco"), 0)
    if campanha_ativa and item.get("preco_pos_promo") is not None:
        return to_num(item["preco_pos_promo"], to_num(item.get("preco"), 0))
    return to_num(item.get("preco"), 0)


def block_around(src: str, needle: str, before: int = 0, after: int = 400) -> str:
    i = src.find(needle)
    if i < 0:
        return ""
    return src[max(0, i - before) : i + after]


def main() -> int:
    promo = JS_PROMO.read_text(encoding="utf-8")
    state = JS_STATE.read_text(encoding="utf-8")
    camp = JS_CAMP.read_text(encoding="utf-8")
    forma = JS_FORMA.read_text(encoding="utf-8")
    consulta = JS_CONSULTA.read_text(encoding="utf-8")

    print("== 1. Fonte JS (guards do fix) ==")
    recalc_blk = block_around(promo, "function recalcCarrinhoComForma", after=900)
    aplicar_blk = block_around(promo, "function aplicarNoItem", after=700)
    update_blk = block_around(state, "function updateItemPrice", after=900)
    camp_aplicar = block_around(camp, "function aplicarNosItens", after=900)
    camp_envio = block_around(camp, "function precoEnvioItem", after=250)
    update_m = re.search(
        r"function updateItemPrice\([\s\S]{0,800}?preco_manual:\s*true[\s\S]{0,400}?preco_pos_promo:\s*p[\s\S]{0,200}?preco_base_forma:\s*p",
        state,
    )

    check("if (item.preco_manual)" in recalc_blk, "recalcCarrinhoComForma: ramo preco_manual")
    check(
        "preco_pos_promo = toNum(item.preco" in recalc_blk
        and "preco = toNum(item.preco_pos_promo" not in block_around(
            recalc_blk, "if (item.preco_manual)", after=350
        ),
        "recalc: manual sincroniza cache, NAO restaura de pos_promo",
    )
    check("if (item.preco_manual)" in aplicar_blk, "aplicarNoItem: ramo preco_manual")
    check(
        re.search(
            r"preco_manual[\s\S]{0,220}preco\s*=\s*toNum\(item\.preco_pos_promo",
            aplicar_blk,
        )
        is None,
        "aplicarNoItem: nao restaura preco de pos_promo se manual",
    )
    check(update_m is not None, "updateItemPrice: grava manual + pos_promo + base_forma")
    check("if (item.preco_manual)" in camp_aplicar, "campanha.aplicarNosItens: pula manual")
    check(
        "if (item.preco_manual) return toNum(item.preco, 0)" in camp_envio,
        "precoEnvioItem: manual usa item.preco",
    )
    check(
        "if (!item || item.preco_manual) return item;" in forma
        and forma.count("item.preco_manual") >= 3,
        "precos_forma_pagamento: guards manual (>=3)",
    )
    check(
        "if (!item || item.preco_manual) return item;" in consulta
        or "if (item.preco_manual) return;" in consulta,
        "consulta_produtos: guard preco_manual",
    )

    print("== 2. Path principal (lista 25 -> digita 32 -> debito) ==")
    item = {
        "id": "p1",
        "qtd": 1,
        "preco": 25.0,
        "preco_padrao": 25.0,
        "preco_pos_promo": 25.0,
        "preco_base_forma": 25.0,
    }
    update_item_price_fix(item, 32.0)
    check(approx(item["preco"], 32) and item["preco_manual"] is True, "apos digitar: 32 + flag")
    check(
        approx(item["preco_pos_promo"], 32) and approx(item["preco_base_forma"], 32),
        "apos digitar: caches = 32",
    )
    check(approx(item["preco"] * item["qtd"], 32), "modal como pagar: total 32")
    recalc_com_forma_fix([item], "debito")
    check(approx(item["preco"], 32), "apos debito: continua 32 (nao 25)")
    check(item.get("preco_manual") is True, "apos debito: flag permanece")
    check(approx(item.get("preco_pos_promo"), 32), "apos debito+campanha off: cache 32")

    print("== 3. Formas / troca / qtd ==")
    for forma_nome in ("pix", "credito", "dinheiro", "fiado"):
        it = {
            "id": "f",
            "qtd": 1,
            "preco": 25.0,
            "preco_pos_promo": 25.0,
            "preco_base_forma": 25.0,
        }
        update_item_price_fix(it, 40.5)
        recalc_com_forma_fix([it], forma_nome)
        check(approx(it["preco"], 40.5), f"forma {forma_nome}: fica 40,50")

    it2 = {
        "id": "t",
        "qtd": 1,
        "preco": 10.0,
        "preco_pos_promo": 10.0,
        "preco_base_forma": 10.0,
    }
    update_item_price_fix(it2, 15.0)
    recalc_com_forma_fix([it2], "debito")
    recalc_com_forma_fix([it2], "pix")
    recalc_com_forma_fix([it2], "credito")
    check(approx(it2["preco"], 15) and it2["preco_manual"], "troca debito->pix->credito: fica 15")

    itq = {
        "id": "q",
        "qtd": 3,
        "preco": 20.0,
        "preco_pos_promo": 20.0,
        "preco_base_forma": 20.0,
    }
    update_item_price_fix(itq, 11.0)
    recalc_com_forma_fix([itq], "debito")
    check(approx(itq["preco"], 11) and approx(itq["preco"] * itq["qtd"], 33), "qtd 3 x 11 = 33 apos forma")

    print("== 4. Carrinho misto (1 manual + 1 lista) ==")
    man = {
        "id": "m",
        "qtd": 1,
        "preco": 50.0,
        "preco_padrao": 50.0,
        "preco_pos_promo": 50.0,
        "preco_base_forma": 50.0,
    }
    update_item_price_fix(man, 60.0)
    auto = {
        "id": "a",
        "qtd": 1,
        "preco": 50.0,
        "preco_padrao": 50.0,
        "preco_pos_promo": 50.0,
        "preco_base_forma": 50.0,
        "preco_manual": False,
    }
    # simula forma debito com tabela: auto cairia p/ 55 se houvesse regra;
    # sem precos_por_forma no espelho, auto so passa pelo ramo nao-manual
    # (espelho nao muda preco do auto sem tabela — so garante manual intacto)
    recalc_com_forma_fix([man, auto], "debito")
    check(approx(man["preco"], 60) and man["preco_manual"], "misto: manual fica 60")
    check(approx(auto["preco"], 50) and not auto.get("preco_manual"), "misto: auto segue caminho normal")

    print("== 5. Campanha ativa nao come preco digitado ==")
    camp_it = {
        "id": "c",
        "qtd": 1,
        "preco": 100.0,
        "preco_pos_promo": 100.0,
        "preco_base_forma": 100.0,
    }
    update_item_price_fix(camp_it, 80.0)
    recalc_com_forma_fix([camp_it], "debito", campanha=True)
    check(approx(camp_it["preco"], 80), "campanha ON: manual nao vira 76 (5%)")
    check(camp_it.get("campanha_id") is None, "campanha ON: sem marca em item manual")
    check(approx(preco_envio_fix(camp_it, True), 80), "envio: manual manda 80 mesmo com campanha")

    print("== 6. Contraste loja (bug ainda em producao) ==")
    loja = {
        "id": "loja",
        "qtd": 1,
        "preco": 25.0,
        "preco_pos_promo": 25.0,
        "preco_base_forma": 25.0,
    }
    update_item_price_loja_bug(loja, 32.0)
    check(approx(loja["preco"], 32) and approx(loja["preco_pos_promo"], 25), "loja: digita 32 mas cache fica 25")
    aplicar_no_item_loja_bug(loja)
    check(approx(loja["preco"], 25), "loja: ao escolher forma VOLTA 25 (bug documentado)")

    fix = {
        "id": "fix",
        "qtd": 1,
        "preco": 32.0,
        "preco_manual": True,
        "preco_pos_promo": 25.0,  # cache sujo hipotetico
        "preco_base_forma": 25.0,
    }
    aplicar_no_item_fix(fix, "pix")
    check(approx(fix["preco"], 32) and approx(fix["preco_pos_promo"], 32), "fix: cache sujo reescrito, preco 32")

    print("== 7. JS syntax (node --check) ==")
    for path in (JS_PROMO, JS_STATE, JS_CAMP, JS_FORMA):
        try:
            r = subprocess.run(
                ["node", "--check", str(path)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            check(r.returncode == 0, f"syntax OK {path.name}")
            if r.returncode != 0:
                print((r.stderr or r.stdout or "")[:200])
        except FileNotFoundError:
            check(False, f"node nao encontrado p/ {path.name}")
        except Exception as e:
            check(False, f"syntax {path.name}: {e}")

    print("== 8. Confirmacao git: fix no teste, nao na loja ==")
    try:
        r1 = subprocess.run(
            ["git", "merge-base", "--is-ancestor", "523c06a", "origin/teste"],
            cwd=ROOT,
            capture_output=True,
        )
        r2 = subprocess.run(
            ["git", "merge-base", "--is-ancestor", "523c06a", "origin/producao"],
            cwd=ROOT,
            capture_output=True,
        )
        check(r1.returncode == 0, "commit 523c06a ancestral de origin/teste")
        check(r2.returncode != 0, "commit 523c06a AINDA NAO esta em origin/producao")
        # loja ainda tem o padrao bug
        r3 = subprocess.run(
            ["git", "show", "origin/producao:produtos/static/produtos/js/pdv_promocoes.js"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        loja_js = r3.stdout or ""
        bug_loja = re.search(
            r"preco_manual[\s\S]{0,280}preco\s*=\s*toNum\(item\.preco_pos_promo",
            loja_js,
        )
        check(bug_loja is not None, "loja: ainda restaura preco_pos_promo (bug vivo)")
        bug_teste = re.search(
            r"preco_manual[\s\S]{0,280}preco\s*=\s*toNum\(item\.preco_pos_promo",
            promo,
        )
        check(bug_teste is None, "teste working tree: padrao do bug ausente")
    except Exception as e:
        check(False, f"git checks: {e}")

    print(f"\nResultado: {PASS} ok, {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
