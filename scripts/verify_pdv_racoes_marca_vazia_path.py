# -*- coding: utf-8 -*-
"""PDV-RACOES-MARCA-VAZIA — prova detalhada do path.

Marca só aparece se houver ração do tipo com peso reconhecido.
Cadastrou peso depois → marca volta. Vale para todos os tipos.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

FAILS: list[str] = []
OKS = 0
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _row(**kw):
    base = {
        "id": "p1",
        "nome": "Racao teste",
        "categoria": "Rações",
        "subcategoria": "Cão",
        "subcategoria_2": "Sênior",
        "marca": "ESTIMACAO",
        "peso_etiqueta": "15",
        "inativo": False,
    }
    base.update(kw)
    return base


def prova_estatico() -> None:
    print("=== Path estatico ===")
    util = read("produtos/pdv_racoes_util.py")
    js = read("produtos/static/produtos/js/pdv_wizard.js")
    tests = read("produtos/tests_pdv_racoes.py")
    verify = read("scripts/verify_pdv_racoes.py")
    wiz = read("produtos/templates/produtos/pdv_wizard.html")

    check("def marcas_racoes_do_tipo" in util, "util marcas_racoes_do_tipo")
    check("peso_key=None" in util.split("def marcas_racoes_do_tipo")[1][:400], "util marcas filtra peso")
    check("pesoKey === undefined || pesoKey === null) return !!parsed" in js, "JS exige peso reconhecido")
    check("pesoKey === undefined) return true" not in js, "JS nao libera peso vazio")
    check("pdvRacoesFiltrar(tipo, undefined, null)" in js, "lista marcas usa filtro peso")
    check("pdvRacoesRenderMarcas(pdvRacoesSel.tipo)" in js, "sync re-render marcas")
    check("pdvRacoesRenderPesos(pdvRacoesSel.tipo, pdvRacoesSel.marca)" in js, "sync re-render pesos")
    check("test_marca_sem_peso_nao_aparece" in tests, "teste unitario marca vazia")
    check("marca sem peso some" in verify, "verify_pdv_racoes cobre marca vazia")
    check("marca volta apos cadastrar peso" in verify, "verify_pdv_racoes cobre volta")
    check('id="pdv-racoes-marcas-grid"' in wiz, "HTML grid marcas")
    check('id="pdv-racoes-todas-marcas"' in wiz, "HTML todas marcas")
    check("api/pdv/racoes-overlay/" in read("produtos/urls.py"), "rota overlay vivo")


def prova_logica() -> None:
    print("=== Logica util ===")
    from produtos.pdv_racoes_util import (
        TIPOS_RACOES,
        filtrar_racoes,
        marcas_racoes_do_tipo,
        parse_peso_racoes,
        tipo_racoes_por_id,
    )

    check(len(TIPOS_RACOES) == 8, "8 tipos")
    senior = tipo_racoes_por_id("cao_senior")
    rows = [
        _row(id="vazio", marca="GRAN PLUS", peso_etiqueta=""),
        _row(id="ruim", marca="ORIGENS", peso_etiqueta="7"),
        _row(id="ok", marca="ESTIMACAO", peso_etiqueta="15"),
        _row(id="ok2", marca="ESTIMACAO", peso_etiqueta="10"),
        _row(id="outro", marca="GOLDEN", subcategoria_2="Adulto", peso_etiqueta="15"),
        _row(id="morto", marca="PREMIER", peso_etiqueta="20", inativo=True),
    ]
    marcas = marcas_racoes_do_tipo(rows, senior)
    check(marcas == ["ESTIMACAO"], f"senior so ESTIMACAO (got {marcas})")
    check("GRAN PLUS" not in marcas, "GRAN PLUS sem peso some")
    check("ORIGENS" not in marcas, "ORIGENS peso 7 some")
    check("GOLDEN" not in marcas, "GOLDEN de outro sub2 some")
    check("PREMIER" not in marcas, "inativo some")

    rows[0]["peso_etiqueta"] = "10"
    marcas2 = marcas_racoes_do_tipo(rows, senior)
    check(marcas2 == ["ESTIMACAO", "GRAN PLUS"], f"GRAN PLUS volta (got {marcas2})")

    # Todas as marcas / todos tamanhos = so com peso
    todas = filtrar_racoes(rows, senior, marca=None, peso_key=None)
    ids = [r["id"] for r in todas]
    check("vazio" in ids and "ok" in ids and "ok2" in ids, "todas com peso inclui GRAN+EST")
    check("ruim" not in ids and "morto" not in ids and "outro" not in ids, "todas exclui invalidos")

    # Marca especifica sem peso → lista vazia
    check(filtrar_racoes([_row(marca="X", peso_etiqueta="")], senior, marca="X", peso_key=None) == [], "marca X sem peso = zero")

    # Pesos reconhecidos
    for raw, key in (("1", "kg:1"), ("2,5", "kg:2.5"), ("pacote", "pacote"), ("25", "kg:25")):
        check(parse_peso_racoes(raw) == key, f"parse {raw}={key}")

    # Cada tipo: marca so do proprio tipo
    for t in TIPOS_RACOES:
        tipo = tipo_racoes_por_id(t["id"])
        mix = [
            _row(
                id=f"{t['id']}_ok",
                subcategoria=t["sub1"],
                subcategoria_2=t["sub2"],
                marca="MARCAOK",
                peso_etiqueta="15",
            ),
            _row(
                id=f"{t['id']}_vazio",
                subcategoria=t["sub1"],
                subcategoria_2=t["sub2"],
                marca="MARCAVAZIA",
                peso_etiqueta="",
            ),
        ]
        got = marcas_racoes_do_tipo(mix, tipo)
        check(got == ["MARCAOK"], f"tipo {t['id']} so MARCAOK")


def prova_js_parity() -> None:
    print("=== Paridade JS ===")
    js = read("produtos/static/produtos/js/pdv_wizard.js")
    chunk = js.split("function pdvRacoesPassaPeso")[1].split("function pdvRacoesFiltrar")[0]
    check("return !!parsed" in chunk, "PassaPeso undefined/null = !!parsed")
    marcas_fn = js.split("function pdvRacoesMarcasDoTipo")[1].split("function pdvRacoesRenderMarcas")[0]
    check("pdvRacoesFiltrar(tipo, undefined, null)" in marcas_fn, "MarcasDoTipo filtra peso")
    seguir = js.split("function pdvRacoesSeguirMarca")[1].split("function pdvRacoesIrPeso")[0]
    check("pdvRacoesFiltrar(tipo, undefined, null)" in seguir, "SeguirMarca conta com peso")
    ir = js.split("function pdvRacoesIrMarca")[1].split("function pdvRacoesSeguirMarca")[0]
    check("pdvRacoesFiltrar(tipo, undefined, null)" in ir, "IrMarca conta com peso")
    pesos = js.split("function pdvRacoesPesosDisponiveis")[1].split("function pdvRacoesRenderPesos")[0]
    check("pdvRacoesFiltrar(tipo, marca)" in pesos, "PesosDisponiveis por marca")
    # PesosDisponiveis chama Filtrar sem 3º arg → undefined → PassaPeso exige parsed
    check("pdvRacoesParsePeso(p.peso_etiqueta)" in pesos, "PesosDisponiveis parse peso")


def prova_django_pin_e_dados() -> None:
    print(f"=== Django / PIN {PIN} / dados vivos ===")
    import django

    django.setup()
    from django.contrib.auth import get_user_model

    from produtos.caixa_util import operador_label_de_pin
    from produtos.pdv_racoes_util import (
        TIPOS_RACOES,
        filtrar_racoes,
        listar_patches_racoes_pdv,
        marcas_racoes_do_tipo,
        parse_peso_racoes,
        produto_passa_tipo_racoes,
        tipo_racoes_por_id,
    )

    User = get_user_model()
    check(User.objects.filter(is_active=True).exists(), "tem usuario ativo")

    ok_pin, label, err = operador_label_de_pin(PIN)
    if not ok_pin:
        fail(f"PIN {PIN}: {err}")
    else:
        ok(f"PIN {PIN} -> {label}")

    patches = listar_patches_racoes_pdv()
    check(isinstance(patches, list), "listar_patches_racoes_pdv retorna lista")
    check(len(patches) > 0, f"patches vivos > 0 (got {len(patches)})")

    # Invariante: toda marca listada tem >=1 produto com peso
    for t in TIPOS_RACOES:
        tipo = tipo_racoes_por_id(t["id"])
        marcas = marcas_racoes_do_tipo(patches, tipo)
        for m in marcas:
            hit2 = filtrar_racoes(patches, tipo, marca=m if m else "", peso_key=None)
            check(len(hit2) >= 1, f"vivo {t['id']}/{m or 'SEM'} tem peso")

        # Marcas "fantasma": mesmo tipo mas sem peso → nao podem estar na lista
        fantasmas = set()
        for p in patches:
            if not produto_passa_tipo_racoes(p, tipo):
                continue
            if parse_peso_racoes(p.get("peso_etiqueta")):
                continue
            fantasmas.add((p.get("marca") or "").strip())
        for fm in fantasmas:
            check(fm not in marcas, f"vivo {t['id']}: fantasma '{fm or 'SEM'}' nao na lista")

    senior = tipo_racoes_por_id("cao_senior")
    marcas_senior = marcas_racoes_do_tipo(patches, senior)
    print(f"INFO cao_senior marcas com peso: {marcas_senior}")
    check(isinstance(marcas_senior, list), "cao_senior marcas lista")

    # Simula o bug do print: GRAN PLUS so no senior sem peso → some
    gran_sem = [
        p
        for p in patches
        if "gran" in (p.get("marca") or "").lower()
        and produto_passa_tipo_racoes(p, senior)
        and not parse_peso_racoes(p.get("peso_etiqueta"))
    ]
    gran_com = [
        p
        for p in patches
        if "gran" in (p.get("marca") or "").lower()
        and produto_passa_tipo_racoes(p, senior)
        and parse_peso_racoes(p.get("peso_etiqueta"))
    ]
    if gran_sem and not gran_com:
        check(
            not any("gran" in (m or "").lower() for m in marcas_senior),
            "vivo GRAN PLUS senior sem peso nao aparece",
        )
    elif gran_com:
        check(
            any("gran" in (m or "").lower() for m in marcas_senior),
            "vivo GRAN PLUS senior com peso aparece",
        )
    else:
        ok("vivo GRAN PLUS senior: sem produtos (noop)")


def prova_api_overlay() -> None:
    print("=== API overlay ===")
    import django

    django.setup()
    from django.test import RequestFactory

    from produtos.views import api_pdv_racoes_overlay

    req = RequestFactory().get("/api/pdv/racoes-overlay/")
    resp = api_pdv_racoes_overlay(req)
    check(resp.status_code == 200, f"overlay HTTP {resp.status_code}")
    import json

    body = json.loads(resp.content.decode("utf-8"))
    check(body.get("ok") is True, "overlay ok=true")
    check(isinstance(body.get("itens"), list) and len(body["itens"]) > 0, "overlay itens > 0")
    amostra = body["itens"][0]
    for k in ("id", "categoria", "subcategoria", "subcategoria_2", "marca"):
        check(k in amostra, f"overlay item tem {k}")


def main() -> int:
    print("VERIFY PDV-RACOES-MARCA-VAZIA")
    print(f"PIN={PIN}")
    prova_estatico()
    prova_logica()
    prova_js_parity()
    prova_django_pin_e_dados()
    prova_api_overlay()
    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" ", f)
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
