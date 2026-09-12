#!/usr/bin/env python
"""Prova NF-BIP-ET2 — bip na etapa 2 (Mudar/busca) vale Ok na etapa 3. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import re
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


def _dig(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())


def _somente_alnum(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", str(s or ""))


def _equiv(a: str, b: str) -> bool:
    xa = _somente_alnum(a).lower()
    xb = _somente_alnum(b).lower()
    if not xa or not xb:
        return False
    if xa == xb:
        return True
    if xa.isdigit() and xb.isdigit():
        return (xa.lstrip("0") or "0") == (xb.lstrip("0") or "0")
    return False


def busca_termo_eh_bip(raw: str) -> bool:
    compacto = str(raw or "").replace(" ", "")
    return bool(re.fullmatch(r"\d{8,}", compacto))


def via_por_termo(raw: str, via_padrao: str, p: dict | None = None) -> str:
    if busca_termo_eh_bip(raw):
        return "scan"
    q_digits = _dig(raw)
    if len(q_digits) >= 8 and p:
        ean = _dig(str(p.get("codigo_barras") or ""))
        if ean and _equiv(q_digits, ean):
            return "scan"
        for x in p.get("index_codigos") or []:
            if _equiv(q_digits, str(x or "")):
                return "scan"
    return via_padrao or "lookup"


def match_tipo_confere_barras(mt: str) -> bool:
    t = str(mt or "").strip().lower()
    if not t:
        return False
    if t in ("ean", "ean_nfe", "ean_pg", "ean_overlay", "codigo_barras"):
        return True
    return t.startswith("ean")


def bip_conf_inicial(d: dict) -> str:
    via_cat = str(d.get("catalogo_via") or "").strip()
    mt = str(d.get("match_tipo") or "").strip().lower()
    bc = str(d.get("bip_conf") if d.get("bip_conf") is not None else "").strip().lower()
    if bc in ("pendente", "pending"):
        bc = ""
    if not bc and (via_cat == "scan" or match_tipo_confere_barras(mt)):
        bc = "ok"
    if bc in ("ok", "similar", "divergente", "dispensado"):
        return bc
    return ""


def main() -> None:
    import django

    django.setup()

    html = _read("produtos/templates/produtos/entrada_nota.html")
    util = _read("produtos/nfe_entrada_util.py")

    print("== Markers UI (call sites etapa 2 -> 3) ==")
    check("fn_termo_eh_bip", "function entradaNfeBuscaTermoEhBip" in html)
    check("fn_via_termo", "function entradaNfeViaPorTermoBusca" in html)
    check("fn_match_ean", "function entradaNfeMatchTipoConfereBarras" in html)
    check("fn_conf_inicial", "function entradaNfeBipConfInicialDaLinha" in html)
    check("fn_auto_ok", "function entradaNfeBipAutoOkLinhaSeCasadoEtapa2" in html)
    check("call_executar_busca", "entradaNfeViaPorTermoBusca(raw, 'lookup', p)" in html)
    check("call_sugestao", "entradaNfeViaPorTermoBusca(qSug, 'lookup', p)" in html)
    check("call_modal", "entradaNfeViaPorTermoBusca(qModal, 'modal', p)" in html)
    check("sem_modal_hardcode", "entradaNfeAplicarProdutoNaLinha(trSel, p, 'modal')" not in html)
    check("sem_lookup_hardcode_sug", "entradaNfeAplicarProdutoNaLinha(tr, p, 'lookup')" not in html)
    check("scan_ean_campo", "entradaNfeAplicarProdutoNaLinha(tr, p, 'scan')" in html)
    check("scan_seta_ok", "via === 'scan'" in html and "tr.dataset.bipConf = 'ok'" in html)
    check("lookup_limpa", "via === 'lookup'" in html and "delete tr.dataset.bipConf" in html)
    check("rebuild_chama_auto_ok", "entradaNfeBipAutoOkLinhaSeCasadoEtapa2(tr)" in html)
    check("coletar_chama_auto_ok", html.count("entradaNfeBipAutoOkLinhaSeCasadoEtapa2(tr)") >= 2)
    check("montar_usa_conf_inicial", "entradaNfeBipConfInicialDaLinha(d)" in html)
    check("persist_via", "catalogo_via: String(tr.dataset.nfeCatVia" in html)
    check("persist_bip", "bip_conf: entradaNfeBipEstadoLinha(tr)" in html)
    check("persist_match", "match_tipo: String(tr.dataset.nfeMatchTipo" in html)
    check("validar_etapa3_ok", "['ok', 'similar', 'dispensado'].includes(stBip)" in html)
    check("help_mudar", "leitor no Mudar" in html)

    print("== Markers backend casar XML ==")
    check("pg_ean_pg", 'mtipo = "ean_pg"' in util)
    check("pg_ean_overlay", 'mtipo = "ean_overlay"' in util)
    check("pg_codigo_pg", 'mtipo = "codigo_pg"' in util)
    check("mongo_ean", 'return doc, "ean"' in _read("produtos/mongo_index_codigos.py"))

    print("== Via por termo (leitor vs nome) ==")
    ean = "7896194700818"
    ean_cx = "7898194700818"
    check("via_ean13_modal", via_por_termo(ean, "modal") == "scan")
    check("via_ean13_lookup", via_por_termo(ean, "lookup") == "scan")
    check("via_8_digitos", via_por_termo("12345678", "modal") == "scan")
    check("via_7_digitos_nao", via_por_termo("1234567", "modal") == "modal")
    check("via_espacos", via_por_termo("7896 1947 00818", "modal") == "scan")
    check("via_nome", via_por_termo("bota jetsky", "modal") == "modal")
    check("via_gm", via_por_termo("GM1622-1", "lookup") == "lookup")
    check("via_vazio", via_por_termo("", "modal") == "modal")
    check("via_230_loja", via_por_termo("2300000000001", "modal") == "scan")
    p_ean = {"codigo_barras": ean, "index_codigos": [ean_cx]}
    check("via_traco_casa_ean", via_por_termo("7896-1947-00818", "modal", p_ean) == "scan")
    check("via_opcional_index", via_por_termo(ean_cx, "lookup", p_ean) == "scan")
    check("via_traco_sem_produto", via_por_termo("7896-1947-00818", "modal") == "modal")

    print("== Match tipo -> Ok / PEND ==")
    for mt in ("ean", "ean_nfe", "ean_pg", "ean_overlay", "ean_embalagem"):
        check(f"mt_ok_{mt}", match_tipo_confere_barras(mt))
    check("mt_ok_codigo_barras", match_tipo_confere_barras("codigo_barras"))
    for mt in ("", "codigo", "codigo_pg", "codigo_overlay", "vinculo_c_prod", "xml_vinculo_pre", "compras", "pg"):
        check(f"mt_nao_{mt or 'vazio'}", not match_tipo_confere_barras(mt))

    print("== Remontagem / rascunho (pendente não bloqueia EAN/scan) ==")
    check("init_scan", bip_conf_inicial({"catalogo_via": "scan", "bip_conf": ""}) == "ok")
    check("init_scan_pendente", bip_conf_inicial({"catalogo_via": "scan", "bip_conf": "pendente"}) == "ok")
    check("init_ean_pg_pendente", bip_conf_inicial({"match_tipo": "ean_pg", "bip_conf": "pendente"}) == "ok")
    check("init_ean_overlay", bip_conf_inicial({"match_tipo": "ean_overlay"}) == "ok")
    check("init_modal_pendente", bip_conf_inicial({"catalogo_via": "modal", "bip_conf": "pendente"}) == "")
    check("init_lookup", bip_conf_inicial({"catalogo_via": "lookup"}) == "")
    check("init_codigo_pg", bip_conf_inicial({"match_tipo": "codigo_pg"}) == "")
    check("init_vinculo", bip_conf_inicial({"match_tipo": "vinculo_c_prod"}) == "")
    check("init_xml_pre", bip_conf_inicial({"match_tipo": "xml_vinculo_pre"}) == "")
    check("init_preserva_similar", bip_conf_inicial({"catalogo_via": "lookup", "match_tipo": "ean", "bip_conf": "similar"}) == "similar")
    check("init_preserva_disp", bip_conf_inicial({"bip_conf": "dispensado", "match_tipo": "codigo_pg"}) == "dispensado")

    print("== Runtime casar_produtos_postgres (ean_pg / codigo_pg) ==")
    from produtos.models import Produto, ProdutoGestaoOverlayAgro
    from produtos.nfe_entrada_util import casar_produtos_postgres

    pid = "verify-et2-ean-pg"
    ean_fix = "7896194799999"
    cprod = "FORN-ET2-X"
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()
    Produto.objects.filter(produto_externo_id=pid).delete()
    Produto.objects.filter(codigo_barras=ean_fix).delete()

    p = Produto.objects.create(
        nome="VERIFY ET2 EAN PG",
        codigo_interno="GM-VERIFY-ET2",
        codigo_nfe="GM-VERIFY-ET2",
        codigo_barras=ean_fix,
        produto_externo_id=pid,
        erp_produto_id=pid,
        ativo=True,
        cadastro_somente_agro=True,
    )
    itens_ean = [{"ean": ean_fix, "c_prod": cprod, "x_prod": "VERIFY ET2"}]
    out_ean = casar_produtos_postgres(itens_ean, emit_cnpj="")
    check("casar_ean_pg", (out_ean[0].get("match_tipo") == "ean_pg"), str(out_ean[0].get("match_tipo")))
    check("casar_ean_pid", str(out_ean[0].get("produto_id") or "") == pid)
    check("casar_ean_vira_ok", bip_conf_inicial({"match_tipo": out_ean[0].get("match_tipo"), "bip_conf": "pendente"}) == "ok")

    itens_cod = [{"ean": "", "c_prod": "GM-VERIFY-ET2", "x_prod": "VERIFY ET2"}]
    out_cod = casar_produtos_postgres(itens_cod, emit_cnpj="")
    check(
        "casar_codigo_nao_ok",
        bip_conf_inicial({"match_tipo": out_cod[0].get("match_tipo"), "bip_conf": "pendente"}) == "",
        str(out_cod[0].get("match_tipo")),
    )

    ov_ean = "7896194788888"
    ProdutoGestaoOverlayAgro.objects.filter(codigo_barras=ov_ean).delete()
    ProdutoGestaoOverlayAgro.objects.create(produto_externo_id=pid, codigo_barras=ov_ean)
    itens_ov = [{"ean": ov_ean, "c_prod": "", "x_prod": "VERIFY ET2 OV"}]
    out_ov = casar_produtos_postgres(itens_ov, emit_cnpj="")
    check("casar_ean_overlay", out_ov[0].get("match_tipo") == "ean_overlay", str(out_ov[0].get("match_tipo")))
    check("casar_overlay_vira_ok", bip_conf_inicial({"match_tipo": "ean_overlay", "bip_conf": "pendente"}) == "ok")

    try:
        ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()
        Produto.objects.filter(pk=p.pk).delete()
    except Exception:
        pass

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
