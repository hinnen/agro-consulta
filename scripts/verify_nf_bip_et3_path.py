#!/usr/bin/env python
"""Prova NF-BIP-ET3 — etapa 3 casa EAN da linha + barras cadastro/overlay. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from unittest.mock import patch

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


def _somente_alnum(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", str(s or ""))


def _dig(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())


def _equiv(a: str, b: str) -> bool:
    """Espelho de entradaNfeCodigosEquivalentes (JS)."""
    xa = _somente_alnum(a).lower()
    xb = _somente_alnum(b).lower()
    if not xa or not xb:
        return False
    if xa == xb:
        return True
    if xa.isdigit() and xb.isdigit():
        return (xa.lstrip("0") or "0") == (xb.lstrip("0") or "0")
    return False


def _candidatos_linha(ean: str, ean_orig: str, similares: list[str] | None, cadastro: list[str] | None = None) -> list[str]:
    """Espelho de entradaNfeBipCodigosCandidatosLinha (JS)."""
    out: list[str] = []
    seen: set[str] = set()

    def push(raw: str) -> None:
        s = str(raw or "").strip()
        if not s:
            return
        dig = _dig(s)
        key = dig if len(dig) >= 8 else _somente_alnum(s).lower()
        if not key or key in seen:
            return
        seen.add(key)
        out.append(s)

    push(ean)
    push(ean_orig)
    for x in similares or []:
        push(x)
    for x in cadastro or []:
        push(x)
    return out


def _match_local(
    linhas: list[dict],
    cod_main: str,
    cod_aux: str = "",
) -> dict | None:
    """Espelho mínimo de entradaNfeBipBuscarMatchLocalNota."""
    termos = [t for t in (cod_main, cod_aux) if str(t or "").strip()]
    if not termos:
        return None
    ordem = list(range(len(linhas)))
    ordem.sort(key=lambda i: (0 if linhas[i].get("bip_conf", "pendente") == "pendente" else 1, i))
    for i in ordem:
        ln = linhas[i]
        if not str(ln.get("produto_id") or "").strip():
            continue
        cands = _candidatos_linha(
            str(ln.get("ean") or ""),
            str(ln.get("ean_orig") or ""),
            list(ln.get("similares") or []),
            list(ln.get("cadastro") or []),
        )
        if not cands:
            continue
        ok = False
        for t in termos:
            for c in cands:
                if _equiv(t, c):
                    ok = True
                    break
            if ok:
                break
        if ok:
            return {"ix": i, "local": True, "nome": ln.get("nome") or ""}
    return None


def main() -> None:
    import django

    django.setup()

    html = _read("produtos/templates/produtos/entrada_nota.html")
    views = _read("produtos/views.py")

    print("== Markers UI etapa 3 ==")
    check("ui_auto_match_erp_true", "ENTRADA_NFE_BIP_AUTO_MATCH_ERP = true" in html)
    check("ui_auto_match_erp_false_gone", "ENTRADA_NFE_BIP_AUTO_MATCH_ERP = false" not in html)
    check("ui_candidatos_fn", "function entradaNfeBipCodigosCandidatosLinha" in html)
    check("ui_match_usa_candidatos", "entradaNfeBipCodigosCandidatosLinha(tr)" in html)
    check("ui_nfe_ean_orig_candidato", "nfeEanOrig" in html and "bipSimilarCodigos" in html)
    check(
        "ui_auto_ok_ean_scan",
        "function entradaNfeBipConfInicialDaLinha" in html
        and "viaCat === 'scan'" in html
        and "bc = 'ok'" in html,
    )
    check("ui_match_tipo_ean_pg", "ean_pg" in html and "function entradaNfeMatchTipoConfereBarras" in html)
    check("ui_via_termo_bip", "function entradaNfeViaPorTermoBusca" in html)
    check("ui_modal_usa_via_termo", "entradaNfeViaPorTermoBusca(qModal" in html)
    check("ui_persist_match_tipo", "match_tipo: String(tr.dataset.nfeMatchTipo" in html)
    check("ui_scan_sets_bip_ok", "via === 'scan'" in html and "tr.dataset.bipConf = 'ok'" in html)
    check("ui_lookup_clear_bip", "via === 'lookup'" in html and "delete tr.dataset.bipConf" in html)
    check("ui_combinado_chama_erp", "entradaNfeBipBuscarMatchNaNota(rows, codMain, codAux)" in html)
    check("ui_url_conf_cod", "api_entrada_nota_conferir_codigo" in html or "URL_CONF_COD" in html)
    check("ui_help_cadastro", "barras do cadastro" in html)
    check("ui_prefetch", "function entradaNfeBipPrefetchCodigosCadastro" in html)
    check("ui_cadastro_candidato", "bipCadastroCodigos" in html)
    check("ui_progress_txt", "nfe-bip-progress-txt" in html)
    check("ui_flash_ok", "nfe-bip-flash-ok" in html)
    check("ui_som", "function entradaNfeBipSom" in html)
    fn_erp = html[html.find("async function entradaNfeBipBuscarMatchNaNota") :]
    fn_erp = fn_erp[: fn_erp.find("function entradaNfeBipLimparDraftEBipIdx")]
    check("ui_lote_produto_ids", "produto_ids" in fn_erp)
    check("ui_sem_fetch_por_linha", "for (let i = 0; i < rows.length; i++)" not in fn_erp)

    print("== Markers API ==")
    api = views[views.find("def api_entrada_nota_conferir_codigo") :]
    api = api[: api.find("\ndef api_entrada_nota_aprovar_wizard")]
    check("api_overlay_fn", "_bate_overlay_agro" in api)
    check("api_overlay_opcionais", "codigos_barras_opcionais_de_cadastro_extras" in api)
    check("api_overlay_ean_emb", "entrada_nfe_ean_embalagem" in api)
    check("api_mongo_fallback_overlay", "if not bate:" in api and "_bate_overlay_agro" in api)
    check("api_nao_gm_docstring", "não GM" in api.lower() or "nao GM" in api.lower() or "não GM" in api)
    check("api_lote_fn", "def entrada_nfe_mapa_codigos_barras_lote" in views)
    check("api_lote_branch", "produto_ids" in api and "mapa" in api)

    print("== Unit equivalência / match local (caso Renan NF 2255) ==")
    ean_papagaio = "7896194700818"
    ean_ferradura = "7898525851015"
    check("equiv_igual", _equiv(ean_papagaio, ean_papagaio))
    check("equiv_zero_pad", _equiv("0" + ean_papagaio, ean_papagaio))
    check("equiv_diff", not _equiv(ean_papagaio, ean_ferradura))
    check("equiv_gm_nao", not _equiv("GM1622-1", ean_papagaio))

    # Caso A: EAN na grade = bip → match local
    linhas_a = [
        {
            "produto_id": "p1",
            "ean": ean_papagaio,
            "ean_orig": "",
            "bip_conf": "pendente",
            "nome": "PAPAGAIO",
        },
        {
            "produto_id": "p6",
            "ean": ean_ferradura,
            "ean_orig": "",
            "bip_conf": "pendente",
            "nome": "FERRADURA",
        },
    ]
    m_a = _match_local(linhas_a, ean_papagaio)
    check("local_papagaio", bool(m_a and m_a["ix"] == 0), str(m_a))

    # Caso B: grade vazia no .in-ean mas nfeEanOrig tem o código (XML)
    linhas_b = [
        {
            "produto_id": "p1",
            "ean": "",
            "ean_orig": ean_papagaio,
            "bip_conf": "pendente",
            "nome": "PAPAGAIO",
        }
    ]
    m_b = _match_local(linhas_b, ean_papagaio)
    check("local_via_ean_orig", bool(m_b and m_b["ix"] == 0), str(m_b))

    # Caso C: EAN da NF (caixa) ≠ bip embalagem → local falha (ERP/overlay deve cobrir)
    ean_caixa = "7898194700818"
    linhas_c = [
        {
            "produto_id": "p1",
            "ean": ean_caixa,
            "ean_orig": ean_caixa,
            "bip_conf": "pendente",
            "nome": "PAPAGAIO",
        }
    ]
    m_c = _match_local(linhas_c, ean_papagaio)
    check("local_caixa_diff_falha", m_c is None, str(m_c))

    # Caso C2: EAN só no cadastro (prefetch) → match local sem round-trip por linha
    linhas_c2 = [
        {
            "produto_id": "p1",
            "ean": ean_caixa,
            "ean_orig": ean_caixa,
            "cadastro": [ean_papagaio],
            "bip_conf": "pendente",
            "nome": "PAPAGAIO",
        }
    ]
    m_c2 = _match_local(linhas_c2, ean_papagaio)
    check("local_via_cadastro", bool(m_c2 and m_c2["ix"] == 0), str(m_c2))

    # Caso D: sem produto_id não casa
    linhas_d = [{"produto_id": "", "ean": ean_papagaio, "bip_conf": "pendente"}]
    check("local_sem_pid", _match_local(linhas_d, ean_papagaio) is None)

    # Caso E: prioriza pendente
    linhas_e = [
        {
            "produto_id": "p_ok",
            "ean": ean_papagaio,
            "bip_conf": "ok",
            "nome": "JA_OK",
        },
        {
            "produto_id": "p_pend",
            "ean": ean_papagaio,
            "bip_conf": "pendente",
            "nome": "PEND",
        },
    ]
    m_e = _match_local(linhas_e, ean_papagaio)
    check("local_prio_pendente", bool(m_e and m_e["ix"] == 1), str(m_e))

    print("== Auto-ok regra montagem ==")
    def match_tipo_ean(mt: str) -> bool:
        t = (mt or "").strip().lower()
        if t in ("ean", "ean_nfe", "ean_pg", "ean_overlay", "codigo_barras"):
            return True
        return t.startswith("ean")

    def auto_ok(via: str, match_tipo: str, bip_conf: str = "") -> str:
        bc = (bip_conf or "").strip().lower()
        mt = (match_tipo or "").strip().lower()
        via_cat = (via or "").strip()
        if bc in ("pendente", "pending"):
            bc = ""
        if not bc and (via_cat == "scan" or match_tipo_ean(mt)):
            bc = "ok"
        return bc or "pendente"

    check("auto_ok_scan", auto_ok("scan", "") == "ok")
    check("auto_ok_ean", auto_ok("", "ean") == "ok")
    check("auto_ok_ean_pg", auto_ok("", "ean_pg") == "ok")
    check("auto_ok_ean_overlay", auto_ok("", "ean_overlay") == "ok")
    check("auto_ok_pendente_ean_pg", auto_ok("", "ean_pg", "pendente") == "ok")
    check("auto_ok_codigo_nao", auto_ok("", "codigo") == "pendente")
    check("auto_ok_modal_nao", auto_ok("modal", "") == "pendente")
    check("auto_ok_lookup_nao", auto_ok("lookup", "") == "pendente")
    check("auto_ok_preserva", auto_ok("lookup", "ean", "similar") == "similar")

    def via_por_termo(raw: str, via_padrao: str) -> str:
        compacto = (raw or "").replace(" ", "")
        return "scan" if re.fullmatch(r"\d{8,}", compacto) else via_padrao

    check("via_bip_modal", via_por_termo("7896194700818", "modal") == "scan")
    check("via_nome_modal", via_por_termo("bota jetsky", "modal") == "modal")

    print("== API conferir_codigo (overlay / sem Mongo) ==")
    from django.contrib.auth import get_user_model
    from django.test import RequestFactory

    from produtos.models import Produto, ProdutoGestaoOverlayAgro
    from produtos.views import api_entrada_nota_conferir_codigo

    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username="verify_nf_bip_et3",
        defaults={"is_staff": True},
    )
    if not user.has_usable_password():
        user.set_password("verify-et3-tmp")
        user.save(update_fields=["password"])

    pid = "verify-et3-papagaio"
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()
    Produto.objects.filter(erp_produto_id=pid).delete()
    Produto.objects.filter(produto_externo_id=pid).delete()
    Produto.objects.filter(codigo_interno="GM-VERIFY-ET3").delete()

    p = Produto.objects.create(
        nome="VERIFY ET3 PAPAGAIO",
        codigo_interno="GM-VERIFY-ET3",
        codigo_nfe="GM-VERIFY-ET3",
        codigo_barras=ean_papagaio,
        produto_externo_id=pid,
        erp_produto_id=pid,
        ativo=True,
        cadastro_somente_agro=True,
    )

    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=pid,
        codigo_barras=ean_papagaio,
        cadastro_extras={
            "codigos_barras_opcionais": [ean_ferradura],
            "entrada_nfe_ean_embalagem": ean_caixa,
        },
    )
    check("fixture_produto", bool(p.pk), str(p.pk))
    check("fixture_overlay", bool(ov.pk))

    rf = RequestFactory()

    def post_conf2(produto_id: str, codigo: str) -> dict:
        req = rf.post(
            "/api/entrada-nota/conferir-codigo/",
            data=json.dumps({"produto_id": produto_id, "codigo": codigo}),
            content_type="application/json",
        )
        req.user = user
        with patch("produtos.views.obter_conexao_mongo", return_value=(None, None)):
            r = api_entrada_nota_conferir_codigo(req)
        try:
            body = json.loads(r.content.decode("utf-8"))
        except Exception:
            body = {"raw": r.content.decode("utf-8", "replace")[:240]}
        return {"status": r.status_code, **body}

    j1 = post_conf2(pid, ean_papagaio)
    check("api_bate_principal", j1.get("status") == 200 and j1.get("bate") is True, str(j1))

    j2 = post_conf2(pid, ean_ferradura)
    check("api_bate_opcional", j2.get("status") == 200 and j2.get("bate") is True, str(j2))

    j3 = post_conf2(pid, ean_caixa)
    check("api_bate_ean_embalagem", j3.get("status") == 200 and j3.get("bate") is True, str(j3))

    j4 = post_conf2(pid, "7890000000000")
    check("api_nao_bate_outro", j4.get("status") == 200 and j4.get("bate") is False, str(j4))

    j5 = post_conf2(pid, "GM-VERIFY-ET3")
    check("api_nao_bate_gm", j5.get("status") == 200 and j5.get("bate") is False, str(j5))

    j6 = post_conf2("pid-inexistente-xyz", ean_papagaio)
    check("api_404_pid", j6.get("status") == 404, str(j6))

    def post_lote(body: dict) -> dict:
        req = rf.post(
            "/api/entrada-nota/conferir-codigo/",
            data=json.dumps(body),
            content_type="application/json",
        )
        req.user = user
        with patch("produtos.views.obter_conexao_mongo", return_value=(None, None)):
            r = api_entrada_nota_conferir_codigo(req)
        try:
            parsed = json.loads(r.content.decode("utf-8"))
        except Exception:
            parsed = {"raw": r.content.decode("utf-8", "replace")[:240]}
        return {"status": r.status_code, **parsed}

    j7 = post_lote({"produto_ids": [pid]})
    mapa7 = j7.get("mapa") or {}
    cands7 = (mapa7.get(pid) or {}).get("codigos") or []
    check(
        "api_lote_mapa",
        j7.get("status") == 200 and j7.get("ok") is True and ean_papagaio in cands7 and ean_ferradura in cands7,
        str(j7)[:400],
    )

    j8 = post_lote({"produto_ids": [pid], "codigo": ean_papagaio})
    check(
        "api_lote_hit",
        j8.get("status") == 200 and j8.get("bate") is True and j8.get("produto_id") == pid,
        str(j8)[:400],
    )

    j9 = post_lote({"produto_ids": [pid], "codigo": "7890000000000"})
    check("api_lote_miss", j9.get("status") == 200 and j9.get("bate") is False, str(j9)[:400])

    # limpa fixture
    try:
        ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()
        Produto.objects.filter(erp_produto_id=pid).delete()
        Produto.objects.filter(codigo_interno="GM-VERIFY-ET3").delete()
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
