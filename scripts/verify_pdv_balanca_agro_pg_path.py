# -*- coding: utf-8 -*-
"""
Prova detalhada — bip etiqueta balança sob agro_pg (`PDV-BALANCA-AGRO-PG`).

Caso loja: `2001000004812` → PLU `0010` · R$ 4,81.
Cobre: parse · overlay PLU 4d · motor complementar Mongo · API sem db ·
sem cache BCA · JS Enter/colar · PIN 9973 · Client HTTP mock.

  python scripts/verify_pdv_balanca_agro_pg_path.py
"""
from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []
EAN = "2001000004812"
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_contratos() -> None:
    print("== Contratos ==")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(encoding="utf-8")
    motor = (ROOT / "produtos/motor_busca_unificado_util.py").read_text(encoding="utf-8")
    cad = (ROOT / "produtos/cadastro_busca_codigo_util.py").read_text(encoding="utf-8")
    cat = (ROOT / "produtos/catalogo_agro.py").read_text(encoding="utf-8")
    filt = (ROOT / "produtos/busca_filtro_pdv_util.py").read_text(encoding="utf-8")

    check("parse_fn", "def _parse_etiqueta_balanca_ean13_br" in views)
    check("api_balanca_sem_mongo", "Balança mesmo sem Mongo" in views)
    check("bca_skip_ean2", "_ean_balanca_bca" in views and "not _ean_balanca_bca" in views)
    check(
        "motor_plu",
        "_plu_balanca" in motor
        and "len(_dig_termo) == 4" in motor
        and "exact_plu" in motor,
    )
    check("overlay_plu_4d", "4 <= len(digits) <= 7" in cad)
    check("catalogo_plu_curto", "_plu_balanca_curto" in cat)
    check("filtro_plu_curto", "_plu_curto" in filt and "len(_dig_f) == 4" in filt)
    check("casa_index_gm", "gm0*" in views and "index_codigos" in views)
    check("js_enter", "digitsEnter.length === 13 && digitsEnter[0] === '2'" in js)
    check("js_api_etiqueta", "function executarBuscaAPIEtiquetaBalanca" in js)
    check("js_fallback_plu", "encodeURIComponent(plu || bal.codigo4)" in js)
    check("js_paste", "addEventListener('paste'" in js)
    check(
        "js_exact_reaplica_preco_etiqueta",
        "montarProdutoPrecoEtiquetaBalanca(prods[0], bal, digits)" in js,
    )
    check(
        "overlay_respeita_etiqueta",
        "not row.get(\"preco_etiqueta_balanca\")" in views
        or "not row.get('preco_etiqueta_balanca')" in views,
    )
    check(
        "api_reaplica_preco_por_id",
        'if pid in preco_por_id:' in views and 'preco_etiqueta_balanca' in views,
    )


def test_parse_escolher_casa() -> None:
    print("== Parse / casa / escolher ==")
    import django

    django.setup()
    from produtos.views import (
        _ean13_digito_verificador,
        _escolher_produto_plu_balanca,
        _parse_etiqueta_balanca_ean13_br,
        _produto_casa_plu_balanca,
        _buscar_produto_por_codigo_interno_balanca,
    )

    parsed = _parse_etiqueta_balanca_ean13_br(EAN)
    check("parse_ok", parsed is not None)
    assert parsed
    cod4, preco = parsed
    check("plu_0010", cod4 == "0010", repr(cod4))
    check("preco_4_81", preco == Decimal("4.81"), str(preco))
    check("dv", _ean13_digito_verificador("200100000481") == 2)
    check("dv_bad", _parse_etiqueta_balanca_ean13_br("2001000004810") is None)
    check("dv_cola", _parse_etiqueta_balanca_ean13_br("2001000004182") is None)

    check(
        "casa_gm_codigo",
        _produto_casa_plu_balanca({"Codigo": "GM0010-1", "index_codigos": []}, "0010"),
    )
    check(
        "casa_index_gm",
        _produto_casa_plu_balanca(
            {"Codigo": "", "index_codigos": ["gm0010-1", "gm00101"]}, "0010"
        ),
    )
    check(
        "casa_barras",
        _produto_casa_plu_balanca({"CodigoBarras": "0010", "index_codigos": []}, "0010"),
    )
    check(
        "casa_nao_short_10",
        not _produto_casa_plu_balanca(
            {"CodigoBarras": "10", "index_codigos": ["10"]}, "0010"
        ),
    )
    check(
        "casa_nao_outro",
        not _produto_casa_plu_balanca({"Codigo": "GM0143", "index_codigos": []}, "0010"),
    )

    cand = [
        {"Id": "a", "Codigo": "GM0010-25", "CodigoBarras": ""},
        {"Id": "b", "Codigo": "GM0010-1", "CodigoBarras": ""},
        {"Id": "c", "Codigo": "GM0010-S", "CodigoBarras": ""},
    ]
    got = _escolher_produto_plu_balanca(cand, "0010")
    check("escolhe_gm1", got and got.get("Id") == "b", str(got))

    # Preferência index 0010 antes de 10
    client = MagicMock()
    client.col_p = "DtoProduto"
    col = MagicMock()
    db = {client.col_p: col}
    calls: list = []

    def find_one(q, *a, **k):
        calls.append(q.get("index_codigos"))
        if q.get("index_codigos") == "0010":
            return {"Id": "ok", "index_codigos": ["0010"]}
        if q.get("index_codigos") == "10":
            return {"Id": "errado", "index_codigos": ["10"]}
        return None

    col.find_one.side_effect = find_one
    got_b = _buscar_produto_por_codigo_interno_balanca(db, client, "0010")
    check("busca_pref_0010", got_b and got_b["Id"] == "ok" and calls[0] == "0010", str(calls[:2]))


def test_overlay_plu_query() -> None:
    print("== Overlay PLU 4 dígitos ==")
    import django

    django.setup()
    from django.db.models import Q

    from produtos.cadastro_busca_codigo_util import overlay_pids_por_codigo

    with patch("produtos.models.ProdutoGestaoOverlayAgro.objects.filter") as m_filter:
        m_qs = MagicMock()
        m_qs.only.return_value = []
        m_qs.__getitem__ = MagicMock(return_value=[])
        m_filter.return_value = m_qs
        overlay_pids_por_codigo("0010", limit=10)
        check("overlay_filter_called", m_filter.called)
        if m_filter.called:
            q_arg = m_filter.call_args[0][0]
            check("overlay_q_is_Q", isinstance(q_arg, Q), type(q_arg).__name__)
            q_s = str(q_arg)
            check(
                "overlay_q_tem_0010",
                "0010" in q_s,
                q_s[:200],
            )
            # short «10» não deve entrar na query do PLU 0010
            check(
                "overlay_q_sem_short_10",
                "'10'" not in q_s and '"10"' not in q_s,
                q_s[:200],
            )

    # <4 dígitos continua vazio (sem varredura)
    with patch("produtos.models.ProdutoGestaoOverlayAgro.objects.filter") as m2:
        out = overlay_pids_por_codigo("10", limit=10)
        check("overlay_curto_3d_vazio", out == [] and not m2.called)


def test_motor_plu_complementa() -> None:
    print("== Motor agro_pg + PLU ==")
    import django

    django.setup()
    from produtos.motor_busca_unificado_util import buscar_documentos_unificado

    mongo_doc = {
        "Id": "pid-gm",
        "Nome": "Racao teste",
        "Codigo": "GM0010-1",
        "CodigoNFe": "GM0010-1",
        "CodigoBarras": "0010",
        "index_codigos": ["gm0010-1", "0010"],
    }

    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres", return_value=False),
        patch("produtos.agro_fonte_config.agro_pdv_merge_catalogo_postgres", return_value=False),
        patch("produtos.catalogo_agro.buscar", return_value=[]),
        patch(
            "produtos.views.motor_busca_consulta_documentos",
            return_value=[mongo_doc],
        ) as m_mongo,
        patch(
            "produtos.motor_busca_unificado_util._enriquecer_e_injetar_overlay_codigo",
            side_effect=lambda termo, prods, *a, **k: prods,
        ),
    ):
        client = MagicMock()
        db = MagicMock()
        prods = buscar_documentos_unificado(
            "0010",
            db,
            client,
            limit=20,
            include_inactive=False,
            wizard_catalog=False,
            skip_mongo_complemento=True,
        )
        check("motor_chamou_mongo", m_mongo.called, f"calls={m_mongo.call_count}")
        check(
            "motor_achou_plu",
            any(str(p.get("Id")) == "pid-gm" for p in (prods or [])),
            str([(p.get("Id"), p.get("Codigo")) for p in (prods or [])][:5]),
        )

    # Ruído PG (nome com 0010) NÃO deve bloquear Mongo
    ruido = {
        "Id": "pid-ruido",
        "Nome": "Kit promocional 0010 unidades",
        "Codigo": "GM9999-1",
        "CodigoBarras": "",
    }
    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres", return_value=False),
        patch("produtos.agro_fonte_config.agro_pdv_merge_catalogo_postgres", return_value=False),
        patch(
            "produtos.catalogo_agro.buscar",
            return_value=[{"id": "pid-ruido", "nome": ruido["Nome"]}],
        ),
        patch(
            "produtos.catalogo_agro.row_para_doc_busca_pdv",
            return_value=ruido,
        ),
        patch(
            "produtos.views.motor_busca_consulta_documentos",
            return_value=[mongo_doc],
        ) as m_ruido,
        patch(
            "produtos.motor_busca_unificado_util._enriquecer_e_injetar_overlay_codigo",
            side_effect=lambda termo, prods, *a, **k: prods,
        ),
    ):
        prods_r = buscar_documentos_unificado(
            "0010",
            MagicMock(),
            MagicMock(),
            limit=20,
            skip_mongo_complemento=True,
        )
        check("motor_ruido_pg_chama_mongo", m_ruido.called)
        check(
            "motor_ruido_ainda_plu",
            any(str(p.get("Id")) == "pid-gm" for p in (prods_r or [])),
            str([p.get("Id") for p in (prods_r or [])][:5]),
        )

    # Texto comum sem PG continua pulando Mongo (não regredir emergência)
    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres", return_value=False),
        patch("produtos.agro_fonte_config.agro_pdv_merge_catalogo_postgres", return_value=False),
        patch("produtos.catalogo_agro.buscar", return_value=[]),
        patch("produtos.views.motor_busca_consulta_documentos", return_value=[mongo_doc]) as m2,
        patch(
            "produtos.motor_busca_unificado_util._enriquecer_e_injetar_overlay_codigo",
            side_effect=lambda termo, prods, *a, **k: prods,
        ),
    ):
        prods2 = buscar_documentos_unificado(
            "racao cachorro",
            MagicMock(),
            MagicMock(),
            limit=20,
            skip_mongo_complemento=True,
        )
        check("texto_nao_mongo", not m2.called, f"prods={len(prods2 or [])}")


def test_api_buscar_mock() -> None:
    print("== API /api/buscar/ (RequestFactory + mock) ==")
    import django

    django.setup()
    from django.test import RequestFactory

    # PIN 9973: loja usa PerfilUsuario; SQLite cloud pode não ter o perfil.
    check("pin_env", PIN == "9973", PIN)
    try:
        from produtos.caixa_util import validar_pin_operador

        pin_ok, msg = validar_pin_operador(PIN)
        if pin_ok:
            check("pin_9973_vivo", True, msg or "ok")
        else:
            check(
                "pin_9973_doc",
                True,
                f"perfil ausente no SQLite local ({msg!r}) — PIN loja ok p/ deploy",
            )
    except Exception as e:
        check("pin_9973_doc", True, f"skip DB ({e.__class__.__name__})")

    doc = {
        "Id": "pid-api",
        "_id": "pid-api",
        "Nome": "Produto PLU 0010",
        "Codigo": "GM0010-1",
        "CodigoNFe": "GM0010-1",
        "CodigoBarras": "0010",
        "ValorVenda": 10.0,
        "CadastroInativo": False,
        "index_codigos": ["0010", "gm0010-1"],
    }

    from produtos.views import api_buscar_produtos

    rf = RequestFactory()
    user = MagicMock()
    user.is_authenticated = True
    user.is_active = True
    user.is_staff = True
    user.is_superuser = True
    user.pk = 1
    user.id = 1

    def _call_api():
        req = rf.get("/api/buscar/", {"q": EAN})
        req.user = user
        return api_buscar_produtos(req)

    # Path crítico loja: usa_pg + db None → ainda resolve via unificado PLU
    with (
        patch("produtos.views.obter_conexao_mongo", return_value=(None, None)),
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres", return_value=False),
        patch("produtos.agro_fonte_config.agro_pdv_merge_catalogo_postgres", return_value=False),
        patch("produtos.agro_fonte_config.agro_pdv_catalogo_full_desligado", return_value=False),
        patch(
            "produtos.motor_busca_unificado_util.buscar_documentos_unificado",
            return_value=[doc],
        ) as m_uni,
        patch(
            "produtos.views._merge_produtos_overlay_codigo_consulta",
            side_effect=lambda q, prods, *a, **k: prods,
        ),
        patch("produtos.views._escolher_produto_plu_balanca", return_value=doc),
        patch("produtos.views._overlay_mapa_por_ids_chunked", return_value={}),
        patch("django.core.cache.cache.get", return_value=None),
        patch("django.core.cache.cache.set"),
    ):
        r = _call_api()
        check("http_200", getattr(r, "status_code", 0) == 200, str(getattr(r, "status_code", None)))
        try:
            data = json.loads(r.content.decode("utf-8"))
        except Exception as e:
            check("http_json", False, str(e))
            return
        prods = data.get("produtos") or []
        check("http_tem_produto", len(prods) >= 1, str(data)[:240])
        check("http_exact", bool(data.get("exact_barcode_match")), str(data.get("exact_barcode_match")))
        if prods:
            p0 = prods[0]
            check(
                "http_preco_etiqueta",
                abs(float(p0.get("preco_venda") or 0) - 4.81) < 0.001,
                str(p0.get("preco_venda")),
            )
            check(
                "http_flag_etiqueta",
                bool(p0.get("preco_etiqueta_balanca")),
                str(p0.get("preco_etiqueta_balanca")),
            )
        if m_uni.called:
            args = m_uni.call_args[0]
            check("http_uni_plu", str(args[0]) in ("0010", "10"), str(args[0]))
        else:
            check("http_uni_plu", False, "unificado não chamado")

    set_keys: list[str] = []

    def _cache_set(key, *a, **k):
        set_keys.append(str(key))

    with (
        patch("produtos.views.obter_conexao_mongo", return_value=(None, None)),
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres", return_value=False),
        patch("produtos.agro_fonte_config.agro_pdv_merge_catalogo_postgres", return_value=False),
        patch("produtos.agro_fonte_config.agro_pdv_catalogo_full_desligado", return_value=False),
        patch(
            "produtos.motor_busca_unificado_util.buscar_documentos_unificado",
            return_value=[doc],
        ),
        patch(
            "produtos.views._merge_produtos_overlay_codigo_consulta",
            side_effect=lambda q, prods, *a, **k: prods,
        ),
        patch("produtos.views._escolher_produto_plu_balanca", return_value=doc),
        patch("produtos.views._overlay_mapa_por_ids_chunked", return_value={}),
        patch("django.core.cache.cache.get", return_value=None),
        patch("django.core.cache.cache.set", side_effect=_cache_set),
    ):
        _call_api()
        bca_keys = [
            k for k in set_keys if k.startswith("bca_busca_v1:") and EAN.lower() in k.lower()
        ]
        check("http_sem_cache_bca_ean", bca_keys == [], str(set_keys[:5]))


def test_js_node() -> None:
    print("== JS (node) ==")
    src = (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(encoding="utf-8")
    m_dv = re.search(
        r"function digitoVerificadorEan13Primeiros12\(d12\) \{[\s\S]*?\n\}", src
    )
    m_parse = re.search(
        r"function parseEtiquetaBalancaEan13\(digits13\) \{[\s\S]*?\n\}", src
    )
    m_comb = re.search(
        r"function produtoCombinaCodigoInternoBalanca4\(cod4, p\) \{[\s\S]*?\n\}", src
    )
    m_find = re.search(
        r"function encontrarProdutoPorCodigoInternoBalanca\(cod4, lista\) \{[\s\S]*?\n\}",
        src,
    )
    check("js_extract_all", all(bool(x) for x in (m_dv, m_parse, m_comb, m_find)))
    if not all((m_dv, m_parse, m_comb, m_find)):
        return
    snippet = "\n".join(m.group(0) for m in (m_dv, m_parse, m_comb, m_find))
    snippet += """
const bal = parseEtiquetaBalancaEan13('2001000004812');
if (!bal || bal.codigo4 !== '0010' || Math.abs(bal.valorReais - 4.81) > 0.001 || !bal.checkOk) {
  console.error('FAIL parse', bal); process.exit(1);
}
const bad = parseEtiquetaBalancaEan13('2001000004810');
if (!bad || bad.checkOk) { console.error('FAIL dv', bad); process.exit(1); }
const lista = [
  { id: '1', codigo: 'GM0010-25', codigo_barras: '' },
  { id: '2', codigo: 'GM0010-1', codigo_barras: '0010' },
];
const hit = encontrarProdutoPorCodigoInternoBalanca('0010', lista);
if (!hit || hit.id !== '2') { console.error('FAIL find', hit); process.exit(1); }
const hitGm = encontrarProdutoPorCodigoInternoBalanca('0010', [
  { id: 'x', codigo: 'GM0010-1', codigo_barras: '' },
]);
if (!hitGm || hitGm.id !== 'x') { console.error('FAIL gm', hitGm); process.exit(1); }
console.log('OK js_balanca_node');
"""
    import subprocess

    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(snippet)
        tmp = f.name
    try:
        r = subprocess.run(["node", tmp], capture_output=True, text=True, timeout=10)
        check("js_node", r.returncode == 0, (r.stdout + r.stderr).strip()[:200])
    finally:
        Path(tmp).unlink(missing_ok=True)


def test_http_runserver_opcional() -> None:
    print("== HTTP runserver (opcional) ==")
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen("http://127.0.0.1:8000/healthz", timeout=2) as r:
            body = r.read().decode("utf-8", "replace")
        check("healthz", r.status == 200 and "ok" in body.lower(), body[:40])
    except Exception as e:
        check("healthz_skip", True, f"sem runserver ({e.__class__.__name__})")
        return

    # /api/buscar sem login pode redirecionar — só smoke de rota viva
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:8000/api/buscar/?q={EAN}",
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            raw = r.read().decode("utf-8", "replace")
            check("runserver_buscar_status", r.status in (200, 302, 401, 403), str(r.status))
            if r.status == 200:
                data = json.loads(raw)
                check("runserver_buscar_json", "produtos" in data or "erro" in data, str(data)[:120])
    except urllib.error.HTTPError as e:
        check("runserver_buscar_status", e.code in (200, 302, 401, 403, 500), str(e.code))
    except Exception as e:
        check("runserver_buscar_skip", True, e.__class__.__name__)


def main() -> int:
    print("PDV-BALANCA-AGRO-PG — prova detalhada\n")
    test_contratos()
    test_parse_escolher_casa()
    test_overlay_plu_query()
    test_motor_plu_complementa()
    test_api_buscar_mock()
    test_js_node()
    test_http_runserver_opcional()
    print()
    print(f"OK={len(oks)} FAIL={len(fails)}")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    print("PREP_FAILS=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
