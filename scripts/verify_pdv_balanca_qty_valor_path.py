# -*- coding: utf-8 -*-
"""
Prova detalhada — PDV-BALANCA-QTY-VALOR (v26.73).

Caso loja: EAN `2001000004812` → PLU `0010` · total etiqueta R$ 4,81 ·
preço unitário overlay/cadastro (ex. R$ 9,40) → qty = 4,81÷9,40 ≈ 0,512.

Cobre: contratos API/JS · parse · overlay unitário · API mock com overlay ·
cálculo qty · atalho R$/$/= · PIN 9973 · Client HTTP (login) se DB ok.

  .venv/bin/python scripts/verify_pdv_balanca_qty_valor_path.py
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []
EAN = "2001000004812"
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
UNIT = Decimal("9.40")
TOTAL = Decimal("4.81")


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_contratos() -> None:
    print("== Contratos QTY-VALOR ==")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(
        encoding="utf-8"
    )
    check("api_valor_etiqueta_map", "valor_etiqueta_por_id" in views)
    check("api_campo_valor_etiqueta", 'row["valor_etiqueta_balanca"]' in views)
    check(
        "api_nao_preco_por_id_venda",
        'row["preco_venda"] = round(_float_api_json(preco_por_id' not in views,
    )
    check(
        "overlay_aplica_unitario",
        "if ov.preco_venda is not None:" in views
        and "not row.get(\"preco_etiqueta_balanca\")" not in views,
    )
    check("js_valor_etq", "valor_etiqueta_balanca: bal.valorReais" in js)
    check("js_nao_preco_etq", "preco_venda: bal.valorReais" not in js)
    check("js_calcular_qtd", "function calcularQtdPorValorTotal" in js)
    check("js_obter_valor", "function obterValorTotalRapido" in js)
    check("js_limpar_atalhos", "function limparAtalhosBusca" in js)
    check("js_enter_balanca", "digitsEnter.length === 13 && digitsEnter[0] === '2'" in js)
    check("js_carrinho_step", 'step="0.001"' in js)
    check("version_26_73", (ROOT / "VERSION").read_text(encoding="utf-8").strip() == "26.73")


def test_math_loja() -> None:
    print("== Matemática loja (4,81 ÷ 9,40) ==")
    q = (TOTAL / UNIT).quantize(Decimal("0.001"))
    check("qty_0_512", q == Decimal("0.512"), str(q))
    linha = (UNIT * q).quantize(Decimal("0.01"))
    check("linha_4_81", linha == TOTAL, str(linha))
    q10 = (Decimal("10") / UNIT).quantize(Decimal("0.001"))
    check("qty_r10", q10 == Decimal("1.064"), str(q10))
    check("linha_r10", (UNIT * q10).quantize(Decimal("0.01")) == Decimal("10.00"))


def test_parse_overlay() -> None:
    print("== Parse + overlay unitário ==")
    import django

    django.setup()
    from produtos.views import (
        _aplicar_produto_gestao_overlay_em_dict,
        _parse_etiqueta_balanca_ean13_br,
    )

    parsed = _parse_etiqueta_balanca_ean13_br(EAN)
    check("parse_ok", parsed is not None)
    assert parsed
    cod4, preco = parsed
    check("plu", cod4 == "0010", repr(cod4))
    check("total_etq", preco == TOTAL, str(preco))

    def _s(v=""):
        return SimpleNamespace(strip=lambda: v)

    ov = SimpleNamespace(
        nome=_s(""),
        marca=_s(""),
        categoria=_s(""),
        fornecedor_texto=_s(""),
        unidade=_s(""),
        peso_etiqueta="",
        preco_venda=UNIT,
        codigo_barras=_s("0010"),
        codigo_nfe=_s("GM0010-1"),
        subcategoria=_s(""),
        descricao=_s(""),
        ativo_exibicao=None,
        cadastro_extras={},
    )
    row = {
        "preco_venda": float(TOTAL),
        "preco_etiqueta_balanca": True,
        "valor_etiqueta_balanca": float(TOTAL),
    }
    with (
        patch(
            "produtos.cashback_venda_util.cashback_percentual_de_overlay",
            return_value=0.0,
        ),
        patch("produtos.views.extrair_precos_por_forma_overlay", return_value=None),
        patch("produtos.views.extrair_precos_modo_overlay", return_value=None),
        patch("produtos.views.extrair_precos_grupos_overlay", return_value=None),
        patch("produtos.views._overlay_subcategorias_para_row"),
    ):
        _aplicar_produto_gestao_overlay_em_dict(row, ov)
    check("overlay_unit_9_40", abs(float(row["preco_venda"]) - 9.40) < 0.001)
    check("overlay_mantem_total", abs(float(row["valor_etiqueta_balanca"]) - 4.81) < 0.001)


def test_api_mock_overlay() -> None:
    print("== API /api/buscar/ + overlay unitário ==")
    import django

    django.setup()
    from django.test import RequestFactory

    from produtos.views import api_buscar_produtos

    doc = {
        "Id": "pid-qty",
        "_id": "pid-qty",
        "Nome": "Produto PLU 0010",
        "Codigo": "GM0010-1",
        "CodigoNFe": "GM0010-1",
        "CodigoBarras": "0010",
        "ValorVenda": 10.0,  # Mongo — overlay deve sobrescrever p/ 9.40
        "CadastroInativo": False,
        "index_codigos": ["0010", "gm0010-1"],
    }

    class _Ov:
        nome = ""
        marca = ""
        categoria = ""
        fornecedor_texto = ""
        unidade = ""
        peso_etiqueta = ""
        preco_venda = Decimal("9.40")
        codigo_barras = "0010"
        codigo_nfe = "GM0010-1"
        subcategoria = ""
        descricao = ""
        ativo_exibicao = None
        cadastro_extras = {}

        def __getattr__(self, k):
            # strip helpers used by overlay apply
            if k in ("nome", "marca", "categoria", "fornecedor_texto", "unidade",
                      "codigo_barras", "codigo_nfe", "subcategoria", "descricao"):
                v = object.__getattribute__(self, k)
                return SimpleNamespace(strip=lambda vv=v: vv)
            raise AttributeError(k)

    ov = _Ov()
    # Make string fields have .strip
    for attr in (
        "nome",
        "marca",
        "categoria",
        "fornecedor_texto",
        "unidade",
        "codigo_barras",
        "codigo_nfe",
        "subcategoria",
        "descricao",
    ):
        setattr(ov, attr, SimpleNamespace(strip=lambda v=getattr(_Ov, attr, ""): str(v)))

    # Rebuild clean ov like unit test
    def _s(v=""):
        return SimpleNamespace(strip=lambda: v)

    ov = SimpleNamespace(
        nome=_s(""),
        marca=_s(""),
        categoria=_s(""),
        fornecedor_texto=_s(""),
        unidade=_s(""),
        peso_etiqueta="",
        preco_venda=Decimal("9.40"),
        codigo_barras=_s("0010"),
        codigo_nfe=_s("GM0010-1"),
        subcategoria=_s(""),
        descricao=_s(""),
        ativo_exibicao=None,
        cadastro_extras={},
    )

    rf = RequestFactory()
    user = MagicMock()
    user.is_authenticated = True
    user.is_active = True
    user.is_staff = True
    user.is_superuser = True
    user.pk = 1
    user.id = 1

    def _call():
        req = rf.get("/api/buscar/", {"q": EAN})
        req.user = user
        return api_buscar_produtos(req)

    with (
        patch("produtos.views.obter_conexao_mongo", return_value=(None, None)),
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch(
            "produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres",
            return_value=False,
        ),
        patch(
            "produtos.agro_fonte_config.agro_pdv_merge_catalogo_postgres",
            return_value=False,
        ),
        patch(
            "produtos.agro_fonte_config.agro_pdv_catalogo_full_desligado",
            return_value=False,
        ),
        patch(
            "produtos.motor_busca_unificado_util.buscar_documentos_unificado",
            return_value=[doc],
        ),
        patch(
            "produtos.views._merge_produtos_overlay_codigo_consulta",
            side_effect=lambda q, prods, *a, **k: prods,
        ),
        patch("produtos.views._escolher_produto_plu_balanca", return_value=doc),
        patch(
            "produtos.views._overlay_mapa_por_ids_chunked",
            return_value={"pid-qty": ov},
        ),
        patch("django.core.cache.cache.get", return_value=None),
        patch("django.core.cache.cache.set"),
        patch(
            "produtos.cashback_venda_util.cashback_percentual_de_overlay",
            return_value=0.0,
        ),
        patch("produtos.views.extrair_precos_por_forma_overlay", return_value=None),
        patch("produtos.views.extrair_precos_modo_overlay", return_value=None),
        patch("produtos.views.extrair_precos_grupos_overlay", return_value=None),
        patch("produtos.views._overlay_subcategorias_para_row"),
    ):
        r = _call()
    check("http_200", getattr(r, "status_code", 0) == 200)
    data = json.loads(r.content.decode("utf-8"))
    prods = data.get("produtos") or []
    check("http_tem", len(prods) >= 1, str(data)[:180])
    check("http_exact", bool(data.get("exact_barcode_match")))
    if prods:
        p0 = prods[0]
        check(
            "http_unit_9_40",
            abs(float(p0.get("preco_venda") or 0) - 9.40) < 0.001,
            str(p0.get("preco_venda")),
        )
        check(
            "http_valor_4_81",
            abs(float(p0.get("valor_etiqueta_balanca") or 0) - 4.81) < 0.001,
            str(p0.get("valor_etiqueta_balanca")),
        )
        check("http_flag", bool(p0.get("preco_etiqueta_balanca")))
        q = round(4.81 / float(p0.get("preco_venda") or 1), 3)
        check("http_qty_loja", abs(q - 0.512) < 0.0001, str(q))
        linha = round(float(p0["preco_venda"]) * q, 2)
        check("http_linha", abs(linha - 4.81) < 0.01, str(linha))


def test_js_node() -> None:
    print("== JS (node) atalhos R$ + qty ==")
    src = (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(
        encoding="utf-8"
    )
    parts = []
    for pat in (
        r"function digitoVerificadorEan13Primeiros12\(d12\) \{[\s\S]*?\n\}",
        r"function parseEtiquetaBalancaEan13\(digits13\) \{[\s\S]*?\n\}",
        r"function obterQuantidadeRapida\(texto\) \{[\s\S]*?\n\}",
        r"function removerSufixoQuantidade\(texto\) \{[\s\S]*?\n\}",
        r"function calcularQtdPorValorTotal\(precoUnit, valorTotal\) \{[\s\S]*?\n\}",
        r"function obterValorTotalRapido\(texto\) \{[\s\S]*?\n\}",
        r"function removerSufixoValorTotal\(texto\) \{[\s\S]*?\n\}",
        r"function limparAtalhosBusca\(texto\) \{[\s\S]*?\n\}",
        r"function formatarQtdPdv\(qtd\) \{[\s\S]*?\n\}",
        r"function montarProdutoPrecoEtiquetaBalanca\(produto, bal, digits\) \{[\s\S]*?\n\}",
    ):
        m = re.search(pat, src)
        check(f"js_extract_{pat[9:30]}", bool(m))
        if m:
            parts.append(m.group(0))
    if len(parts) < 10:
        return
    snippet = "\n".join(parts) + """
const bal = parseEtiquetaBalancaEan13('2001000004812');
if (!bal || !bal.checkOk || Math.abs(bal.valorReais - 4.81) > 0.001) {
  console.error('FAIL bal', bal); process.exit(1);
}
const prod = montarProdutoPrecoEtiquetaBalanca({ id: '1', preco_venda: 9.40, nome: 'X' }, bal, '2001000004812');
if (Math.abs(Number(prod.preco_venda) - 9.40) > 0.001) { console.error('FAIL unit', prod); process.exit(1); }
if (Math.abs(Number(prod.valor_etiqueta_balanca) - 4.81) > 0.001) { console.error('FAIL etq', prod); process.exit(1); }
const q = calcularQtdPorValorTotal(prod.preco_venda, prod.valor_etiqueta_balanca);
if (Math.abs(q - 0.512) > 0.0001) { console.error('FAIL q', q); process.exit(1); }
const casos = [
  ['racao R$10', 10],
  ['racao r$ 10,50', 10.5],
  ['banana $10', 10],
  ['milho=10', 10],
  ['milho = 10,00', 10],
  ['feijao 10$', 10],
];
for (const [t, esp] of casos) {
  const v = obterValorTotalRapido(t);
  if (Math.abs(v - esp) > 0.001) { console.error('FAIL valor', t, v); process.exit(1); }
  const limpo = limparAtalhosBusca(t);
  if (!limpo || /\\$|=|R\\$/i.test(limpo)) { console.error('FAIL limpo', t, limpo); process.exit(1); }
}
if (obterValorTotalRapido('so texto') !== null) { console.error('FAIL null'); process.exit(1); }
if (formatarQtdPdv(0.512) !== '0.512') { console.error('FAIL fmt', formatarQtdPdv(0.512)); process.exit(1); }
console.log('OK js_qty_valor');
"""
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(snippet)
        tmp = f.name
    try:
        r = subprocess.run(["node", tmp], capture_output=True, text=True, timeout=10)
        check("js_node", r.returncode == 0, (r.stdout + r.stderr).strip()[:220])
    finally:
        Path(tmp).unlink(missing_ok=True)


def test_pin_9973() -> None:
    print("== PIN 9973 ==")
    import django

    django.setup()
    from base.models import PerfilUsuario
    from django.contrib.auth import get_user_model
    from produtos.caixa_util import validar_pin_operador

    User = get_user_model()
    check("pin_env", PIN == "9973", PIN)

    # Garante operador local para prova (SQLite cloud costuma nascer vazio).
    user, _ = User.objects.get_or_create(
        username="pin9973_prova",
        defaults={
            "is_staff": True,
            "is_superuser": True,
            "is_active": True,
            "first_name": "Prova",
        },
    )
    user.set_password("prova-local-9973")
    user.is_staff = True
    user.is_superuser = True
    user.is_active = True
    user.save()
    perfil = PerfilUsuario.objects.filter(senha_rapida=PIN).first()
    created = False
    if not perfil:
        perfil = PerfilUsuario.objects.filter(user=user).first()
    if not perfil:
        cod = "9973"
        while PerfilUsuario.objects.filter(codigo_vendedor=cod).exists():
            cod = f"9{str(PerfilUsuario.objects.count()).zfill(3)}"[:4]
        perfil = PerfilUsuario.objects.create(
            user=user,
            codigo_vendedor=cod,
            senha_rapida=PIN,
            ativo=True,
            primeiro_acesso=False,
        )
        created = True
    else:
        perfil.senha_rapida = PIN
        perfil.ativo = True
        perfil.primeiro_acesso = False
        if perfil.user_id != user.id:
            # Outro user com o PIN — só reativa
            pass
        perfil.save()
    ok, msg = validar_pin_operador(PIN)
    check("pin_9973_vivo", ok, msg or ("created" if created else "ok"))


def test_client_http_login() -> None:
    print("== Client HTTP login + /api/buscar/ ==")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings

    User = get_user_model()
    user = User.objects.filter(username="pin9973_prova").first()
    if not user:
        check("client_user", False, "sem user pin9973_prova")
        return

    doc = {
        "Id": "pid-http",
        "_id": "pid-http",
        "Nome": "HTTP PLU",
        "Codigo": "GM0010-1",
        "CodigoNFe": "GM0010-1",
        "CodigoBarras": "0010",
        "ValorVenda": 9.40,
        "CadastroInativo": False,
        "index_codigos": ["0010"],
    }
    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client()
        logged = c.login(username="pin9973_prova", password="prova-local-9973")
        check("client_login", bool(logged))
        if not logged:
            return
        with (
            patch("produtos.views.obter_conexao_mongo", return_value=(None, None)),
            patch(
                "produtos.agro_fonte_config.agro_catalogo_usa_postgres",
                return_value=True,
            ),
            patch(
                "produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres",
                return_value=False,
            ),
            patch(
                "produtos.agro_fonte_config.agro_pdv_merge_catalogo_postgres",
                return_value=False,
            ),
            patch(
                "produtos.agro_fonte_config.agro_pdv_catalogo_full_desligado",
                return_value=False,
            ),
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
            patch("django.core.cache.cache.set"),
        ):
            r = c.get(f"/api/buscar/?q={EAN}")
        check("client_buscar_200", r.status_code == 200, str(r.status_code))
        try:
            data = r.json()
        except Exception as e:
            check("client_json", False, str(e))
            return
        prods = data.get("produtos") or []
        check("client_produto", len(prods) >= 1)
        if prods:
            p0 = prods[0]
            check(
                "client_unit",
                abs(float(p0.get("preco_venda") or 0) - 9.40) < 0.001,
                str(p0.get("preco_venda")),
            )
            check(
                "client_valor",
                abs(float(p0.get("valor_etiqueta_balanca") or 0) - 4.81) < 0.001,
                str(p0.get("valor_etiqueta_balanca")),
            )
            q = round(
                float(p0["valor_etiqueta_balanca"]) / float(p0["preco_venda"]), 3
            )
            check("client_qty", abs(q - 0.512) < 0.0001, str(q))

        r2 = c.get("/consulta/")
        check("client_consulta", r2.status_code in (200, 302), str(r2.status_code))
        if r2.status_code == 200:
            body = r2.content.decode("utf-8", "replace")
            check(
                "client_js_ref",
                "consulta_produtos.js" in body or "consulta_produtos" in body,
                "script tag",
            )


def test_unit_django() -> None:
    print("== Unit Django tests_balanca_etiqueta ==")
    r = subprocess.run(
        [
            str(ROOT / ".venv/bin/python"),
            "manage.py",
            "test",
            "produtos.tests_balanca_etiqueta",
            "-v1",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=120,
    )
    out = (r.stdout + r.stderr).strip()
    check("unit_ok", r.returncode == 0, out[-180:].replace("\n", " "))


def main() -> int:
    print("PDV-BALANCA-QTY-VALOR — prova detalhada\n")
    test_contratos()
    test_math_loja()
    test_parse_overlay()
    test_api_mock_overlay()
    test_js_node()
    test_pin_9973()
    test_client_http_login()
    test_unit_django()
    print(f"\nOK={len(oks)} FAIL={len(fails)}")
    if fails:
        print("FAILS:", ", ".join(fails))
        print("PREP_FAILS=" + str(len(fails)))
        return 1
    print("PREP_FAILS=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
