"""VERIFY PDV-RACOES — path cadastro → catálogo → PDV → carrinho.

Run: python scripts/verify_pdv_racoes.py
"""
from __future__ import annotations

import ast
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails = 0


def ok(msg: str) -> None:
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    global fails
    fails += 1
    print(f" FAIL {msg}")


def read(rel: str) -> str:
    p = ROOT / rel
    if not p.is_file():
        fail(f"ausente {rel}")
        return ""
    return p.read_text(encoding="utf-8")


def check_ast() -> None:
    for rel in (
        "produtos/pdv_racoes_util.py",
        "produtos/tests_pdv_racoes.py",
        "produtos/catalogo_agro.py",
        "produtos/views.py",
    ):
        try:
            ast.parse(read(rel))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_cadastro() -> None:
    modal = read("produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html")
    for needle, label in (
        ('id="edit-subcategoria"', "cadastro Sub 1"),
        ('id="edit-subcategoria-2"', "cadastro Sub 2"),
        ('id="edit-peso-etiqueta"', "cadastro Peso"),
        ("subcategoria_2: gv('edit-subcategoria-2')", "save Sub 2"),
        ("peso_etiqueta: gv('edit-peso-etiqueta')", "save Peso"),
        ("subcategoria: gv('edit-subcategoria')", "save Sub 1"),
    ):
        if needle not in modal:
            fail(label)
        else:
            ok(label)
    views = read("produtos/views.py")
    peso_grava = (
        ('if "peso_etiqueta" in payload:' in views and "ov.peso_etiqueta" in views)
        or '_aplicar_txt_ov("peso_etiqueta"' in views
    )
    if not peso_grava:
        fail("overlay grava peso")
    else:
        ok("overlay grava peso")
    sub2_grava = (
        ('if "subcategoria_2" in payload:' in views and "ov.subcategoria_2" in views)
        or '_aplicar_txt_ov("subcategoria_2"' in views
    )
    if not sub2_grava:
        fail("overlay grava sub2")
    else:
        ok("overlay grava sub2")
    sub1_grava = (
        'if "subcategoria" in payload:' in views or '_aplicar_txt_ov("subcategoria"' in views
    )
    if not sub1_grava:
        fail("overlay grava sub1")
    else:
        ok("overlay grava sub1")
    save_idx = views.find("_api_produtos_gestao_overlay_salvar_core")
    if save_idx < 0:
        save_idx = views.find('if "peso_etiqueta" in payload:')
    if save_idx < 0:
        save_idx = views.find('_aplicar_txt_ov("peso_etiqueta"')
    inv_idx = views.find("cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)", max(save_idx, 0))
    save_chunk = views[save_idx : save_idx + 80000] if save_idx >= 0 else ""
    if save_idx < 0 or inv_idx < 0 or inv_idx - save_idx > 80000:
        fail("salvar Agro não invalida catálogo PDV")
    else:
        ok("salvar Agro invalida catálogo PDV")
    if "pdv_catalogo_slim_v3:" not in save_chunk:
        fail("salvar Agro não invalida slim PDV")
    else:
        ok("salvar Agro invalida slim PDV")


def check_catalogo() -> None:
    views = read("produtos/views.py")
    chunk_start = views.find("def _catalogo_pdv_montar_produtos_somente_postgres")
    chunk = views[chunk_start : chunk_start + 5000] if chunk_start >= 0 else ""
    for fld in ('"subcategoria_2"', '"peso_etiqueta"', '"categoria"', '"subcategoria"', '"marca"'):
        if fld not in chunk:
            fail(f"catálogo PG sem {fld}")
        else:
            ok(f"catálogo PG {fld}")
    if 'CATALOGO_PDV_CACHE_ENTRY_KEY = "pdv_catalogo_produtos_por_dia_v3"' not in views:
        fail("cache catálogo v3")
    else:
        ok("cache catálogo v3")
    if "p.get('subcategoria_2'" not in views or "p.get('peso_etiqueta'" not in views:
        fail("versão hash catálogo sem sub2/peso")
    else:
        ok("versão hash catálogo sub2+peso")
    agro = read("produtos/catalogo_agro.py")
    slim_fn = agro.find("def listar_slim_rows_pdv")
    slim_chunk = agro[slim_fn : slim_fn + 9000] if slim_fn >= 0 else ""
    for fld in ('"subcategoria_2"', '"peso_etiqueta"', '"categoria"', '"subcategoria"'):
        if fld not in slim_chunk:
            fail(f"slim sem {fld}")
        else:
            ok(f"slim {fld}")
    mesclar_fn = agro.find("def mesclar_catalogo_pdv_cache")
    mesclar = agro[mesclar_fn : mesclar_fn + 4000] if mesclar_fn >= 0 else ""
    if 'ex["subcategoria_2"]' not in mesclar or 'ex["peso_etiqueta"]' not in mesclar:
        fail("mesclar sem sub2/peso")
    else:
        ok("mesclar sub2+peso")
    apply_fn = views.find("def _aplicar_produto_gestao_overlay_em_dict")
    apply_chunk = views[apply_fn : apply_fn + 2500] if apply_fn >= 0 else ""
    if 'row["peso_etiqueta"]' not in apply_chunk:
        fail("overlay apply sem peso")
    else:
        ok("overlay apply peso")
    if "_overlay_subcategorias_para_row" not in apply_chunk:
        fail("overlay apply sem sub 2–4")
    else:
        ok("overlay apply sub 2–4")


def check_frontend_parity() -> None:
    from produtos.pdv_racoes_util import PESOS_KG_RACOES, TIPOS_RACOES

    js = read("produtos/static/produtos/js/pdv_wizard.js")
    wiz = read("produtos/templates/produtos/pdv_wizard.html")
    step = read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    if 'id="pdv-btn-racoes"' not in step:
        fail("botão Rações")
    else:
        ok("botão Rações")
    if "wireRacoesUi();" not in js:
        fail("bind wireRacoesUi")
    else:
        ok("bind wireRacoesUi")
    if "pdvRacoesOverlayAberto()" not in js or "F2" not in js:
        fail("F2 vs overlay Rações")
    else:
        ok("F2 não rouba overlay")
    if "State.addItem(p, 1)" not in js:
        fail("addItem carrinho")
    else:
        ok("addItem carrinho")
    check_lista_overlay_path(js, wiz)
    urls = read("produtos/urls.py")
    if "api/pdv/racoes-overlay/" not in urls:
        fail("rota racoes-overlay")
    else:
        ok("rota racoes-overlay")
    if "/api/pdv/racoes-overlay/" not in js or "pdvRacoesSincronizarCadastro" not in js:
        fail("JS puxa cadastro vivo Racoes")
    elif "loadWizardCatalog()" not in js.split("function pdvRacoesSincronizarCadastro")[1].split("function pdvRacoesAbrir")[0]:
        fail("sync Racoes nao espera catalogo")
    elif "aplicarWizardPatchesProdutos(d.itens)" not in js:
        fail("JS nao aplica patch overlay no catalogo")
    elif "pdvRacoesSyncP.finally" not in js:
        fail("IrMarca nao espera sync cadastro")
    else:
        ok("JS puxa cadastro vivo Racoes (catalogo + patch + espera)")
    m = re.search(r"var PDV_RACOES_TIPOS = \[(.*?)\];", js, re.S)
    if not m:
        fail("JS PDV_RACOES_TIPOS")
        return
    bloco = m.group(1)
    for t in TIPOS_RACOES:
        if f"id: '{t['id']}'" not in bloco and f'id: "{t["id"]}"' not in bloco:
            fail(f"JS tipo id {t['id']}")
        elif f"sub1: '{t['sub1']}'" not in bloco and f'sub1: "{t["sub1"]}"' not in bloco:
            fail(f"JS sub1 {t['id']}")
        elif f"sub2: '{t['sub2']}'" not in bloco and f'sub2: "{t["sub2"]}"' not in bloco:
            fail(f"JS sub2 {t['id']}")
        else:
            ok(f"JS={t['id']}")
        if f'data-tipo="{t["id"]}"' not in wiz:
            fail(f"HTML data-tipo {t['id']}")
        else:
            ok(f"HTML={t['id']}")
    for kg in PESOS_KG_RACOES:
        if f"'kg:{kg}'" not in js and f'"kg:{kg}"' not in js:
            fail(f"JS peso kg:{kg}")
        else:
            ok(f"JS peso kg:{kg}")
    if "key: 'kg:2.5'" not in js and 'key: "kg:2.5"' not in js:
        fail("JS botao saco 2,5")
    elif "Saco 2,5 kg" not in js:
        fail("JS label Saco 2,5 kg")
    else:
        ok("JS botao+label Saco 2,5 kg")
    if "'pacote'" not in js and '"pacote"' not in js:
        fail("JS pacote")
    else:
        ok("JS pacote")
    for needle, label in (
        ('id="pdv-racoes-overlay"', "modal"),
        ("Todas as marcas", "todas marcas"),
        ("Todos os tamanhos", "todos tamanhos"),
        ("pdv-racoes-voltar", "voltar"),
    ):
        if needle not in wiz:
            fail(label)
        else:
            ok(label)
    home = read("pdv/views.py")
    if "pdv_wizard.html" not in home:
        fail("pdv_home não usa wizard")
    else:
            ok("pdv_home usa wizard")


def check_lista_overlay_path(js: str, wiz: str) -> None:
    """Path: tipo → marca → tamanho → lista (não vai direto ao carrinho)."""
    if "function pdvRacoesAdicionar" in js:
        fail("peso ainda chama pdvRacoesAdicionar (deveria abrir lista)")
    else:
        ok("peso nao manda direto ao carrinho")
    peso_click = js.split("var pesosGrid = document.getElementById('pdv-racoes-pesos-grid')")
    if len(peso_click) < 2:
        fail("bind pesosGrid")
    else:
        chunk = peso_click[1].split("var addTodas")[0]
        if "pdvRacoesIrLista(b.getAttribute('data-peso'))" not in chunk:
            fail("clique tamanho nao abre lista")
        else:
            ok("clique tamanho abre lista")
    if "pdvRacoesIrLista(null)" not in js:
        fail("Todos os tamanhos nao abre lista")
    else:
        ok("Todos os tamanhos abre lista")
    ir = js.split("function pdvRacoesIrLista")[1].split("function pdvRacoesAddUm")[0] if "function pdvRacoesIrLista" in js else ""
    if "return pa - pb" not in ir or "pdvRacoesShowStep('lista')" not in ir:
        fail("IrLista sem sort preco ou sem step lista")
    elif "caixaAbertoParaVenda()" not in ir:
        fail("IrLista sem checar caixa")
    else:
        ok("IrLista ordena preco crescente + checa caixa")
    add_um = js.split("function pdvRacoesAddUm")[1].split("function pdvRacoesAddTodas")[0] if "function pdvRacoesAddUm" in js else ""
    if "State.addItem(p, 1)" not in add_um:
        fail("AddUm sem addItem")
    elif "pdvRacoesFechar" in add_um:
        fail("AddUm fecha overlay (deveria ficar aberto)")
    elif "pdvRacoesRenderLista()" not in add_um:
        fail("AddUm sem refresh visual")
    else:
        ok("AddUm entra no carrinho e atualiza linha")
    add_todas = js.split("function pdvRacoesAddTodas")[1].split("function pdvRacoesIrMarca")[0] if "function pdvRacoesAddTodas" in js else ""
    if "pdvRacoesQtdCarrinho(resolveProdutoId(p)) > 0" not in add_todas:
        fail("AddTodas nao pula ja no carrinho")
    elif "pdvRacoesFechar" in add_todas:
        fail("AddTodas fecha overlay")
    else:
        ok("AddTodas so as que faltam + overlay aberto")
    wire = js.split("function wireRacoesUi")[1].split("function wireCadastroRapidoUi")[0] if "function wireRacoesUi" in js else ""
    if "step === 'lista'" not in wire or "ShowStep('peso')" not in wire:
        fail("Voltar da lista nao volta no tamanho")
    else:
        ok("Voltar lista -> tamanho")
    if "if (pdvRacoesSel.step === 'lista') return;" not in wire:
        fail("clique fora na lista fecha (nao deveria)")
    else:
        ok("clique fora na lista nao fecha")
    if "ev.key !== 'Escape'" not in wire or "pdvRacoesOverlayAberto()" not in wire or "pdvRacoesFechar()" not in wire:
        fail("Esc nao fecha overlay Racoes")
    else:
        ok("Esc fecha overlay")
    for needle, label in (
        ('id="pdv-racoes-step-lista"', "HTML step lista"),
        ('id="pdv-racoes-add-todas"', "HTML Adicionar todas"),
        ('id="pdv-racoes-lista-body"', "HTML corpo tabela"),
        ('id="pdv-racoes-fechar"', "HTML Fechar ×"),
        ("Adicionar todas", "texto Adicionar todas"),
        ("pdv-racoes-row-ok", "CSS/JS linha verde"),
        ("No carrinho", "badge No carrinho"),
        ("Adicionar +1", "botao +1 depois de adicionar"),
        ("#pdv-racoes-overlay.is-lista", "CSS overlay grande"),
        ("pdv-racoes-panel", "CSS painel"),
    ):
        blob = js + wiz
        if needle not in blob:
            fail(label)
        else:
            ok(label)
    if "cancelar.textContent = step === 'lista' ? 'Fechar'" not in js:
        fail("botao Fechar na lista")
    else:
        ok("botao Fechar na lista")
    check_lista_dense(js, wiz)
    try:
        import subprocess

        r = subprocess.run(
            ["node", "--check", str(ROOT / "produtos/static/produtos/js/pdv_wizard.js")],
            capture_output=True,
            text=True,
            timeout=20,
        )
        if r.returncode != 0:
            fail(f"node --check: {(r.stderr or r.stdout or '').strip()[:200]}")
        else:
            ok("node --check pdv_wizard.js")
    except (OSError, subprocess.TimeoutExpired) as exc:
        fail(f"node --check indisponivel: {exc}")


def check_lista_dense(js: str, wiz: str) -> None:
    """Lista compacta: uma linha por produto, sem zoom local."""
    css_a = wiz.find("#pdv-racoes-overlay.is-lista .pdv-racoes-panel")
    css_b = wiz.find("#pdv-step1-search-wrap .pdv-step1-search-f2 {")
    css = wiz[css_a:css_b] if css_a >= 0 and css_b > css_a else ""
    if not css:
        fail("bloco CSS lista Racoes")
        return
    if "min(96dvh, 70rem)" not in css or "min(96rem, 99vw)" not in css:
        fail("overlay lista tamanho compacto")
    else:
        ok("overlay lista maior (70rem / 96rem)")
    if "zoom:" in css:
        fail("zoom local na lista Racoes")
    else:
        ok("lista sem zoom local")
    tbl = css[css.find("#pdv-racoes-lista-table {") :] if "#pdv-racoes-lista-table {" in css else ""
    if "white-space: nowrap" not in tbl:
        fail("td/th lista sem nowrap")
    else:
        ok("td/th lista nowrap")
    if "padding: 0.28rem 0.55rem" not in tbl:
        fail("padding lista nao compacto")
    else:
        ok("padding lista compacto")
    if "padding: 0.7rem 0.85rem" in tbl:
        fail("padding lista ainda alto")
    else:
        ok("padding alto removido")
    if "min-height: 3.1rem" in tbl:
        fail("botao Adicionar ainda alto")
    elif "min-height: 2.25rem" not in tbl or "white-space: nowrap" not in css[css.find(".pdv-racoes-add-btn") :]:
        fail("botao Adicionar nao compacto/nowrap")
    else:
        ok("botao Adicionar compacto + nowrap")
    if ".pdv-racoes-qtd-ok" not in css or "white-space: nowrap" not in css[css.find(".pdv-racoes-qtd-ok") :]:
        fail("badge No carrinho quebra linha")
    else:
        ok("badge No carrinho nowrap")
    foot = css[css.find("#pdv-racoes-overlay.is-lista footer") :]
    if "flex-wrap: nowrap" not in foot[:180]:
        fail("footer lista quebra linha")
    else:
        ok("footer lista nowrap")
    render = js.split("function pdvRacoesRenderLista")[1].split("function pdvRacoesIrLista")[0] if "function pdvRacoesRenderLista" in js else ""
    if 'title="' not in render:
        fail("nome da lista sem title")
    else:
        ok("nome da lista com title")


def check_util_cenarios() -> None:
    from produtos.pdv_racoes_util import (
        TIPOS_RACOES,
        filtrar_racoes,
        parse_peso_racoes,
        patch_racoes_de_campos,
        tipo_racoes_por_id,
    )

    def _js_parse_peso(raw: str) -> str | None:
        t = (raw or "").strip().lower()
        if not t:
            return None
        if t.startswith("pacote") or t in ("pct", "p10"):
            return "pacote"
        t = t.replace(",", ".")
        t = re.sub(r"\s*k\s*g\s*$", "", t, flags=re.I)
        t = re.sub(r"\s*quilos?\s*$", "", t, flags=re.I).strip()
        try:
            n = float(t)
        except ValueError:
            return None
        for p in (1, 2.5, 5, 10, 15, 20, 25):
            if abs(n - p) <= 0.05:
                return f"kg:{p}" if p != int(p) else f"kg:{int(p)}"
        return None

    for amostra in ("2,5", "2.5", "2,50 kg", "25", "15 kg", "pacote"):
        py = parse_peso_racoes(amostra)
        js = _js_parse_peso(amostra)
        if py != js:
            fail(f"JS!=Python peso '{amostra}' py={py} js={js}")
            break
    else:
        ok("JS=Python chave peso 2,5/25/pacote")

    if parse_peso_racoes("15 kg") != "kg:15" or parse_peso_racoes("PACOTE") != "pacote":
        fail("parse peso 15/pacote")
    elif parse_peso_racoes("2,5") != "kg:2.5":
        fail("parse peso 2,5")
    elif parse_peso_racoes("2.5kg") != "kg:2.5" or parse_peso_racoes("2,50") != "kg:2.5":
        fail("parse peso 2.5kg/2,50")
    elif parse_peso_racoes("25") != "kg:25" or parse_peso_racoes("2,5") == parse_peso_racoes("25"):
        fail("2,5 nao pode virar 25")
    else:
        ok("parse peso 15/pacote/2,5 isolado do 25")
    if len(TIPOS_RACOES) != 8:
        fail("8 tipos")
    else:
        ok("8 tipos")
    catalogo = [
        {
            "id": "gm50",
            "categoria": "Rações",
            "subcategoria": "Cão",
            "subcategoria_2": "Adulto",
            "marca": "ESTIMACAO",
            "peso_etiqueta": "15kg",
        },
        {
            "id": "rp10",
            "categoria": "Rações",
            "subcategoria": "Cão",
            "subcategoria_2": "Adulto RP",
            "marca": "GOLDEN",
            "peso_etiqueta": "10",
        },
        {
            "id": "gfil",
            "categoria": "Rações",
            "subcategoria": "Gato",
            "subcategoria_2": "Filhote",
            "marca": "PREMIER",
            "peso_etiqueta": "1",
        },
        {
            "id": "pct",
            "categoria": "Rações",
            "subcategoria": "Cão",
            "subcategoria_2": "Adulto",
            "marca": "ESTIMACAO",
            "peso_etiqueta": "pacote",
        },
        {
            "id": "s25",
            "categoria": "Rações",
            "subcategoria": "Cão",
            "subcategoria_2": "Adulto",
            "marca": "ESTIMACAO",
            "peso_etiqueta": "2,5",
        },
        {
            "id": "old",
            "categoria": "Rações",
            "subcategoria": "cachorro",
            "subcategoria_2": "Adulto",
            "marca": "X",
            "peso_etiqueta": "15",
        },
    ]
    adulto = tipo_racoes_por_id("cao_adulto")
    hit = filtrar_racoes(catalogo, adulto, marca="estimacao", peso_key="kg:15")
    if [r["id"] for r in hit] != ["gm50"]:
        fail(f"cenário GM50 adulto 15kg: {[r['id'] for r in hit]}")
    else:
        ok("cenário GM50 adulto 15kg")
    if [r["id"] for r in filtrar_racoes(catalogo, adulto, peso_key=None)] != ["gm50", "pct", "s25"]:
        fail("todas marcas+tamanhos adulto")
    else:
        ok("todas marcas+tamanhos adulto")
    if [r["id"] for r in filtrar_racoes(catalogo, adulto, marca="estimacao", peso_key="kg:2.5")] != ["s25"]:
        fail("cenario adulto 2,5 kg")
    else:
        ok("cenario adulto 2,5 kg")
    if [r["id"] for r in filtrar_racoes(catalogo, adulto, peso_key="kg:25")]:
        fail("2,5 nao deve aparecer em 25 kg")
    else:
        ok("2,5 isolado do 25 kg")
    rp = tipo_racoes_por_id("cao_adulto_rp")
    if [r["id"] for r in filtrar_racoes(catalogo, rp)] != ["rp10"]:
        fail("Adulto RP isolado")
    else:
        ok("Adulto RP isolado")
    gato = tipo_racoes_por_id("gato_filhote")
    if [r["id"] for r in filtrar_racoes(catalogo, gato, peso_key="kg:1")] != ["gfil"]:
        fail("gato filhote granel")
    else:
        ok("gato filhote granel")
    stale = {
        "id": "origens15",
        "categoria": "",
        "subcategoria": "cachorro",
        "subcategoria_2": "",
        "marca": "ORIGENS",
        "peso_etiqueta": "",
    }
    if filtrar_racoes([stale], adulto):
        fail("origens velho nao deveria achar")
    else:
        ok("origens catalogo velho vazio")
    patch = patch_racoes_de_campos(
        pid="origens15",
        categoria="Rações",
        sub1="Cão",
        sub2="Adulto",
        peso="15",
        marca="ORIGENS",
    )
    merged = {**stale, **(patch or {})}
    if [r["id"] for r in filtrar_racoes([merged], adulto, marca="ORIGENS", peso_key="kg:15")] != ["origens15"]:
        fail("origens apos patch cadastro")
    else:
        ok("origens apos patch cadastro")


def check_overlay_row() -> None:
    import django

    django.setup()
    from types import SimpleNamespace
    from unittest.mock import patch

    from produtos.views import _aplicar_produto_gestao_overlay_em_dict

    ov = SimpleNamespace(
        nome="",
        marca="ESTIMACAO",
        categoria="Rações",
        fornecedor_texto="",
        unidade="UN",
        peso_etiqueta="15",
        codigo_barras="",
        codigo_nfe="",
        subcategoria="Cão",
        descricao="",
        ativo_exibicao=None,
        subcategoria_2="Adulto",
        subcategoria_3="",
        subcategoria_4="",
        preco_venda=None,
        cadastro_extras={},
        cashback_percentual=None,
    )
    row = {
        "categoria": "velha",
        "subcategoria": "cachorro",
        "subcategoria_2": "",
        "peso_etiqueta": "",
        "marca": "",
    }
    with patch("produtos.catalogo_delivery_util.aplicar_imagem_delivery_no_row", lambda r, o: r):
        _aplicar_produto_gestao_overlay_em_dict(row, ov)
    if row.get("categoria") != "Rações":
        fail(f"overlay cat={row.get('categoria')}")
    elif row.get("subcategoria") != "Cão":
        fail(f"overlay sub1={row.get('subcategoria')}")
    elif row.get("subcategoria_2") != "Adulto":
        fail(f"overlay sub2={row.get('subcategoria_2')}")
    elif row.get("peso_etiqueta") != "15":
        fail(f"overlay peso={row.get('peso_etiqueta')}")
    elif row.get("marca") != "ESTIMACAO":
        fail(f"overlay marca={row.get('marca')}")
    else:
        ok("overlay aplica cat/sub1/sub2/peso/marca no row PDV")

    ov.peso_etiqueta = "2,5"
    row2 = {
        "categoria": "velha",
        "subcategoria": "cachorro",
        "subcategoria_2": "",
        "peso_etiqueta": "",
        "marca": "",
    }
    with patch("produtos.catalogo_delivery_util.aplicar_imagem_delivery_no_row", lambda r, o: r):
        _aplicar_produto_gestao_overlay_em_dict(row2, ov)
    if row2.get("peso_etiqueta") != "2,5":
        fail(f"overlay peso 2,5={row2.get('peso_etiqueta')}")
    else:
        ok("overlay grava peso 2,5 como texto")


def main() -> int:
    print("VERIFY PDV-RACOES")
    check_ast()
    check_cadastro()
    check_catalogo()
    check_frontend_parity()
    check_util_cenarios()
    check_overlay_row()
    if fails:
        print(f"FAIL {fails}")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
