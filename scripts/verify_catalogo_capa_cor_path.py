"""VERIFY CATALOGO-CAPA-COR — path PG → API → gestão/aba 10 → vitrine.

Cobre: capa e cor em N1–N5 no Postgres (não localStorage); API em qualquer
nível; gestão + cadastro aba 10; cards da vitrine; skip-geral intacto.

Run: python3 scripts/verify_catalogo_capa_cor_path.py
"""
from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails = 0
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
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


def must_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    for n in needles:
        if n not in txt:
            fail(f"{label or rel}: falta `{n[:80]}`")
        else:
            ok(f"{label or rel}: `{n[:52]}`")


def must_not_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    for n in needles:
        if n in txt:
            fail(f"{label or rel}: não deveria ter `{n[:70]}`")
        else:
            ok(f"{label or rel}: sem `{n[:40]}`")


def _fn(src: str, name: str) -> ast.FunctionDef | None:
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        fail(f"AST parse: {e}")
        return None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def _dump(fn: ast.FunctionDef | None) -> str:
    if fn is None:
        return ""
    return ast.dump(fn, include_attributes=False)


def check_ast() -> None:
    for rel in (
        "produtos/catalogo_delivery_util.py",
        "produtos/views_catalogo_delivery.py",
        "produtos/models.py",
        "produtos/migrations/0098_catalogo_categoria_cor.py",
        "produtos/tests_catalogo_categoria_visual.py",
        "scripts/verify_catalogo_capa_cor_path.py",
    ):
        try:
            ast.parse(read(rel))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_postgres_fonte() -> None:
    must_contain(
        "produtos/models.py",
        [
            "class CatalogoDeliveryCategoria",
            'help_text="Foto de capa do card no catálogo (qualquer nível)."',
            'help_text="Cor do card (#059669). Vazio = verde padrão. Vale em qualquer nível."',
            "max_length=7",
        ],
        "model",
    )
    mig = read("produtos/migrations/0098_catalogo_categoria_cor.py")
    if "0097_repasse_reserva_lucro_log" not in mig:
        fail("0098 não depende de 0097")
    else:
        ok("0098 depende de 0097")
    if 'name="cor"' not in mig and "name='cor'" not in mig:
        fail("0098 sem AddField cor")
    else:
        ok("0098 AddField cor")
    must_not_contain(
        "produtos/templates/produtos/catalogo/catalogo_gestao.html",
        ["localStorage"],
        "gestão sem localStorage",
    )
    must_not_contain(
        "produtos/templates/produtos/catalogo/_gestao_cat_visual.html",
        ["localStorage"],
        "visual sem localStorage",
    )
    gestao_js_capa = read("produtos/templates/produtos/catalogo/catalogo_gestao.html")
    if "localStorage.setItem" in gestao_js_capa and (
        "imagem_base64" in gestao_js_capa[gestao_js_capa.find("localStorage") : gestao_js_capa.find("localStorage") + 200]
        if "localStorage" in gestao_js_capa
        else False
    ):
        fail("gestão grava capa em localStorage")
    else:
        ok("gestão não grava capa em localStorage")


def check_util() -> None:
    must_contain(
        "produtos/catalogo_delivery_util.py",
        [
            'COR_CARD_PADRAO = "#059669"',
            "def normalizar_cor_categoria",
            "def cor_card_categoria",
            "def url_imagem_categoria",
            "def salvar_cor_categoria",
            "def salvar_foto_categoria",
            'return f"/catalogo/cat-img/{int(cat.pk)}/?v={len(b64)}"',
            'cat.save(update_fields=["cor"])',
            '"cor": normalizar_cor_categoria(getattr(c, "cor", "") or "")',
            '"imagem": url_imagem_categoria(c)',
            '"cor": c.get("cor") or ""',
            '"imagem": c.get("imagem") or ""',
            '"cor": node.get("cor") or ""',
            '"imagem": node.get("imagem") or ""',
        ],
        "util",
    )
    src = read("produtos/catalogo_delivery_util.py")
    fn = _fn(src, "listar_categorias_arvore")
    dumped = _dump(fn)
    if "cor" not in dumped or "url_imagem_categoria" not in dumped:
        fail("listar_categorias_arvore sem cor/imagem no nó")
    else:
        ok("listar_categorias_arvore inclui cor+imagem em todo nó")
    # Árvore não embute data URL (estoura a página)
    arvore = src.split("def listar_categorias_arvore")[1].split("def opcoes_pai_categoria")[0]
    if "data_url_imagem_categoria" in arvore or "data:image" in arvore:
        fail("árvore embute data URL da capa")
    else:
        ok("árvore usa URL /catalogo/cat-img/ (não data URL)")


def check_api() -> None:
    src = read("produtos/views_catalogo_delivery.py")
    must_not_contain(
        "produtos/views_catalogo_delivery.py",
        ["parent__isnull=True", "só principal"],
        "API qualquer nível",
    )
    must_contain(
        "produtos/views_catalogo_delivery.py",
        [
            "qualquer nível",
            "def api_catalogo_categoria_foto",
            "def catalogo_categoria_imagem_view",
            "salvar_cor_categoria",
            'if "cor" in payload',
            "if not tem_imagem and cor_in is not None",
            "data_url_imagem_categoria",
            "url_imagem_categoria",
            "cor=cor",
        ],
        "views",
    )
    foto = src.split("def api_catalogo_categoria_foto")[1].split("def catalogo_categoria_imagem_view")[0]
    if "parent__isnull" in foto:
        fail("api_catalogo_categoria_foto ainda filtra raiz")
    else:
        ok("api foto sem filtro de raiz")
    if "if not tem_imagem and cor_in is not None" not in foto:
        fail("API sem ramo só-cor")
    else:
        ok("API aceita gravar só a cor")
    # Foto sem chave cor não pode apagar cor
    if 'if "cor" in payload' not in foto and 'if "cor" in request.POST' not in foto:
        fail("API sempre manda cor (risco de apagar)")
    else:
        ok("API só grava cor se a chave vier no payload")
    img = src.split("def catalogo_categoria_imagem_view")[1].split("def catalogo_gestao_view")[0]
    if "parent__isnull" in img:
        fail("cat-img restringe raiz")
    else:
        ok("cat-img serve qualquer pk")
    criar = src.split("def api_catalogo_categoria_criar")[1].split("def api_catalogo_categoria_excluir")[0]
    if "cor=cor" not in criar:
        fail("criar categoria ignora cor")
    else:
        ok("criar categoria aceita cor")
    fallback = src.split('if acao == "foto_categoria"')[1].split("if msg ==")[0]
    if "parent__isnull" in fallback:
        fail("POST gestão foto_categoria ainda é só raiz")
    else:
        ok("POST gestão foto_categoria qualquer nível")
    must_contain(
        "produtos/urls.py",
        [
            "api_catalogo_categoria_foto",
            "catalogo/cat-img/<int:pk>/",
            "catalogo_categoria_imagem",
        ],
        "urls",
    )


def check_gestao() -> None:
    must_contain(
        "produtos/templates/produtos/catalogo/_gestao_cat_visual.html",
        [
            "js-foto-cat-box",
            "js-foto-cat-btn",
            "js-cor-cat-btn",
            "Salvar capa",
            "Salvar cor",
            "js-cor-cat-input",
            "{% csrf_token %}",
        ],
        "partial visual",
    )
    must_contain(
        "produtos/templates/produtos/catalogo/catalogo_gestao.html",
        [
            '{% include "produtos/catalogo/_gestao_cat_visual.html" with no=c %}',
            "Salvar capa",
            "Salvar cor",
            "não use «Salvar loja»",
            "if (payload.cor !== undefined) o.cor = payload.cor",
            "API_FOTO",
            "postFoto",
        ],
        "gestão N1",
    )
    must_contain(
        "produtos/templates/produtos/catalogo/_gestao_cat_filho.html",
        [
            '{% include "produtos/catalogo/_gestao_cat_visual.html" with no=no %}',
            "profundidade < 5",
            "_gestao_cat_filho.html",
        ],
        "gestão N2–N5",
    )
    js = read("produtos/templates/produtos/catalogo/catalogo_gestao.html")
    # Salvar capa não manda cor (não apaga)
    chunk = js.split("btn.addEventListener('click'")[1].split("if (rem)")[0]
    if "cor:" in chunk.replace("corInp", ""):
        # o click da foto não deve passar cor
        if "cor:" in chunk and "payload.cor" not in chunk:
            fail("Salvar capa da gestão envia cor e pode apagar")
        else:
            ok("Salvar capa da gestão não manda cor")
    else:
        ok("Salvar capa da gestão não manda cor")
    if "postFoto({ id: catId, dataUrl: dataUrl, csrf: csrfFrom(box) })" not in js:
        fail("payload da capa diferente do esperado")
    else:
        ok("payload da capa: id + dataUrl (sem cor)")
    if "postFoto({ id: catId, cor: corInp ? corInp.value : '', csrf: csrfFrom(box) })" not in js:
        fail("Salvar cor da gestão não chama API")
    else:
        ok("Salvar cor da gestão chama API só com cor")


def check_aba10() -> None:
    must_contain(
        "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html",
        [
            "panel-delivery-cat-visual",
            "Capa e cor desta categoria",
            "delivery-cat-visual-salvar-foto",
            "delivery-cat-visual-salvar-cor",
            "Foto do produto (opcional)",
            "Capa da categoria fica no bloco verde acima",
            "function _deliveryFindCat",
            "function _deliveryCatMaisFunda",
            "function atualizarPainelVisualCatDelivery",
            "edit-delivery-subcategoria4",
            "refreshFrom(5)",
            "/catalogo/api/categorias/foto/",
            "card da categoria",
        ],
        "aba 10",
    )
    html = read("produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html")
    # Foto do produto ≠ capa
    if "Foto (opcional)" in html and "Foto do produto (opcional)" not in html:
        fail("label ainda genérico «Foto (opcional)»")
    else:
        ok("label distingue foto do produto")
    save_foto = html.split("delivery-cat-visual-salvar-foto")[2].split("if (btnTirar)")[0]
    if "cor:" in save_foto and "imagem_base64" in save_foto:
        fail("aba 10 Salvar capa também manda cor")
    else:
        ok("aba 10 Salvar capa não manda cor")
    if "{ id: id, imagem_base64: dataUrl, imagem_mime: 'image/jpeg' }" not in html:
        fail("aba 10 payload da capa ausente")
    else:
        ok("aba 10 payload da capa sem cor")
    if "{ id: id, cor: corEl ? corEl.value : '' }" not in html:
        fail("aba 10 payload da cor ausente")
    else:
        ok("aba 10 payload só-cor")
    # N5 atualiza o painel
    if "selSub4.addEventListener('change', function () { refreshFrom(5); })" not in html:
        fail("N5 não atualiza painel visual")
    else:
        ok("N5 (selSub4) chama refreshFrom(5)")
    if "atualizarPainelVisualCatDelivery()" not in html:
        fail("aplicarLista não refresca o painel")
    else:
        ok("aplicarLista refresca painel visual")
    ids = html.split("function _deliveryCatMaisFunda")[1].split("function atualizarPainelVisualCatDelivery")[0]
    for sel in (
        "edit-delivery-subcategoria4",
        "edit-delivery-subcategoria3",
        "edit-delivery-subcategoria2",
        "edit-delivery-subcategoria",
        "edit-delivery-categoria",
    ):
        if sel not in ids:
            fail(f"_deliveryCatMaisFunda sem {sel}")
        else:
            ok(f"_deliveryCatMaisFunda lê {sel}")


def check_vitrine() -> None:
    must_contain(
        "produtos/templates/produtos/catalogo/catalogo_delivery.html",
        [
            "--cat-card: #059669",
            "style=\"--cat-card: {{ c.cor|default:'#059669' }}\"",
            "{% if c.imagem %}",
            "card-cat-img",
            "catalogo_delivery.js",
        ],
        "html vitrine",
    )
    js = read("produtos/static/produtos/js/catalogo_delivery.js")
    must_contain(
        "produtos/static/produtos/js/catalogo_delivery.js",
        [
            "function renderCardsNivel",
            "function opcoesFilhosNo",
            "imagem: f.imagem || \"\"",
            "cor: f.cor || \"\"",
            "cor: no.cor || \"\"",
            "slug: \"_geral\"",
            "/^#[0-9a-fA-F]{6}$/",
            "--cat-card:",
        ],
        "JS vitrine",
    )
    render = js.split("function renderCardsNivel")[1].split("function ")[1] if False else js.split("function renderCardsNivel")[1][:900]
    if "s.imagem" not in js[js.find("function renderCardsNivel") : js.find("function renderCardsNivel") + 1200]:
        fail("renderCardsNivel não usa s.imagem")
    else:
        ok("renderCardsNivel usa foto ou letra")
    geral = js.split('slug: "_geral"')[1][:250]
    if "cor: no.cor" not in geral:
        fail("_geral não herda cor do pai")
    else:
        ok("_geral herda cor do pai")
    # skip-geral intacto
    must_contain(
        "produtos/static/produtos/js/catalogo_delivery.js",
        [
            "function filhosReaisNo()",
            "if (!filhosReaisNo().length)",
            "function irParaPesosOuFilhos()",
        ],
        "skip-geral intacto",
    )
    chunk = js.split("function irParaPesosOuFilhos()")[1].split("function abrirNivel")[0]
    if 'nome: "Geral"' in chunk:
        fail("irParaPesosOuFilhos voltou a injetar Geral")
    else:
        ok("irParaPesosOuFilhos não injeta Geral")


def check_hex_runtime() -> None:
    sys.path.insert(0, str(ROOT))
    try:
        import django
        from django.conf import settings

        if not settings.configured:
            settings.configure(
                SECRET_KEY="verify-capa-cor",
                INSTALLED_APPS=["django.contrib.contenttypes"],
            )
            django.setup()
    except Exception:
        # Importa só o módulo se o Django do projeto já estiver configurado
        pass
    # Testa a função sem bater no ORM: copia a lógica canônica
    src = read("produtos/catalogo_delivery_util.py")
    ns: dict = {}
    # Extrai só as funções de cor (não precisam de Django)
    m = re.search(
        r"COR_CARD_PADRAO = .+\n\n\ndef normalizar_cor_categoria.*?\n\ndef cor_card_categoria.*?\n    return normalizar_cor_categoria\(valor\) or COR_CARD_PADRAO\n",
        src,
        re.S,
    )
    if not m:
        fail("não isolou funções de cor")
        return
    exec(m.group(0), ns)
    norm = ns["normalizar_cor_categoria"]
    card = ns["cor_card_categoria"]
    pad = ns["COR_CARD_PADRAO"]
    cases = [
        ("#059669", "#059669"),
        ("FF8800", "#ff8800"),
        ("#abc", "#aabbcc"),
        ("", ""),
        ("verde", ""),
        ("#gg0000", ""),
    ]
    for raw, exp in cases:
        got = norm(raw)
        if got != exp:
            fail(f"normalizar({raw!r})={got!r} esperado {exp!r}")
        else:
            ok(f"normalizar({raw!r}) → {exp!r}")
    if card("") != pad:
        fail(f"cor_card vazio ≠ {pad}")
    else:
        ok(f"cor_card vazio → {pad}")
    if card("#123456") != "#123456":
        fail("cor_card hex direto falhou")
    else:
        ok("cor_card hex direto")


def check_js_runtime() -> None:
    r = subprocess.run(
        ["node", "--check", str(ROOT / "produtos/static/produtos/js/catalogo_delivery.js")],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        fail(f"node --check: {r.stderr.strip()}")
    else:
        ok("node --check catalogo_delivery.js")
    skip_js = ROOT / "scripts/verify_catalogo_skip_geral.js"
    if skip_js.is_file():
        r2 = subprocess.run(["node", str(skip_js)], capture_output=True, text=True)
        sys.stdout.write(r2.stdout)
        if r2.returncode != 0:
            fail("FSM skip-geral VERIFY_FAIL (capa/cor quebrou a navegação)")
            if r2.stderr:
                print(r2.stderr)
        else:
            ok("FSM skip-geral ainda VERIFY_OK")


def check_django_tests() -> None:
    r = subprocess.run(
        [
            str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").is_file() else sys.executable,
            "manage.py",
            "test",
            "produtos.tests_catalogo_categoria_visual",
            "produtos.tests_catalogo_excluir_categoria",
            "--verbosity",
            "1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    sys.stdout.write(r.stdout)
    if r.returncode != 0:
        fail("Django tests capa/cor ou excluir categoria")
        if r.stderr:
            print(r.stderr[-2000:])
    else:
        ok("Django tests visual + excluir categoria")


def check_banana_version() -> None:
    txt = read("banana.md")
    if "CATALOGO-CAPA-COR" not in txt:
        fail("banana.md sem CATALOGO-CAPA-COR")
    else:
        ok("banana.md CATALOGO-CAPA-COR")
    # O checklist novo (topo do CHECKPOINT) precisa estar PRONTO, não Live
    head = txt.split("## CHECKPOINT")[1][:3500] if "## CHECKPOINT" in txt else txt[:3500]
    if "CATALOGO-CAPA-COR" not in head:
        fail("CHECKPOINT sem CATALOGO-CAPA-COR no topo")
    else:
        ok("CHECKPOINT topo tem CATALOGO-CAPA-COR")
    if "0098" not in head:
        fail("CHECKPOINT sem migrate 0098")
    else:
        ok("CHECKPOINT migrate 0098")
    if "enviado / Live v17.79" in head or "Live v17.79" in head:
        ok("CHECKPOINT CATALOGO-CAPA-COR Live v17.79")
    elif "pronto para envio" in head.lower() or "PRONTO" in head:
        ok("CHECKPOINT marca pronto para envio")
    else:
        fail("CHECKPOINT sem PRONTO nem Live v17.79")
    v = read("VERSION").strip()
    try:
        major, minor = v.split(".", 1)
        ok_ver = int(major) > 17 or (int(major) == 17 and int(minor) >= 79)
    except ValueError:
        ok_ver = False
    if not ok_ver:
        fail(f"VERSION={v} (esperado >= 17.79)")
    else:
        ok(f"VERSION {v} (>=17.79)")


def main() -> None:
    print("=== VERIFY CATALOGO-CAPA-COR PATH ===")
    check_ast()
    check_postgres_fonte()
    check_util()
    check_api()
    check_gestao()
    check_aba10()
    check_vitrine()
    check_hex_runtime()
    check_js_runtime()
    check_django_tests()
    check_banana_version()
    print("---")
    print(f"checks OK={oks} FAIL={fails}")
    if fails:
        print(f"VERIFY_FAIL ({fails})")
        sys.exit(1)
    print("VERIFY_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
