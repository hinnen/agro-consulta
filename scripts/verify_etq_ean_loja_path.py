"""
ETQ-EAN-LOJA — path completo (geração + legado + impressão + contratos).
python scripts/verify_etq_ean_loja_path.py
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from produtos.agro_codigo_barras_loja_util import (  # noqa: E402
    _seqs_para_max_alocacao,
    eh_codigo_barras_loja,
    ean13_checksum_ok,
    ean13_digito_verificador,
    formatar_codigo_barras_loja,
    parsear_seq_codigo_barras_loja,
)

n = 0
fail = 0


def ok(cond: bool, msg: str) -> None:
    global n, fail
    n += 1
    if not cond:
        fail += 1
        print("FAIL", msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    # --- Util Python: legado ---
    legado = "2300000001571"
    ok(eh_codigo_barras_loja(legado), "legado eh loja")
    ok(not ean13_checksum_ok(legado), "legado DV invalido")
    ok(parsear_seq_codigo_barras_loja(legado) == 1571, "legado seq 1571")
    ok(legado == "230" + f"{1571:010d}", "legado = 230+10 digitos seq")

    # --- Util Python: novo EAN ---
    samples = []
    for seq in (1, 15, 1571, 1572, 9999, 100000, 999999999):
        cb = formatar_codigo_barras_loja(seq)
        samples.append((seq, cb))
        ok(len(cb) == 13 and cb.startswith("230"), f"novo seq {seq} formato")
        ok(ean13_checksum_ok(cb), f"novo seq {seq} DV ok {cb}")
        ok(parsear_seq_codigo_barras_loja(cb) == seq, f"novo seq {seq} parse")
        ok(eh_codigo_barras_loja(cb), f"novo seq {seq} eh loja")

    # Número novo ≠ legado com mesma “aparência” de seq baixa
    ok(formatar_codigo_barras_loja(1571) != legado, "novo 1571 != string legado 1571")
    ok(formatar_codigo_barras_loja(1571) == "2300000015713", "novo 1571 = 2300000015713")

    # Colisão conhecida: New(1571)=2300000015713 == Old(15713)
    ok(formatar_codigo_barras_loja(1571) == "230" + f"{15713:010d}", "colisao teorica new1571/old15713")
    # ocupado check usaria string; max considera ambos
    seqs_new = _seqs_para_max_alocacao(formatar_codigo_barras_loja(1571))
    ok(1571 in seqs_new, f"max inclui payload 9 {seqs_new}")
    ok(15713 in seqs_new, f"max inclui leitura 10 (DV) {seqs_new}")

    lucky = "2300000001570"
    ok(ean13_checksum_ok(lucky), "1570 legado DV ok por acaso")
    seqs_lucky = _seqs_para_max_alocacao(lucky)
    ok(1570 in seqs_lucky and 157 in seqs_lucky, f"max lucky {seqs_lucky}")

    ok(not eh_codigo_barras_loja("7898752405197"), "789 nao loja")
    ok(not eh_codigo_barras_loja("0120125412229"), "012 nao loja")
    ok(ean13_digito_verificador("230000001572") == int(formatar_codigo_barras_loja(1572)[-1]), "DV bate")

    # Unicidade de novos em amostra
    codes = [formatar_codigo_barras_loja(s) for s in range(1, 200)]
    ok(len(codes) == len(set(codes)), "200 primeiros novos unicos")

    # --- Contratos arquivos ---
    util = read("produtos/agro_codigo_barras_loja_util.py")
    ok("EAN-13 válido" in util or "EAN-13" in util, "util menciona EAN-13")
    ok("CB_LOJA_SEQ_LEN_NOVO = 9" in util, "payload novo 9 digitos")

    views = read("produtos/views.py")
    ok("alocar_proximo_codigo_barras_loja" in views, "views aloca cb loja")
    ok("api_produtos_cadastro_proximo_cb_loja" in views, "API proximo cb loja")

    core = read("produtos/static/produtos/js/produtos_etiquetas_core.js")
    ok("ean_force" in core, "core ean_force")
    ok("encodeEan13Bits" in core, "core encode EAN force")
    ok("drawEan13ForceSvg" in core, "core draw force")
    ok("_drawEanForce" in core, "html imprint _drawEanForce")
    ok("formato: 'EAN13'" in core and "codigo_loja: true" in core, "loja imprime EAN13")
    # Não deve forçar CODE128 no retorno da loja
    loja_block = re.search(
        r"function extrairCodigoBarrasLojaInterno[\s\S]*?return null;\s*\}",
        core,
    )
    ok(bool(loja_block), "bloco extrairCodigoBarrasLojaInterno")
    if loja_block:
        ok("CODE128" not in loja_block.group(0), "loja nao retorna CODE128")
        ok("ean_force" in loja_block.group(0), "loja seta ean_force")

    cad = read("produtos/static/produtos/js/cadastro_erp_panel.js")
    ok("EAN-13" in cad and "legado" in cad, "cadastro aviso EAN loja")

    for rel in (
        "produtos/templates/produtos/produtos_etiquetas.html",
        "produtos/templates/produtos/produtos_etiquetas_lote.html",
        "produtos/templates/produtos/entrada_nota.html",
        "produtos/templates/produtos/produtos_cadastro_erp.html",
    ):
        html = read(rel)
        ok("produtos_etiquetas_core.js' %}?v=26" in html or 'produtos_etiquetas_core.js" %}?v=26' in html or "etiquetas_core.js' %}?v=26" in html, f"{rel} core v=26")

    # URL name no urls
    urls = read("produtos/urls.py")
    ok("proximo_cb_loja" in urls or "api_produtos_cadastro_proximo_cb_loja" in urls, "rota proximo cb")

    # --- Provas JS irmãs ---
    for script, label in (
        ("scripts/verify_etiquetas_termica_path.js", "termica"),
        ("scripts/verify_etiquetas_termica_varias.js", "varias"),
    ):
        r = subprocess.run(
            ["node", str(ROOT / script)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        out = (r.stdout or "") + (r.stderr or "")
        ok(r.returncode == 0, f"{label} exit0")
        ok("FAIL" not in out.splitlines()[0] if out.strip() else False or "OK" in out or "39/39" in out or "56/56" in out, f"{label} ok out")

    # Node inline: legado/novo via Core
    node = subprocess.run(
        [
            "node",
            "-e",
            r"""
const fs=require('fs');const vm=require('vm');const path=require('path');
const root=process.cwd();
const code=fs.readFileSync(path.join(root,'produtos/static/produtos/js/produtos_etiquetas_core.js'),'utf8');
const s={document:{cookie:''}};s.window=s;vm.createContext(s);vm.runInContext(code,s);
const C=s.AgroEtiquetasCore;
const L=C.valorBarcodeProduto({codigo_barras:'2300000001571',nome:'x',preco_venda:1});
if(L.formato!=='EAN13'||L.valor!=='2300000001571'||!L.ean_force) process.exit(2);
const bits=C.encodeEan13Bits('2300000001571');
if(!bits||bits.length!==95) process.exit(3);
const d12='230000001572';
let soma=0;for(let i=0;i<12;i++) soma+=parseInt(d12[i],10)*(i%2===0?1:3);
const novo=d12+String((10-(soma%10))%10);
const N=C.valorBarcodeProduto({codigo_barras:novo,nome:'x',preco_venda:1});
if(N.formato!=='EAN13'||N.ean_force) process.exit(4);
const html=C.montarHtmlImpressao(C.normalizarPreset({estilo:'termica',largura_mm:100,altura_mm:70}),
  [{nome:'loja',preco_venda:2,codigo_barras:'2300000001571',codigo_gm:'GM9',qtd:1}],'R');
if(!html.includes('"ean_force":true')||!html.includes('_drawEanForce')) process.exit(5);
if(html.includes('format:"CODE128"')&&html.includes('2300000001571')&&!html.includes('ean_force')) process.exit(6);
console.log('NODE_OK',novo);
""",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    ok(node.returncode == 0, f"node core path exit={node.returncode} {(node.stderr or node.stdout or '')[:200]}")
    ok("NODE_OK" in (node.stdout or ""), "node core NODE_OK")

    # Django: API + alocação (ambiente local com PG)
    try:
        import os

        import django
        from django.test import Client, override_settings

        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
        django.setup()
        from produtos.agro_codigo_barras_loja_util import (  # noqa: WPS433
            alocar_proximo_codigo_barras_loja_postgres,
        )
        from produtos.agro_fonte_config import agro_catalogo_usa_postgres

        with override_settings(ALLOWED_HOSTS=["*"]):
            resp = Client().get("/api/produtos/cadastro/proximo-cb-loja/")
        ok(resp.status_code == 200, f"API status {resp.status_code}")
        data = resp.json() if resp.status_code == 200 else {}
        ok(bool(data.get("ok")), f"API ok field {data}")
        cb_api = str(data.get("codigo_barras") or "")
        ok(eh_codigo_barras_loja(cb_api), f"API cb loja {cb_api}")
        ok(ean13_checksum_ok(cb_api), f"API cb EAN valido {cb_api}")

        if agro_catalogo_usa_postgres():
            err, cb_al = alocar_proximo_codigo_barras_loja_postgres()
            ok(err is None and bool(cb_al), "alocar postgres livre")
            ok(ean13_checksum_ok(str(cb_al or "")), f"alocar EAN {cb_al}")
            ok(str(cb_al) == cb_api, "API = alocar direto")
        else:
            ok(True, "catalogo nao-PG — alocar skip")
    except Exception as e:
        ok(False, f"Django API obrigatorio falhou: {e}")

    print("FAIL" if fail else "OK", f"{n - fail}/{n}" if fail else f"{n}/{n}")
    if fail:
        print(json.dumps({"fail": fail, "n": n}, ensure_ascii=False))
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
