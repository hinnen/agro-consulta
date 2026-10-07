"""
Prova path — FIADO-CUPOM-SALDO-MISTO (cupom 80mm: saldo fiado em destaque no misto).

Path:
  PDV / reimpressão → payload (valor_fiado / ja_pago / fiado_misto)
    → venda_cupom_80mm.js → TOTAL menor + Já pago + SALDO FIADO
  100% fiado → layout antigo (TOTAL grande, sem caixa saldo)
  Sem migrate. PIN loja não entra neste path (só impressão).

  python scripts/verify_fiado_cupom_saldo_misto_path.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client
from django.utils import timezone

from produtos.models import ClienteAgro, ItemVendaAgro, VendaAgro
from produtos.venda_cupom_util import serializar_venda_cupom_80mm

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Arquivos / contratos ==")
    cupom = _read("produtos/static/produtos/js/venda_cupom_80mm.js")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    util = _read("produtos/venda_cupom_util.py")

    check("js_resolver", "function resolverFiadoMistoCupom" in cupom)
    check("js_saldo_box", "SALDO FIADO" in cupom and "saldo-fiado-box" in cupom)
    check("js_ja_pago", "Já pago:" in cupom)
    check("js_total_sec", "total-linha-sec" in cupom)
    check("js_parse_fallback", "forma_pagamento" in cupom and r"\s*[+·|]\s*" in cupom)
    check("py_helper", "def _fiado_misto_cupom_campos" in util)
    check("py_campos", all(k in util for k in ('"fiado_misto"', '"valor_fiado"', '"ja_pago_texto"')))
    check("wizard_payload", "fiado_misto:" in wizard and "ja_pago_texto:" in wizard and "valor_fiado:" in wizard)
    check("wizard_build_cupom", "function buildCupomPayloadFromWizard" in wizard)

    r = subprocess.run(
        ["node", "--check", str(ROOT / "produtos/static/produtos/js/venda_cupom_80mm.js")],
        capture_output=True,
        text=True,
    )
    check("node_check_cupom", r.returncode == 0, (r.stderr or "")[:120])


def _mk_venda(*, total: Decimal, pagamentos: list, forma: str, suf: str, deposito: str = "centro") -> VendaAgro:
    cli = ClienteAgro.objects.create(
        nome=f"Fiado Misto Path {suf}",
        ativo=True,
        limite_fiado_local=Decimal("5000"),
    )
    v = VendaAgro.objects.create(
        cliente_nome=cli.nome,
        cliente_id_erp=f"agro:{cli.pk}",
        total=total,
        forma_pagamento=forma,
        pagamentos_json=pagamentos,
        deposito=deposito,
        usuario_registro="prova-fiado-misto-path",
    )
    ItemVendaAgro.objects.create(
        venda=v,
        descricao="Item prova fiado misto",
        codigo="PFIADO",
        quantidade=Decimal("1"),
        valor_unitario=total,
        valor_total=total,
    )
    return v


def test_serializar() -> None:
    print("== Serializar payload ==")
    suf = timezone.now().strftime("%H%M%S%f")

    v_misto = _mk_venda(
        total=Decimal("101.00"),
        forma="Dinheiro + Fiado",
        pagamentos=[
            {"forma": "Dinheiro", "valor": 44},
            {"forma": "Fiado", "valor": 57, "fiado_parcelas": 1, "fiado_dias_primeiro": 30},
        ],
        suf=suf + "a",
    )
    c = serializar_venda_cupom_80mm(v_misto)
    check("misto_flag", c.get("fiado_misto") is True)
    check("misto_valor", abs(float(c.get("valor_fiado") or 0) - 57.0) < 0.01, str(c.get("valor_fiado")))
    check("misto_texto", "57" in str(c.get("valor_fiado_texto") or ""))
    check("misto_ja_pago", "Dinheiro" in str(c.get("ja_pago_texto") or "") and "44" in str(c.get("ja_pago_texto") or ""))
    check("misto_eh_fiado", bool(c.get("eh_fiado")))
    check("misto_total", abs(float(c.get("total") or 0) - 101.0) < 0.01)

    v_so = _mk_venda(
        total=Decimal("80.00"),
        forma="Fiado",
        pagamentos=[{"forma": "Fiado", "valor": 80, "fiado_parcelas": 1, "fiado_dias_primeiro": 30}],
        suf=suf + "b",
    )
    c2 = serializar_venda_cupom_80mm(v_so)
    check("so_fiado_nao_misto", c2.get("fiado_misto") is False)
    check("so_fiado_eh", bool(c2.get("eh_fiado")))
    check("so_fiado_valor", abs(float(c2.get("valor_fiado") or 0) - 80.0) < 0.01)

    v_din = _mk_venda(
        total=Decimal("30.00"),
        forma="Dinheiro",
        pagamentos=[{"forma": "Dinheiro", "valor": 30}],
        suf=suf + "c",
    )
    c3 = serializar_venda_cupom_80mm(v_din)
    check("dinheiro_nao_fiado", c3.get("eh_fiado") is False)
    check("dinheiro_nao_misto", c3.get("fiado_misto") is False)

    v_pix = _mk_venda(
        total=Decimal("100.00"),
        forma="PIX + Fiado",
        pagamentos=[
            {"forma": "PIX", "valor": 40},
            {"forma": "Fiado", "valor": 60, "fiado_parcelas": 1, "fiado_dias_primeiro": 30},
        ],
        suf=suf + "d",
    )
    c4 = serializar_venda_cupom_80mm(v_pix)
    check("pix_misto", c4.get("fiado_misto") is True and abs(float(c4.get("valor_fiado") or 0) - 60) < 0.01)
    check("pix_ja_pago", "PIX" in str(c4.get("ja_pago_texto") or "") or "Pix" in str(c4.get("ja_pago_texto") or ""))

    v_tri = _mk_venda(
        total=Decimal("150.00"),
        forma="Dinheiro + PIX + Fiado",
        pagamentos=[
            {"forma": "Dinheiro", "valor": 20},
            {"forma": "PIX", "valor": 30},
            {"forma": "Fiado", "valor": 100, "fiado_parcelas": 1, "fiado_dias_primeiro": 30},
        ],
        suf=suf + "e",
    )
    c5 = serializar_venda_cupom_80mm(v_tri)
    check("tri_misto", c5.get("fiado_misto") is True and abs(float(c5.get("valor_fiado") or 0) - 100) < 0.01)
    jp = str(c5.get("ja_pago_texto") or "")
    check("tri_ja_pago_duas", "Dinheiro" in jp and ("PIX" in jp or "Pix" in jp) and "20" in jp and "30" in jp)

    return v_misto.pk, v_so.pk


def test_html_node() -> None:
    print("== HTML cupom (Node) ==")
    js_path = str(ROOT / "produtos/static/produtos/js/venda_cupom_80mm.js").replace("\\", "/")
    cases = [
        {
            "name": "html_misto_payload",
            "cupom": {
                "eh_fiado": True,
                "fiado_misto": True,
                "valor_fiado": 57,
                "valor_fiado_texto": "R$ 57,00",
                "ja_pago_texto": "Dinheiro R$ 44,00",
                "forma_pagamento": "Dinheiro R$ 44,00 + Fiado R$ 57,00",
                "total": 101,
                "total_texto": "R$ 101,00",
                "itens": [{"nome": "milho", "qtd": 1, "preco": 101, "subtotal": 101}],
                "com_assinatura": False,
                "via_rotulo": "VIA DO CLIENTE",
            },
            "need": ["SALDO FIADO", "R$ 57,00", "Já pago:", "Dinheiro", "total-linha-sec", "R$ 101,00"],
            "forbid": ["<strong>Pag.:</strong>"],
        },
        {
            "name": "html_so_fiado",
            "cupom": {
                "eh_fiado": True,
                "fiado_misto": False,
                "valor_fiado": 80,
                "valor_fiado_texto": "R$ 80,00",
                "ja_pago_texto": "",
                "forma_pagamento": "Fiado R$ 80,00",
                "total": 80,
                "total_texto": "R$ 80,00",
                "itens": [{"nome": "racao", "qtd": 1, "preco": 80, "subtotal": 80}],
                "com_assinatura": False,
            },
            "need": ["TOTAL", "R$ 80,00", "Pag.:", "COMPROVANTE FIADO"],
            "forbid": ["SALDO FIADO", "Já pago:", "total-linha-sec"],
        },
        {
            "name": "html_parse_fallback",
            "cupom": {
                "eh_fiado": True,
                "forma_pagamento": "Dinheiro R$ 44,00 · Fiado R$ 57,00",
                "total": 101,
                "total_texto": "R$ 101,00",
                "itens": [{"nome": "x", "qtd": 1, "preco": 101, "subtotal": 101}],
                "com_assinatura": False,
            },
            "need": ["SALDO FIADO", "Já pago:", "Dinheiro"],
            "forbid": [],
        },
        {
            "name": "html_dinheiro_puro",
            "cupom": {
                "eh_fiado": False,
                "forma_pagamento": "Dinheiro R$ 30,00",
                "total": 30,
                "total_texto": "R$ 30,00",
                "itens": [{"nome": "y", "qtd": 1, "preco": 30, "subtotal": 30}],
                "com_assinatura": False,
            },
            "need": ["TOTAL", "Pag.:", "COMPROVANTE DE VENDA"],
            "forbid": ["SALDO FIADO", "COMPROVANTE FIADO"],
        },
        {
            "name": "html_2vias_misto",
            "pages": True,
            "cupom": {
                "eh_fiado": True,
                "fiado_misto": True,
                "valor_fiado": 57,
                "valor_fiado_texto": "R$ 57,00",
                "ja_pago_texto": "Dinheiro R$ 44,00",
                "forma_pagamento": "Dinheiro R$ 44,00 + Fiado R$ 57,00",
                "total": 101,
                "total_texto": "R$ 101,00",
                "itens": [{"nome": "z", "qtd": 1, "preco": 101, "subtotal": 101}],
            },
            "need": ["VIA DO CLIENTE", "VIA DA LOJA", "SALDO FIADO", "Assinatura do cliente"],
            "forbid": [],
        },
    ]

    for case in cases:
        payload = json.dumps(case["cupom"], ensure_ascii=False)
        use_pages = bool(case.get("pages"))
        node_src = f"""
const fs = require('fs');
const vm = require('vm');
const code = fs.readFileSync({json.dumps(js_path)}, 'utf8');
const global = {{}};
const sandbox = {{
  global: global,
  window: global,
  console,
  Math,
  Number,
  String,
  Array,
  Object,
  Date,
  isFinite,
  parseInt,
  encodeURIComponent,
  setTimeout: function () {{}},
  clearTimeout: function () {{}},
  document: {{ createElement: function () {{ return {{}}; }}, body: {{ appendChild: function () {{}} }}, head: null, documentElement: {{}} }},
  Image: function () {{ this.src = ''; }},
  location: {{ origin: 'http://127.0.0.1' }}
}};
sandbox.global = sandbox;
sandbox.window = sandbox;
vm.runInNewContext(code, sandbox);
const fn = sandbox.agroCupomPagesInnerHtml || sandbox.agroCupomInnerHtml;
if (typeof fn !== 'function') {{ console.error('missing fn'); process.exit(2); }}
const cupom = {payload};
const html = {('sandbox.agroCupomPagesInnerHtml(cupom)' if use_pages else 'sandbox.agroCupomInnerHtml(cupom)')};
const need = {json.dumps(case['need'], ensure_ascii=False)};
const forbid = {json.dumps(case['forbid'], ensure_ascii=False)};
for (const s of need) {{
  if (!html.includes(s)) {{ console.error('MISSING '+s); process.exit(3); }}
}}
for (const s of forbid) {{
  if (html.includes(s)) {{ console.error('FORBIDDEN '+s); process.exit(4); }}
}}
if ({str(use_pages).lower()}) {{
  const n = (html.match(/SALDO FIADO/g) || []).length;
  if (n < 2) {{ console.error('expected 2 vias SALDO FIADO got '+n); process.exit(5); }}
}}
process.stdout.write('OK');
"""
        r = subprocess.run(["node", "-e", node_src], capture_output=True, text=True)
        check(case["name"], r.returncode == 0 and "OK" in (r.stdout or ""), (r.stderr or r.stdout or "")[:160])


def test_api(v_misto_pk: int, v_so_pk: int) -> None:
    print("== API reimpressão ==")
    User = get_user_model()
    suf = timezone.now().strftime("%H%M%S")
    user, _ = User.objects.get_or_create(username=f"prova_fiado_misto_{suf}", defaults={"is_staff": True})
    user.set_password("x")
    user.save()
    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(user)

    r = c.get(f"/venda/{v_misto_pk}/cupom/", {"interno": "1", "segunda_via": "1"})
    check("api_misto_200", r.status_code == 200 and r.json().get("ok") is True, str(r.status_code))
    cupom = (r.json() or {}).get("cupom") or {}
    check("api_misto_flag", cupom.get("fiado_misto") is True)
    check("api_misto_57", abs(float(cupom.get("valor_fiado") or 0) - 57.0) < 0.01)

    r2 = c.get(f"/venda/{v_so_pk}/cupom/", {"interno": "1"})
    cupom2 = (r2.json() or {}).get("cupom") or {}
    check("api_so_fiado", r2.status_code == 200 and cupom2.get("fiado_misto") is False and bool(cupom2.get("eh_fiado")))


def main() -> int:
    print("FIADO-CUPOM-SALDO-MISTO — prova path")
    test_arquivos()
    pks = test_serializar()
    test_html_node()
    if pks:
        test_api(pks[0], pks[1])
    print()
    print(f"{'OK' if not fails else 'FAIL'} {len(oks)} · FAIL {len(fails)}")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
