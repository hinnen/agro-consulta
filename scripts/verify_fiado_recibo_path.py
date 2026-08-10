"""
Prova FL-019 — recibo pagamento fiado 80mm (FIADO-RECIBO).

Path:
  PDV baixa fiado → pergunta Imprimir recibo / Agora não
  Tela /fiado/ → Pagamentos recentes → Reimprimir
  API GET /api/fiado/recibo/<id>/ + /api/fiado/recibos/
  Térmica 80mm (venda_cupom_80mm.js) · sem migrate

  python scripts/verify_fiado_recibo_path.py
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" - {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" - {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Path arquivos ==")
    urls = _read("produtos/urls.py")
    views = _read("produtos/fiado_gestao_views.py")
    util_g = _read("produtos/fiado_gestao_util.py")
    util_r = _read("produtos/fiado_recibo_util.py")
    cupom = _read("produtos/static/produtos/js/venda_cupom_80mm.js")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    fiado_js = _read("produtos/static/produtos/js/fiado_gestao.js")
    fiado_html = _read("produtos/templates/produtos/fiado_gestao.html")

    check("url_recibos", 'api/fiado/recibos/' in urls and 'name=\'api_fiado_recibos\'' in urls)
    check("url_recibo_id", "api/fiado/recibo/<int:recibo_id>/" in urls)
    check("url_recibo_baixas", "name='api_fiado_recibo_baixas'" in urls)
    check("view_recibos", "def api_fiado_recibos" in views)
    check("view_recibo", "def api_fiado_recibo" in views)
    check("util_montar", "def montar_recibo_pagamento_fiado" in util_r)
    check("util_listar", "def listar_recibos_pagamento_fiado" in util_r)
    check("util_saldo", "def saldo_aberto_cliente_fiado" in util_r)
    check("baixa_recibo_id", '"recibo_id"' in util_g and "saldo_restante" in util_g)
    check("baixa_formas", '"formas"' in util_g and '"recibo": True' in util_g)
    check("cupom_tipo", "tipo === 'recibo_fiado'" in cupom or 'tipo === "recibo_fiado"' in cupom)
    check("cupom_titulo", "RECIBO DE PAGAMENTO FIADO" in cupom)
    check("cupom_assinatura", "Assinatura do cliente" in cupom)
    check("cupom_ainda_deve", "Ainda deve" in cupom)
    check("cupom_fn_print", "function agroImprimirReciboFiado80mm" in cupom)
    check("cupom_fn_escolher", "function agroEscolherImprimirReciboFiado" in cupom)
    check("cupom_global", "agroEscolherImprimirReciboFiado" in cupom)
    check("wizard_perguntar", "agroEscolherImprimirReciboFiado" in wizard)
    check("wizard_recibo_id", "recibo_id" in wizard and "finalizeFiadoCobrancaOk" in wizard)
    check("fiado_html_box", 'id="fiado-recibos-box"' in fiado_html)
    check("fiado_html_reimprimir", "Reimprimir" in fiado_js or "Reimprimir" in fiado_html)
    check("fiado_html_cupom_js", "venda_cupom_80mm.js" in fiado_html)
    check("fiado_js_load", "function carregarRecibosCliente" in fiado_js)
    check("fiado_js_print", "agroCarregarEImprimirReciboFiado" in fiado_js)
    check("html_url_recibos", "api_fiado_recibos" in fiado_html)


def test_django() -> None:
    print("== Django ==")
    from django.urls import reverse

    check("reverse_recibos", reverse("api_fiado_recibos") == "/api/fiado/recibos/")
    check("reverse_recibo", reverse("api_fiado_recibo", args=[1]) == "/api/fiado/recibo/1/")
    check("reverse_recibo_q", reverse("api_fiado_recibo_baixas") == "/api/fiado/recibo/")

    from produtos.fiado_recibo_util import montar_recibo_pagamento_fiado

    raised = False
    try:
        montar_recibo_pagamento_fiado(recibo_id=None, baixas_ids=[])
    except ValueError:
        raised = True
    check("montar_vazio_erro", raised)

    r = subprocess.run(
        [sys.executable, "manage.py", "check"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    check("manage_check", r.returncode == 0, (r.stdout or r.stderr or "")[-180:])


def main() -> int:
    test_arquivos()
    test_django()
    print()
    print(f"OK {len(oks)}  FAIL {len(fails)}")
    if fails:
        print("Falhou:", ", ".join(fails))
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
