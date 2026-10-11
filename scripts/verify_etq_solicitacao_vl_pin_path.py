"""Prova — ETQ-SOLIC-MOBILE + VL-HUB-PIN-VENDAS-TAREFAS (paths estáticos + util)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("AGRO_VL_HUB_PINS_VENDAS_TAREFAS", "9973,1,2,3,4,8")

import django

django.setup()

from django.test import override_settings

from produtos.vendas_lojas_hub_acesso_util import pin_liberado_vendas_tarefas

OK = FAIL = 0


def check(label: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL {label}" + (f" — {detail}" if detail else ""))


def main() -> int:
    print("VERIFY ETQ-SOLIC + VL-HUB-PIN")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    hub = (ROOT / "produtos/templates/produtos/vendas_lojas_hub.html").read_text(encoding="utf-8")
    etq_html = (ROOT / "produtos/templates/produtos/produtos_etiquetas.html").read_text(encoding="utf-8")
    etq_js = (ROOT / "produtos/static/produtos/js/produtos_etiquetas.js").read_text(encoding="utf-8")
    mig = (ROOT / "produtos/migrations/0142_etiqueta_solicitacao_pacote_agro.py").read_text(encoding="utf-8")

    print("== Etiquetas solicitação ==")
    check("url_api_desktop", "api_etiquetas_solicitacoes_lista" in urls)
    check("url_importar", "api_etiquetas_solicitacao_importar" in urls)
    check("url_mobile_criar", "api_etiquetas_solicitacao_mobile_criar" in urls)
    check("url_mobile_app", "etiquetas_solicitacao_app" in urls)
    check("hub_etiquetas", "etiquetas_solicitacao_app" in hub and "vl-hub-etiquetas" in hub)
    check("topbar_solicitacoes", "etq-btn-solicitacoes" in etq_html and "solicitacoesUrl" in etq_html)
    check("js_importar_fila", "importarSolicitacaoParaFila" in etq_js)
    check("js_manter_busca", "optsLimparBuscaAoAdicionar" in etq_js)
    check("migrate_model", "EtiquetaSolicitacaoPacoteAgro" in mig)
    check("template_mobile", (ROOT / "produtos/templates/produtos/etiquetas_solicitacao/app.html").is_file())

    print("== Hub PIN Vendas/Tarefas ==")
    check("url_vendas_pin", "vendas_lojas_pin" in urls)
    check("url_vendas_sair", "vendas_lojas_logout" in urls)
    check("pin_template", (ROOT / "produtos/templates/produtos/vendas_lojas_pin.html").is_file())
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check("vendas_exigir_pin", "exigir_vendas_lojas_html" in views.split("def vendas_lojas_resumo")[0][-800:])
    t_pin = (ROOT / "tarefas/pin_util.py").read_text(encoding="utf-8")
    check("tarefas_allowlist", "pin_liberado_vendas_tarefas" in t_pin)
    check("settings_env", "AGRO_VL_HUB_PINS_VENDAS_TAREFAS" in (ROOT / "config/settings.py").read_text(encoding="utf-8"))

    print("== Util PIN ==")
    with override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8,9973"):
        check("pin_9973_dev", pin_liberado_vendas_tarefas("9973"))
        check("pin_1", pin_liberado_vendas_tarefas("1"))
        check("pin_5_block", not pin_liberado_vendas_tarefas("5"))

    print(f"\nTOTAL OK={OK} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
