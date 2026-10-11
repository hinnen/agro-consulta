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
    v_pin = (ROOT / "produtos/vendas_lojas_pin_util.py").read_text(encoding="utf-8")
    check("vendas_sem_acesso_click", "resposta_sem_acesso_html" in v_pin)
    check("tarefas_sem_acesso_click", "resposta_sem_acesso_html" in t_pin)
    check("hub_sem_aviso_pin", "vl-acesso" not in hub and "acesso=vendas" not in hub)
    check(
        "sem_acesso_template",
        (ROOT / "produtos/templates/produtos/vendas_lojas_sem_acesso.html").is_file(),
    )
    check("settings_env", "AGRO_VL_HUB_PINS_VENDAS_TAREFAS" in (ROOT / "config/settings.py").read_text(encoding="utf-8"))
    acc = (ROOT / "produtos/vendas_lojas_hub_acesso_util.py").read_text(encoding="utf-8")
    check("allowlist_slot_codigo", "_slot_operador_por_senha_rapida" in acc and "codigo_vendedor" in acc)

    print("== Util PIN ==")
    with override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8,9973"):
        check("pin_9973_dev", pin_liberado_vendas_tarefas("9973"))
        check("pin_1", pin_liberado_vendas_tarefas("1"))
        check("pin_5_block", not pin_liberado_vendas_tarefas("5"))

    print("== Hub sessão 7d + log ==")
    hub_sess = (ROOT / "produtos/vendas_lojas_hub_sessao_util.py").read_text(encoding="utf-8")
    check("pin_ttl_7d", "PIN_TTL_SEGUNDOS = 7 * 24 * 3600" in hub_sess)
    views_py = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check(
        "hub_exigir_pin",
        "@exigir_hub_operador_html" in views_py.split("def vendas_lojas_hub", 1)[0],
    )
    check("hub_pin_central", "vendas_lojas_pin" in urls and "redirect_para_pin" in (ROOT / "produtos/views_fotos_produto.py").read_text(encoding="utf-8"))
    check("model_log", "GmLojasAppLogAgro" in (ROOT / "produtos/models.py").read_text(encoding="utf-8"))
    check("middleware_log", "GmLojasAppLogMiddleware" in (ROOT / "config/settings.py").read_text(encoding="utf-8"))

    print(f"\nTOTAL OK={OK} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
