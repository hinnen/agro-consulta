"""
Fluxo etiquetas celular → loja (paths + template mobile).

  python3 scripts/verify_etiquetas_mobile_flow_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.urls import reverse

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
    print("VERIFY ETQ mobile flow (celular → Postgres → desktop)")
    app = (ROOT / "produtos/templates/produtos/etiquetas_solicitacao/app.html").read_text(encoding="utf-8")

    routes = [
        ("etiquetas_solicitacao_app", "/vendas/lojas/etiquetas/"),
        ("api_etiquetas_solicitacao_mobile_criar", "/api/vendas/lojas/etiquetas/solicitacoes/"),
        ("api_etiquetas_solicitacoes_lista", "/api/produtos/etiquetas/solicitacoes/"),
        ("api_etiquetas_solicitacao_importar", "/importar/"),
    ]
    print("== Rotas ==")
    for name, fragment in routes:
        try:
            path = reverse(name, kwargs={"pk": 1}) if "importar" in name else reverse(name)
            check(f"reverse_{name}", fragment in path, path)
        except Exception as exc:
            check(f"reverse_{name}", False, str(exc))

    print("== App mobile (UX celular) ==")
    check("dock_fixo", "dockSacola" in app and "scroll-main" in app)
    check("viewport_teclado", "--app-h" in app and "visualViewport" in app)
    check("delegacao_sacola", "bindSacolaOnce" in app and "box._delegated" in app)
    check("delegacao_lista", "bindListaOnce" in app)
    check("enviar_guard", "enviando" in app and "Enviando" in app)
    check("bip_reabrir_camera", "limparReaderBox" in app and "processarBipScan" in app)
    check("bip_enviar_direto_opcao", "chkBipEnviar" in app and "agro_etq_bip_enviar_direto" in app)
    check("sem_cadastro_painel", "panelSemCadastro" in app and "sem_cadastro" in app)
    check("pwa_head", "vendas_lojas_pwa_head.html" in app)
    check("catalogo_slim", "url_catalogo_slim" in app or "URL_SLIM" in app)

    util = (ROOT / "produtos/etiquetas_solicitacao_util.py").read_text(encoding="utf-8")
    check("util_hidratar", "hidratar_itens_para_fila" in util)
    check("util_marcar_importado", "marcar_pacote_importado" in util)

    print(f"\nTOTAL OK={OK} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
