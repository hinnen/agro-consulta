# -*- coding: utf-8 -*-
"""Prova path MP-POINT-VILA — 2ª conta Point (CNPJ Vila) sem misturar com Centro."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.conf import settings

from types import SimpleNamespace

from produtos.caixa_util import (
    PONTO_CAIXA_GAVETA,
    PONTO_CAIXA_NOTEBOOK,
    PONTO_CAIXA_VILA,
    SESSION_MP_POINT_HOST_KEY,
    SESSION_PONTO_OPERACAO_KEY,
    _MAQUININHAS_MP_POINT_AUTO_VILA_IDS,
    filtrar_maquininhas_pdv_sem_mp,
    navegador_pode_mp_point_automatico,
    pagamento_linha_eh_mp_point_auto,
)
from produtos.views_mp_point import _conta_do_pedido_local, _resolver_mp_point_conta
from produtos.mercado_pago_point import (
    MAQUININHAS_MP_POINT_AUTO_CENTRO,
    MAQUININHAS_MP_POINT_AUTO_VILA,
    mp_point_conta_configurada,
    mp_point_conta_de_maquina,
    mp_point_credenciais,
    normalizar_mp_point_conta,
)

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def main() -> int:
    settings_py = (ROOT / "config/settings.py").read_text(encoding="utf-8")
    caixa_py = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8")
    views_mp = (ROOT / "produtos/views_mp_point.py").read_text(encoding="utf-8")
    views_caixa = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    pdv_views = (ROOT / "pdv/views.py").read_text(encoding="utf-8")
    wizard = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    state_js = (ROOT / "produtos/static/produtos/js/pdv_state.js").read_text(encoding="utf-8")
    env_ex = (ROOT / ".env.example").read_text(encoding="utf-8")

    check("MP_POINT_VILA_ACCESS_TOKEN" in settings_py, "settings token Vila")
    check("MP_POINT_VILA_TERMINAL_ID" in settings_py, "settings terminal Vila")
    check("MP_POINT_VILA_ACCESS_TOKEN" in env_ex, "env.example Vila")
    check("mp_point_host_conta" in caixa_py, "host conta helper")
    check('marcar_navegador_host_mp_point(request, conta="vila")' in views_caixa, "abrir Vila marca host")
    check("mpPointVilaEnabled" in pdv_views, "bootstrap flag Vila")
    check("mp_point_conta" in views_mp, "pedido grava conta")
    check("mp_point_credenciais(conta)" in views_mp, "criar usa credencial da conta")
    check("mid === 'mp_vila'" in wizard, "JS cartao Vila auto")
    check("mid === 'pix_mp_vila'" in wizard, "JS Pix Vila auto")
    check("mpPointVilaEnabled" in wizard, "JS le flag Vila")
    check("pix_mp_vila" in state_js, "JS state hidrata Pix Vila")

    check(normalizar_mp_point_conta("vila") == "vila", "normaliza vila")
    check(mp_point_conta_de_maquina("mp_vila") == "vila", "mp_vila conta vila")
    check(mp_point_conta_de_maquina("pix_mp_qr") == "centro", "pix_mp_qr conta centro")
    check(mp_point_conta_de_maquina("mp_renan") is None, "renan nao e Point")

    tok_c, ter_c = mp_point_credenciais("centro")
    tok_v, ter_v = mp_point_credenciais("vila")
    check(isinstance(tok_c, str) and isinstance(ter_c, str), "credencial centro tupla")
    check(isinstance(tok_v, str) and isinstance(ter_v, str), "credencial vila tupla")
    check(tok_v != tok_c or not tok_v, "Vila nao reusa token Centro vazio-ok")

    check("mp_vila" in MAQUININHAS_MP_POINT_AUTO_VILA, "id auto vila")
    check("mp_balcao" in MAQUININHAS_MP_POINT_AUTO_CENTRO, "id auto centro")
    check("mp_vila" not in MAQUININHAS_MP_POINT_AUTO_CENTRO, "vila fora do filtro centro")

    lista = [
        {"id": "mp_vila", "nome": "Vila"},
        {"id": "sicredi_1", "nome": "Sicredi"},
        {"id": "mp_balcao", "nome": "Centro"},
    ]
    sem_c = filtrar_maquininhas_pdv_sem_mp(lista, MAQUININHAS_MP_POINT_AUTO_CENTRO)
    ids_c = [m["id"] for m in sem_c]
    check("mp_balcao" not in ids_c and "mp_vila" in ids_c, "filtro centro nao apaga Vila")
    sem_v = filtrar_maquininhas_pdv_sem_mp(lista, MAQUININHAS_MP_POINT_AUTO_VILA)
    ids_v = [m["id"] for m in sem_v]
    check("mp_vila" not in ids_v and "sicredi_1" in ids_v, "filtro vila remove so Point Vila")

    check(
        not pagamento_linha_eh_mp_point_auto({"maquinaId": "mp_vila"}),
        "conferencia mp_vila manual",
    )
    check(
        pagamento_linha_eh_mp_point_auto({"maquinaId": "mp_vila", "cobrarNoPointMp": True}),
        "conferencia mp_vila com marcador",
    )
    check("mp_vila" in _MAQUININHAS_MP_POINT_AUTO_VILA_IDS, "caixa_util ids vila")

    # Sem credencial Vila, a conta não liga (não dispara Point na loja até colar token+ID).
    check(
        not mp_point_conta_configurada("vila")
        or bool((getattr(settings, "MP_POINT_VILA_ACCESS_TOKEN", "") or "").strip()),
        "Vila so configura com token",
    )
    check(ter_c != ter_v or not ter_v, "terminais Centro e Vila distintos")
    check(
        not ter_v or ter_v.startswith("NEWLAND_N950__"),
        "terminal Vila formato NEWLAND",
    )
    check(
        pagamento_linha_eh_mp_point_auto(
            {"maquinaId": "pix_mp_vila", "mpBalcaoModo": "point"}
        ),
        "conferencia pix_mp_vila com modo point",
    )

    def _req(host: str | None, ponto: str) -> SimpleNamespace:
        sess = {SESSION_PONTO_OPERACAO_KEY: ponto}
        if host:
            sess[SESSION_MP_POINT_HOST_KEY] = host
        return SimpleNamespace(session=sess)

    check(
        navegador_pode_mp_point_automatico(_req("vila", PONTO_CAIXA_VILA), conta="vila"),
        "host Vila + caixa Vila libera auto",
    )
    check(
        not navegador_pode_mp_point_automatico(
            _req("vila", PONTO_CAIXA_NOTEBOOK), conta="vila"
        ),
        "Notebook Vila bloqueia auto",
    )
    check(
        not navegador_pode_mp_point_automatico(
            _req("centro", PONTO_CAIXA_GAVETA), conta="vila"
        ),
        "host Centro nao cobra na conta Vila",
    )
    check(
        not navegador_pode_mp_point_automatico(
            _req("vila", PONTO_CAIXA_VILA), conta="centro"
        ),
        "host Vila nao cobra na conta Centro",
    )
    check(
        navegador_pode_mp_point_automatico(
            _req("centro", PONTO_CAIXA_GAVETA), conta="centro"
        ),
        "host Centro + gaveta libera auto Centro",
    )
    check(
        not navegador_pode_mp_point_automatico(_req(None, PONTO_CAIXA_VILA), conta="vila"),
        "sem host nao dispara Point",
    )

    req_vila = _req("vila", PONTO_CAIXA_VILA)
    check(
        _resolver_mp_point_conta(req_vila, {"maquinaId": "mp_vila"}) == "vila",
        "resolver mp_vila = vila",
    )
    check(
        _resolver_mp_point_conta(req_vila, {"maquinaId": "mp_balcao"}) == "centro",
        "resolver mp_balcao = centro mesmo no host Vila",
    )
    check(
        _resolver_mp_point_conta(_req("centro", PONTO_CAIXA_GAVETA), {}) == "centro",
        "resolver vazio no host Centro = centro",
    )
    row_vila = SimpleNamespace(erp_payload={"mp_point_conta": "vila", "maquinaId": "mp_vila"})
    check(
        _conta_do_pedido_local(
            _req("centro", PONTO_CAIXA_GAVETA),
            row_vila,
            {"mp_point_conta": "centro"},
        )
        == "vila",
        "pedido local ignora override do browser",
    )

    print(f"---\noks={OKS} fails={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" ", f)
        return 1
    print("VERIFY_MP_POINT_VILA_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
