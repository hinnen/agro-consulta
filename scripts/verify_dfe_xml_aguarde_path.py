#!/usr/bin/env python
"""Prova DFE-XML-AGUARDE — XML fora do Aguarde 1h da lista. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAIL: list[str] = []
OK = 0
CNPJ = "48900774000103"
CHAVE = "35260848900774000103550010000012341000012345"


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" — {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8").replace("\r\n", "\n")


def provar_contratos_fonte() -> None:
    client = _read("produtos/sefaz_dfe_client.py")
    inbox = _read("produtos/dfe_inbox_util.py")
    views = _read("produtos/views.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")

    check("client tem modo cons_chave", "modo == \"cons_chave\"" in client or 'modo == "cons_chave"' in client)
    check("client cooldown_tipo", "_dfe_cooldown_tipo" in client and '"tipo": tipo' in client)
    check(
        "chave nao aplica 137 lista",
        "origem == \"cons_chave\"" in client and "return" in client[client.find("origem == \"cons_chave\"") :][:200],
    )
    check("inbox auto completar resumos", "dfe_completar_resumos_pendentes" in inbox)
    check(
        "consulta chama auto xml",
        "dfe_completar_resumos_pendentes(cnpj)" in inbox
        and 'out["auto_xml"]' in inbox,
    )
    check("ciencia+baixar existe", "dfe_manifestar_ciencia_e_baixar" in inbox)
    check("views status xml_liberado", '"xml_liberado"' in views)
    check("views auto_xml na msg", "auto_xml" in views and "pronta(s) para Carregar na grade" in views)
    check("UI LiberarXmlsRestantes", "entradaNfeDfeLiberarXmlsRestantes" in html)
    check("UI chave nao trava com Buscar lista", "XML bloqueado pela Receita (656)" in html)
    check("UI xml_liberado no limite", "xml_liberado" in html and "aguardar_xml_segundos" in html)
    check(
        "UI Carregar na grade manual",
        "btn-dfe-carregar" in html and "Carregar na grade" in html,
    )
    check(
        "ajuda: nao carrega grade sozinho",
        "Não carrega na grade sozinho" in html or "não carrega na grade sozinho" in html.lower(),
    )
    # Não deve existir auto aplicar nota na grade após ciência
    ciencia_bloco = html
    check(
        "ciencia nao chama AplicarNotaNaGrade",
        "entradaNfeDfeAplicarNotaNaGrade" not in ciencia_bloco.split("btn-dfe-ciencia")[1].split("btn-dfe-ignorar")[0]
        if "btn-dfe-ciencia" in ciencia_bloco
        else False,
    )


def provar_cooldown_runtime() -> None:
    import django

    django.setup()

    from django.core.cache import cache

    from produtos.sefaz_dfe_client import (
        dfe_aplicar_cooldown_apos_resposta,
        dfe_checar_limite_consulta,
        dfe_registrar_consulta_enviada,
        dfe_status_limite,
    )

    for k in (
        f"agro_dfe_cooldown:{CNPJ}",
        f"agro_dfe_last_call:nsu:{CNPJ}",
        f"agro_dfe_last_call:chave:{CNPJ}",
        f"agro_dfe_last_call:{CNPJ}",  # legado
    ):
        cache.delete(k)

    dfe_aplicar_cooldown_apos_resposta(
        CNPJ,
        c_stat=137,
        ult_nsu="10",
        max_nsu="10",
        x_motivo="Nenhum documento localizado",
        origem="dist_nsu",
    )
    b_lista = dfe_checar_limite_consulta(CNPJ, modo="dist_nsu")
    b_xml = dfe_checar_limite_consulta(CNPJ, modo="cons_chave")
    st = dfe_status_limite(CNPJ)
    check("137 trava lista", bool(b_lista and int(b_lista.get("aguardar_segundos") or 0) > 0))
    check("137 libera XML", b_xml is None)
    check("status: lista bloqueada xml livre", st.get("liberado") is False and st.get("xml_liberado") is True)

    dfe_aplicar_cooldown_apos_resposta(
        CNPJ,
        c_stat=656,
        ult_nsu="10",
        max_nsu="10",
        x_motivo="Consumo indevido",
        origem="cons_chave",
    )
    b_xml2 = dfe_checar_limite_consulta(CNPJ, modo="cons_chave")
    b_lista2 = dfe_checar_limite_consulta(CNPJ, modo="dist_nsu")
    check("656 trava XML", bool(b_xml2 and b_xml2.get("c_stat") == 656))
    check("656 trava lista", bool(b_lista2))

    cache.delete(f"agro_dfe_cooldown:{CNPJ}")
    dfe_aplicar_cooldown_apos_resposta(
        CNPJ,
        c_stat=138,
        ult_nsu="99",
        max_nsu="99",
        x_motivo="Documento localizado",
        origem="cons_chave",
    )
    check(
        "138 chave nao grava Aguarde lista",
        dfe_checar_limite_consulta(CNPJ, modo="dist_nsu") is None,
    )

    cache.delete(f"agro_dfe_cooldown:{CNPJ}")
    cache.delete(f"agro_dfe_last_call:chave:{CNPJ}")
    dfe_registrar_consulta_enviada(CNPJ, modo="cons_chave")
    b_min = dfe_checar_limite_consulta(CNPJ, modo="cons_chave")
    check(
        "intervalo minimo chave curto",
        bool(b_min and int(b_min.get("aguardar_segundos") or 0) <= 5),
    )
    # lista nao herda last_call da chave
    check(
        "last_call chave separado da lista",
        dfe_checar_limite_consulta(CNPJ, modo="dist_nsu") is None,
    )
    cache.delete(f"agro_dfe_last_call:chave:{CNPJ}")
    _ = time.time()


def provar_fluxo_auto_e_ciencia() -> None:
    import django

    django.setup()

    from produtos import dfe_inbox_util as inbox

    class FakeRow:
        pk = 42
        cnpj = CNPJ
        chave = CHAVE
        schema = "resumo"
        xml = "<resNFe/>"
        manifestacao_status = ""
        manifestacao_protocolo = ""
        manifestacao_mensagem = ""
        manifestacao_em = None

        def save(self, **_kwargs):
            return None

        def refresh_from_db(self):
            return None

    row = FakeRow()
    chamadas = {"ciencia": 0, "download": 0}

    def ciencia(_chave):
        chamadas["ciencia"] += 1
        return {
            "ok": True,
            "c_stat": "135",
            "x_motivo": "Evento registrado",
            "protocolo": "135260000000001",
        }

    def download(_chave):
        chamadas["download"] += 1
        row.schema = "nfe"
        row.xml = "<nfeProc><NFe/></nfeProc>"
        return {"ok": True, "c_stat": 138}

    with (
        patch("produtos.dfe_inbox_util.dfe_inbox_obter", return_value=row),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value={"cnpj": CNPJ}),
        patch("produtos.sefaz_dfe_client.nfe_manifestar_ciencia_operacao", side_effect=ciencia),
        patch("produtos.dfe_inbox_util.dfe_executar_download_por_chave", side_effect=download),
        patch("produtos.dfe_inbox_util.dfe_inbox_listar", return_value=[]),
    ):
        out = inbox.dfe_manifestar_ciencia_e_baixar(row.pk)

    check("ciencia+xml completo", bool(out.get("ok") and out.get("xml_completo")))
    check("ciencia uma vez", chamadas["ciencia"] == 1)
    check("download apos ciencia", chamadas["download"] == 1)

    # completar lote: 2 resumos → 2 xml
    rows = [FakeRow(), FakeRow()]
    rows[0].pk = 1
    rows[1].pk = 2
    rows[1].chave = CHAVE[:-1] + "6"

    class QS(list):
        def order_by(self, *_a, **_k):
            return self

        def __getitem__(self, item):
            if isinstance(item, slice):
                return QS(list.__getitem__(self, item))
            return list.__getitem__(self, item)

    class FakeManager:
        def filter(self, **_kwargs):
            return QS(rows)

    with (
        patch("produtos.models.AgroNfeDistDfeDocumento") as Mod,
        patch("produtos.dfe_inbox_util.dfe_manifestar_ciencia_e_baixar") as mock_m,
    ):
        Mod.objects = FakeManager()
        Mod.Schema.RESUMO = "resumo"
        mock_m.side_effect = [
            {"ok": True, "xml_completo": True},
            {"ok": True, "xml_completo": True},
        ]
        auto = inbox.dfe_completar_resumos_pendentes(CNPJ, limite=8)
    check("auto lote xml_completos", auto.get("xml_completos") == 2 and auto.get("tentadas") == 2)

    with (
        patch("produtos.models.AgroNfeDistDfeDocumento") as Mod,
        patch("produtos.dfe_inbox_util.dfe_manifestar_ciencia_e_baixar") as mock_m,
    ):
        Mod.objects = FakeManager()
        Mod.Schema.RESUMO = "resumo"
        mock_m.side_effect = [
            {"ok": True, "xml_completo": False, "c_stat": 656, "aguardar_segundos": 3600, "erro": "656"},
            {"ok": True, "xml_completo": True},
        ]
        auto656 = inbox.dfe_completar_resumos_pendentes(CNPJ, limite=8)
    check("auto para no 656", auto656.get("parar_656") is True and auto656.get("tentadas") == 1)

    # consulta grava auto_xml mesmo com bloqueio lista
    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value={"cnpj": CNPJ}),
        patch(
            "produtos.sefaz_dfe_client.nfe_distribuicao_dfe_interesse",
            return_value={
                "ok": False,
                "c_stat": 137,
                "erro": "Aguarde",
                "aguardar_segundos": 3600,
                "bloqueio_local": True,
                "ult_nsu": None,
                "max_nsu": None,
                "notas_xml": [],
            },
        ),
        patch("produtos.nfe_entrada_util.obter_ult_nsu", return_value="000000000000001"),
        patch(
            "produtos.dfe_inbox_util.dfe_completar_resumos_pendentes",
            return_value={"xml_completos": 3, "tentadas": 3},
        ),
        patch(
            "produtos.dfe_inbox_util.dfe_inbox_listar",
            return_value=[{"id": 1, "pode_carregar": True}],
        ),
    ):
        out_c = inbox.dfe_executar_consulta_e_gravar(origem="manual")
    check(
        "consulta bloqueada ainda roda auto_xml",
        isinstance(out_c.get("auto_xml"), dict) and out_c["auto_xml"].get("xml_completos") == 3,
    )


def provar_urls_e_check() -> None:
    import django

    django.setup()
    from django.core.management import call_command
    from django.urls import reverse

    url = reverse("api_entrada_nota_dfe_inbox_ciencia", args=[7])
    check("rota ciencia", url.endswith("/ciencia/"))
    url_dist = reverse("api_entrada_nota_dist_dfe")
    check("rota dist dfe", "dist" in url_dist or "dfe" in url_dist)
    try:
        call_command("check")
        check("manage.py check", True)
    except Exception as exc:
        check("manage.py check", False, str(exc)[:120])


def main() -> int:
    print("=== verify_dfe_xml_aguarde_path ===")
    provar_contratos_fonte()
    provar_cooldown_runtime()
    provar_fluxo_auto_e_ciencia()
    provar_urls_e_check()
    print(f"\nRESULTADO: {OK} ok · {len(FAIL)} fail")
    if FAIL:
        for f in FAIL:
            print(f"  - {f}")
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
