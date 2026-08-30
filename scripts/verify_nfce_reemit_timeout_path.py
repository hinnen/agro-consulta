# -*- coding: utf-8 -*-
"""VERIFY NFCE-REEMIT-TIMEOUT — reemitir nao trava + SEFAZ 537.

Cobre: timeout sync < proxy Render · Abort 28s nas telas · lock anti-duplo ·
vDesc em todos os itens se ha desconto · sefaz_perfil=sync no emitir · AST ·
reusa prova NFCE-DESC · sem thread BG no reemitir (loading eterno).

Uso: python scripts/verify_nfce_reemit_timeout_path.py
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

fails = 0
oks = 0

FILES = (
    "produtos/sefaz_soap_util.py",
    "produtos/views_nfce.py",
    "produtos/nfce_sp_emissao_util.py",
    "produtos/templates/produtos/vendas_lista.html",
    "produtos/templates/produtos/venda_agro_detalhe.html",
    "scripts/verify_nfce_reemit_timeout_path.py",
)


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


def check_ast() -> None:
    print("\n[1] AST / parse")
    for rel in FILES:
        if rel.endswith(".html"):
            txt = read(rel)
            if not txt:
                continue
            if "AbortController" in txt or "nfce/emitir" in txt:
                ok(f"html reemit path: {rel}")
            else:
                fail(f"html sem path reemit: {rel}")
            continue
        p = ROOT / rel
        if not p.is_file():
            fail(f"ausente {rel}")
            continue
        try:
            ast.parse(p.read_text(encoding="utf-8"))
            ok(f"ast {rel}")
        except SyntaxError as exc:
            fail(f"ast {rel}: {exc}")


def check_timeouts() -> None:
    print("\n[2] Timeout sync cabe no Render (~30s)")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.sefaz_soap_util import (
        SEFAZ_HTTP_RETRY_DELAYS_SYNC,
        SEFAZ_HTTP_TIMEOUT_SYNC,
    )

    connect, read = SEFAZ_HTTP_TIMEOUT_SYNC
    delays = SEFAZ_HTTP_RETRY_DELAYS_SYNC
    n_try = max(1, len(delays))
    worst = n_try * (connect + read) + sum(delays[1:] if len(delays) > 1 else [])
    if connect <= 4 and read <= 14:
        ok(f"TIMEOUT_SYNC=({connect},{read})")
    else:
        fail(f"TIMEOUT_SYNC alto ({connect},{read}) — estoura teto 20s")
    if n_try <= 1:
        ok(f"RETRY_DELAYS_SYNC 1 tentativa ({delays})")
    elif worst <= 18:
        ok(f"RETRY_DELAYS_SYNC={delays} pior~{worst:.1f}s")
    else:
        fail(f"RETRY_DELAYS_SYNC pesado: {delays} pior~{worst:.1f}s")
    if worst <= 18:
        ok(f"orcamento SEFAZ sync pior caso ~{worst:.1f}s (<=18)")
    else:
        fail(f"orcamento SEFAZ sync ~{worst:.1f}s > 18s")

    from produtos.sefaz_soap_util import SEFAZ_HTTP_TIMEOUT

    if SEFAZ_HTTP_TIMEOUT[1] >= SEFAZ_HTTP_TIMEOUT_SYNC[1]:
        ok("perfil completo >= sync (background)")
    else:
        fail("perfil completo menor que sync")


def check_views_lock() -> None:
    print("\n[3] views_nfce — lock + sync")
    txt = read("produtos/views_nfce.py")
    found_lock_msg = ("já está sendo emitido" in txt) or ("ja esta sendo emitido" in txt)
    if found_lock_msg:
        ok("mensagem lock 409")
    else:
        fail("falta mensagem lock 409")
    for n in (
        "nfce_emit_lock_",
        "cache.add(lock_key",
        "cache.delete(lock_key)",
        'sefaz_perfil="sync"',
        "status=409",
        "_api_venda_agro_nfce_emitir_locked",
        "_nfce_emitir_json_response",
        "timeout=120",
    ):
        if n in txt:
            ok(f"views: `{n[:48]}`")
        else:
            fail(f"views: falta `{n}`")
    if "_nfce_reemitir_background_worker" in txt:
        fail("reemitir ainda usa background worker (loading eterno no Render)")
    else:
        ok("reemitir sem background worker")
    if "status=202" in txt and "processando" in txt and "Thread" in txt:
        fail("ainda responde 202 + thread no reemitir")
    else:
        ok("reemitir HTTP sincrono (sem 202/thread)")


def check_vdesc_all_items() -> None:
    print("\n[4] XML — vDesc em todos os itens (537) + gravar apos SEFAZ")
    em = read("produtos/nfce_sp_emissao_util.py")
    if "SEFAZ 537" in em or "537" in em:
        ok("comentario/ref 537")
    else:
        fail("falta ref 537 no emissao")
    if "if v_desc > 0:" in em and '_sub(prod, "vDesc"' in em:
        ok("vDesc escrito quando total > 0")
    else:
        fail("padrao vDesc em todos itens ausente")
    if re.search(r"if v_desc_item > 0:\s*\n\s*_sub\(prod, \"vDesc\"", em):
        fail("ainda so escreve vDesc se item > 0 (omitir 0.00)")
    else:
        ok("nao omite vDesc 0.00 quando ha desconto total")
    if "def _gravar_doc_nfce_venda" in em:
        ok("_gravar_doc_nfce_venda")
    else:
        fail("falta _gravar_doc_nfce_venda")
    if re.search(
        r"reutilizada.: True,\s*\n\s*\}\s*\n\s*NfceDocumentoAgro\.objects\.filter\(venda=venda\)\.exclude",
        em,
    ):
        fail("ainda apaga doc rejeitado ANTES da SEFAZ")
    else:
        ok("nao apaga rejeitada antes da SEFAZ")


def check_ui_abort() -> None:
    print("\n[5] UI reemitir — teto duro 20s + erro na tela")
    for rel in (
        "produtos/templates/produtos/vendas_lista.html",
        "produtos/templates/produtos/venda_agro_detalhe.html",
    ):
        txt = read(rel)
        if "AbortController" in txt:
            ok(f"{rel}: AbortController")
        else:
            fail(f"{rel}: falta AbortController")
        if "20000" in txt and "hardTimer" in txt:
            ok(f"{rel}: teto duro 20s")
        else:
            fail(f"{rel}: falta hardTimer 20s")
        if "pollResultado" in txt:
            fail(f"{rel}: ainda tem pollResultado (BG)")
        else:
            ok(f"{rel}: sem poll BG")
        if "terminar(" in txt or "function terminar" in txt:
            ok(f"{rel}: termina UI com mensagem")
        else:
            fail(f"{rel}: falta terminar()")
        if "não respondeu em 20s" in txt or "nao respondeu em 20s" in txt or "Não respondeu em 20s" in txt:
            ok(f"{rel}: mensagem timeout na tela")
        else:
            fail(f"{rel}: falta msg timeout")
        if "var finished = false" in txt or "finished = false" in txt:
            ok(f"{rel}: flag finished anti-duplo")
        else:
            fail(f"{rel}: falta flag finished")
        if "dataset.busy" in txt:
            ok(f"{rel}: busy anti double-click")
        else:
            fail(f"{rel}: falta dataset.busy")
    lista = read("produtos/templates/produtos/vendas_lista.html")
    if "Desconto já foi corrigido" in lista:
        ok("vendas_lista: tip 537 no modal")
    else:
        fail("vendas_lista: falta tip 537")
    if "function csrf()" in lista and "agro-csrf-nfce-vendas" in lista and "{{ csrf_token }}" in lista:
        ok("vendas_lista: csrf() definido + token no HTML")
    else:
        fail("vendas_lista: falta csrf() / token (POST nunca saia)")
    if "Falha de segurança (CSRF)" in lista:
        ok("vendas_lista: erro CSRF imediato na tela")
    else:
        fail("vendas_lista: falta aviso CSRF na tela")
    if "'X-CSRFToken': token" in lista:
        ok("vendas_lista: POST usa token (nao csrf() quebrado)")
    else:
        fail("vendas_lista: POST ainda sem token var")
    views = read("produtos/views_nfce.py")
    if 'sefaz_perfil="sync"' in views:
        ok("views: reemitir perfil sync")
    else:
        fail("views: falta sefaz_perfil=sync")
    if "Tentativa " in views and "enviando à SEFAZ" in views:
        ok("views: carimbo tentativa no doc")
    else:
        fail("views: falta carimbo Tentativa")
    if "result(timeout=18)" in views or "fut.result(timeout=18)" in views:
        ok("views: teto 18s no worker")
    else:
        fail("views: falta futures timeout 18s")
    if "Carimbo ANTES do lock Redis" in views or "carimba na hora" in views.lower() or "ANTES do lock Redis" in views:
        ok("views: carimbo antes do lock Redis")
    else:
        fail("views: carimbo ainda depois do Redis lock")
    if "SOCKET_CONNECT_TIMEOUT" in read("config/settings.py"):
        ok("settings: Redis socket timeout curto")
    else:
        fail("settings: Redis sem SOCKET timeout")
    if "shutdown(wait=False" in views and "cancel_futures=True" in views:
        ok("views: shutdown sem wait (HTTP nao prende no orfao)")
    else:
        fail("views: ThreadPool ainda pode wait=True apos timeout")
    if "with ThreadPoolExecutor" in views:
        fail("views: with ThreadPoolExecutor espera worker no exit")
    else:
        ok("views: sem with ThreadPool (evita hang no exit)")


def check_budget_js_vs_server() -> None:
    print("\n[6] Orcamento teto UI vs SEFAZ sync")
    views = read("produtos/views_nfce.py")
    if "_nfce_reemitir_background_worker" in views or (
        "nfce-reemit" in views and "daemon=True" in views
    ):
        fail("POST ainda dispara thread reemit BG")
    else:
        ok("POST reemitir com teto (sem BG eterno)")
    lista = read("produtos/templates/produtos/vendas_lista.html")
    if "hardTimer" in lista and "20000" in lista:
        ok("UI teto duro independente do fetch")
    else:
        fail("UI sem teto duro")
    if "máx. 20s" in lista or "max. 20s" in lista:
        ok("texto máx 20s no modal")
    else:
        fail("falta texto máx 20s")


def check_desc_path() -> None:
    print("\n[7] Subpath NFCE-DESC (537 rateio)")
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "verify_nfce_desc_itens_path.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    out = (r.stdout or "") + (r.stderr or "")
    if r.returncode == 0 and "VERIFY_OK" in out:
        m = re.search(r"(\d+) OK", out)
        ok(f"verify_nfce_desc_itens_path OK ({m.group(1) if m else '?'} checks)")
    else:
        fail("verify_nfce_desc_itens_path FAIL")
        print(out[-600:])


def check_casos_loja_6478_6507() -> None:
    """XML das vendas reais que travaram no reemitir (537)."""
    print("\n[8] Casos loja #6478 / #6507 (vDesc apos tostring)")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from datetime import datetime
    from decimal import Decimal
    from unittest.mock import patch
    import xml.etree.ElementTree as ET

    from produtos.nfce_sp_emissao_util import NS, _montar_xml_nfce
    from produtos.sefaz_xml_fiscal_util import tostring_sem_prefixos

    class Item:
        def __init__(self, q, vu, vt, nome="P"):
            self.quantidade = Decimal(str(q))
            self.valor_unitario = Decimal(str(vu))
            self.valor_total = Decimal(str(vt))
            self.codigo = "GM"
            self.produto_id_externo = "1"
            self.descricao = nome
            self.unidade = "UN"

    class Venda:
        def __init__(self, pk, total):
            self.pk = pk
            self.total = Decimal(str(total))
            self.frete = Decimal("0")
            self.pagamentos_json = [{"forma": "Cartao de debito", "valor": float(total)}]
            self.cliente_nome = ""

    CFG = {
        "tp_amb": 2,
        "cnpj": "48900774000103",
        "razao_social": "T",
        "fantasia": "T",
        "logradouro": "R",
        "numero": "1",
        "bairro": "C",
        "cmun": "3524600",
        "cidade": "J",
        "uf": "SP",
        "cep": "11940000",
        "fone": "",
        "ie": "1",
        "csc_id": "1",
        "csc_token": "x",
    }
    FIS = {"ncm": "01012100", "cfop": "5102", "origem": "0", "csosn": "102", "cest": ""}
    casos = [
        (6478, [(1, 136, 136), (12, 18, 216)], "334.00"),
        (6507, [(1, 87, 87)], "82.90"),
    ]
    for pk, specs, total in casos:
        itens = [Item(*s) for s in specs]
        venda = Venda(pk, total)
        with patch("produtos.nfce_sp_emissao_util.ibpt_valor_item", return_value=Decimal("0")), patch(
            "produtos.nfce_sp_emissao_util.calcular_ibpt_venda_itens",
            return_value={"ibpt_texto": "ok"},
        ), patch("produtos.nfce_sp_emissao_util._qr_code_url", return_value="https://x"):
            xml_body, _ = _montar_xml_nfce(
                CFG,
                venda,
                itens,
                serie=21,
                numero=1,
                chave="35" + "0" * 42,
                dh_emi=datetime(2026, 8, 29, 12, 0, 0),
                cpf_dest="",
                fiscal_itens=[FIS] * len(itens),
            )
        xml2 = tostring_sem_prefixos(xml_body)
        root = ET.fromstring(xml2)
        ns = {"n": NS}
        v_tot = Decimal(root.findtext(".//n:ICMSTot/n:vDesc", namespaces=ns) or "0")
        itens_d = [
            Decimal(el.text or "0")
            for el in root.findall(".//n:det/n:prod/n:vDesc", namespaces=ns)
        ]
        soma = sum(itens_d, Decimal("0"))
        if v_tot > 0 and len(itens_d) == len(itens) and v_tot == soma:
            ok(f"#{pk}: vDesc tot={v_tot} itens={itens_d} apos tostring")
        else:
            fail(f"#{pk}: FAIL tot={v_tot} itens={itens_d} (esperado tags={len(itens)})")
        # XML rejeitado antigo da loja NAO tinha vDesc nos itens — regressao
        if any(x is None for x in itens_d) or len(itens_d) == 0:
            fail(f"#{pk}: regressao bug #7 (sem vDesc nos itens)")


def check_processando_contrato() -> None:
    print("\n[9] Contrato processando (lock) + UI sync")
    util = read("produtos/nfce_venda_util.py")
    if 'cache.get(f"nfce_emit_lock_' in util or "nfce_emit_lock_" in util:
        ok("processando consulta lock")
    else:
        fail("processando sem lock")
    fn = util
    start = fn.find("def venda_nfce_processando")
    end = fn.find("\ndef ", start + 1)
    body = fn[start:end] if start >= 0 else ""
    if 'startswith("Em emissão")' in body or "startswith('Em emissão')" in body:
        fail("processando ainda trava por texto Em emissão (orfao)")
    else:
        ok("processando nao trava por texto orfao")
    for rel in (
        "produtos/templates/produtos/vendas_lista.html",
        "produtos/templates/produtos/venda_agro_detalhe.html",
    ):
        txt = read(rel)
        if "pollResultado" in txt:
            fail(f"{rel}: ainda poll BG")
        else:
            ok(f"{rel}: reemitir sync (sem poll)")
        if "p.processando || /Em emissão" in txt or "processando || /Em" in txt:
            fail(f"{rel}: loop por texto Em emissão")
    views = read("produtos/views_nfce.py")
    if "finally:" in views and "cache.delete(lock_key)" in views:
        ok("API finally libera lock")
    else:
        fail("API sem finally delete lock")


def main() -> int:
    print("VERIFY NFCE-REEMIT-TIMEOUT")
    check_ast()
    check_timeouts()
    check_views_lock()
    check_vdesc_all_items()
    check_ui_abort()
    check_budget_js_vs_server()
    check_desc_path()
    check_casos_loja_6478_6507()
    check_processando_contrato()
    print(f"\n=== RESULTADO: {oks} OK · {fails} FAIL ===")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
