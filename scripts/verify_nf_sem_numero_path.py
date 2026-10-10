"""Prova detalhada NF-SEM-NUMERO: Nº NF vazio → SEM-DDMM-XXXX.

Fonte · lógica (extrator/vínculo/rascunho) · unit · PIN 9973 · HTTP local.
VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import date
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

CHECKS = 0
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
_SEM_RE = re.compile(r"^SEM-\d{4}-[A-Z0-9]{4}$")


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"OK {msg}")


def _read(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def prova_fonte() -> None:
    util = _read("produtos/nfe_entrada_util.py")
    views = _read("produtos/views.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")

    if "def gerar_numero_nf_sem_nota" not in util:
        fail("util sem gerar_numero_nf_sem_nota")
    if "def garantir_numero_nf_cabecalho" not in util:
        fail("util sem garantir_numero_nf_cabecalho")
    if 'return f"SEM-{ddmm}-{suf}"' not in util:
        fail("formato SEM-DDMM-XXXX ausente")
    ok("util: gera SEM-DDMM-XXXX")

    if "cab_norm = garantir_numero_nf_cabecalho" not in util:
        fail("salvar_rascunho sem garantir")
    if "cab_prev" not in util or "existente=" not in util:
        fail("atualizar_rascunho sem reusar numero existente")
    ok("rascunho: garante/reusa numero")

    if "_NF_SEM_RE" not in util:
        fail("extrator sem _NF_SEM_RE")
    # SEM antes de dígitos no extrator (ordem importa)
    idx_sem = util.find("_NF_SEM_RE.search")
    idx_dig = util.find("_NF_DIGITS_RE.search")
    if idx_sem < 0 or idx_dig < 0 or idx_sem > idx_dig:
        fail("extrator deve tentar SEM antes de dígitos")
    ok("extrator CP: SEM antes de dígitos")

    if "garantir_numero_nf_cabecalho" not in views:
        fail("views sem import/uso garantir")
    if "garantir_numero_nf_cabecalho(cab_pin" not in views:
        fail("PIN sem garantir SEM")
    if "garantir_numero_nf_cabecalho(cab, existente=" not in views:
        fail("financeiro sem garantir SEM")
    ok("PIN + financeiro: rede de segurança SEM")

    if "entradaNfeGerarNumeroSemNota" not in html:
        fail("UI sem gerador SEM")
    if "entradaNfeGarantirNumeroNfCampo" not in html:
        fail("UI sem garantir no campo")
    # Confirmar chama garantir ANTES de validar
    m = re.search(
        r"async function entradaNfeConfirmarFornecedorEtapa\(\)\s*\{(.{0,400})\}",
        html,
        re.S,
    )
    if not m:
        fail("não achou entradaNfeConfirmarFornecedorEtapa")
    body = m.group(1)
    if "entradaNfeGarantirNumeroNfCampo()" not in body:
        fail("confirmar não chama garantir")
    if body.find("entradaNfeGarantirNumeroNfCampo()") > body.find(
        "entradaNfeWizardValidarEtapaDetalhe(1"
    ):
        fail("garantir deve rodar antes da validação no confirmar")
    if "Informe o número da NF" in html and "Nº NF opcional" not in html:
        # ainda pode haver texto antigo em outro lugar; etapa 1 não pode obrigar
        pass
    if "Nº NF opcional" not in html and "vazio = SEM" not in html:
        fail("UI sem hint de Nº opcional / SEM")
    # Validação etapa 1 não exige numero (não regenera no hover do botão)
    if re.search(
        r"if \(s === 1\) \{[^}]{0,800}Informe o número da NF",
        html,
        re.S,
    ):
        fail("etapa 1 ainda exige Nº NF (quebraria SEM automático)")
    ok("UI: confirmar gera SEM; etapa 1 não exige Nº")


def prova_logica() -> None:
    import django

    django.setup()
    from produtos.nfe_entrada_util import (
        _extrair_nf_numero_lancamento,
        _nf_numero_norm,
        atualizar_rascunho_entrada,
        gerar_numero_nf_sem_nota,
        garantir_numero_nf_cabecalho,
        rascunho_entrada_valido_para_aprovacao_wizard,
        salvar_rascunho_entrada,
        validar_vinculo_financeiro_entrada_nfe,
        _titulos_entrada_nfe_ids_do_rascunho,
    )
    from produtos.tests_entrada_nf_reabertura_estoque import FakeCollection, RID, _doc

    # 1) Formato + unicidade
    a = gerar_numero_nf_sem_nota()
    b = gerar_numero_nf_sem_nota()
    if not _SEM_RE.match(a) or not _SEM_RE.match(b):
        fail(f"formato inválido: {a!r} / {b!r}")
    if a == b:
        fail("dois SEM gerados iguais (colisão improvável)")
    ok(f"gerador formato+único ({a} ≠ {b})")

    # 2) garantir: vazio / mantém / reusa
    g = garantir_numero_nf_cabecalho({"numero": ""})
    if not _SEM_RE.match(str(g["numero"])):
        fail(f"garantir vazio falhou: {g}")
    if garantir_numero_nf_cabecalho({"numero": "76468"})["numero"] != "76468":
        fail("garantir sobrescreveu NF real")
    with patch(
        "produtos.nfe_entrada_util.gerar_numero_nf_sem_nota",
        return_value="SEM-0101-ZZZZ",
    ):
        kept = garantir_numero_nf_cabecalho({"numero": "  "}, existente="SEM-0710-A3F2")
    if kept["numero"] != "SEM-0710-A3F2":
        fail(f"não reusou existente: {kept}")
    ok("garantir: gera / mantém / reusa")

    # 3) Extrator + norm
    desc = "NF SEM-0710-A3F2 — Sn - Ms Comercio (parcela 1/1)"
    tit0 = {"Descricao": desc, "Cliente": "Sn - Ms Comercio"}
    ext = _extrair_nf_numero_lancamento(tit0)
    if ext != "SEM-0710-A3F2":
        fail(f"extrator SEM falhou: {ext!r}")
    if _nf_numero_norm(ext) != _nf_numero_norm("SEM-0710-A3F2"):
        fail("norm SEM não casa")
    if _nf_numero_norm("sem-0710-a3f2") != _nf_numero_norm("SEM-0710-A3F2"):
        fail("norm casefold SEM falhou")
    # «não tem» e dígitos intactos
    if _nf_numero_norm(_extrair_nf_numero_lancamento({"Descricao": "NF não tem — X"})) != "nao tem":
        fail("extrator «não tem» quebrou")
    if _extrair_nf_numero_lancamento({"Descricao": "NF 12345 — Y"}) != "12345":
        fail("extrator numérico quebrou")
    ok("extrator+norm: SEM / não tem / dígitos")

    # 4) Vínculo financeiro com SEM (1 parcela)
    d = _doc(
        {
            "financeiro_ui": {
                "parcelas_manual": [
                    {"data_vencimento": "2026-10-10", "valor": "150.00"},
                ]
            },
        },
        status="pronta",
    )
    for k in ("financeiro_lancado", "financeiro_ids", "financeiro_lote"):
        d["extra"].pop(k, None)
    d["cabecalho"].update(
        {
            "numero": "SEM-0710-A3F2",
            "serie": "",
            "emit_nome": "Fornecedor SEM Teste",
            "emit_fornecedor_id": "forn-sem-1",
            "emit_cnpj": "",
            "chave": "",
            "plano_conta": "COMPRA MERCADORIA SN",
            "data_entrada": "2026-10-07",
            "empresa_faturada_id": "emp1",
            "deposito_entrada": "centro",
        }
    )
    tid = "tit-sem-1"
    titulos = [
        {
            "_id": tid,
            "Cliente": "Fornecedor SEM Teste",
            "ClienteID": "forn-sem-1",
            "Descricao": "NF SEM-0710-A3F2 — Fornecedor SEM Teste (parcela 1/1)",
            "Observacao": "Entrada NF-e Agro · chave —",
            "ValorBruto": "150.00",
            "DataVencimento": date(2026, 10, 10),
            "Despesa": True,
        }
    ]
    out = validar_vinculo_financeiro_entrada_nfe(d, titulos, [tid])
    if not out.get("valido"):
        fail(f"validador rejeitou SEM: {out}")
    with (
        patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[]),
        patch(
            "produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro",
            return_value=titulos,
        ),
    ):
        achados = _titulos_entrada_nfe_ids_do_rascunho(None, d)
    if achados != [tid]:
        fail(f"ids rascunho SEM={achados}, esperado {[tid]}")
    ok("vínculo CP: SEM casa 1 parcela")

    # 5) atualizar rascunho: cliente manda numero vazio → reusa SEM gravado
    d2 = _doc({}, status="pronta")
    d2["cabecalho"] = {
        "emit_nome": "Fornecedor SEM Teste",
        "numero": "SEM-0710-KEEP",
        "plano_conta": "COMPRA MERCADORIA SN",
        "data_entrada": "2026-10-07",
        "empresa_faturada_id": "emp1",
        "deposito_entrada": "centro",
    }
    d2["linhas"] = [
        {
            "produto_id": "p1",
            "qtd": 1,
            "v_un": 10,
            "nome_catalogo": "Item",
        }
    ]
    col = FakeCollection(d2)
    cab_vazio = dict(d2["cabecalho"])
    cab_vazio["numero"] = ""
    with (
        patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
        patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
        patch(
            "produtos.nfe_entrada_util.entrada_nfe_bloqueio_troca_produto_com_estoque",
            return_value=None,
        ),
    ):
        r = atualizar_rascunho_entrada(
            None,
            RID,
            usuario="prova",
            modo="manual",
            cabecalho=cab_vazio,
            linhas=d2["linhas"],
        )
    if not r.get("ok"):
        fail(f"atualizar falhou: {r}")
    num_pos = str((col.doc.get("cabecalho") or {}).get("numero") or "")
    if num_pos != "SEM-0710-KEEP":
        fail(f"update com vazio sobrescreveu SEM: {num_pos!r}")
    ok("atualizar rascunho: vazio reusa SEM existente")

    # 6) salvar novo com vazio gera SEM
    col_new = FakeCollection({"_id": RID})
    # FakeCollection insert: salvar usa insert_one — FakeCollection pode não ter
    if not hasattr(col_new, "insert_one"):

        def _ins(doc):
            col_new.doc = dict(doc)
            col_new.doc["_id"] = RID
            return type("R", (), {"inserted_id": RID})()

        col_new.insert_one = _ins  # type: ignore[attr-defined]

    with (
        patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col_new),
        patch(
            "produtos.nfe_entrada_util.gerar_numero_nf_sem_nota",
            return_value="SEM-0710-NEWS",
        ),
    ):
        r_ins = salvar_rascunho_entrada(
            None,
            usuario="prova",
            modo="manual",
            cabecalho={
                "emit_nome": "F",
                "numero": "",
                "plano_conta": "X",
                "data_entrada": "2026-10-07",
                "empresa_faturada_id": "e",
                "deposito_entrada": "centro",
            },
            linhas=[{"produto_id": "p1", "qtd": 1}],
        )
    if not r_ins.get("ok"):
        fail(f"salvar falhou: {r_ins}")
    num_ins = str((col_new.doc.get("cabecalho") or {}).get("numero") or "")
    if num_ins != "SEM-0710-NEWS":
        fail(f"salvar não gerou SEM: {num_ins!r}")
    ok("salvar rascunho: vazio gera SEM")

    # 7) validação PIN: com SEM ok; sem numero ainda falha (rede PIN preenche antes)
    d_ok = _doc(
        {
            "financeiro_lancado": True,
            "financeiro_ids": ["x"],
            "financeiro_lancado_em": "2026-10-07T12:00:00+00:00",
        },
        status="pronta",
    )
    d_ok["cabecalho"].update(
        {
            "emit_nome": "F",
            "numero": "SEM-0710-PIN1",
            "plano_conta": "COMPRA MERCADORIA SN",
            "data_entrada": "2026-10-07",
            "empresa_faturada_id": "e1",
            "deposito_entrada": "centro",
        }
    )
    d_ok["linhas"] = [{"produto_id": "p1", "qtd": 1, "v_un": 1}]
    ok_r, err_r = rascunho_entrada_valido_para_aprovacao_wizard(d_ok)
    if not ok_r:
        fail(f"validação PIN rejeitou SEM: {err_r}")
    d_bad = dict(d_ok)
    d_bad["cabecalho"] = dict(d_ok["cabecalho"])
    d_bad["cabecalho"]["numero"] = ""
    ok_b, err_b = rascunho_entrada_valido_para_aprovacao_wizard(d_bad)
    if ok_b:
        fail("validação PIN aceitou numero vazio (deveria falhar sem garantir)")
    if "Número da NF ausente" not in (err_b or ""):
        fail(f"mensagem inesperada: {err_b!r}")
    ok("validação PIN: SEM ok · vazio bloqueia (PIN garante antes)")


def prova_unit() -> None:
    r = subprocess.run(
        [
            sys.executable,
            "manage.py",
            "test",
            "produtos.tests_entrada_nf_sem_numero",
            "-v",
            "1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out = (r.stdout or "") + (r.stderr or "")
    if r.returncode != 0:
        fail(f"unit tests falhou:\n{out[-2000:]}")
    # Conta testes OK no output
    m = re.search(r"Ran (\d+) test", out)
    n = int(m.group(1)) if m else 0
    if n < 5:
        fail(f"esperava ≥5 unit tests, ran {n}")
    ok(f"unit tests_entrada_nf_sem_numero ({n})")


def prova_pin_e_http() -> None:
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client

    try:
        from produtos.pin_operador_util import operador_label_de_pin
    except Exception:
        try:
            from produtos.caixa_util import operador_label_de_pin  # type: ignore
        except Exception:
            operador_label_de_pin = None  # type: ignore

    pin_ok = False
    label = ""
    if operador_label_de_pin:
        try:
            pin_ok, label, err = operador_label_de_pin(PIN)
            if not pin_ok:
                # algumas signatures retornam (ok, label) só
                pass
        except TypeError:
            try:
                res = operador_label_de_pin(PIN)
                if isinstance(res, tuple) and len(res) >= 2:
                    pin_ok, label = bool(res[0]), str(res[1] or "")
            except Exception as exc:
                fail(f"PIN util erro: {exc}")
        except Exception as exc:
            fail(f"PIN util erro: {exc}")
    if not pin_ok:
        # fallback: PerfilUsuario
        try:
            from produtos.models import PerfilUsuario

            pin_ok = PerfilUsuario.objects.filter(senha_rapida=PIN).exists()
            label = label or "perfil"
        except Exception:
            pass
    if not pin_ok:
        fail(f"PIN {PIN} não resolve no banco local")
    ok(f"PIN {PIN} resolve ({label or 'ok'})")

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    if user is None:
        fail("sem usuário local para force_login")
    c = Client(HTTP_HOST="127.0.0.1")
    anon = c.get("/entrada-nota/", follow=False)
    if anon.status_code not in (302, 301, 401, 403):
        # 200 sem login seria falha de auth
        if anon.status_code == 200:
            fail("entrada-nota abriu sem login")
    ok(f"entrada-nota exige login (HTTP {anon.status_code})")

    c.force_login(user)
    r = c.get("/entrada-nota/")
    if r.status_code != 200:
        fail(f"/entrada-nota/ status {r.status_code}")
    body = r.content.decode("utf-8", errors="replace")
    for marker in (
        "entradaNfeGerarNumeroSemNota",
        "entradaNfeGarantirNumeroNfCampo",
        "vazio = SEM",
        "nfe-btn-confirmar-fornecedor",
    ):
        if marker not in body:
            fail(f"página sem marcador {marker!r}")
    ok("/entrada-nota/ 200 com SEM na UI (login + PIN ok)")


def main() -> None:
    print(f"=== NF-SEM-NUMERO path · PIN={PIN} ===")
    prova_fonte()
    prova_logica()
    prova_unit()
    prova_pin_e_http()
    print(f"VERIFY_OK: NF-SEM-NUMERO ({CHECKS} checks)")


if __name__ == "__main__":
    main()
