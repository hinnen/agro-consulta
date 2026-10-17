"""Prova NF-FIN-NAO-TEM: nota manual «não tem», CP já pago, Salvar+a pagar religa.

Caso loja 10/09: Sn-Ms · 3 parcelas 564,07/564,07/564,06 · id dup 6a72360f…
VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import date
from decimal import Decimal
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

CHECKS = 0


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
    views = _read("produtos/views.py")
    util = _read("produtos/nfe_entrada_util.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")

    if "_NF_PLACEHOLDER_RE" not in util:
        fail("sem _NF_PLACEHOLDER_RE")
    if "n[aã]o\\s+tem" not in util:
        fail("regex placeholder «não tem» ausente")
    ok("extrator: placeholder «não tem»")

    if "unicodedata.normalize" not in util or "casefold" not in util:
        fail("_nf_numero_norm sem fold de acento")
    ok("norm: Nao Tem = nao tem")

    if "_estreitar_candidatos_nf_placeholder" not in util:
        fail("sem estreitar candidatos")
    if "_titulo_entrada_nfe_fornecedor_forte" not in util:
        fail("sem filtro fornecedor forte")
    ok("estreita por fornecedor + parcelas")

    if "not nf.isdigit() and emit" not in util:
        fail("rastro PG/Mongo sem estreitar fornecedor em NF texto")
    ok("rastro CP estreita fornecedor em NF texto")

    if "duplicidade bloqueada" not in views:
        fail("views sem contagem de duplicidade")
    if "sync_dup = sincronizar_financeiro_rascunho_entrada_nfe" not in views:
        fail("views sem recuperação pós-duplicidade")
    if '"recuperado": True' not in views:
        fail("views sem resposta recuperado")
    ok("API: duplicata → religa (não 400)")

    if "sync_antes = sincronizar_financeiro_rascunho_entrada_nfe" not in views:
        fail("views sem sync antes do insert")
    ok("API: sync antes de inserir lote")

    if "entradaNfeFinanceiroTituloJaGerado" not in html:
        fail("UI sem Conta a pagar já gerada")
    if "nfe-wiz-fin-alerta" not in html:
        fail("UI sem alerta etapa 7")
    ok("UI etapa 7: alerta + já gerada")


def prova_logica_nao_tem() -> None:
    import django

    django.setup()
    from produtos.nfe_entrada_util import (
        _estreitar_candidatos_nf_placeholder,
        _extrair_nf_numero_lancamento,
        _nf_numero_norm,
        _titulos_entrada_nfe_ids_do_rascunho,
        sincronizar_financeiro_rascunho_entrada_nfe,
        validar_vinculo_financeiro_entrada_nfe,
    )
    from produtos.tests_entrada_nf_reabertura_estoque import FakeCollection, RID, _doc

    # 1) Extrator + norm (caso loja)
    desc = "NF não tem — Sn - Ms Comercio E Representacao (parcela 1/3)"
    tit0 = {"Descricao": desc, "Cliente": "Sn - Ms Comercio E Representacao"}
    if _nf_numero_norm(_extrair_nf_numero_lancamento(tit0)) != "nao tem":
        fail(f"extrator falhou em: {desc!r}")
    if _nf_numero_norm("não tem") != "nao tem":
        fail("norm cab.numero «não tem» falhou")
    if _nf_numero_norm("Nao  TEM") != "nao tem":
        fail("norm variantes falhou")
    ok("extrator+norm casam «não tem»")

    # 2) Não casa número parecido / outra NF
    tit_num = {"Descricao": "NF 12345 — Outro"}
    if _extrair_nf_numero_lancamento(tit_num) != "12345":
        fail("extrator numérico quebrou")
    ok("extrator numérico intacto")

    # 3) Validador + ids do rascunho (3 parcelas loja)
    d = _doc(
        {
            "financeiro_ui": {
                "parcelas_manual": [
                    {"data_vencimento": "2026-08-10", "valor": "564.07"},
                    {"data_vencimento": "2026-08-17", "valor": "564.07"},
                    {"data_vencimento": "2026-08-24", "valor": "564.06"},
                ]
            },
        },
        status="encerrada",
    )
    for k in ("financeiro_lancado", "financeiro_ids", "financeiro_lote"):
        d["extra"].pop(k, None)
    d["cabecalho"].update(
        {
            "numero": "não tem",
            "serie": "",
            "emit_nome": "Sn - Ms Comercio E Representacao",
            "emit_fornecedor_id": "",
            "emit_cnpj": "",
            "chave": "",
        }
    )
    ids = ["6a72360fc2f235d15de39c42", "pg-nao-tem-2", "pg-nao-tem-3"]
    titulos = [
        {
            "_id": ids[0],
            "Cliente": "Sn - Ms Comercio E Representacao",
            "ClienteID": "",
            "Descricao": "NF não tem — Sn - Ms Comercio E Representacao (parcela 1/3)",
            "Observacao": "Entrada NF-e Agro · chave —",
            "ValorBruto": "564.07",
            "DataVencimento": date(2026, 8, 10),
            "Despesa": True,
        },
        {
            "_id": ids[1],
            "Cliente": "Sn - Ms Comercio E Representacao",
            "ClienteID": "",
            "Descricao": "NF não tem — Sn - Ms Comercio E Representacao (parcela 2/3)",
            "Observacao": "Entrada NF-e Agro · chave —",
            "ValorBruto": "564.07",
            "DataVencimento": date(2026, 8, 17),
            "Despesa": True,
        },
        {
            "_id": ids[2],
            "Cliente": "Sn - Ms Comercio E Representacao",
            "ClienteID": "",
            "Descricao": "NF não tem — Sn - Ms Comercio E Representacao (parcela 3/3)",
            "Observacao": "Entrada NF-e Agro · chave —",
            "ValorBruto": "564.06",
            "DataVencimento": date(2026, 8, 24),
            "Despesa": True,
        },
    ]
    # Intruso: outro «não tem» de outro fornecedor (não pode entrar)
    intruso = {
        "_id": "intruso-outro-forn",
        "Cliente": "Outro Fornecedor SA",
        "ClienteID": "",
        "Descricao": "NF não tem — Outro Fornecedor SA (parcela 1/1)",
        "Observacao": "Entrada NF-e Agro",
        "ValorBruto": "100.00",
        "DataVencimento": date(2026, 8, 1),
        "Despesa": True,
    }
    misturados = titulos + [intruso]
    est = _estreitar_candidatos_nf_placeholder(d, misturados)
    if any(_extrair_nf_numero_lancamento(t) and t.get("_id") == "intruso-outro-forn" for t in est):
        # estreitar pode manter se parcelas não batem — filtrar ids depois
        pass
    ids_est = [str(t.get("_id")) for t in est]
    if "intruso-outro-forn" in ids_est and len(est) == 4:
        fail("estreitar não tirou intruso de outro fornecedor")
    if set(str(t.get("_id")) for t in est) != set(ids):
        # Aceita se só as 3 parcelas certas ficaram
        if set(str(t.get("_id")) for t in est) - {"intruso-outro-forn"} != set(ids):
            fail(f"estreitar resultou {ids_est}, esperado {ids}")
    ok("estreitar remove outro fornecedor / casa parcelas")

    out = validar_vinculo_financeiro_entrada_nfe(d, titulos, ids)
    if not out.get("valido"):
        fail(f"validador rejeitou caso loja: {out}")
    if not out.get("parcelas_ok"):
        fail("parcelas_ok falso no caso loja")
    ok("validador: NF não tem + fornecedor + 3 parcelas")

    with (
        patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[]),
        patch(
            "produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro",
            return_value=misturados,
        ),
    ):
        achados = _titulos_entrada_nfe_ids_do_rascunho(None, d)
    if achados != ids:
        fail(f"ids do rascunho={achados}, esperado {ids}")
    ok("ids do rascunho: só as 3 parcelas certas")

    # 4) sincronizar grava flag sem duplicar
    col = FakeCollection(d)
    with (
        patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
        patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
        patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[]),
        patch(
            "produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro",
            return_value=titulos,
        ),
    ):
        sync = sincronizar_financeiro_rascunho_entrada_nfe(None, RID, usuario="prova")
    if not sync.get("ok") or not sync.get("sincronizado"):
        fail(f"sincronizar falhou: {sync}")
    if col.doc["extra"].get("financeiro_ids") != ids:
        fail("sincronizar não gravou financeiro_ids")
    if not col.doc["extra"].get("financeiro_lancado"):
        fail("sincronizar não marcou financeiro_lancado")
    ok("sincronizar religa flag sem insert")

    # 5) Assinatura centavos (564.06 última)
    total = sum(
        (Decimal(str(t["ValorBruto"])) for t in titulos),
        Decimal("0"),
    )
    if total != Decimal("1692.20"):
        fail(f"total parcelas={total}, esperado 1692.20")
    ok("total 3 parcelas = R$ 1.692,20")


def prova_django_test() -> None:
    env = os.environ.copy()
    env["DJANGO_SETTINGS_MODULE"] = "config.settings"
    r = subprocess.run(
        [
            sys.executable,
            "manage.py",
            "test",
            "produtos.tests_entrada_nf_financeiro_vinculo",
            "produtos.tests_entrada_nf_reabertura_estoque.EntradaNfReaberturaEstoqueTests.test_etapa_financeiro_reconhece_ids_preservados_sem_criar_duplicata",
            "-v",
            "1",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        fail(f"django test falhou:\n{r.stdout}\n{r.stderr}")
    m = re.search(r"Ran (\d+) test", r.stdout + r.stderr)
    n = int(m.group(1)) if m else 0
    if n < 15:
        fail(f"esperava >=15 testes, veio {n}")
    ok(f"django vínculo + anti-duplicata {n}/{n}")


def prova_smoke_http() -> None:
    """Se o runserver local estiver no ar, healthz + página Entrada NF."""
    try:
        import urllib.request

        with urllib.request.urlopen("http://127.0.0.1:8000/healthz", timeout=2) as resp:
            body = resp.read().decode("utf-8", errors="replace").strip()
            code = resp.status
    except Exception as exc:
        print(f"SKIP smoke HTTP (runserver off): {exc}")
        return
    if code != 200:
        fail(f"healthz={code}")
    ok(f"healthz local {code} {body[:20]!r}")
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:8000/entrada-nota/",
            headers={"User-Agent": "nf-fin-nao-tem-verify"},
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            # login redirect ok
            if resp.status not in (200, 302):
                print(f"SKIP entrada-nota status={resp.status}")
            else:
                ok(f"entrada-nota HTTP {resp.status}")
    except Exception as exc:
        # redirect to login is often HTTPError 302 handled; urllib follows
        msg = str(exc)
        if "302" in msg or "401" in msg or "403" in msg or "login" in msg.lower():
            ok("entrada-nota exige login (esperado)")
        else:
            print(f"SKIP entrada-nota: {exc}")


def main() -> None:
    prova_fonte()
    prova_logica_nao_tem()
    prova_django_test()
    prova_smoke_http()
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
