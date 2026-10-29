"""Prova NF-PIN-EXIGE-FIN (bug #18): PIN/finalizar exige conta a pagar (exceto bonificação).

VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys

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
    util = _read("produtos/nfe_entrada_util.py")
    views = _read("produtos/views.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")

    if "bug #18" not in util.lower() and "Bug #18" not in util:
        # aceita qualquer menção no bloco novo
        if "não some em Concluída" not in util and "nao some em Concluida" not in util:
            if "Gere a conta a pagar na etapa 7" not in util:
                fail("util sem trava de financeiro no PIN")
    if "Gere a conta a pagar na etapa 7" not in util:
        fail("rascunho_entrada_valido sem mensagem etapa 7")
    ok("util: PIN exige financeiro")

    if "sync financeiro pré-PIN" not in views and "sync financeiro pre-PIN" not in views:
        if "entrada_nfe_extra_financeiro_ok" not in views[views.find("api_entrada_nota_aprovar_wizard") : views.find("api_entrada_nota_aprovar_wizard") + 2500]:
            fail("API aprovar sem sync/check financeiro")
    if "entrada_nfe_extra_financeiro_ok" not in views:
        fail("views sem entrada_nfe_extra_financeiro_ok")
    ok("API: sync + trava pré-PIN")

    if "Gere a conta a pagar com «Salvar + a pagar»" not in html:
        fail("UI etapa 7 ainda libera sem a pagar")
    if "GERE A PAGAR NA ETAPA 7" not in html:
        fail("botão PIN sem bloqueio visual")
    if "PIN da etapa 8 + conta a pagar" not in html:
        fail("chip Concluída sem texto novo")
    ok("UI: etapa 7 + botão PIN + chip")


def prova_bucket_e_validacao() -> None:
    import django

    django.setup()
    from produtos.nfe_entrada_util import (
        entrada_nfe_fila_bucket_lista,
        entrada_nfe_status_efetivo,
        rascunho_entrada_valido_para_aprovacao_wizard,
    )
    from produtos.tests_entrada_nf_reabertura_estoque import _doc

    # PIN sem financeiro → financeiro (não concluida)
    d = _doc({"aprovacao_wizard_em": "2026-09-03T22:20:00+00:00"}, status="estoque_aplicado")
    d["entrada_status_efetivo"] = entrada_nfe_status_efetivo(d)
    d["entrada_financeiro_lancado"] = False
    bk = entrada_nfe_fila_bucket_lista(d)
    if bk != "financeiro":
        fail(f"PIN sem fin esperava financeiro, veio {bk}")
    ok("bucket: PIN sem fin = financeiro")

    # PIN + financeiro → concluida
    d2 = _doc(
        {
            "aprovacao_wizard_em": "2026-09-03T22:20:00+00:00",
            "financeiro_lancado": True,
            "financeiro_ids": ["abc"],
        },
        status="estoque_aplicado",
    )
    d2["entrada_status_efetivo"] = entrada_nfe_status_efetivo(d2)
    d2["entrada_financeiro_lancado"] = True
    if entrada_nfe_fila_bucket_lista(d2) != "concluida":
        fail("PIN+fin não ficou concluida")
    ok("bucket: PIN+fin = concluida")

    # Bonificação + PIN sem fin → concluida
    d3 = _doc(
        {
            "aprovacao_wizard_em": "2026-09-03T22:20:00+00:00",
            "nfe_tipo_entrada": "bonificacao",
        },
        status="estoque_aplicado",
    )
    d3["entrada_status_efetivo"] = entrada_nfe_status_efetivo(d3)
    d3["entrada_financeiro_lancado"] = False
    if entrada_nfe_fila_bucket_lista(d3) != "concluida":
        fail("bonificação+PIN não ficou concluida")
    ok("bucket: bonificação+PIN = concluida")

    # Validação PIN: compras sem fin → recusa
    doc_bad = _doc({}, status="estoque_aplicado")
    doc_bad["extra"].pop("financeiro_lancado", None)
    doc_bad["extra"].pop("financeiro_ids", None)
    ok_r, err = rascunho_entrada_valido_para_aprovacao_wizard(doc_bad)
    if ok_r:
        fail("validação liberou PIN sem financeiro")
    if "Salvar + a pagar" not in err and "conta a pagar" not in err.lower():
        fail(f"mensagem estranha: {err}")
    ok("validação: compras sem fin = bloqueia")

    # Bonificação passa
    doc_bon = _doc({"nfe_tipo_entrada": "bonificacao"}, status="estoque_aplicado")
    ok_b, err_b = rascunho_entrada_valido_para_aprovacao_wizard(doc_bon)
    if not ok_b:
        fail(f"bonificação bloqueada: {err_b}")
    ok("validação: bonificação libera")

    # Com flag financeiro passa
    doc_ok = _doc({"financeiro_lancado": True, "financeiro_ids": ["x1"]}, status="estoque_aplicado")
    ok_f, err_f = rascunho_entrada_valido_para_aprovacao_wizard(doc_ok)
    if not ok_f:
        fail(f"com financeiro bloqueado: {err_f}")
    ok("validação: com financeiro libera")


def main() -> None:
    prova_fonte()
    prova_bucket_e_validacao()
    print(f"VERIFY_OK checks={CHECKS}")


if __name__ == "__main__":
    main()
