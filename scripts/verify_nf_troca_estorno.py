"""Prova do fix NF-TROCA-ESTORNO: trocar/remover produto ou mudar quantidade exige estorno.

Roda com o banco local (rascunho de teste é criado e apagado no fim).
VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile

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


def _linhas(*pares) -> list[dict]:
    out = []
    for pid, q in pares:
        out.append(
            {
                "x_prod": f"ITEM {pid}",
                "produto_id": pid,
                "q_com": q,
                "un_por_embalagem": 1,
                "q_estoque": q,
                "v_un_com": 10,
            }
        )
    return out


def prova_unitaria() -> None:
    from produtos.nfe_entrada_util import entrada_nfe_bloqueio_troca_produto_com_estoque as guard

    base = _linhas(("A", 10), ("B", 2))
    carimbos = [
        {"status": "estoque_aplicado", "linhas": base},
        {"estoque_aplicado_em": "2026-08-05T12:00:00", "linhas": base},
        {"extra": {"estoque_agro_registrado_em": "2026-08-05T12:00:00"}, "linhas": base},
        {"extra": {"estoque_agro_ajuste_ids": [7]}, "linhas": base},
    ]
    for doc in carimbos:
        if not guard(doc, _linhas(("A", 10), ("C", 2))):
            fail(f"carimbo não detectado: {list(doc)}")
    ok("carimbos de estoque reconhecidos (status, estoque_aplicado_em, registrado_em, ajuste_ids)")

    doc = {"status": "estoque_aplicado", "linhas": base}
    if guard(doc, _linhas(("B", 2), ("A", 10))):
        fail("mesma nota em outra ordem não pode bloquear")
    if not guard(doc, _linhas(("A", 10), ("C", 2))):
        fail("troca de produto deveria bloquear")
    if not guard(doc, _linhas(("A", 10))):
        fail("remover linha deveria bloquear")
    if not guard(doc, _linhas(("A", 10), ("B", 2), ("D", 1))):
        fail("incluir produto deveria bloquear")
    if not guard(doc, _linhas(("A", 11), ("B", 2))):
        fail("mudar Qtd deveria bloquear")
    ok("troca / remoção / inclusão / quantidade bloqueiam; mesma grade passa")

    emb_antes = [{"produto_id": "A", "q_com": 2, "un_por_embalagem": 12, "q_estoque": 24}]
    doc_emb = {"status": "estoque_aplicado", "linhas": emb_antes}
    if guard(doc_emb, [{"produto_id": "A", "q_com": "2", "un_por_embalagem": "12", "q_estoque": "24"}]):
        fail("mesma quantidade como texto não pode bloquear")
    if not guard(doc_emb, [{"produto_id": "A", "q_com": 2, "un_por_embalagem": 6, "q_estoque": 12}]):
        fail("mudar Un/emb deveria bloquear")
    if guard(doc_emb, [{"produto_id": "A", "q_com": "2,0", "un_por_embalagem": "12", "q_estoque": "24,0"}]):
        fail("vírgula decimal não pode virar bloqueio falso")
    ok("Un/emb, texto e vírgula decimal tratados sem falso positivo")

    doc_livre = {"status": "nota_aberta", "extra": {}, "linhas": base}
    if guard(doc_livre, _linhas(("Z", 1))):
        fail("sem estoque aplicado não pode bloquear")
    if guard(doc, [{"produto_id": "local:1", "q_com": 5}] + base):
        fail("linha ainda sem catálogo (local:) não pode bloquear")
    if guard(doc, base + [{"x_prod": "linha nova sem produto", "q_com": 1}]):
        fail("linha nova sem produto não pode bloquear")
    ok("nota sem estoque, linha `local:` e linha em branco não bloqueiam")


def prova_banco() -> dict:
    """Fluxo real no Postgres: salvar → carimbar estoque → tentar troca → estornar → trocar."""
    from produtos.nfe_entrada_util import (
        atualizar_rascunho_entrada,
        obter_rascunho_entrada,
        reverter_integracao_entrada_nota_para_reabertura,
        salvar_rascunho_entrada,
    )
    from produtos.entrada_nota_rascunho_pg_util import EntradaNotaRascunhoPgCollection
    from produtos.agro_fonte_config import agro_entrada_nota_rascunho_postgres

    if not agro_entrada_nota_rascunho_postgres():
        fail("rascunho de entrada NF não está no Postgres neste ambiente")

    cab = {
        "emit_nome": "FORNECEDOR VERIFY",
        "numero": "999999",
        "data_entrada": "2026-08-05",
        "plano_conta": "COMPRA MERCADORIA SN",
        "empresa_faturada_id": "1",
        "deposito_entrada": "centro",
    }
    r = salvar_rascunho_entrada(
        None,
        usuario="verify@local",
        modo="manual",
        cabecalho=cab,
        linhas=_linhas(("A", 10), ("B", 2)),
        extra={"verify_nf_troca": True},
    )
    if not r.get("ok"):
        fail(f"não criou rascunho de prova: {r}")
    rid = str(r["id"])

    col = EntradaNotaRascunhoPgCollection()
    col.update_one(
        {"_id": rid},
        {
            "$set": {
                "status": "estoque_aplicado",
                "extra": {
                    "verify_nf_troca": True,
                    "estoque_agro_registrado_em": "2026-08-05T12:00:00+00:00",
                    "estoque_agro_ajuste_ids": [999999123],
                },
            }
        },
    )

    def salvar(linhas):
        return atualizar_rascunho_entrada(
            None,
            rid,
            usuario="verify@local",
            modo="manual",
            cabecalho=cab,
            linhas=linhas,
            extra={"verify_nf_troca": True},
        )

    r_igual = salvar(_linhas(("A", 10), ("B", 2)))
    if not r_igual.get("ok"):
        fail(f"salvar a mesma grade deveria passar: {r_igual}")
    r_troca = salvar(_linhas(("A", 10), ("C", 2)))
    if r_troca.get("ok") or not r_troca.get("requer_estorno"):
        fail(f"troca de produto deveria ser recusada: {r_troca}")
    r_qtd = salvar(_linhas(("A", 99), ("B", 2)))
    if r_qtd.get("ok") or not r_qtd.get("requer_estorno"):
        fail(f"mudança de quantidade deveria ser recusada: {r_qtd}")
    doc_meio = obter_rascunho_entrada(None, rid) or {}
    pids_meio = sorted(str(x.get("produto_id")) for x in (doc_meio.get("linhas") or []))
    if pids_meio != ["A", "B"]:
        fail(f"grade no banco mudou apesar do bloqueio: {pids_meio}")
    ok("banco: mesma grade salva; troca e quantidade recusadas sem alterar o rascunho")

    rr = reverter_integracao_entrada_nota_para_reabertura(None, rid, usuario="verify@local")
    if not rr.get("ok"):
        fail(f"estorno (reabrir) falhou: {rr}")
    doc_pos = obter_rascunho_entrada(None, rid) or {}
    ex_pos = doc_pos.get("extra") if isinstance(doc_pos.get("extra"), dict) else {}
    if ex_pos.get("estoque_agro_registrado_em") or ex_pos.get("estoque_agro_ajuste_ids"):
        fail(f"carimbo de estoque sobreviveu ao estorno: {ex_pos}")
    if str(doc_pos.get("status") or "").lower() == "estoque_aplicado":
        fail("status continuou estoque_aplicado após estorno")
    r_pos = salvar(_linhas(("A", 10), ("C", 2)))
    if not r_pos.get("ok"):
        fail(f"depois do estorno a troca deveria passar: {r_pos}")
    doc_fim = obter_rascunho_entrada(None, rid) or {}
    pids_fim = sorted(str(x.get("produto_id")) for x in (doc_fim.get("linhas") or []))
    if pids_fim != ["A", "C"]:
        fail(f"troca não gravou após estorno: {pids_fim}")
    ok("banco: estorno limpa carimbos e libera a troca do produto")
    return {"rid": rid}


def prova_http(rid_apagar: str) -> str:
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if not user:
        fail("sem usuário local para login")
    c = Client()
    c.force_login(user)
    host = {"HTTP_HOST": "127.0.0.1"}

    from produtos.entrada_nota_rascunho_pg_util import EntradaNotaRascunhoPgCollection

    col = EntradaNotaRascunhoPgCollection()
    col.update_one(
        {"_id": rid_apagar},
        {
            "$set": {
                "status": "estoque_aplicado",
                "linhas": _linhas(("A", 10), ("B", 2)),
                "extra": {
                    "verify_nf_troca": True,
                    "estoque_agro_registrado_em": "2026-08-05T12:00:00+00:00",
                    "estoque_agro_ajuste_ids": [999999123],
                },
            }
        },
    )
    body = {
        "id": rid_apagar,
        "modo": "manual",
        "cabecalho": {"emit_nome": "FORNECEDOR VERIFY", "numero": "999999"},
        "linhas": _linhas(("A", 10), ("C", 2)),
    }
    r = c.post(
        reverse("api_entrada_nota_rascunho_atualizar"),
        data=body,
        content_type="application/json",
        **host,
    )
    if r.status_code != 400:
        fail(f"API deveria recusar a troca (status {r.status_code})")
    j = r.json()
    if not j.get("requer_estorno") or "estorno" not in str(j.get("erro", "")).lower():
        fail(f"API sem sinal requer_estorno: {j}")
    ok("API rascunho/atualizar recusa troca com estoque aplicado (400 + requer_estorno)")

    r_pg = c.get(reverse("entrada_nota"), **host)
    if r_pg.status_code != 200:
        fail(f"/entrada-nota/ status {r_pg.status_code}")
    html = r_pg.content.decode("utf-8", "replace")
    marcadores = (
        'id="modal-nfe-troca-estorno"',
        "Estornar e trocar",
        "entradaNfeIntegracoesAplicadasCliente",
        "entradaNfeTrocarProdutoComEstorno",
        "entradaNfeRemoverLinhaComEstorno",
        "entradaNfeQtdAlteradaComEstorno",
        "entradaNfeEstornarParaEditarProdutos",
        "entradaNfeAplicarEstadoLocalAposEstorno",
        "entradaNfeAplicarProdutoNaLinhaInterno",
        "requer_estorno",
    )
    for m in marcadores:
        if m not in html:
            fail(f"tela sem marcador: {m}")
    ok(f"/entrada-nota/ renderiza 200 com {len(marcadores)} marcadores do fix")
    return html


def prova_estrutura_js(html: str) -> None:
    """Confere que cada caminho que mexe no produto passa pelo estorno (e não pela função interna)."""

    def bloco(nome: str, tamanho: int = 2600) -> str:
        i = html.find(nome)
        if i < 0:
            fail(f"função {nome} não encontrada na tela")
        return html[i : i + tamanho]

    wrapper = bloco("function entradaNfeAplicarProdutoNaLinha(tr, p, via)")
    if "entradaNfeIntegracoesAplicadasCliente()" not in wrapper:
        fail("troca de produto não consulta as integrações lançadas")
    if wrapper.find("entradaNfeTrocarProdutoComEstorno") > wrapper.find(
        "entradaNfeAplicarProdutoNaLinhaInterno(tr, p, via);"
    ):
        fail("aplicação do produto acontece antes do desvio para o estorno")

    del_handler = html[html.find(".btn-del').addEventListener") :][:600]
    if "entradaNfeRemoverLinhaComEstorno" not in del_handler:
        fail("botão × (remover linha) não passa pelo estorno")
    if "entradaNfeIntegracoesAplicadasCliente()" not in del_handler:
        fail("botão × não consulta integrações lançadas")

    qtd = bloco("async function entradaNfeQtdAlteradaComEstorno", 1200)
    if "el.value = anterior" not in qtd:
        fail("cancelar a troca de quantidade não devolve o valor anterior")

    estorno = bloco("async function entradaNfeEstornarParaEditarProdutos", 1800)
    for needle in ("URL_REABRIR_NOTA", "pin", "entradaNfeAplicarEstadoLocalAposEstorno(j)"):
        if needle not in estorno:
            fail(f"rotina de estorno sem '{needle}'")

    troca = bloco("async function entradaNfeTrocarProdutoComEstorno", 900)
    if "if (!j) return;" not in troca:
        fail("troca aplica o produto mesmo com estorno falhado/cancelado")

    xml = bloco("function entradaNfeXmlMergeConfirmar()", 900)
    if "entradaNfeIntegracoesAplicadasCliente()" not in xml:
        fail("«Confirmar na grade» do XML não bloqueia nota com estoque lançado")

    margem = html[html.find("j.produto_id_atualizado") :][:200]
    if "!entradaNfeIntegracoesAplicadasCliente()" not in margem:
        fail("id do produto pode ser repontado pela margem com estoque lançado")

    limpeza = bloco("function entradaNfeAplicarEstadoLocalAposEstorno", 2200)
    for chave in (
        "aprovacao_wizard_em",
        "financeiro_lancado",
        "estoque_agro_ajuste_ids",
        "estoque_agro_registrado_em",
        "estoque_aplicado_em",
    ):
        if chave not in limpeza:
            fail(f"limpeza local pós-estorno sem '{chave}'")
    ok("caminhos de troca / remoção / quantidade / XML / margem passam pelo estorno")


def prova_js(html: str) -> None:
    """Sintaxe dos scripts renderizados (Node) — pega erro de digitação no JS da tela."""
    blocos = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", html, re.S)
    grandes = [b for b in blocos if len(b) > 400]
    if not grandes:
        fail("nenhum script inline grande encontrado na tela")
    tmp = tempfile.mkdtemp(prefix="verify_nf_js_")
    for i, b in enumerate(grandes):
        if 'type="application/json"' in html[: html.find(b)][-120:]:
            continue
        caminho = os.path.join(tmp, f"bloco_{i}.js")
        with open(caminho, "w", encoding="utf-8") as fh:
            fh.write(b)
        p = subprocess.run(
            ["node", "--check", caminho], capture_output=True, text=True, encoding="utf-8"
        )
        if p.returncode != 0:
            fail(f"erro de sintaxe JS no bloco {i}: {(p.stderr or '')[:400]}")
    ok(f"{len(grandes)} bloco(s) de JS da tela passam no node --check")


def limpar(rid: str) -> None:
    from produtos.models import EntradaNotaRascunhoAgro

    EntradaNotaRascunhoAgro.objects.filter(rascunho_id=rid).delete()
    if EntradaNotaRascunhoAgro.objects.filter(rascunho_id=rid).exists():
        fail("rascunho de prova não foi apagado")
    ok("rascunho de prova removido do banco")


def main() -> None:
    import django

    django.setup()

    prova_unitaria()
    info = prova_banco()
    html = prova_http(info["rid"])
    prova_estrutura_js(html)
    prova_js(html)
    limpar(info["rid"])
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
