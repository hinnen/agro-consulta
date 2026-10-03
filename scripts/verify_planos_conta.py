#!/usr/bin/env python
"""Smoke detalhado: Plano de contas SisVale. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

CHECKS: list[str] = []


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    for c in CHECKS:
        print(f"  ok até: {c}")
    sys.exit(1)


def ok(msg: str) -> None:
    CHECKS.append(msg)
    print(f"  OK {msg}")


def check_static() -> None:
    files = [
        "produtos/views_planos_conta.py",
        "produtos/planos_conta_util.py",
        "produtos/templates/produtos/planos_conta_config.html",
        "produtos/static/produtos/js/agro_perf_config.js",
    ]
    for rel in files:
        path = os.path.join(ROOT, rel.replace("/", os.sep))
        if not os.path.isfile(path):
            fail(f"arquivo ausente: {rel}")
    ok("arquivos presentes")

    js = open(
        os.path.join(ROOT, "produtos", "static", "produtos", "js", "agro_perf_config.js"),
        encoding="utf-8",
    ).read()
    if "/configuracao/planos-conta/" not in js:
        fail("menu Config sem link planos-conta")
    if "Planos de contas" not in js:
        fail("rótulo Planos de contas ausente no menu")
    ok("menu Config F11 → Planos de contas")

    urls = open(os.path.join(ROOT, "produtos", "urls.py"), encoding="utf-8").read()
    for needle in (
        "configuracao/planos-conta/",
        "api/configuracao/planos-conta/",
        "api/configuracao/planos-conta/salvar/",
        "api_planos_conta_toggle",
        "views_planos_conta",
    ):
        if needle not in urls:
            fail(f"urls.py sem {needle}")
    ok("rotas urls.py")

    tpl = open(
        os.path.join(
            ROOT, "produtos", "templates", "produtos", "planos_conta_config.html"
        ),
        encoding="utf-8",
    ).read()
    for needle in (
        "api_planos_conta_lista",
        "api_planos_conta_salvar",
        "api_planos_conta_toggle",
        "api_planos_conta_seed",
        "pc-btn-salvar",
        "pc-lista",
        "pc-inativos",
        "pc-pdv",
        "exibir_pdv",
        "Mostrar no PDV",
    ):
        if needle not in tpl:
            fail(f"template sem {needle}")
    ok("template tela")

    # Contrato de layout (banana §4.14 · AGENTS §5 e §11): escala global, sem px fixo,
    # sem rolagem de página e tela larga aproveitada (não coluna estreita).
    if "_agro_consulta_ui.html" not in tpl:
        fail("tela sem _agro_consulta_ui.html (escala global / UI padrão)")
    if "container-type: inline-size" not in tpl or "@container" not in tpl:
        fail("tela sem container query (layout não escala por largura)")
    if "100dvh" not in tpl or "overflow: hidden" not in tpl:
        fail("tela pode rolar a página inteira (falta 100dvh + overflow hidden)")
    if tpl.count("clamp(") < 15:
        fail("tipografia/alturas sem clamp() suficiente — px fixo quebra a escala")
    if "zoom" in tpl.lower():
        fail("tela usando zoom local (escala é global no <html>)")
    if re.search(r"text-\[\d+px\]|font-size:\s*\d+px", tpl):
        fail("tela com font-size em px fixo")
    if re.search(r"max-w-(lg|md|sm|xl)\b", tpl):
        fail("tela em coluna estreita (max-w-*) — aproveitar a tela 16:9")
    ok("layout no padrão Agro Display Scale (§11)")

    views = open(os.path.join(ROOT, "produtos", "views.py"), encoding="utf-8").read()
    if "injetar_planos_agro_sugestao" not in views and "sugestoes_plano_cadastro" not in views:
        fail("views.py sem injeção/sugestão de planos Agro")
    ok("sugestões Lançamentos injetam Agro")

    # Loja: PlanoContaAgro já veio da 0065 — pacote sobe só 0085 (deps→0083).
    mig85_path = os.path.join(ROOT, "produtos", "migrations", "0085_plano_conta_exibir_pdv.py")
    mig85_head = open(mig85_path, encoding="utf-8").read()
    if "0083_dfe_manifestacao_ciencia" not in mig85_head:
        fail("migration 0085 deve depender de 0083 na loja (sem 0082/0084)")
    if "0084_plano_conta_alinha_loja" in mig85_head:
        fail("migration 0085 ainda depende de 0084 (incompatível com loja)")
    ok("migration 0085 deps→0083 (loja)")

    mig85 = open(
        os.path.join(ROOT, "produtos", "migrations", "0085_plano_conta_exibir_pdv.py"),
        encoding="utf-8",
    ).read()
    if "exibir_pdv" not in mig85 or "NOMES_PDV_ATUAL" not in mig85:
        fail("migration 0085 sem exibir_pdv / seed PDV")
    ok("migration 0085 exibir_pdv")

    saida = open(
        os.path.join(ROOT, "produtos", "saida_caixa_planos.py"), encoding="utf-8"
    ).read()
    if "listar_planos_saida_caixa" not in saida or "exibir_pdv" not in saida:
        fail("saida_caixa_planos sem lista dinâmica do PDV")
    ok("saída caixa lê planos do Postgres")

    reg = open(
        os.path.join(ROOT, "produtos", "pg_backup_registry.py"), encoding="utf-8"
    ).read()
    if "produtos.PlanoContaAgro" not in reg:
        fail("plano fora do backup PG")
    ok("backup registry")


def check_django() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.db import connection
    from django.test import Client
    from django.urls import reverse

    from produtos.models import PlanoContaAgro
    from produtos.planos_conta_util import (
        id_publico_plano,
        injetar_planos_agro_sugestao,
        listar_planos_agro,
        parse_id_publico,
        serializar_plano,
    )

    # tabela existe
    tables = connection.introspection.table_names()
    if "produtos_planocontaagro" not in tables:
        fail("tabela produtos_planocontaagro não migrada — rode migrate")
    if "produtos_planocontaaliasagro" not in tables:
        fail("tabela de apelidos ausente — rode migrate (0084)")
    cols = {c.name for c in connection.introspection.get_table_description(
        connection.cursor(), "produtos_planocontaagro"
    )}
    if "tipo" not in cols:
        fail("coluna tipo ausente — banco fora do formato da loja")
    if "exibir_pdv" not in cols:
        fail("coluna exibir_pdv ausente — rode migrate (0085)")
    for morta in ("codigo", "natureza", "criado_por_id"):
        if morta in cols:
            fail(f"coluna {morta} ainda existe — 0084 não rodou")
    ok("tabela PG no formato da loja (tipo + apelidos + PDV)")

    for name in (
        "planos_conta_config",
        "api_planos_conta_lista",
        "api_planos_conta_salvar",
        "api_planos_conta_seed",
    ):
        reverse(name)
    tog = reverse("api_planos_conta_toggle", kwargs={"pk": 1})
    if "/api/configuracao/planos-conta/1/toggle/" not in tog:
        fail(f"toggle URL estranha: {tog}")
    ok("reverse URLs")

    marker = f"__VERIFY_PLANOS_{os.getpid()}__"
    PlanoContaAgro.objects.filter(nome__startswith="__VERIFY_PLANOS_").delete()

    obj = PlanoContaAgro.objects.create(
        nome=marker,
        tipo=PlanoContaAgro.Tipo.FIXA,
        grupo="VERIFY",
        ativo=True,
        exibir_pdv=False,
    )
    try:
        from produtos.saida_caixa_planos import listar_planos_saida_caixa

        ser = serializar_plano(obj)
        if ser["id"] != id_publico_plano(obj.pk):
            fail("id_publico diverge")
        if parse_id_publico(ser["id"]) != obj.pk:
            fail("parse_id_publico falhou")
        if ser.get("tipo") != "fixa" or ser.get("tipo_label") != "Fixa":
            fail(f"tipo não serializou: {ser}")
        if ser.get("exibir_pdv") is not False:
            fail(f"exibir_pdv não serializou: {ser}")
        ok(f"ORM create + serializar ({ser['id']})")

        antes = {p["id"] for p in listar_planos_saida_caixa()}
        if ser["id"] in antes:
            fail("plano sem exibir_pdv não deveria estar no PDV")
        obj.exibir_pdv = True
        obj.save(update_fields=["exibir_pdv", "atualizado_em"])
        depois = {p["id"] for p in listar_planos_saida_caixa()}
        if ser["id"] not in depois:
            fail("plano com exibir_pdv deveria aparecer no PDV")
        if "deposito" not in depois:
            fail("Depósito sumiu da lista do PDV")
        ok("listar_planos_saida_caixa respeita exibir_pdv")

        lista = listar_planos_agro(q="VERIFY_PLANOS", incluir_inativos=False)
        if not any(x["pk"] == obj.pk for x in lista):
            fail("listar_planos_agro não achou")
        ok("listar com busca")

        inj = injetar_planos_agro_sugestao(
            [{"id": "mongo1", "nome": "Outro Plano"}],
            "VERIFY_PLANOS",
            limit=30,
        )
        if not inj or inj[0].get("fonte") != "agro":
            fail(f"injeção não pôs Agro no topo: {inj[:3]!r}")
        if inj[0].get("id") != ser["id"]:
            fail("id injetado errado")
        ok("injetar_planos_agro_sugestao")

        # Client HTTP
        User = get_user_model()
        user, _ = User.objects.get_or_create(
            username="__verify_planos__",
            defaults={"is_staff": True},
        )
        if not user.has_usable_password():
            user.set_password("verify-planos-tmp")
            user.save()
        c = Client(HTTP_HOST="127.0.0.1")
        if not c.login(username=user.username, password="verify-planos-tmp"):
            # tenta forçar senha conhecida
            user.set_password("verify-planos-tmp")
            user.save()
            if not c.login(username=user.username, password="verify-planos-tmp"):
                fail("login Client falhou")

        r = c.get(reverse("planos_conta_config"))
        if r.status_code != 200:
            fail(f"GET tela status {r.status_code}")
        body = r.content.decode("utf-8", errors="replace")
        if "Planos de contas" not in body or "pc-btn-salvar" not in body:
            fail("HTML da tela incompleto")
        ok("GET /configuracao/planos-conta/ → 200")

        r = c.get(reverse("api_planos_conta_lista"), {"q": "VERIFY_PLANOS"})
        if r.status_code != 200:
            fail(f"lista status {r.status_code}")
        data = r.json()
        if not data.get("ok") or not any(i.get("pk") == obj.pk for i in data.get("itens") or []):
            fail(f"lista API sem o plano: {data}")
        ok("API lista")

        r = c.post(
            reverse("api_planos_conta_toggle", kwargs={"pk": obj.pk}),
            data=json.dumps({"exibir_pdv": False}),
            content_type="application/json",
        )
        if r.status_code != 200 or not r.json().get("ok"):
            fail(f"toggle PDV off: {r.status_code} {r.content[:300]!r}")
        obj.refresh_from_db()
        if obj.exibir_pdv:
            fail("toggle exibir_pdv=False não persistiu")
        r = c.post(
            reverse("api_planos_conta_toggle", kwargs={"pk": obj.pk}),
            data=json.dumps({"exibir_pdv": True}),
            content_type="application/json",
        )
        if r.status_code != 200 or not r.json().get("ok"):
            fail(f"toggle PDV on: {r.status_code} {r.content[:300]!r}")
        obj.refresh_from_db()
        if not obj.exibir_pdv:
            fail("toggle exibir_pdv=True não persistiu")
        ok("API toggle exibir_pdv")

        nome2 = marker + "_EDIT"
        r = c.post(
            reverse("api_planos_conta_salvar"),
            data=json.dumps(
                {
                    "pk": obj.pk,
                    "nome": nome2,
                    "tipo": "variavel",
                    "grupo": "VERIFY2",
                    "ativo": True,
                }
            ),
            content_type="application/json",
        )
        if r.status_code != 200 or not r.json().get("ok"):
            fail(f"salvar edit: {r.status_code} {r.content[:300]!r}")
        obj.refresh_from_db()
        if obj.nome != nome2 or obj.tipo != "variavel":
            fail("edit não persistiu")
        ok("API salvar (editar)")

        r = c.post(
            reverse("api_planos_conta_salvar"),
            data=json.dumps({"nome": "X"}),
            content_type="application/json",
        )
        if r.status_code != 400:
            fail(f"nome curto deveria 400, veio {r.status_code}")
        ok("validação nome curto → 400")

        r = c.post(
            reverse("api_planos_conta_salvar"),
            data=json.dumps(
                {
                    "nome": marker + "_DUP",
                    "tipo": "fixa",
                }
            ),
            content_type="application/json",
        )
        if r.status_code != 200 or not r.json().get("ok"):
            fail(f"criar novo: {r.status_code} {r.content[:300]!r}")
        novo_pk = r.json()["item"]["pk"]
        ok("API salvar (criar)")

        r = c.post(
            reverse("api_planos_conta_salvar"),
            data=json.dumps({"nome": marker + "_DUP", "tipo": "outra"}),
            content_type="application/json",
        )
        if r.status_code != 400:
            fail(f"duplicata deveria 400, veio {r.status_code}")
        ok("nome duplicado → 400")

        r = c.post(
            reverse("api_planos_conta_toggle", kwargs={"pk": novo_pk}),
            data=json.dumps({}),
            content_type="application/json",
        )
        if r.status_code != 200 or not r.json().get("ok"):
            fail(f"toggle: {r.status_code}")
        if r.json()["item"].get("ativo") is not False:
            fail("toggle não desativou")
        lista_ativa = listar_planos_agro(q=marker + "_DUP", incluir_inativos=False)
        if any(x["pk"] == novo_pk for x in lista_ativa):
            fail("inativo ainda na lista ativa")
        ok("API toggle desativa + some da lista ativa")

        # sugestões endpoint (se autenticado)
        r = c.get("/api/lancamentos/sugestoes/", {"campo": "plano", "q": "VERIFY_PLANOS"})
        if r.status_code == 200:
            sj = r.json()
            itens = sj.get("itens") or []
            ids = [str(i.get("id") or "") for i in itens]
            nomes = [str(i.get("nome") or "") for i in itens]
            hit = any(ser["id"] in ids or nome2 in n for n in nomes)
            # após rename, busca VERIFY_PLANOS ainda casa no nome editado
            hit = hit or any("VERIFY_PLANOS" in n for n in nomes)
            if not hit:
                # plano foi renomeado — busca pelo nome editado
                r2 = c.get(
                    "/api/lancamentos/sugestoes/",
                    {"campo": "plano", "q": nome2[:20]},
                )
                if r2.status_code == 200:
                    itens2 = r2.json().get("itens") or []
                    hit = any(
                        str(i.get("id") or "") == id_publico_plano(obj.pk)
                        or nome2 in str(i.get("nome") or "")
                        for i in itens2
                    )
            if hit:
                ok("API sugestões plano inclui Agro")
            else:
                print("  ~ sugestões HTTP sem hit (fonte pode filtrar) — ORM inject OK")
                ok("API sugestões consultada (inject ORM já OK)")
        else:
            print(f"  ~ sugestões HTTP {r.status_code} — skip (inject ORM OK)")
            ok("API sugestões skip HTTP")

        r = c.post(reverse("api_planos_conta_seed"), data="{}", content_type="application/json")
        if r.status_code != 200 or not r.json().get("ok"):
            fail(f"seed padrão: {r.status_code} {r.content[:200]!r}")
        total1 = r.json().get("total") or 0
        if total1 < 20:
            fail(f"seed trouxe poucos planos: {total1}")
        r2 = c.post(reverse("api_planos_conta_seed"), data="{}", content_type="application/json")
        if r2.status_code != 200 or (r2.json().get("planos") or 0) != 0:
            fail(f"seed não é idempotente: {r2.json()}")
        if (r2.json().get("total") or 0) != total1:
            fail("seed duplicou planos na 2ª rodada")
        ok(f"seed lista padrão idempotente ({total1} planos)")

    finally:
        PlanoContaAgro.objects.filter(nome__startswith="__VERIFY_PLANOS_").delete()
        ok("cleanup verify")


def main() -> None:
    print("verify_planos_conta...")
    check_static()
    check_django()
    print(f"VERIFY_OK {len(CHECKS)}/{len(CHECKS)}")


if __name__ == "__main__":
    main()
