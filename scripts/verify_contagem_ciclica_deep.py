#!/usr/bin/env python
"""Prova detalhada — Contagem cíclica (local Postgres)."""
from __future__ import annotations

import os
import sys
import uuid
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import Client
from django.urls import reverse

from estoque.models import (
    AjusteRapidoEstoque,
    ContagemCiclicaEscopo,
    ContagemCiclicaLinha,
    ContagemCiclicaSessao,
    ContagemCiclicaStatus,
    OrigemAjusteEstoque,
)
from produtos.contagem_ciclica_util import (
    abrir_sessao,
    cancelar_sessao,
    entrar_sessao,
    fechar_passagem_1,
    gravar_fechamento,
    registrar_contagem,
    sessao_payload,
)
from produtos.models import Produto

fails: list[str] = []
oks = 0
TAG = f"ciclica-verify-{uuid.uuid4().hex[:8]}"


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def _cleanup_sessoes(*ids: int) -> None:
    for sid in ids:
        if not sid:
            continue
        ContagemCiclicaSessao.objects.filter(pk=sid).delete()


def _mk_produto(*, pid: str, nome: str, cat: str, custo: str = "10.00") -> Produto:
    return Produto.objects.create(
        produto_externo_id=pid[:64],
        codigo_interno=f"GM-{pid[-6:]}",
        codigo_nfe=f"GM-{pid[-6:]}",
        nome=nome[:300],
        categoria=cat,
        custo=Decimal(custo),
        preco_venda=Decimal("20.00"),
        ativo=True,
        cadastro_inativo=False,
    )


# --- 0) URLs resolvem ---
for name in (
    "api_ciclica_sessoes",
    "api_ciclica_categorias",
    "api_ciclica_abrir",
):
    try:
        reverse(name)
        ok(f"url {name}")
    except Exception as e:
        fail(f"url {name}: {e}")

# --- 1) Corredor: linhas sob demanda + multi-operador ---
s1 = None
p_a = p_b = None
try:
    pid_a = f"{TAG}-a"
    pid_b = f"{TAG}-b"
    p_a = _mk_produto(pid=pid_a, nome=f"{TAG} Alfa", cat=f"{TAG}-cat")
    p_b = _mk_produto(pid=pid_b, nome=f"{TAG} Beta", cat=f"{TAG}-cat")

    # Congela referência via linhas (mapa pode ser 0 sem Mongo — OK para prova)
    s1 = abrir_sessao(
        deposito="centro",
        escopo_tipo=ContagemCiclicaEscopo.CORREDOR,
        escopo_valor=f"{TAG}-corredor",
        operador_rotulo="Op1",
    )
    if s1.total_itens != 0:
        fail(f"corredor deveria nascer vazio, total={s1.total_itens}")
    else:
        ok("corredor abre vazio")

    entrar_sessao(s1, "Op2")
    parts = list(s1.participantes.values_list("operador_rotulo", flat=True))
    if "Op1" in parts and "Op2" in parts:
        ok("multi-operador na sessão")
    else:
        fail(f"participantes={parts}")

    # Força saldo_referencia conhecidos ao criar linhas via contar
    ln_a = registrar_contagem(
        s1,
        produto_externo_id=pid_a,
        qtd="5",
        operador_rotulo="Op1",
        nome_produto=p_a.nome,
        codigo_interno=p_a.codigo_nfe,
        categoria=p_a.categoria or "",
    )
    ContagemCiclicaLinha.objects.filter(pk=ln_a.pk).update(saldo_referencia=Decimal("3"))
    ln_a.refresh_from_db()

    ln_b = registrar_contagem(
        s1,
        produto_externo_id=pid_b,
        qtd="3",
        operador_rotulo="Op2",
        nome_produto=p_b.nome,
        codigo_interno=p_b.codigo_nfe,
        categoria=p_b.categoria or "",
    )
    ContagemCiclicaLinha.objects.filter(pk=ln_b.pk).update(saldo_referencia=Decimal("3"))
    ln_b.refresh_from_db()

    if not ln_a.contado_pass1 or not ln_b.contado_pass1:
        fail("pass1 nao marcou contado")
    else:
        ok("dois operadores contaram na pass1")

    # Soma: mesmo produto em 2 lugares (5 + 2 = 7)
    ln_a2 = registrar_contagem(
        s1,
        produto_externo_id=pid_a,
        qtd="2",
        operador_rotulo="Op1",
        nome_produto=p_a.nome,
    )
    if Decimal(str(ln_a2.qtd_pass1)) != Decimal("7"):
        fail(f"soma falhou qtd={ln_a2.qtd_pass1} esperado 7")
    else:
        ok("sempre soma (5+2=7)")

    # Payload cego
    pay = sessao_payload(s1, detalhe=True)
    if pay.get("cego") is not True:
        fail("payload sem cego=True")
    else:
        ok("payload cego")
    for row in pay.get("linhas") or []:
        for ban in ("saldo_referencia", "qtd_pass1", "qtd_pass2", "saldo"):
            if ban in row:
                fail(f"payload vazou {ban}")
                break
    else:
        ok("linhas sem saldo/qtd (cego)")

    # Conflito mesma sessão
    try:
        abrir_sessao(
            deposito="centro",
            escopo_tipo=ContagemCiclicaEscopo.CORREDOR,
            escopo_valor=f"{TAG}-corredor",
            operador_rotulo="OpX",
        )
        fail("deveria bloquear 2ª sessão mesmo escopo")
    except ValueError:
        ok("bloqueia sessão duplicada no escopo")

    resumo = fechar_passagem_1(s1)
    s1.refresh_from_db()
    if s1.status != ContagemCiclicaStatus.PASS2:
        fail(f"status apos pass1={s1.status}")
    else:
        ok("fechou passagem 1 -> pass2")

    # A com diff (5 vs 3) precisa recontagem; B igual (3 vs 3) não
    ln_a.refresh_from_db()
    ln_b.refresh_from_db()
    if not ln_a.precisa_recontagem:
        fail("item divergente deveria ir pra recontagem")
    else:
        ok("divergente na fila")
    if ln_b.precisa_recontagem:
        fail("item igual não deveria ir pra recontagem")
    else:
        ok("igual fora da fila")

    # Estoque ainda não mudou
    if AjusteRapidoEstoque.objects.filter(
        produto_externo_id=pid_a, origem=OrigemAjusteEstoque.CONTAGEM_CICLICA
    ).exists():
        fail("ajustou estoque antes do gravar")
    else:
        ok("estoque intacto até o gravar")

    # Gravar sem recontar → erro
    try:
        gravar_fechamento(s1, operador_rotulo="Op1")
        fail("gravar sem recontagem deveria falhar")
    except ValueError:
        ok("trava gravar com pendência pass2")

    # Pass2: produto fora da fila → erro
    try:
        registrar_contagem(s1, produto_externo_id=pid_b, qtd="9", operador_rotulo="Op1")
        fail("pass2 aceitou item fora da fila")
    except ValueError:
        ok("pass2 só fila")

    registrar_contagem(s1, produto_externo_id=pid_a, qtd="4", operador_rotulo="Op2")
    out = gravar_fechamento(s1, operador_rotulo="Op1")
    s1.refresh_from_db()
    if s1.status != ContagemCiclicaStatus.FECHADA:
        fail(f"após gravar status={s1.status}")
    else:
        ok("sessão fechada")

    aj = (
        AjusteRapidoEstoque.objects.filter(
            produto_externo_id=pid_a, origem=OrigemAjusteEstoque.CONTAGEM_CICLICA
        )
        .order_by("-id")
        .first()
    )
    if aj is None:
        fail("não criou AjusteRapidoEstoque")
    elif Decimal(str(aj.saldo_informado)) != Decimal("4"):
        fail(f"saldo_informado={aj.saldo_informado} esperado 4")
    elif Decimal(str(aj.saldo_erp_referencia)) != Decimal("3"):
        fail(f"saldo_ref ajuste={aj.saldo_erp_referencia}")
    else:
        ok(f"ajuste origem contagem_ciclica (ajustes={out.get('ajustes')})")

    # B sem diff → sem ajuste
    if AjusteRapidoEstoque.objects.filter(
        produto_externo_id=pid_b, origem=OrigemAjusteEstoque.CONTAGEM_CICLICA
    ).exists():
        fail("criou ajuste para item sem diferença")
    else:
        ok("sem ajuste quando qtd=ref")

except Exception as e:
    fail(f"fluxo corredor: {e}")
finally:
    if s1:
        _cleanup_sessoes(s1.pk)
    AjusteRapidoEstoque.objects.filter(
        produto_externo_id__startswith=TAG,
        origem=OrigemAjusteEstoque.CONTAGEM_CICLICA,
    ).delete()

# --- 2) Categoria: auto-zero dos não bipados ---
s2 = None
p_c = p_d = None
try:
    cat = f"{TAG}-cat2"
    pid_c = f"{TAG}-c"
    pid_d = f"{TAG}-d"
    p_c = _mk_produto(pid=pid_c, nome=f"{TAG} Charlie", cat=cat)
    p_d = _mk_produto(pid=pid_d, nome=f"{TAG} Delta", cat=cat)

    s2 = abrir_sessao(
        deposito="centro",
        escopo_tipo=ContagemCiclicaEscopo.CATEGORIA,
        escopo_valor=cat,
        operador_rotulo="OpCat",
        dias_movimentacao=0,
    )
    n = ContagemCiclicaLinha.objects.filter(sessao=s2).count()
    if n < 2:
        fail(f"categoria materializou {n} linhas (esperado ≥2)")
    else:
        ok(f"categoria materializou {n} linhas")

    ContagemCiclicaLinha.objects.filter(sessao=s2, produto_externo_id=pid_c).update(
        saldo_referencia=Decimal("2")
    )
    ContagemCiclicaLinha.objects.filter(sessao=s2, produto_externo_id=pid_d).update(
        saldo_referencia=Decimal("7")
    )

    registrar_contagem(
        s2,
        produto_externo_id=pid_c,
        qtd="2",
        operador_rotulo="OpCat",
        nome_produto=p_c.nome,
    )
    # D não bipado → auto zero na fechar pass1
    fechar_passagem_1(s2)
    ln_d = ContagemCiclicaLinha.objects.get(sessao=s2, produto_externo_id=pid_d)
    if (not ln_d.auto_zero_pass1) or ln_d.qtd_pass1 is None or Decimal(str(ln_d.qtd_pass1)) != Decimal("0"):
        fail(f"auto-zero falhou: auto={ln_d.auto_zero_pass1} qtd={ln_d.qtd_pass1}")
    else:
        ok("nao bipado = auto zero")
    if not ln_d.precisa_recontagem:
        fail("auto-zero vs saldo 7 deveria ir pra recontagem")
    else:
        ok("auto-zero divergente na fila")

    # Cancela em vez de gravar (não poluir estoque com zero)
    cancelar_sessao(s2)
    s2.refresh_from_db()
    if s2.status != ContagemCiclicaStatus.CANCELADA:
        fail("cancelar falhou")
    else:
        ok("cancelar sessao")

except Exception as e:
    fail(f"fluxo categoria: {e}")
finally:
    if s2:
        _cleanup_sessoes(s2.pk)

# --- 2b) Filtro dias_movimentacao ---
s3 = None
try:
    cat3 = f"{TAG}-cat3"
    pid_e = f"{TAG}-e"
    pid_f = f"{TAG}-f"
    _mk_produto(pid=pid_e, nome=f"{TAG} Eco", cat=cat3)
    _mk_produto(pid=pid_f, nome=f"{TAG} Fox", cat=cat3)
    # Só E teve movimento recente
    AjusteRapidoEstoque.objects.create(
        produto_externo_id=pid_e,
        deposito="centro",
        saldo_erp_referencia=0,
        saldo_informado=1,
        origem=OrigemAjusteEstoque.AJUSTE_PIN,
        nome_produto="mov recent",
    )
    s3 = abrir_sessao(
        deposito="centro",
        escopo_tipo=ContagemCiclicaEscopo.CATEGORIA,
        escopo_valor=cat3,
        operador_rotulo="OpDias",
        dias_movimentacao=60,
    )
    pids3 = set(
        ContagemCiclicaLinha.objects.filter(sessao=s3).values_list(
            "produto_externo_id", flat=True
        )
    )
    if pids3 == {pid_e}:
        ok("filtro 60d so quem moveu")
    else:
        fail(f"filtro dias pids={pids3}")
    if int(s3.dias_movimentacao) != 60:
        fail(f"dias_movimentacao={s3.dias_movimentacao}")
    else:
        ok("sessao guarda dias_movimentacao=60")
    cancelar_sessao(s3)

except Exception as e:
    fail(f"fluxo dias: {e}")
finally:
    if s3:
        _cleanup_sessoes(s3.pk)
    AjusteRapidoEstoque.objects.filter(produto_externo_id__startswith=TAG).delete()

# --- 3) HTTP gate + APIs com sessão PIN ---
try:
    from django.conf import settings
    from django.test import override_settings

    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client(HTTP_HOST="127.0.0.1")
        # Sem gate
        r = c.get(reverse("api_ciclica_sessoes"))
        if r.status_code == 403:
            ok("API sem PIN -> 403")
        else:
            fail(f"API sem PIN status={r.status_code}")

        session = c.session
        session["ajuste_mobile_operador"] = "VerifyCiclica"
        session["ajuste_mobile_user_id"] = None
        session.save()

        r2 = c.get(reverse("api_ciclica_sessoes") + "?deposito=centro")
        if r2.status_code == 200 and (r2.json() or {}).get("ok"):
            ok("API sessoes com gate")
        else:
            fail(f"API sessoes gate fail: {r2.status_code} {r2.content[:200]}")

        r3 = c.get(reverse("api_ciclica_categorias"))
        if r3.status_code == 200 and (r3.json() or {}).get("ok"):
            ok("API categorias")
        else:
            fail(f"API categorias: {r3.status_code}")

        # Abrir corredor via HTTP e cancelar
        r4 = c.post(
            reverse("api_ciclica_abrir"),
            {
                "deposito": "centro",
                "escopo_tipo": "corredor",
                "escopo_valor": f"{TAG}-http",
            },
        )
        data4 = r4.json() if r4.status_code == 200 else {}
        sid = (data4.get("sessao") or {}).get("id")
        if data4.get("ok") and sid:
            ok(f"HTTP abrir sessao #{sid}")
            r5 = c.post(reverse("api_ciclica_cancelar", kwargs={"pk": sid}))
            if (r5.json() or {}).get("ok"):
                ok("HTTP cancelar")
            else:
                fail(f"HTTP cancelar: {r5.content[:200]}")
            _cleanup_sessoes(sid)
        else:
            fail(f"HTTP abrir: {r4.status_code} {r4.content[:300]}")

except Exception as e:
    fail(f"HTTP: {e}")

# cleanup produtos fake
Produto.objects.filter(produto_externo_id__startswith=TAG).delete()
AjusteRapidoEstoque.objects.filter(produto_externo_id__startswith=TAG).delete()
ContagemCiclicaSessao.objects.filter(escopo_valor__startswith=TAG).delete()

print(f"checks_ok={oks} fails={len(fails)}")
for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)
print("VERIFY_DEEP_OK")
