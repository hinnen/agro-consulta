#!/usr/bin/env python
"""Path detalhado — Cíclica Bip +1 (não grava estoque; soma 1; pisca tela)."""
from __future__ import annotations

import os
import sys
import uuid
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def check_text(text: str, *needles: str, label: str = "") -> None:
    for n in needles:
        if n not in text:
            fail(f"{label} missing {n!r}")
        else:
            ok(f"{label} {n!r}")


ma_path = ROOT / "produtos" / "templates" / "produtos" / "mobile_ajuste.html"
js = ma_path.read_text(encoding="utf-8")

# --- JS contrato ---
check_text(
    js,
    "maCiclicaBipMaisUm",
    "maCiclicaBipFilaDrain",
    "maCiclicaBipFila",
    "MA_CICLICA_BIP_GAP_MS",
    "maBipPiscarTela",
    "ma-bip-tela",
    "is-ok",
    "is-err",
    "qtd: '1'",
    "maBip1AntesCiclica",
    "maBip1On = true",
    "estoque só no Gravar",
    "maEscolherProduto",
    "maTalvezOferecerCodigo",
    "ma-bip-tela-n",
    "is-qtd",
    "qtd_contada",
    "código de barras",
    label="js",
)

# Bip+1 cíclica NÃO chama ajuste de estoque
fn_start = js.find("function maCiclicaBipFilaDrain")
fn_end = js.find("function maCiclicaAtivar", fn_start)
chunk = js[fn_start:fn_end] if fn_start >= 0 and fn_end > fn_start else ""
if not chunk:
    fail("não achou maCiclicaBipFilaDrain")
elif "maPostAjuste" in chunk:
    fail("Drain cíclica chama maPostAjuste (gravaria estoque)")
else:
    ok("Drain cíclica sem maPostAjuste")
if "api/ajustar" in chunk or "maPostAjuste" in chunk:
    fail("Drain cíclica toca API de ajuste")
if "contar" not in chunk:
    fail("Drain cíclica não posta em contar")
else:
    ok("Drain posta em contar")

# Salvar bip roteia pra cíclica ANTES do busy de estoque
salvar = js[js.find("function maSalvarBipMaisUm") : js.find("function maMostrarUltimoBipado")]
if "if (maCiclicaAtiva()) return maCiclicaBipMaisUm(p);" not in salvar:
    fail("maSalvarBipMaisUm não roteia pra cíclica")
else:
    ok("salvar roteia cíclica antes do estoque")
if salvar.find("maCiclicaBipMaisUm") > salvar.find("maPostAjuste") and "maPostAjuste" in salvar:
    # roteio precisa aparecer antes de maPostAjuste
    if salvar.find("maCiclicaAtiva()") < salvar.find("maPostAjuste"):
        ok("roteio cíclica antes de maPostAjuste")
    else:
        fail("roteio cíclica depois de maPostAjuste")

# Debounce cíclica 350 (não 1200) pra repetir o mesmo EAN
if "MA_CICLICA_BIP_GAP_MS = 350" not in js:
    fail("gap cíclica não é 350")
else:
    ok("gap cíclica 350 ms")
if "(maCiclicaAtiva() && maBip1On) ? MA_CICLICA_BIP_GAP_MS : 1200" not in js:
    fail("debounce não separa cíclica vs ajuste")
else:
    ok("debounce cíclica separado do ajuste")

# Liga sozinho / restaura ao sair
ativar = js[js.find("function maCiclicaAtivar") : js.find("function maCiclicaSairModo")]
sair = js[js.find("function maCiclicaSairModo") : js.find("function maCiclicaCancelarSessao")]
if "maBip1On = true" not in ativar:
    fail("ativar não liga Bip+1")
else:
    ok("ativar liga Bip+1")
if "maBip1On = !!maBip1AntesCiclica" not in sair:
    fail("sair não restaura Bip+1")
else:
    ok("sair restaura Bip+1")
if "maCiclicaBipFila = []" not in sair:
    fail("sair não limpa fila de bip")
else:
    ok("sair limpa fila")

# Pisca tela: verde ok / vermelho erro; pointer-events none
if "pointer-events: none" not in js or ".ma-bip-tela" not in js:
    fail("overlay pisca sem pointer-events none")
else:
    ok("pisca não bloqueia o próximo bip")
if "maBipPiscarTela(true," not in chunk and "maBipPiscarTela(true)" not in chunk:
    fail("sucesso sem pisca verde")
else:
    ok("sucesso pisca verde")
if "maBipPiscarTela(false)" not in js:
    fail("erro sem pisca vermelho")
else:
    ok("erro pisca vermelho")

# Sem o bloqueio antigo
if "Bip +1 fica desligado" in js or "Bip +1 off" in js:
    fail("ainda força Bip+1 off na cíclica")
else:
    ok("não força Bip+1 off")

# --- Runtime HTTP (mesmo contrato do Bip+1: qtd=1 repetido) ---
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django

django.setup()

from django.test import Client, override_settings
from django.urls import reverse

from estoque.models import (
    AjusteRapidoEstoque,
    ContagemCiclicaLinha,
    ContagemCiclicaSessao,
    OrigemAjusteEstoque,
)
from produtos.models import Produto

TAG = f"bip1path-{uuid.uuid4().hex[:8]}"


def _mk(pid: str, nome: str, cat: str, barras: str = "") -> Produto:
    return Produto.objects.create(
        produto_externo_id=pid[:64],
        codigo_interno=f"GM-{pid[-6:]}",
        codigo_nfe=f"GM-{pid[-6:]}",
        codigo_barras=barras or None,
        nome=nome[:300],
        categoria=cat,
        custo=Decimal("1"),
        preco_venda=Decimal("2"),
        ativo=True,
        cadastro_inativo=False,
    )


def _contar(c, sid, pid, nome, codigo, qtd="1", forcar=False):
    payload = {
        "produto_id": pid,
        "qtd": qtd,
        "nome_produto": nome,
        "codigo_interno": codigo,
    }
    if forcar:
        payload["forcar"] = "1"
    return c.post(reverse("api_ciclica_contar", kwargs={"pk": sid}), payload)


try:
    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client(HTTP_HOST="127.0.0.1")
        session = c.session
        session["ajuste_mobile_operador"] = "VerifyBip1Ciclica"
        session["ajuste_mobile_user_id"] = None
        session.save()

        pid_a = f"{TAG}-a"
        pid_b = f"{TAG}-b"
        pid_fora = f"{TAG}-x"
        p_a = _mk(pid_a, f"{TAG} Alfa", f"{TAG}-cat", barras="7898416703263")
        p_b = _mk(pid_b, f"{TAG} Beta", f"{TAG}-cat", barras="7890000000001")
        p_x = _mk(pid_fora, f"{TAG} Fora", "OUTRA", barras="7890000000002")

        r_open = c.post(
            reverse("api_ciclica_abrir"),
            {
                "deposito": "centro",
                "escopo_tipo": "corredor",
                "escopo_valor": f"{TAG}-corredor",
            },
        )
        d_open = r_open.json() if r_open.status_code == 200 else {}
        sid = (d_open.get("sessao") or {}).get("id")
        if d_open.get("ok") and sid:
            ok(f"HTTP corredor abre vazio #{sid}")
        else:
            fail(f"HTTP abrir {r_open.status_code} {r_open.content[:200]}")
            sid = None

        if sid:
            # 3 bips +1 do mesmo produto (corredor, sem forcar)
            acum = None
            for i in range(3):
                r = _contar(c, sid, pid_a, p_a.nome, p_a.codigo_nfe)
                d = r.json() if r.status_code == 200 else {}
                acum = d.get("qtd_acumulada")
                if not d.get("ok"):
                    fail(f"bip {i+1} falhou {r.status_code} {r.content[:180]}")
                    break
            if acum is not None and abs(float(acum) - 3) < 0.001:
                ok("3x qtd=1 = acumulado 3")
            else:
                fail(f"acumulado após 3 bips={acum}")

            # segundo ponto do corredor: outro produto +1
            r_b = _contar(c, sid, pid_b, p_b.nome, p_b.codigo_nfe)
            d_b = r_b.json() if r_b.status_code == 200 else {}
            if d_b.get("ok") and abs(float(d_b.get("qtd_acumulada") or 0) - 1) < 0.001:
                ok("outro ponto do corredor +1")
            else:
                fail(f"produto B {r_b.status_code} {r_b.content[:180]}")

            # estoque intacto
            if AjusteRapidoEstoque.objects.filter(
                produto_externo_id__in=[pid_a, pid_b],
                origem=OrigemAjusteEstoque.CONTAGEM_CICLICA,
            ).exists():
                fail("estoque mudou antes do Gravar")
            else:
                ok("estoque intacto após bips")

            # detalhe cego
            r_det = c.get(reverse("api_ciclica_detalhe", kwargs={"pk": sid}))
            sess = ((r_det.json() or {}).get("sessao") or {})
            if sess.get("cego") is True:
                bad = [
                    x
                    for x in (sess.get("linhas") or [])
                    if "saldo" in x or "qtd" in x or "quantidade" in x
                ]
                if not bad:
                    ok("detalhe cego após bips")
                else:
                    fail("detalhe vazou qtd/saldo")
            else:
                fail("detalhe sem cego")

            # categoria: fora da lista sem forcar = 400 (vermelho)
            r_cat = c.post(
                reverse("api_ciclica_abrir"),
                {
                    "deposito": "centro",
                    "escopo_tipo": "categoria",
                    "escopo_valor": f"{TAG}-cat",
                    "dias_movimentacao": "0",
                },
            )
            d_cat = r_cat.json() if r_cat.status_code == 200 else {}
            sid_cat = (d_cat.get("sessao") or {}).get("id")
            if d_cat.get("ok") and sid_cat:
                ok(f"HTTP categoria #{sid_cat}")
                r_out = _contar(c, sid_cat, pid_fora, p_x.nome, p_x.codigo_nfe)
                if r_out.status_code == 400 and not (r_out.json() or {}).get("ok"):
                    ok("fora da lista sem forcar = 400")
                else:
                    fail(f"deveria 400 fora: {r_out.status_code}")
                # com forcar entra
                r_in = _contar(c, sid_cat, pid_fora, p_x.nome, p_x.codigo_nfe, forcar=True)
                d_in = r_in.json() if r_in.status_code == 200 else {}
                if d_in.get("ok") and abs(float(d_in.get("qtd_acumulada") or 0) - 1) < 0.001:
                    ok("Incluir fora +1")
                else:
                    fail(f"forcar categoria {r_in.status_code}")
                c.post(reverse("api_ciclica_cancelar", kwargs={"pk": sid_cat}))
                ContagemCiclicaSessao.objects.filter(pk=sid_cat).delete()
            else:
                fail(f"HTTP categoria abrir {r_cat.status_code}")

            # fechar pass1 + recontar só divergente + gravar
            ln_a = ContagemCiclicaLinha.objects.filter(
                sessao_id=sid, produto_externo_id=pid_a
            ).first()
            ln_b = ContagemCiclicaLinha.objects.filter(
                sessao_id=sid, produto_externo_id=pid_b
            ).first()
            if ln_a:
                ContagemCiclicaLinha.objects.filter(pk=ln_a.pk).update(
                    saldo_referencia=Decimal("0")
                )
            if ln_b:
                ContagemCiclicaLinha.objects.filter(pk=ln_b.pk).update(
                    saldo_referencia=Decimal("1")
                )
            r_p1 = c.post(reverse("api_ciclica_fechar_pass1", kwargs={"pk": sid}))
            if (r_p1.json() or {}).get("ok"):
                ok("fechou passagem 1")
            else:
                fail(f"fechar pass1 {r_p1.status_code} {r_p1.content[:180]}")

            # B bateu com ref 1 → fora da fila; A (3 vs 0) recontar
            r_pass2_b = _contar(c, sid, pid_b, p_b.nome, p_b.codigo_nfe)
            if r_pass2_b.status_code == 400:
                ok("pass2 recusa item igual (sem forcar)")
            else:
                fail(f"pass2 deveria recusar B: {r_pass2_b.status_code}")

            r_pass2_a = _contar(c, sid, pid_a, p_a.nome, p_a.codigo_nfe, qtd="3")
            d2 = r_pass2_a.json() if r_pass2_a.status_code == 200 else {}
            if d2.get("ok"):
                ok("recontagem A")
            else:
                fail(f"recontagem A {r_pass2_a.status_code} {r_pass2_a.content[:180]}")

            n_aj_antes = AjusteRapidoEstoque.objects.filter(
                produto_externo_id=pid_a, origem=OrigemAjusteEstoque.CONTAGEM_CICLICA
            ).count()
            r_g = c.post(reverse("api_ciclica_gravar", kwargs={"pk": sid}))
            d_g = r_g.json() if r_g.status_code == 200 else {}
            if d_g.get("ok"):
                ok("gravar")
            else:
                fail(f"gravar {r_g.status_code} {r_g.content[:220]}")
            n_aj = AjusteRapidoEstoque.objects.filter(
                produto_externo_id=pid_a, origem=OrigemAjusteEstoque.CONTAGEM_CICLICA
            ).count()
            if n_aj > n_aj_antes:
                ok("ajuste só no Gravar")
            else:
                fail("Gravar não criou ajuste")

            aj = (
                AjusteRapidoEstoque.objects.filter(
                    produto_externo_id=pid_a,
                    origem=OrigemAjusteEstoque.CONTAGEM_CICLICA,
                )
                .order_by("-id")
                .first()
            )
            if aj is not None and Decimal(str(aj.saldo_informado)) == Decimal("3"):
                ok("saldo_informado=3 (recontagem)")
            else:
                fail(f"saldo_informado={getattr(aj, 'saldo_informado', None)}")

            c.post(reverse("api_ciclica_cancelar", kwargs={"pk": sid}))
            ContagemCiclicaSessao.objects.filter(pk=sid).delete()

        # página com PIN traz o JS do Bip+1 cíclica
        session = c.session
        session["ajuste_mobile_gate"] = True
        session["ajuste_mobile_operador"] = "VerifyBip1Ciclica"
        session.save()
        r_page = c.get("/ajuste-mobile/")
        html = r_page.content.decode("utf-8", "replace")
        for n in ("maCiclicaBipMaisUm", "maBipPiscarTela", "MA_CICLICA_BIP_GAP_MS", "ma-bip-tela"):
            if n in html:
                ok(f"HTML PIN {n}")
            else:
                fail(f"HTML PIN sem {n}")

except Exception as exc:
    fail(f"runtime: {exc}")
finally:
    ContagemCiclicaSessao.objects.filter(escopo_valor__startswith=TAG).delete()
    AjusteRapidoEstoque.objects.filter(
        produto_externo_id__startswith=TAG,
        origem=OrigemAjusteEstoque.CONTAGEM_CICLICA,
    ).delete()
    Produto.objects.filter(produto_externo_id__startswith=TAG).delete()

print(f"checks_ok={oks} fails={len(fails)}")
for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)
print("VERIFY_BIP1_CICLICA_OK")
