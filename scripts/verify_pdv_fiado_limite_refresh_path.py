# -*- coding: utf-8 -*-
"""
Prova detalhada — limite fiado atualiza no PDV sem fechar/abrir
(`PDV-FIADO-LIMITE-REFRESH`).

  set AGRO_PIN_TESTE=9973
  python scripts/verify_pdv_fiado_limite_refresh_path.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_js_contratos() -> None:
    print("== 1) Contratos JS (pdv_wizard) ==")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")

    check("helper_ensure", "function ensureCreditoFiadoFrescos" in js)
    check("helper_msg", "function mensagemFiadoAcimaLimite" in js)
    check("helper_valor", "function valorFiadoParaConsultaCredito" in js)
    check("helper_foco", "function agroFiadoCreditoRefreshNoFoco" in js)
    check("cache_valor_key", "var creditoFiadoValorKey" in js)
    check("bust_cache_http", "q += '&_t=' + encodeURIComponent(String(Date.now()))" in js)
    check(
        "cache_considera_valor",
        "creditoFiadoValorKey === valorKey" in js and "creditoFiadoValorKey = valorKey" in js,
    )
    check(
        "select_fiado_refresh",
        "if (forma === 'Fiado')" in js
        and "ensureCreditoFiadoFrescos(valorFiadoParaConsultaCredito" in js,
    )
    check(
        "commit_fiado_refresh",
        "String(st.pagamento.forma || '') === 'Fiado'" in js
        and "ensureCreditoFiadoFrescos(valorFiadoParaConsultaCredito(st, cur))" in js,
    )
    check(
        "confirm_fiado_refresh",
        "temFiadoConfirm && clientePodeFiado(state0)" in js
        and "ensureCreditoFiadoFrescos(valorFiadoNosLancamentos(state0))" in js,
    )
    check("foco_visibility", "agroFiadoCreditoRefreshNoFoco()" in js)
    check(
        "gravar_limite_refresh",
        "function gravarLimiteFiadoPdv" in js
        and "ensureCreditoFiadoFrescos(valorFiadoNosLancamentos" in js,
    )
    check(
        "aviso_nao_alert_nativo",
        "showPdvAviso(validation0, { tone: 'error', title: 'Atenção' })" in js,
    )
    check(
        "erro_usa_mensagem_helper",
        "var msgLim = mensagemFiadoAcimaLimite()" in js
        and js.count("mensagemFiadoAcimaLimite()") >= 3,
    )
    check(
        "reset_limpa_valor_key",
        "creditoFiadoValorKey = ''" in js,
    )
    # regressão: não pode voltar ao early-return que ignorava mudança de limite
    trecho = js.split("function refreshCreditoFiadoCliente", 1)[-1].split(
        "function valorFiadoNosLancamentos", 1
    )[0]
    check(
        "refresh_exige_force_ou_valor",
        "!opts.force &&" in trecho and "creditoFiadoValorKey === valorKey" in trecho,
    )
    check(
        "refresh_sempre_timestamp",
        trecho.count("&_t=") >= 1 and "opts.force" in trecho,
    )


def test_node() -> None:
    print("== 2) Sintaxe JS ==")
    try:
        r = subprocess.run(
            ["node", "--check", str(ROOT / "produtos/static/produtos/js/pdv_wizard.js")],
            capture_output=True,
            text=True,
            timeout=30,
        )
        check("node_check", r.returncode == 0, (r.stderr or "")[:160])
    except FileNotFoundError:
        check("node_check_skip", True, "node off")


def test_runtime() -> None:
    print("== 3) Runtime Django (API crédito + limite + PIN) ==")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.fiado_credito_util import resumo_credito_fiado_cliente
    from produtos.fiado_gestao_util import definir_limite_fiado_cliente
    from produtos.models import ClienteAgro
    from produtos.pin_gerencial_util import validar_pin_gerencial

    ok_pin, rotulo, err_pin = validar_pin_gerencial(PIN)
    check("pin_9973_gerencial", ok_pin, err_pin or rotulo)
    if not ok_pin:
        print("  (PIN local não gerencial — resto da prova de API usa util direto)")

    cli = (
        ClienteAgro.objects.filter(ativo=True)
        .exclude(nome__icontains="consumidor")
        .order_by("pk")
        .first()
    )
    if not cli:
        cli = ClienteAgro.objects.order_by("pk").first()
    if not cli:
        check("cliente", False, "sem ClienteAgro no banco local")
        return
    check("cliente", True, f"pk={cli.pk} · {(cli.nome or '')[:40]}")

    anterior = Decimal(str(cli.limite_fiado_local or 0))
    User = get_user_model()
    u = User.objects.filter(is_active=True).order_by("pk").first()
    if not u:
        check("user", False, "sem usuario")
        return
    check("user", True, u.get_username())

    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(u)
    url_cred = reverse("api_pdv_cliente_credito_fiado")
    url_lim = reverse("api_pdv_fiado_limite")

    try:
        # 1) Limite baixo artificial → venda 92 deve exceder
        limite_baixo = Decimal("50.00")
        definir_limite_fiado_cliente(cli.pk, limite_baixo, usuario="verify-refresh-setup")
        cli.refresh_from_db()

        cred0 = resumo_credito_fiado_cliente(
            (cli.externo_id or "").strip() or f"agro:{cli.pk}",
            cliente_agro_pk=cli.pk,
            cliente_nome=cli.nome or "",
            valor_nova_venda_fiado=Decimal("92.00"),
        )
        usado = Decimal(str(cred0.get("usado") or 0))
        disponivel0 = (limite_baixo - usado).quantize(Decimal("0.01"))
        check(
            "setup_limite_baixo",
            Decimal(str(cli.limite_fiado_local or 0)) == limite_baixo,
            str(cli.limite_fiado_local),
        )
        check(
            "setup_excede_92",
            bool(cred0.get("excede")) is True,
            f"disp={cred0.get('disponivel')} usado={usado}",
        )

        # 2) API GET crédito (como o PDV faz) — espelha excede
        r_old = c.get(
            url_cred,
            {
                "cliente_agro_pk": cli.pk,
                "cliente_id": (cli.externo_id or "").strip(),
                "cliente_nome": cli.nome or "",
                "valor_fiado": "92,00",
                "_t": "1",
            },
        )
        check("api_credito_200_antes", r_old.status_code == 200, str(r_old.status_code))
        j_old = r_old.json()
        check("api_credito_excede_antes", j_old.get("excede") is True, str(j_old.get("disponivel")))

        # 3) Sobe o limite o bastante para liberar 92 (usado + 200)
        limite_alto = (usado + Decimal("200.00")).quantize(Decimal("0.01"))
        if limite_alto < Decimal("300.00"):
            limite_alto = Decimal("300.00")

        if ok_pin:
            r_lim = c.post(
                url_lim,
                data=json.dumps(
                    {
                        "cliente_agro_pk": cli.pk,
                        "limite": f"{limite_alto:.2f}".replace(".", ","),
                        "pin": PIN,
                    }
                ),
                content_type="application/json",
            )
            check("api_limite_pin_200", r_lim.status_code == 200, str(r_lim.status_code))
            j_lim = r_lim.json() if r_lim.status_code == 200 else {}
            check("api_limite_pin_ok", j_lim.get("ok") is True, str(j_lim.get("erro")))
            check(
                "api_limite_pin_valor",
                abs(float(j_lim.get("limite_fiado_local") or 0) - float(limite_alto)) < 0.001,
                str(j_lim.get("limite_fiado_local")),
            )
            check("api_limite_devolve_credito", isinstance(j_lim.get("credito"), dict))
        else:
            definir_limite_fiado_cliente(cli.pk, limite_alto, usuario="verify-refresh-alto")
            check("api_limite_pin_skip_util", True, "usou util (PIN não gerencial no DB local)")

        cli.refresh_from_db()
        check(
            "pg_limite_novo",
            Decimal(str(cli.limite_fiado_local or 0)) == limite_alto,
            str(cli.limite_fiado_local),
        )

        # 4) Mesma consulta crédito SEM reabrir PDV (= novo GET) → não excede
        r_new = c.get(
            url_cred,
            {
                "cliente_agro_pk": cli.pk,
                "cliente_id": (cli.externo_id or "").strip(),
                "cliente_nome": cli.nome or "",
                "valor_fiado": "92,00",
                "_t": "2",
            },
        )
        check("api_credito_200_depois", r_new.status_code == 200, str(r_new.status_code))
        j_new = r_new.json()
        check(
            "api_credito_nao_excede_depois",
            j_new.get("excede") is False,
            f"limite={j_new.get('limite')} disp={j_new.get('disponivel')}",
        )
        check(
            "api_credito_limite_bate",
            abs(float(j_new.get("limite") or 0) - float(limite_alto)) < 0.02,
            str(j_new.get("limite")),
        )
        disp_new = Decimal(str(j_new.get("disponivel") or 0))
        check(
            "api_disponivel_sobe",
            disp_new > disponivel0,
            f"{disponivel0} → {disp_new}",
        )

        # 5) Util resumo (mesma regra do backend ao confirmar venda)
        cred1 = resumo_credito_fiado_cliente(
            (cli.externo_id or "").strip() or f"agro:{cli.pk}",
            cliente_agro_pk=cli.pk,
            cliente_nome=cli.nome or "",
            valor_nova_venda_fiado=Decimal("92.00"),
        )
        check("resumo_nao_excede_pos_limite", cred1.get("excede") is False, str(cred1.get("disponivel")))

        # 6) Timestamp bust: duas GETs com _t diferente não quebram
        r_t1 = c.get(url_cred, {"cliente_agro_pk": cli.pk, "valor_fiado": "10,00", "_t": "aaa"})
        r_t2 = c.get(url_cred, {"cliente_agro_pk": cli.pk, "valor_fiado": "10,00", "_t": "bbb"})
        check("bust_t_ok", r_t1.status_code == 200 and r_t2.status_code == 200)

        # 7) PIN errado não grava
        if ok_pin:
            r_bad = c.post(
                url_lim,
                data=json.dumps(
                    {
                        "cliente_agro_pk": cli.pk,
                        "limite": "1,00",
                        "pin": "0000",
                    }
                ),
                content_type="application/json",
            )
            check("pin_errado_403", r_bad.status_code == 403, str(r_bad.status_code))
            cli.refresh_from_db()
            check(
                "pin_errado_nao_grava",
                Decimal(str(cli.limite_fiado_local or 0)) == limite_alto,
                str(cli.limite_fiado_local),
            )

    finally:
        definir_limite_fiado_cliente(cli.pk, anterior, usuario="verify-refresh-restore")
        cli.refresh_from_db()
        check(
            "restore_limite",
            Decimal(str(cli.limite_fiado_local or 0)) == anterior,
            str(anterior),
        )


def main() -> int:
    print("PDV-FIADO-LIMITE-REFRESH · prova detalhada")
    print(f"PIN teste: {'*' * max(0, len(PIN) - 1)}{(PIN[-1:] if PIN else '')}")
    test_js_contratos()
    test_node()
    try:
        test_runtime()
    except Exception as exc:
        check("runtime_crash", False, str(exc).split("\n")[0][:180])
    print()
    total = len(oks) + len(fails)
    print(f"Resultado: {len(oks)}/{total} OK · {len(fails)} FAIL")
    if fails:
        print("Falhas:")
        for name in fails:
            print(f"  - {name}")
        print("PREP_FAILS=" + str(len(fails)))
        return 1
    print(f"OK verify_pdv_fiado_limite_refresh_path — {len(oks)}/{len(oks)}")
    print("PREP_FAILS=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
