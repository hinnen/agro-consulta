#!/usr/bin/env python
"""Prova detalhada PDV-CLI-CADASTRO — ORM + APIs (clientes de teste, limpa no fim)."""
from __future__ import annotations

import json
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

from django.contrib.auth import get_user_model
from django.test import Client
from django.urls import reverse
from django.utils import timezone

from base.models import PerfilUsuario
from produtos.cliente_operacoes_util import (
    aplicar_vale_pago_apos_venda,
    creditar_vale_manual,
    excluir_cliente,
    limpar_whatsapp_duplicado,
    listar_eventos_cliente,
    payload_e_compra_vale_credito,
    preview_exclusao,
    transferir_saldos,
)
from produtos.cliente_whatsapp_util import info_whatsapp_duplicado
from produtos.models import (
    ClienteAgro,
    ClienteAgroEventoAgro,
    FiadoTituloAgro,
)

User = get_user_model()
fails: list[str] = []
oks = 0
TAG = f"ZZ-VERIFY-PDVCLI-{uuid.uuid4().hex[:8]}"
PIN = "881177"
COD_VEND = "Z9V1"
PHONE = "11900001177"


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def _cli(nome: str, **kw) -> ClienteAgro:
    defaults = {
        "nome": f"{TAG} {nome}",
        "whatsapp": "",
        "ativo": True,
        "editado_local": True,
    }
    defaults.update(kw)
    return ClienteAgro.objects.create(**defaults)


def _limpar() -> None:
    qs = ClienteAgro.objects.filter(nome__startswith=TAG)
    pks = list(qs.values_list("pk", flat=True))
    FiadoTituloAgro.objects.filter(chave_unica__startswith=f"verify:{TAG}").delete()
    ClienteAgroEventoAgro.objects.filter(cliente_nome_snap__startswith=TAG).delete()
    ClienteAgroEventoAgro.objects.filter(destino_nome_snap__startswith=TAG).delete()
    ClienteAgroEventoAgro.objects.filter(cliente_pk_snap__in=pks).delete()
    qs.delete()
    User.objects.filter(username="pdvcli_verify_bot").delete()


def main() -> int:
    try:
        _limpar()
        user, _ = User.objects.get_or_create(
            username="pdvcli_verify_bot",
            defaults={"is_staff": True, "first_name": "VerifyBot"},
        )
        user.first_name = "VerifyBot"
        user.set_password("verify-bot-x")
        user.save()
        perfil, _ = PerfilUsuario.objects.get_or_create(
            user=user,
            defaults={"codigo_vendedor": COD_VEND, "senha_rapida": PIN},
        )
        if perfil.senha_rapida != PIN or perfil.codigo_vendedor != COD_VEND:
            perfil.senha_rapida = PIN
            perfil.codigo_vendedor = COD_VEND
            perfil.save()

        a = _cli("ORIGEM", whatsapp=PHONE, saldo_cashback=Decimal("12.50"), saldo_vale_credito=Decimal("3.00"))
        b = _cli("DESTINO", whatsapp=PHONE, saldo_cashback=Decimal("1.00"), saldo_vale_credito=Decimal("0"))
        c_limpo = _cli("LIMPAR", whatsapp="11900002288")

        dup = info_whatsapp_duplicado(PHONE, excluir_pk=a.pk)
        if dup and dup.get("pk") == b.pk:
            ok("duplicata encontra o outro")
        else:
            fail(f"duplicata {dup}")

        pin_bad = limpar_whatsapp_duplicado(alvo_pk=c_limpo.pk, pin="1234")
        ok("pin 1234 bloqueado") if not pin_bad.get("ok") else fail("pin 1234 passou")
        pin_wrong = limpar_whatsapp_duplicado(alvo_pk=c_limpo.pk, pin="000000")
        ok("pin errado recusa") if not pin_wrong.get("ok") else fail("pin errado passou")

        limpo = limpar_whatsapp_duplicado(alvo_pk=c_limpo.pk, pin=PIN, origem_tela="pdv")
        c_limpo.refresh_from_db()
        if limpo.get("ok") and not c_limpo.whatsapp:
            ok("limpar telefone")
        else:
            fail(f"limpar {limpo} whatsapp={c_limpo.whatsapp}")
        evs = listar_eventos_cliente(c_limpo.pk)
        ok("evento limpar") if any(e["tipo"] == "limpar_whatsapp" for e in evs) else fail("sem evento limpar")

        vm = creditar_vale_manual(pk=b.pk, valor="10,00", motivo="prova deep", pin=PIN)
        b.refresh_from_db()
        if vm.get("ok") and b.saldo_vale_credito == Decimal("10.00"):
            ok("vale manual +10")
        else:
            fail(f"vale manual {vm} saldo={b.saldo_vale_credito}")
        payload_m = next((e["payload"] for e in listar_eventos_cliente(b.pk) if e["tipo"] == "vale_manual"), {})
        ok("vale manual sem caixa") if payload_m.get("caixa") is False else fail(f"caixa manual {payload_m}")

        pago = aplicar_vale_pago_apos_venda(
            cliente=b, valor=Decimal("7.25"), venda_pk=None, usuario="VerifyBot"
        )
        b.refresh_from_db()
        if pago.get("ok") and b.saldo_vale_credito == Decimal("17.25"):
            ok("vale pago +7.25")
        else:
            fail(f"vale pago {pago} saldo={b.saldo_vale_credito}")
        payload_p = next((e["payload"] for e in listar_eventos_cliente(b.pk) if e["tipo"] == "vale_pago"), {})
        ok("vale pago com caixa") if payload_p.get("caixa") is True else fail(f"caixa pago {payload_p}")

        tit = FiadoTituloAgro.objects.create(
            chave_unica=f"verify:{TAG}:aberto",
            cliente_agro=a,
            cliente_nome=a.nome,
            vencimento=timezone.localdate(),
            valor_bruto=Decimal("40.00"),
            valor_pago=Decimal("0"),
            situacao=FiadoTituloAgro.Situacao.ABERTO,
        )
        prev = preview_exclusao(a.pk)
        if prev.get("ok") and not prev.get("pode_excluir") and "fiado" in (prev.get("bloqueio") or "").lower():
            ok("preview bloqueia fiado aberto")
        else:
            fail(f"preview fiado {prev}")
        bloq = prev.get("bloqueio") or ""
        if "40,00" in bloq and bloq.endswith("excluir."):
            ok("texto fiado (vírgula no valor, ponto na frase)")
        else:
            fail(f"texto bloqueio {bloq}")

        tit.situacao = FiadoTituloAgro.Situacao.QUITADO
        tit.valor_pago = Decimal("40.00")
        tit.save()
        prev2 = preview_exclusao(a.pk)
        if prev2.get("pode_excluir") and prev2.get("precisa_transferir"):
            ok("quitado libera exclusão e pede transferir")
        else:
            fail(f"preview quitado {prev2}")

        sem_dest = excluir_cliente(pk=a.pk, pin=PIN)
        ok("excluir sem destino recusa") if not sem_dest.get("ok") else fail("excluir sem destino passou")

        tr = transferir_saldos(origem_pk=a.pk, destino_pk=b.pk, pin=PIN)
        a.refresh_from_db()
        b.refresh_from_db()
        if (
            tr.get("ok")
            and a.saldo_cashback == Decimal("0")
            and a.saldo_vale_credito == Decimal("0")
            and b.saldo_cashback == Decimal("13.50")
        ):
            ok("transferir cashback/vale")
        else:
            fail(f"transferir {tr} a={a.saldo_cashback}/{a.saldo_vale_credito} b={b.saldo_cashback}/{b.saldo_vale_credito}")

        a.saldo_cashback = Decimal("5.00")
        a.save(update_fields=["saldo_cashback"])
        ex = excluir_cliente(pk=a.pk, pin=PIN, destino_pk=b.pk, origem_tela="pdv")
        if ex.get("ok") and not ClienteAgro.objects.filter(pk=a.pk).exists():
            ok("excluir com transferência")
        else:
            fail(f"excluir {ex}")
        ev_ex = ClienteAgroEventoAgro.objects.filter(tipo="excluir", cliente_pk_snap=a.pk).first()
        ok("evento excluir permanece") if ev_ex else fail("sumiu evento excluir")

        payload_nao = payload_e_compra_vale_credito({}, [{"id": "fiado-cobranca", "preco": 9}])
        ok("payload não casa fiado") if not payload_nao else fail("fiado virou vale")

        http = Client(HTTP_HOST="127.0.0.1")
        http.force_login(user)
        r_dup = http.get(reverse("api_cliente_whatsapp_duplicado"), {"whatsapp": PHONE, "excluir_pk": c_limpo.pk})
        body = r_dup.json()
        if r_dup.status_code == 200 and body.get("ok") and body.get("duplicado"):
            ok("API duplicado")
        else:
            fail(f"API duplicado {r_dup.status_code} {body}")

        r_limpar = http.post(
            reverse("api_cliente_limpar_whatsapp", args=[b.pk]),
            data=json.dumps({"pin": PIN, "origem_tela": "pdv"}),
            content_type="application/json",
        )
        jb = r_limpar.json()
        if r_limpar.status_code == 200 and jb.get("ok"):
            ok("API limpar whatsapp")
        else:
            fail(f"API limpar {r_limpar.status_code} {jb}")

        r_prev = http.get(reverse("api_cliente_exclusao_preview", args=[b.pk]))
        if r_prev.status_code == 200 and r_prev.json().get("ok"):
            ok("API preview exclusão")
        else:
            fail(f"API preview {r_prev.status_code} {r_prev.content[:200]}")

        r_vale = http.post(
            reverse("api_cliente_vale_credito_manual", args=[c_limpo.pk]),
            data=json.dumps({"pin": PIN, "valor": "4,50", "motivo": "api deep"}),
            content_type="application/json",
        )
        jv = r_vale.json()
        if r_vale.status_code == 200 and jv.get("ok"):
            ok("API vale manual")
        else:
            fail(f"API vale {r_vale.status_code} {jv}")

        r_evt = http.get(reverse("api_cliente_eventos", args=[c_limpo.pk]))
        if r_evt.status_code == 200 and r_evt.json().get("ok"):
            ok("API eventos")
        else:
            fail(f"API eventos {r_evt.status_code}")

        dest2 = _cli("DESTINO2", whatsapp="11900003399")
        r_ex = http.post(
            reverse("api_cliente_excluir", args=[c_limpo.pk]),
            data=json.dumps({"pin": PIN, "destino_pk": dest2.pk, "origem_tela": "pdv"}),
            content_type="application/json",
        )
        je = r_ex.json()
        if r_ex.status_code == 200 and je.get("ok"):
            ok("API excluir")
        else:
            fail(f"API excluir {r_ex.status_code} {je}")

        from produtos.cliente_operacoes_util import preview_exclusao as _prev_fn
        import inspect

        src_prev = inspect.getsource(_prev_fn)
        if "Funcionario" in src_prev and "funcionário no RH" in src_prev:
            ok("preview tem bloqueio RH")
        else:
            fail("preview sem path RH")
    except Exception as exc:
        fail(f"exceção {type(exc).__name__}: {exc}")
    finally:
        try:
            _limpar()
        except Exception as exc:
            print("WARN limpeza:", exc)

    print(f"\n{oks} OK · {len(fails)} FAIL")
    if fails:
        print("Falhou:", "; ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
