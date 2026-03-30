"""
Envia por WhatsApp (CallMeBot) e/ou webhook: vendas do dia (DtoVenda), mais vencimentos
em aberto com data de vencimento hoje — a pagar e a receber (DtoLancamento).

Janela de envio (horário local do Django, TIME_ZONE):
  seg–sex: 09:00–20:00
  sábado:  09:00–18:00
  domingo: 10:00–14:00

Agendar no cron a cada 1 hora; fora da janela o comando sai sem enviar.
Use --force para ignorar janela e anti-duplicata (testes).

Evita duplicar no mesmo horário civil (cache 50 min).
"""

from django.core.cache import cache
from django.core.management.base import BaseCommand
from django.utils import timezone

from integracoes.notificacao_whatsapp import enviar_alerta_custom_url, enviar_whatsapp_callmebot
from produtos.mongo_financeiro_util import obter_vencimentos_abertos_dia_mongo
from produtos.mongo_vendas_util import obter_valor_total_vendas_dia_mongo
from produtos.views import obter_conexao_mongo


def _dentro_janela_envio(agora) -> tuple[bool, str]:
    """
    Retorna (True, '') se o horário atual permite envio; senão (False, mensagem).
    weekday: 0=seg … 6=dom.
    """
    wd = agora.weekday()
    h = agora.hour
    if wd <= 4:
        if 9 <= h <= 20:
            return True, ""
        return False, "Fora da janela de envio (segunda a sexta: 09:00–20:00, horário local)."
    if wd == 5:
        if 9 <= h <= 18:
            return True, ""
        return False, "Fora da janela de envio (sábado: 09:00–18:00, horário local)."
    if 10 <= h <= 14:
        return True, ""
    return False, "Fora da janela de envio (domingo: 10:00–14:00, horário local)."


def _formatar_brl(val) -> str:
    try:
        from decimal import Decimal

        v = Decimal(str(val))
        s = f"{v:,.2f}"
        return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(val)


class Command(BaseCommand):
    help = "Envia alerta com valor de vendas do dia (DtoVenda / Mongo)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--force",
            action="store_true",
            help="Envia mesmo se já tiver enviado nesta hora",
        )

    def handle(self, *args, **options):
        out = executar_alerta_vendas_dia(force=bool(options.get("force")))
        if not out.get("executado"):
            self.stdout.write(out.get("motivo") or "Não executado.")
            return
        if out.get("ok"):
            self.stdout.write(self.style.SUCCESS(out.get("mensagem") or "OK"))
            if out.get("ok_whatsapp"):
                self.stdout.write(f"WhatsApp: OK {str(out.get('info_whatsapp') or '')[:120]}")
            if out.get("ok_webhook"):
                self.stdout.write(f"Webhook: OK {str(out.get('info_webhook') or '')[:120]}")
            return
        self.stdout.write(
            self.style.WARNING(
                f"Nada enviado. WA: {out.get('info_whatsapp')} | Hook: {out.get('info_webhook')}"
            )
        )


def executar_alerta_vendas_dia(*, force: bool = False) -> dict:
    """
    Executa o envio e retorna resultado estruturado para comando e endpoint HTTP.
    """
    agora = timezone.localtime()
    if not force:
        ok_janela, msg_janela = _dentro_janela_envio(agora)
        if not ok_janela:
            return {"executado": False, "ok": False, "motivo": msg_janela, "agora": agora.isoformat()}

    chave = f"alerta_vendas_enviado:{agora.date().isoformat()}:{agora.hour}"
    if not force and cache.get(chave):
        return {
            "executado": False,
            "ok": False,
            "motivo": "Já enviado nesta hora — use force para repetir.",
            "agora": agora.isoformat(),
        }

    _, db = obter_conexao_mongo()
    if db is None:
        return {"executado": True, "ok": False, "motivo": "Mongo indisponível", "agora": agora.isoformat()}

    total = obter_valor_total_vendas_dia_mongo(db)
    v_pagar, v_receber = obter_vencimentos_abertos_dia_mongo(db)
    msg = (
        f"Agro — {agora.strftime('%d/%m/%Y')} às {agora.strftime('%H:%M')}\n"
        f"Vendas do dia: {_formatar_brl(total)}\n"
        f"Vence hoje — a pagar (não pago): {_formatar_brl(v_pagar)}\n"
        f"Vence hoje — a receber (não recebido): {_formatar_brl(v_receber)}"
    )

    ok_wa, info_wa = enviar_whatsapp_callmebot(msg)
    ok_hook, info_hook = enviar_alerta_custom_url(msg)
    ok_any = bool(ok_wa or ok_hook)
    if ok_any:
        cache.set(chave, 1, timeout=50 * 60)
    return {
        "executado": True,
        "ok": ok_any,
        "agora": agora.isoformat(),
        "mensagem": msg,
        "ok_whatsapp": bool(ok_wa),
        "ok_webhook": bool(ok_hook),
        "info_whatsapp": str(info_wa or "")[:400],
        "info_webhook": str(info_hook or "")[:400],
    }
