"""Cartão do cliente: barras 981 no PDV e QR com token longo no celular."""
from __future__ import annotations

import hashlib
import re
import secrets
from datetime import date, timedelta

from django.db import transaction
from django.utils import timezone

from produtos.models import ClienteAgro, ClienteCartaoAgro, ItemVendaAgro, VendaAgro

PREFIXO = "981"
SESSAO_OK = "cliente_cartao_ok"


def digito_ean13(corpo12: str) -> str:
    soma = 0
    for i, ch in enumerate(corpo12):
        n = int(ch)
        soma += n if i % 2 == 0 else n * 3
    return str((10 - (soma % 10)) % 10)


def montar_ean13(seq: int) -> str:
    corpo = PREFIXO + f"{int(seq):09d}"
    return corpo + digito_ean13(corpo)


def numero_curto(codigo: str) -> str:
    """Número que a pessoa vê e pode decorar. As barras continuam com os 13 dígitos."""
    digitos = normalizar_codigo(codigo)
    if len(digitos) == 13 and digitos.startswith(PREFIXO):
        seq = int(digitos[3:12])
    else:
        seq = int(digitos or "0")
    if seq <= 99999:
        return f"{seq:05d}"
    return str(seq)


def normalizar_codigo(bruto: str) -> str:
    return re.sub(r"\D", "", bruto or "")


def eh_codigo_cartao(bruto: str) -> bool:
    codigo = normalizar_codigo(bruto)
    if len(codigo) != 13 or not codigo.startswith(PREFIXO):
        return False
    return digito_ean13(codigo[:12]) == codigo[12]


def codigo_cartao_lido(bruto: str) -> str:
    """O leitor às vezes manda 12 dígitos, um zero na frente ou um dígito a mais."""
    d = normalizar_codigo(bruto)
    if len(d) == 14 and d.startswith("0"):
        d = d[1:]
    if len(d) == 12 and d.startswith(PREFIXO):
        d = d + digito_ean13(d)
    if eh_codigo_cartao(d):
        return d
    if len(d) > 13 and d.startswith(PREFIXO) and eh_codigo_cartao(d[:13]):
        return d[:13]
    return ""


def _proximo_seq() -> int:
    ultimo = 0
    for codigo in ClienteCartaoAgro.objects.values_list("codigo", flat=True):
        if codigo and codigo.startswith(PREFIXO) and len(codigo) == 13:
            try:
                ultimo = max(ultimo, int(codigo[3:12]))
            except ValueError:
                continue
    return ultimo + 1


def emitir_cartoes(quantidade: int) -> list[ClienteCartaoAgro]:
    qtd = max(1, min(int(quantidade or 10), 40))
    criados: list[ClienteCartaoAgro] = []
    with transaction.atomic():
        seq = _proximo_seq()
        for _ in range(qtd):
            codigo = montar_ean13(seq)
            while ClienteCartaoAgro.objects.filter(codigo=codigo).exists():
                seq += 1
                codigo = montar_ean13(seq)
            criados.append(
                ClienteCartaoAgro.objects.create(
                    codigo=codigo,
                    token=secrets.token_urlsafe(32),
                )
            )
            seq += 1
    return criados


def cartao_por_codigo(bruto: str) -> ClienteCartaoAgro | None:
    codigo = normalizar_codigo(bruto)
    if not eh_codigo_cartao(codigo):
        return None
    return ClienteCartaoAgro.objects.select_related("cliente").filter(codigo=codigo).first()


def cartao_por_token(token: str) -> ClienteCartaoAgro | None:
    tok = (token or "").strip()
    if len(tok) < 20:
        return None
    return ClienteCartaoAgro.objects.select_related("cliente").filter(token=tok).first()


def vincular(cartao: ClienteCartaoAgro, cliente: ClienteAgro) -> None:
    agora = timezone.now()
    with transaction.atomic():
        ClienteCartaoAgro.objects.filter(cliente=cliente, ativo=True).exclude(pk=cartao.pk).update(
            ativo=False
        )
        if cartao.cliente_id and cartao.cliente_id != cliente.pk:
            cartao.ativo = False
            cartao.save(update_fields=["ativo"])
            raise ValueError("este cartão já é de outra pessoa")
        cartao.cliente = cliente
        cartao.ativo = True
        cartao.vinculado_em = agora
        cartao.codigo_whatsapp_hash = ""
        cartao.codigo_whatsapp_expira = None
        cartao.save(
            update_fields=[
                "cliente",
                "ativo",
                "vinculado_em",
                "codigo_whatsapp_hash",
                "codigo_whatsapp_expira",
            ]
        )


def mesmo_whatsapp(cadastro: str, digitado: str) -> bool:
    a = re.sub(r"\D", "", cadastro or "")
    b = re.sub(r"\D", "", digitado or "")
    if len(a) < 10 or len(b) < 10:
        return False
    return a[-10:] == b[-10:]


def _hash_codigo(token: str, codigo: str) -> str:
    return hashlib.sha256(f"{token}:{codigo}".encode()).hexdigest()


def pedir_codigo_whatsapp(cartao: ClienteCartaoAgro, whatsapp: str) -> tuple[str, str]:
    """Devolve (codigo, situacao). situacao: enviado | sem_conversa | falha | sem_whatsapp | numero_diferente."""
    cliente = cartao.cliente
    if cliente is None or not cartao.ativo:
        return "", "invalido"
    if not (cliente.whatsapp or "").strip():
        return "", "sem_whatsapp"
    if not mesmo_whatsapp(cliente.whatsapp, whatsapp):
        return "", "numero_diferente"
    codigo = f"{secrets.randbelow(1000000):06d}"
    cartao.codigo_whatsapp_hash = _hash_codigo(cartao.token, codigo)
    cartao.codigo_whatsapp_expira = timezone.now() + timedelta(minutes=10)
    cartao.save(update_fields=["codigo_whatsapp_hash", "codigo_whatsapp_expira"])
    ok, _motivo = _enviar_codigo(cliente, codigo)
    if ok:
        return codigo, "enviado"
    return codigo, "sem_conversa"


def _enviar_codigo(cliente: ClienteAgro, codigo: str) -> tuple[bool, str]:
    try:
        from produtos.atendimento_whatsapp_recursos import enviar_texto_cliente
        from produtos.models import WhatsAppConversaAgro

        digits = re.sub(r"\D", "", cliente.whatsapp or "")
        cauda = digits[-11:] if len(digits) >= 11 else digits[-10:]
        if len(cauda) < 10:
            return False, "sem_conversa"
        conv = (
            WhatsAppConversaAgro.objects.filter(telefone__icontains=cauda)
            .order_by("-ultima_em")
            .first()
        )
        if conv is None:
            return False, "sem_conversa"
        texto = f"GM Agro: código do seu cartão {codigo}. Vale por 10 minutos."
        return enviar_texto_cliente(conv.pk, texto, autor="Cartão")
    except Exception:
        return False, "sem_conversa"


def confirmar_codigo(cartao: ClienteCartaoAgro, codigo: str) -> bool:
    bruto = re.sub(r"\D", "", codigo or "")
    if len(bruto) != 6 or not cartao.codigo_whatsapp_hash or not cartao.codigo_whatsapp_expira:
        return False
    if timezone.now() > cartao.codigo_whatsapp_expira:
        return False
    ok = secrets.compare_digest(cartao.codigo_whatsapp_hash, _hash_codigo(cartao.token, bruto))
    if ok:
        cartao.codigo_whatsapp_hash = ""
        cartao.codigo_whatsapp_expira = None
        cartao.save(update_fields=["codigo_whatsapp_hash", "codigo_whatsapp_expira"])
    return ok


_MESES = (
    "",
    "janeiro",
    "fevereiro",
    "março",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
)


def texto_faz(quando) -> str:
    if not quando:
        return ""
    dia = quando.date() if hasattr(quando, "date") else quando
    dias = (timezone.localdate() - dia).days
    if dias <= 0:
        return "hoje"
    if dias == 1:
        return "ontem"
    return f"faz {dias} dias"


def gravar_aniversario(cliente: ClienteAgro, dia_bruto: str, mes_bruto: str) -> str:
    """Grava dia e mês. Devolve aviso vazio quando deu certo."""
    try:
        dia = int(dia_bruto)
        mes = int(mes_bruto)
    except (TypeError, ValueError):
        return "Escolha o dia e o mês."
    if mes < 1 or mes > 12 or dia < 1 or dia > 31:
        return "Essa data não existe."
    ultimo = (date(2024, mes, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    if dia > ultimo.day:
        return "Essa data não existe."
    cliente.aniversario_dia = dia
    cliente.aniversario_mes = mes
    cliente.save(update_fields=["aniversario_dia", "aniversario_mes"])
    return ""


def texto_aniversario(cliente: ClienteAgro) -> dict:
    dia = cliente.aniversario_dia
    mes = cliente.aniversario_mes
    if not dia or not mes:
        return {"preenchido": False, "frase": "", "aviso": ""}
    frase = f"{dia} de {_MESES[mes]}"
    hoje = timezone.localdate()
    try:
        neste = date(hoje.year, mes, dia)
    except ValueError:
        neste = date(hoje.year, 2, 28)
    if neste < hoje:
        try:
            neste = date(hoje.year + 1, mes, dia)
        except ValueError:
            neste = date(hoje.year + 1, 2, 28)
    falta = (neste - hoje).days
    aviso = ""
    if falta == 0:
        aviso = "Hoje é o seu aniversário."
    elif falta <= 7:
        aviso = "Seu aniversário está chegando."
    return {"preenchido": True, "frase": frase, "aviso": aviso}


def _refs_cliente(cliente: ClienteAgro) -> list[str]:
    refs = [str(cliente.pk), f"local:{cliente.pk}", f"agro:{cliente.pk}"]
    externo = (cliente.externo_id or "").strip()
    if externo:
        refs.append(externo)
    return refs


def compras_do_cliente(cliente: ClienteAgro, limite: int = 20) -> list[dict]:
    refs = [str(cliente.pk), f"local:{cliente.pk}", f"agro:{cliente.pk}"]
    externo = (cliente.externo_id or "").strip()
    if externo:
        refs.append(externo)
    vendas = (
        VendaAgro.objects.filter(cliente_id_erp__in=refs, devolvida_em__isnull=True)
        .order_by("-criado_em")
        .prefetch_related("itens")[:limite]
    )
    saida = []
    for venda in vendas:
        itens = []
        for item in venda.itens.all():
            if not isinstance(item, ItemVendaAgro):
                continue
            itens.append(
                {
                    "descricao": item.descricao,
                    "quantidade": item.quantidade,
                    "total": item.valor_total,
                }
            )
        saida.append(
            {
                "quando": venda.criado_em,
                "faz": texto_faz(venda.criado_em),
                "total": venda.total,
                "forma": venda.forma_pagamento,
                "itens": itens,
            }
        )
    return saida


def mais_levados(cliente: ClienteAgro, limite: int = 3) -> list[dict]:
    from django.db.models import Count, Max

    refs = _refs_cliente(cliente)
    venda_ids = list(
        VendaAgro.objects.filter(cliente_id_erp__in=refs, devolvida_em__isnull=True).values_list("pk", flat=True)[:400]
    )
    if not venda_ids:
        return []
    linhas = (
        ItemVendaAgro.objects.filter(venda_id__in=venda_ids)
        .exclude(descricao="")
        .values("descricao")
        .annotate(vezes=Count("id"), ultima=Max("venda__criado_em"))
        .order_by("-vezes", "-ultima")[:limite]
    )
    saida = []
    for row in linhas:
        ultima = row["ultima"]
        saida.append(
            {
                "descricao": row["descricao"],
                "vezes": row["vezes"],
                "ultima": ultima,
                "faz": texto_faz(ultima),
            }
        )
    return saida


def aviso_racao(cliente: ClienteAgro) -> list[str]:
    from produtos.relacionamento_cliente_util import _ciclo_racao

    refs = _refs_cliente(cliente)
    venda_ids = list(
        VendaAgro.objects.filter(cliente_id_erp__in=refs, devolvida_em__isnull=True).values_list("pk", flat=True)[:400]
    )
    frases = []
    for item in _ciclo_racao(venda_ids):
        if item.get("status") not in ("recompra", "atrasado"):
            continue
        nome = item.get("descricao") or "ração"
        ref = item.get("media_intervalo_dias") or item.get("estimativa_dias_pacote")
        ultima = item.get("ultima_compra")
        if item.get("status") == "atrasado":
            frases.append(f"A ração {nome} já passou do prazo habitual.")
            continue
        if ultima and ref:
            base = date.fromisoformat(ultima)
            ate = base + timedelta(days=int(ref))
            frases.append(f"A ração {nome} costuma acabar por volta de {ate.strftime('%d/%m')}.")
        else:
            frases.append(f"A ração {nome} está perto de acabar.")
    return frases[:2]


def fiado_em_aberto(cliente: ClienteAgro) -> dict | None:
    from produtos.relacionamento_cliente_util import _fiado_resumo

    resumo = _fiado_resumo(cliente)
    total = resumo.get("total_aberto") or 0
    if total <= 0:
        return None
    datas = [t.get("vencimento") for t in resumo.get("titulos_abertos") or [] if t.get("vencimento")]
    mais_antigo = min(datas) if datas else ""
    frase_data = ""
    if mais_antigo:
        quando = date.fromisoformat(mais_antigo)
        if quando < timezone.localdate():
            frase_data = f"O mais antigo venceu em {quando.strftime('%d/%m/%Y')}."
        else:
            frase_data = f"O mais antigo vence em {quando.strftime('%d/%m/%Y')}."
    valor = f"{float(total):.2f}"
    return {"total": valor, "frase_data": frase_data}
